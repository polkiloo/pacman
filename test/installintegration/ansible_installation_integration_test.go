//go:build integration

package installintegration_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"

	"github.com/polkiloo/pacman/test/testenv"
)

const (
	ansibleInventoryPath   = "/workspace/inventory.ini"
	ansiblePlaybookPath    = "/workspace/deploy/ansible/site.yml"
	ansibleVarsPath        = "/workspace/test-vars.yml"
	ansibleRPMPath         = "/workspace/artifacts/pacman.rpm"
	pacmandConfigPath      = "/etc/pacman/pacmand.yaml"
	pacmandSmokeConfigPath = "/var/lib/pacman/pacmand-smoke.yaml"
	pacmandAPIPort         = "8080"
	pacmandControlPort     = "9090"
	psqlBinary             = "/usr/pgsql-17/bin/psql"
	psqlDBFlag             = "--dbname=postgres"
	etcdStartupTimeout     = 60 * time.Second
	pacmandStartupTimeout  = 90 * time.Second
)

func TestAnsibleThreeNodeInstallationUsingTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed install test in short mode")
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, ansibleInstallImage())

	repoRoot := integrationRepoRoot(t)
	pacmanRPM := findBuiltPacmanRPM(t)
	workspaceFiles := ansibleInstallWorkspaceFiles(t, repoRoot, pacmanRPM)

	dcs := env.StartService(t, testenv.ServiceConfig{
		Name:    "ansible-dcs",
		Image:   ansibleInstallImage(),
		Aliases: []string{"pacman-dcs"},
		Files:   workspaceFiles,
	})
	primary := env.StartService(t, testenv.ServiceConfig{
		Name:    "ansible-primary",
		Image:   ansibleInstallImage(),
		Aliases: []string{"pacman-primary"},
		Files:   workspaceFiles,
	})
	replica := env.StartService(t, testenv.ServiceConfig{
		Name:    "ansible-replica",
		Image:   ansibleInstallImage(),
		Aliases: []string{"pacman-replica"},
		Files:   workspaceFiles,
	})

	runAnsibleInstall(t, dcs, "alpha-dcs")
	runAnsibleInstall(t, primary, "alpha-1")
	runAnsibleInstall(t, replica, "alpha-2")

	assertPackageInstalled(t, dcs, "etcd")
	assertFileContains(t, dcs, "/etc/systemd/system/etcd.service.d/override.conf", "--name alpha-dcs")
	assertFileContains(t, dcs, "/etc/systemd/system/etcd.service.d/override.conf", "http://pacman-dcs:2380")

	assertPackageInstalled(t, primary, "pacman")
	assertPackageInstalled(t, primary, "postgresql17-server")
	assertPackageInstalled(t, replica, "pacman")
	assertPackageInstalled(t, replica, "postgresql17-server")
	assertFileContains(t, primary, pacmandConfigPath, "initialPrimary: \"alpha-1\"")
	assertFileContains(t, primary, pacmandConfigPath, "http://pacman-dcs:2379")
	assertFileContains(t, replica, pacmandConfigPath, "http://pacman-dcs:2379")
	assertFileContains(t, replica, "/var/lib/pgsql/17/data/postgresql.auto.conf", "primary_slot_name = 'pacman_alpha_2'")
	assertFileExists(t, replica, "/var/lib/pgsql/17/data/standby.signal")

	primaryState := primary.RequireExec(
		t,
		"runuser", "-u", "postgres", "--",
		psqlBinary, psqlDBFlag, "-tAc",
		"SELECT CASE WHEN NOT pg_is_in_recovery() THEN 'primary' ELSE 'other' END",
	)
	if strings.TrimSpace(primaryState) != "primary" {
		t.Fatalf("unexpected primary PostgreSQL state: %q", primaryState)
	}

	replicaState := replica.RequireExec(
		t,
		"runuser", "-u", "postgres", "--",
		psqlBinary, psqlDBFlag, "-tAc",
		"SELECT CASE WHEN pg_is_in_recovery() AND EXISTS (SELECT 1 FROM pg_stat_wal_receiver WHERE status = 'streaming') THEN 'ready' ELSE 'not-ready' END",
	)
	if strings.TrimSpace(replicaState) != "ready" {
		t.Fatalf("unexpected replica PostgreSQL state: %q", replicaState)
	}

	t.Run("pacmand runtime smoke test", func(t *testing.T) {
		dcs.RequireExec(t, "/bin/bash", "-c",
			"nohup /usr/bin/etcd"+
				" --name alpha-dcs"+
				" --data-dir /var/lib/etcd/pacman"+
				" --listen-client-urls http://0.0.0.0:2379"+
				" --advertise-client-urls http://pacman-dcs:2379"+
				" --listen-peer-urls http://0.0.0.0:2380"+
				" --initial-advertise-peer-urls http://pacman-dcs:2380"+
				" --initial-cluster alpha-dcs=http://pacman-dcs:2380"+
				" --initial-cluster-state new"+
				" --initial-cluster-token pacman-cluster"+
				" </dev/null >>/var/log/etcd.log 2>&1 &",
		)
		pollEtcdHealth(t, dcs, etcdStartupTimeout)
		for _, svc := range []*testenv.Service{primary, replica} {
			preparePacmandSmokeConfig(t, svc)
			svc.RequireExec(t, "/bin/bash", "-c",
				"cd /var/lib/pacman && nohup runuser -u postgres -- /usr/bin/pacmand -config "+pacmandSmokeConfigPath+
					" </dev/null >>/var/log/pacmand.log 2>&1 &",
			)
		}

		pollPacmandHealth(t, primary, "http://127.0.0.1:"+pacmandAPIPort+"/health", pacmandStartupTimeout)
		pollPacmandHealth(t, replica, "http://127.0.0.1:"+pacmandAPIPort+"/health", pacmandStartupTimeout)
	})
}

func ansibleInstallImage() string {
	if image := strings.TrimSpace(os.Getenv("PACMAN_ANSIBLE_INSTALL_IMAGE")); image != "" {
		return image
	}

	return "pacman-ansible-install:local"
}

func integrationRepoRoot(t *testing.T) string {
	t.Helper()

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	return filepath.Clean(filepath.Join(workingDir, "..", ".."))
}

func findBuiltPacmanRPM(t *testing.T) string {
	t.Helper()

	rpmDir := strings.TrimSpace(os.Getenv("PACMAN_ANSIBLE_INSTALL_RPM_DIR"))
	if rpmDir == "" {
		t.Fatal("PACMAN_ANSIBLE_INSTALL_RPM_DIR must point to a directory containing a built pacman RPM")
	}

	entries, err := os.ReadDir(rpmDir)
	if err != nil {
		t.Fatalf("read ansible install rpm directory %q: %v", rpmDir, err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "pacman-") || strings.HasPrefix(name, "pacman-postgresql17-agent-") || strings.HasSuffix(name, ".src.rpm") || !strings.HasSuffix(name, ".rpm") {
			continue
		}

		return filepath.Join(rpmDir, name)
	}

	t.Fatalf("no pacman runtime rpm found in %q", rpmDir)
	return ""
}

func ansibleInstallWorkspaceFiles(t *testing.T, repoRoot, pacmanRPM string) []testcontainers.ContainerFile {
	t.Helper()

	var files []testcontainers.ContainerFile
	ansibleRoot := filepath.Join(repoRoot, "deploy", "ansible")
	files = append(files, containerFilesFromTree(t, ansibleRoot, "/workspace/deploy/ansible")...)

	inventoryPath := filepath.Join(t.TempDir(), "inventory.ini")
	if err := os.WriteFile(inventoryPath, []byte(ansibleInstallInventory()), 0o644); err != nil {
		t.Fatalf("write install inventory: %v", err)
	}
	files = append(files, testcontainers.ContainerFile{
		HostFilePath:      inventoryPath,
		ContainerFilePath: ansibleInventoryPath,
		FileMode:          0o644,
	})

	varsPath := filepath.Join(t.TempDir(), "test-vars.yml")
	if err := os.WriteFile(varsPath, []byte(ansibleInstallVars()), 0o644); err != nil {
		t.Fatalf("write install vars: %v", err)
	}
	files = append(files, testcontainers.ContainerFile{
		HostFilePath:      varsPath,
		ContainerFilePath: ansibleVarsPath,
		FileMode:          0o644,
	})

	files = append(files, testcontainers.ContainerFile{
		HostFilePath:      pacmanRPM,
		ContainerFilePath: ansibleRPMPath,
		FileMode:          0o644,
	})

	return files
}

func containerFilesFromTree(t *testing.T, sourceRoot, containerRoot string) []testcontainers.ContainerFile {
	t.Helper()

	var files []testcontainers.ContainerFile
	err := filepath.WalkDir(sourceRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		relativePath, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}

		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      path,
			ContainerFilePath: filepath.ToSlash(filepath.Join(containerRoot, relativePath)),
			FileMode:          int64(info.Mode().Perm()),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk ansible source tree %q: %v", sourceRoot, err)
	}

	return files
}

func ansibleInstallInventory() string {
	return `[dcs_nodes]
alpha-dcs ansible_host=pacman-dcs ansible_connection=local ansible_python_interpreter=/usr/bin/python3 etcd_name=alpha-dcs

[postgres_nodes]
alpha-1 ansible_host=pacman-primary ansible_connection=local ansible_python_interpreter=/usr/bin/python3 pacman_node_name=alpha-1
alpha-2 ansible_host=pacman-replica ansible_connection=local ansible_python_interpreter=/usr/bin/python3 pacman_node_name=alpha-2

[all:vars]
ansible_user=root
pacman_cluster_name=alpha
pacman_initial_primary=alpha-1
`
}

func ansibleInstallVars() string {
	return `---
pacman_package_name: /workspace/artifacts/pacman.rpm
pacman_disable_gpg_check: true
pacman_manage_service: false
postgres_manage_service: false
etcd_manage_service: false
etcd_user: root
etcd_group: root
pacman_admin_token_inline: integration-token
`
}

func runAnsibleInstall(t *testing.T, service *testenv.Service, limit string) {
	t.Helper()

	service.RequireExec(
		t,
		"ansible-playbook",
		"-i", ansibleInventoryPath,
		ansiblePlaybookPath,
		"-e", "@"+ansibleVarsPath,
		"--limit", limit,
	)
}

func assertPackageInstalled(t *testing.T, service *testenv.Service, pkg string) {
	t.Helper()

	service.RequireExec(t, "rpm", "-q", pkg)
}

func assertFileContains(t *testing.T, service *testenv.Service, path, want string) {
	t.Helper()

	output := service.RequireExec(t, "cat", path)
	if !strings.Contains(output, want) {
		t.Fatalf("expected %q in %s for service %q, got %q", want, path, service.Name(), output)
	}
}

func assertFileExists(t *testing.T, service *testenv.Service, path string) {
	t.Helper()

	service.RequireExec(t, "test", "-f", path)
}

func preparePacmandSmokeConfig(t *testing.T, svc *testenv.Service) {
	t.Helper()

	command := fmt.Sprintf(
		"python3 - <<'PY'\n"+
			"from pathlib import Path\n"+
			"\n"+
			"src = Path(%q)\n"+
			"dst = Path(%q)\n"+
			"lines = []\n"+
			"for raw in src.read_text().splitlines():\n"+
			"    stripped = raw.lstrip()\n"+
			"    indent = raw[:len(raw) - len(stripped)]\n"+
			"    if stripped.startswith(\"apiAddress:\"):\n"+
			"        lines.append(f'{indent}apiAddress: \"0.0.0.0:%s\"')\n"+
			"    elif stripped.startswith(\"controlAddress:\"):\n"+
			"        lines.append(f'{indent}controlAddress: \"0.0.0.0:%s\"')\n"+
			"    else:\n"+
			"        lines.append(raw)\n"+
			"dst.write_text(\"\\n\".join(lines) + \"\\n\")\n"+
			"PY",
		pacmandConfigPath,
		pacmandSmokeConfigPath,
		pacmandAPIPort,
		pacmandControlPort,
	)

	svc.RequireExec(t, "/bin/bash", "-lc", command)
}

func pollPacmandHealth(t *testing.T, svc *testenv.Service, rawURL string, timeout time.Duration) {
	t.Helper()

	script := `
import urllib.request, sys
try:
    urllib.request.urlopen('` + rawURL + `', timeout=3)
    sys.exit(0)
except Exception as err:
    print(err)
    sys.exit(1)
`
	deadline := time.Now().Add(timeout)
	lastProbeError := ""
	for {
		result := svc.Exec(t, "python3", "-c", script)
		if result.ExitCode == 0 {
			return
		}
		lastProbeError = strings.TrimSpace(result.Output)
		if time.Now().After(deadline) {
			t.Fatalf("pacmand on %q did not become healthy within %s; probe=%q; listeners:\n%s\nprocesses:\n%s\npacmand log:\n%s",
				svc.Name(),
				timeout,
				lastProbeError,
				serviceCommandOutput(t, svc, "/bin/sh", "-lc", "ss -lnt"),
				serviceCommandOutput(t, svc, "/bin/sh", "-lc", "ps -ef | grep '[p]acmand'"),
				serviceLogTail(t, svc, "/var/log/pacmand.log")+"\nsmoke config:\n"+serviceCommandOutput(t, svc, "cat", pacmandSmokeConfigPath))
		}
		time.Sleep(2 * time.Second)
	}
}

func pollEtcdHealth(t *testing.T, svc *testenv.Service, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		result := svc.Exec(t, "/bin/bash", "-lc",
			"ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health")
		if result.ExitCode == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("etcd on %q did not become healthy within %s; etcdctl output=%q; etcd log:\n%s",
				svc.Name(), timeout, result.Output, serviceLogTail(t, svc, "/var/log/etcd.log"))
		}
		time.Sleep(2 * time.Second)
	}
}

func serviceLogTail(t *testing.T, svc *testenv.Service, path string) string {
	t.Helper()

	result := svc.Exec(t, "/bin/sh", "-lc", "tail -n 200 "+path)
	if strings.TrimSpace(result.Output) == "" {
		return "<log unavailable>"
	}
	return result.Output
}

func serviceCommandOutput(t *testing.T, svc *testenv.Service, cmd ...string) string {
	t.Helper()

	result := svc.Exec(t, cmd...)
	if strings.TrimSpace(result.Output) == "" {
		return "<no output>"
	}
	return result.Output
}
