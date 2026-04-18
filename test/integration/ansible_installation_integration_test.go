//go:build integration

package integration_test

import (
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
	ansibleInventoryPath = "/workspace/inventory.ini"
	ansiblePlaybookPath  = "/workspace/deploy/ansible/site.yml"
	ansibleVarsPath      = "/workspace/test-vars.yml"
	ansibleRPMPath       = "/workspace/artifacts/pacman.rpm"
	pacmandConfigPath    = "/etc/pacman/pacmand.yaml"
	pacmandAdminToken    = "integration-token"
	pacmandAPIPort       = "8080"
	psqlBinary          = psqlBinary
	psqlDBFlag          = psqlDBFlag
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

	t.Run("switchover smoke test", func(t *testing.T) {
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
		for _, svc := range []*testenv.Service{primary, replica} {
			svc.RequireExec(t, "/bin/bash", "-c",
				"nohup runuser -s /bin/sh -u postgres -- /usr/bin/pacmand -config "+pacmandConfigPath+
					" </dev/null >>/var/log/pacmand.log 2>&1 &",
			)
		}

		pollPacmandHealth(t, primary, 60*time.Second)
		pollPacmandHealth(t, replica, 60*time.Second)

		triggerSwitchover(t, primary, "alpha-2")

		pollPostgreSQLRole(t, replica, "primary", 120*time.Second)
		pollPostgreSQLRole(t, primary, "replica", 30*time.Second)
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

func pollPacmandHealth(t *testing.T, svc *testenv.Service, timeout time.Duration) {
	t.Helper()

	const script = `
import urllib.request, sys
try:
    urllib.request.urlopen('http://localhost:` + pacmandAPIPort + `/health', timeout=3)
    sys.exit(0)
except Exception:
    sys.exit(1)
`
	deadline := time.Now().Add(timeout)
	for {
		if svc.Exec(t, "python3", "-c", script).ExitCode == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pacmand on %q did not become healthy within %s", svc.Name(), timeout)
		}
		time.Sleep(2 * time.Second)
	}
}

func triggerSwitchover(t *testing.T, svc *testenv.Service, candidate string) {
	t.Helper()

	script := `
import urllib.request, json
body = json.dumps({'candidate': '` + candidate + `', 'reason': 'smoke test', 'requestedBy': 'integration-test'}).encode()
req = urllib.request.Request(
    'http://localhost:` + pacmandAPIPort + `/api/v1/operations/switchover',
    data=body,
    headers={'Authorization': 'Bearer ` + pacmandAdminToken + `', 'Content-Type': 'application/json'},
    method='POST',
)
urllib.request.urlopen(req)
`
	svc.RequireExec(t, "python3", "-c", script)
}

func pollPostgreSQLRole(t *testing.T, svc *testenv.Service, wantRole string, timeout time.Duration) {
	t.Helper()

	var query string
	switch wantRole {
	case "primary":
		query = "SELECT CASE WHEN NOT pg_is_in_recovery() THEN 'primary' ELSE 'other' END"
	case "replica":
		query = "SELECT CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'other' END"
	default:
		t.Fatalf("pollPostgreSQLRole: unknown role %q", wantRole)
		return
	}

	deadline := time.Now().Add(timeout)
	for {
		result := svc.Exec(t,
			"runuser", "-u", "postgres", "--",
			psqlBinary, psqlDBFlag, "-tAc", query,
		)
		if result.ExitCode == 0 && strings.TrimSpace(result.Output) == wantRole {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("service %q did not reach PostgreSQL role %q within %s (exit=%d, output=%q)",
				svc.Name(), wantRole, timeout, result.ExitCode, result.Output)
		}
		time.Sleep(3 * time.Second)
	}
}
