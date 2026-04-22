//go:build integration

package installintegration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	dockernetwork "github.com/docker/docker/api/types/network"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"

	nativeapi "github.com/polkiloo/pacman/internal/api/native"
	"github.com/polkiloo/pacman/internal/cluster"
	pgobs "github.com/polkiloo/pacman/internal/postgres"
	"github.com/polkiloo/pacman/test/testenv"
)

const (
	demoVIPInterface           = "eth0"
	demoAdminToken             = "integration-token"
	demoPostgresPassword       = "pacman-demo-password"
	demoPacmandAPIURL          = "http://pacman-primary:8080"
	demoPacmandLogPath         = "/var/log/pacman/pacmand.log"
	demoVIPManagerLogPath      = "/var/log/pacman/vip-manager.log"
	demoPGbenchLogPath         = "/tmp/pacman-demo-pgbench.log"
	demoPGbenchDonePath        = "/tmp/pacman-demo-pgbench.done"
	demoPGbenchSuccessPath     = "/tmp/pacman-demo-pgbench.success"
	demoPGbenchAfterPath       = "/tmp/pacman-demo-pgbench.after-success"
	demoPGbenchFailurePath     = "/tmp/pacman-demo-pgbench.failures"
	demoPGbenchMarkerPath      = "/tmp/pacman-demo-pgbench.after-switchover"
	demoSwitchoverWaitTimeout  = 2 * time.Minute
	demoObservationWaitTimeout = 2 * time.Minute
)

type demoClusterNetwork struct {
	Subnet  string
	VIP     string
	DCS     string
	Primary string
	Replica string
}

type pacmanAPIClient struct {
	baseURL string
	token   string
	client  *http.Client
}

type pgbenchLoadSummary struct {
	TotalSuccess int
	AfterSuccess int
	Failures     int
	Log          string
}

func TestAnsibleDemoSwitchoverWithVIPManagerUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed install test in short mode")
	}

	testenv.RequireLocalImage(t, ansibleInstallImage())

	networkCfg := newDemoClusterNetwork(t)
	env := testenv.NewWithNetworkOptions(t, tcnetwork.WithIPAM(&dockernetwork.IPAM{
		Config: []dockernetwork.IPAMConfig{{Subnet: networkCfg.Subnet}},
	}))

	repoRoot := integrationRepoRoot(t)
	pacmanRPM := findBuiltPacmanRPM(t)
	workspaceFiles := demoInstallWorkspaceFiles(t, repoRoot, pacmanRPM, ansibleDemoVars(networkCfg.VIP))

	dcs := env.StartService(t, testenv.ServiceConfig{
		Name:        "demo-dcs",
		Image:       ansibleInstallImage(),
		Aliases:     []string{"pacman-dcs"},
		IPv4Address: networkCfg.DCS,
		Files:       workspaceFiles,
	})
	primary := env.StartService(t, testenv.ServiceConfig{
		Name:         "demo-primary",
		Image:        ansibleInstallImage(),
		Aliases:      []string{"pacman-primary"},
		IPv4Address:  networkCfg.Primary,
		CapAdd:       []string{"NET_ADMIN", "NET_RAW"},
		Files:        workspaceFiles,
		ExposedPorts: []string{"5432/tcp", "8080/tcp"},
	})
	replica := env.StartService(t, testenv.ServiceConfig{
		Name:         "demo-replica",
		Image:        ansibleInstallImage(),
		Aliases:      []string{"pacman-replica"},
		IPv4Address:  networkCfg.Replica,
		CapAdd:       []string{"NET_ADMIN", "NET_RAW"},
		Files:        workspaceFiles,
		ExposedPorts: []string{"5432/tcp", "8080/tcp"},
	})

	runAnsibleInstall(t, dcs, "alpha-dcs")
	runAnsibleInstall(t, primary, "alpha-1")
	runAnsibleInstall(t, replica, "alpha-2")

	assertFileExists(t, primary, "/usr/local/bin/vip-manager")
	assertFileExists(t, replica, "/usr/local/bin/vip-manager")

	startDemoEtcd(t, dcs)
	startDemoPacmand(t, primary, "http://pacman-primary:8080/health")
	startDemoPacmand(t, replica, "http://pacman-replica:8080/health")
	startDemoVIPManager(t, primary)
	startDemoVIPManager(t, replica)

	waitForVIPAssignment(t, primary, networkCfg.VIP, true)
	waitForVIPAssignment(t, replica, networkCfg.VIP, false)
	waitForVIPPostgresReady(t, primary, networkCfg.VIP)

	t.Setenv("PGDATABASE", "postgres")
	t.Setenv("PGUSER", "postgres")
	t.Setenv("PGPASSWORD", demoPostgresPassword)
	t.Setenv("PGSSLMODE", "disable")

	api := newPACMANAPIClient("http://"+primary.Address(t, pacmandAPIPort), demoAdminToken)
	initialMembers := waitForMembers(
		t,
		api,
		func(resp nativeapi.MembersResponse) bool {
			primaryMember, ok := memberStatusByName(resp, "alpha-1")
			if !ok {
				return false
			}
			replicaMember, ok := memberStatusByName(resp, "alpha-2")
			if !ok {
				return false
			}
			return primaryMember.Role == "primary" &&
				replicaMember.Role == "replica" &&
				primaryMember.Healthy &&
				replicaMember.Healthy &&
				primaryMember.Timeline > 0 &&
				replicaMember.Timeline == primaryMember.Timeline
		},
		demoSwitchoverWaitTimeout,
	)

	initialPrimaryMember := mustMemberStatus(t, initialMembers, "alpha-1")
	initialReplicaMember := mustMemberStatus(t, initialMembers, "alpha-2")

	initialPrimaryObservation := waitForPostgresObservation(
		t,
		primary.Address(t, "5432"),
		func(observation pgobs.Observation) bool {
			return observation.Role == cluster.MemberRolePrimary &&
				!observation.InRecovery &&
				observation.Details.Timeline > 0
		},
		demoObservationWaitTimeout,
	)
	initialReplicaObservation := waitForPostgresObservation(
		t,
		replica.Address(t, "5432"),
		func(observation pgobs.Observation) bool {
			return observation.Role == cluster.MemberRoleReplica &&
				observation.InRecovery &&
				observation.Details.Timeline > 0
		},
		demoObservationWaitTimeout,
	)

	if initialPrimaryObservation.Details.Timeline != initialReplicaObservation.Details.Timeline {
		t.Fatalf(
			"expected initial timeline parity before switchover, primary=%d replica=%d",
			initialPrimaryObservation.Details.Timeline,
			initialReplicaObservation.Details.Timeline,
		)
	}

	if initialPrimaryMember.Timeline != initialPrimaryObservation.Details.Timeline {
		t.Fatalf(
			"expected PACMAN primary timeline to match PostgreSQL observation, api=%d postgres=%d",
			initialPrimaryMember.Timeline,
			initialPrimaryObservation.Details.Timeline,
		)
	}

	if initialReplicaMember.Timeline != initialReplicaObservation.Details.Timeline {
		t.Fatalf(
			"expected PACMAN replica timeline to match PostgreSQL observation, api=%d postgres=%d",
			initialReplicaMember.Timeline,
			initialReplicaObservation.Details.Timeline,
		)
	}

	initializePGbenchDataset(t, primary, networkCfg.VIP)
	startPGbenchLoad(t, primary, networkCfg.VIP)
	waitForPGbenchCounter(t, primary, demoPGbenchSuccessPath, 1, 45*time.Second, "pgbench successful chunks")

	triggerSwitchoverViaPacmanctl(t, primary)
	markPGbenchAfterSwitchover(t, primary)

	promotedMembers := waitForMembers(
		t,
		api,
		func(resp nativeapi.MembersResponse) bool {
			newPrimary, ok := memberStatusByName(resp, "alpha-2")
			if !ok {
				return false
			}
			formerPrimary, ok := memberStatusByName(resp, "alpha-1")
			if !ok {
				return false
			}
			return newPrimary.Role == "primary" &&
				newPrimary.Healthy &&
				newPrimary.Timeline > initialPrimaryObservation.Details.Timeline &&
				formerPrimary.Role == "replica"
		},
		demoSwitchoverWaitTimeout,
	)

	promotedMember := mustMemberStatus(t, promotedMembers, "alpha-2")
	formerPrimaryMember := mustMemberStatus(t, promotedMembers, "alpha-1")

	promotedObservation := waitForPostgresObservation(
		t,
		replica.Address(t, "5432"),
		func(observation pgobs.Observation) bool {
			return observation.Role == cluster.MemberRolePrimary &&
				!observation.InRecovery &&
				observation.Details.Timeline > initialPrimaryObservation.Details.Timeline
		},
		demoObservationWaitTimeout,
	)
	waitForPostgresObservation(
		t,
		primary.Address(t, "5432"),
		func(observation pgobs.Observation) bool {
			return observation.Role == cluster.MemberRoleReplica && observation.InRecovery
		},
		demoObservationWaitTimeout,
	)

	if promotedMember.Timeline != promotedObservation.Details.Timeline {
		t.Fatalf(
			"expected promoted PACMAN timeline to match PostgreSQL observation, api=%d postgres=%d",
			promotedMember.Timeline,
			promotedObservation.Details.Timeline,
		)
	}

	if formerPrimaryMember.Role != "replica" {
		t.Fatalf("expected former primary member to be demoted to replica, got %+v", formerPrimaryMember)
	}

	waitForVIPAssignment(t, primary, networkCfg.VIP, false)
	waitForVIPAssignment(t, replica, networkCfg.VIP, true)
	waitForVIPPostgresReady(t, primary, networkCfg.VIP)
	waitForVIPWritablePrimary(t, primary, networkCfg.VIP)

	loadSummary := waitForPGbenchCompletion(t, primary, demoSwitchoverWaitTimeout)
	if loadSummary.TotalSuccess < 2 {
		t.Fatalf("expected at least two successful pgbench chunks, got %+v", loadSummary)
	}
	if loadSummary.AfterSuccess < 1 {
		t.Fatalf("expected pgbench to recover and succeed after switchover, got %+v", loadSummary)
	}

	historyEntry := waitForHistoryEntry(
		t,
		api,
		func(entry nativeapi.HistoryEntry) bool {
			return entry.Kind == "switchover" &&
				entry.FromMember == "alpha-1" &&
				entry.ToMember == "alpha-2" &&
				entry.Result == "succeeded"
		},
		demoSwitchoverWaitTimeout,
	)

	if historyEntry.OperationID == "" {
		t.Fatalf("expected switchover history entry to expose an operation id, got %+v", historyEntry)
	}
}

func newDemoClusterNetwork(t *testing.T) demoClusterNetwork {
	t.Helper()

	checksum := uint64(crc32.ChecksumIEEE([]byte(t.Name())))
	thirdOctet := 1 + int((uint64(time.Now().UnixNano())^checksum)%250)
	prefix := fmt.Sprintf("198.19.%d", thirdOctet)

	return demoClusterNetwork{
		Subnet:  prefix + ".0/24",
		VIP:     prefix + ".100",
		DCS:     prefix + ".10",
		Primary: prefix + ".11",
		Replica: prefix + ".12",
	}
}

func demoInstallWorkspaceFiles(t *testing.T, repoRoot, pacmanRPM, varsBody string) []testcontainers.ContainerFile {
	t.Helper()

	var files []testcontainers.ContainerFile
	ansibleRoot := filepath.Join(repoRoot, "deploy", "ansible")
	files = append(files, containerFilesFromTree(t, ansibleRoot, "/workspace/deploy/ansible")...)

	inventoryPath := filepath.Join(t.TempDir(), "inventory.ini")
	if err := os.WriteFile(inventoryPath, []byte(ansibleInstallInventory()), 0o644); err != nil {
		t.Fatalf("write demo install inventory: %v", err)
	}
	files = append(files, testcontainers.ContainerFile{
		HostFilePath:      inventoryPath,
		ContainerFilePath: ansibleInventoryPath,
		FileMode:          0o644,
	})

	varsPath := filepath.Join(t.TempDir(), "demo-vars.yml")
	if err := os.WriteFile(varsPath, []byte(varsBody), 0o644); err != nil {
		t.Fatalf("write demo install vars: %v", err)
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

func ansibleDemoVars(vipAddress string) string {
	return fmt.Sprintf(`---
pacman_package_name: /workspace/artifacts/pacman.rpm
pacman_disable_gpg_check: true
pacman_manage_service: false
postgres_manage_service: false
etcd_manage_service: false
vip_manager_manage_service: false

pacman_admin_token_inline: %s
postgres_superuser_password: %s
pacmand_environment:
  PGPASSWORD: %s
etcd_user: root
etcd_group: root
vip_manager_enabled: true
vip_manager_ip: %s
vip_manager_interface: %s
vip_manager_verbose: true
`, demoAdminToken, demoPostgresPassword, demoPostgresPassword, vipAddress, demoVIPInterface)
}

func startDemoEtcd(t *testing.T, dcs *testenv.Service) {
	t.Helper()

	dcs.RequireExec(
		t,
		"/bin/bash", "-lc",
		"mkdir -p /var/log && nohup /usr/bin/etcd"+
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
}

func startDemoPacmand(t *testing.T, svc *testenv.Service, healthURL string) {
	t.Helper()

	svc.RequireExec(
		t,
		"/bin/bash", "-lc",
		"mkdir -p /var/log/pacman /var/lib/pacman && "+
			"cd /var/lib/pacman && "+
			"nohup runuser -u postgres -- /bin/bash -lc '. /etc/sysconfig/pacmand 2>/dev/null || true; export PACMAND_CONFIG PACMAND_EXTRA_ARGS PGPASSWORD; exec /usr/bin/pacmand -config \"${PACMAND_CONFIG:-/etc/pacman/pacmand.yaml}\" ${PACMAND_EXTRA_ARGS:-}'"+
			" </dev/null >>"+demoPacmandLogPath+" 2>&1 &",
	)

	pollDemoPacmandHealth(t, svc, healthURL, pacmandStartupTimeout)
}

func startDemoVIPManager(t *testing.T, svc *testenv.Service) {
	t.Helper()

	svc.RequireExec(
		t,
		"/bin/bash", "-lc",
		"mkdir -p /var/log/pacman && nohup /usr/local/bin/vip-manager --config /etc/pacman/vip-manager.yml </dev/null >>"+demoVIPManagerLogPath+" 2>&1 &",
	)
}

func pollDemoPacmandHealth(t *testing.T, svc *testenv.Service, rawURL string, timeout time.Duration) {
	t.Helper()

	script := `
import sys
import urllib.request

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
			t.Fatalf(
				"pacmand on %q did not become healthy within %s; probe=%q; listeners:\n%s\nprocesses:\n%s\npacmand log:\n%s",
				svc.Name(),
				timeout,
				lastProbeError,
				serviceCommandOutput(t, svc, "/bin/sh", "-lc", "ss -lnt"),
				serviceCommandOutput(t, svc, "/bin/sh", "-lc", "ps -ef | grep '[p]acmand'"),
				serviceLogTail(t, svc, demoPacmandLogPath),
			)
		}

		time.Sleep(2 * time.Second)
	}
}

func waitForVIPAssignment(t *testing.T, svc *testenv.Service, vipAddress string, wantPresent bool) {
	t.Helper()

	deadline := time.Now().Add(demoSwitchoverWaitTimeout)
	wantToken := vipAddress + "/"
	for time.Now().Before(deadline) {
		output := svc.RequireExec(t, "/bin/sh", "-lc", "ip -o -4 addr show dev "+demoVIPInterface)
		hasVIP := strings.Contains(output, wantToken)
		if hasVIP == wantPresent {
			return
		}

		time.Sleep(time.Second)
	}

	t.Fatalf(
		"vip assignment on %q did not reach present=%v; addresses:\n%s\nvip-manager log:\n%s",
		svc.Name(),
		wantPresent,
		svc.RequireExec(t, "/bin/sh", "-lc", "ip -brief addr show dev "+demoVIPInterface),
		serviceLogTail(t, svc, demoVIPManagerLogPath),
	)
}

func waitForVIPPostgresReady(t *testing.T, svc *testenv.Service, vipAddress string) {
	t.Helper()

	deadline := time.Now().Add(demoSwitchoverWaitTimeout)
	command := fmt.Sprintf(
		"PGPASSWORD=%q /usr/pgsql-17/bin/pg_isready -h %q -p 5432 -U postgres -d postgres >/dev/null 2>&1",
		demoPostgresPassword,
		vipAddress,
	)

	for time.Now().Before(deadline) {
		result := svc.Exec(t, "/bin/sh", "-lc", command)
		if result.ExitCode == 0 {
			return
		}

		time.Sleep(time.Second)
	}

	t.Fatalf(
		"VIP PostgreSQL route %s did not become ready from %q; addresses:\n%s\nvip-manager log:\n%s",
		vipAddress,
		svc.Name(),
		svc.RequireExec(t, "/bin/sh", "-lc", "ip -brief addr show dev "+demoVIPInterface),
		serviceLogTail(t, svc, demoVIPManagerLogPath),
	)
}

func initializePGbenchDataset(t *testing.T, svc *testenv.Service, vipAddress string) {
	t.Helper()

	svc.RequireExec(
		t,
		"/bin/bash", "-lc",
		fmt.Sprintf(
			"PGPASSWORD=%q /usr/pgsql-17/bin/pgbench -i -s 5 -h %q -p 5432 -U postgres postgres",
			demoPostgresPassword,
			vipAddress,
		),
	)
}

func startPGbenchLoad(t *testing.T, svc *testenv.Service, vipAddress string) {
	t.Helper()

	script := fmt.Sprintf(`cat > /tmp/pacman-demo-pgbench.sh <<'SH'
#!/usr/bin/env bash
set -u

rm -f %[1]s %[2]s %[3]s %[4]s %[5]s
printf '0\n' > %[2]s
printf '0\n' > %[3]s
printf '0\n' > %[4]s

deadline=$(( $(date +%%s) + 45 ))
total_success=0
after_success=0
failures=0

while [ "$(date +%%s)" -lt "${deadline}" ]; do
  printf 'chunk-start %%s\n' "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" >> %[1]s

  PGPASSWORD=%[6]q /usr/pgsql-17/bin/pgbench \
    -n \
    -h %[7]q \
    -p 5432 \
    -U postgres \
    -c 4 \
    -j 2 \
    -T 5 \
    postgres >> %[1]s 2>&1
  rc=$?

  if [ "${rc}" -eq 0 ]; then
    total_success=$((total_success + 1))
    printf '%%s\n' "${total_success}" > %[2]s
    if [ -f %[5]s ]; then
      after_success=$((after_success + 1))
      printf '%%s\n' "${after_success}" > %[3]s
    fi
    printf 'chunk-success %%s total_success=%%s after_success=%%s\n' "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" "${total_success}" "${after_success}" >> %[1]s
    continue
  fi

  failures=$((failures + 1))
  printf '%%s\n' "${failures}" > %[4]s
  printf 'chunk-failure %%s rc=%%s failures=%%s\n' "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" "${rc}" "${failures}" >> %[1]s
  sleep 1
done

touch %[8]s
SH
chmod +x /tmp/pacman-demo-pgbench.sh
nohup /tmp/pacman-demo-pgbench.sh >/tmp/pacman-demo-pgbench.supervisor.log 2>&1 </dev/null &
`, demoPGbenchLogPath, demoPGbenchSuccessPath, demoPGbenchAfterPath, demoPGbenchFailurePath, demoPGbenchMarkerPath, demoPostgresPassword, vipAddress, demoPGbenchDonePath)

	svc.RequireExec(t, "/bin/bash", "-lc", script)
}

func waitForPGbenchCounter(t *testing.T, svc *testenv.Service, path string, wantAtLeast int, timeout time.Duration, label string) int {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value := readServiceCounter(t, svc, path)
		if value >= wantAtLeast {
			return value
		}

		time.Sleep(time.Second)
	}

	t.Fatalf(
		"%s on %q did not reach %d before deadline; log:\n%s",
		label,
		svc.Name(),
		wantAtLeast,
		serviceLogTail(t, svc, demoPGbenchLogPath),
	)
	return 0
}

func markPGbenchAfterSwitchover(t *testing.T, svc *testenv.Service) {
	t.Helper()

	svc.RequireExec(t, "/bin/sh", "-lc", "touch "+demoPGbenchMarkerPath)
}

func waitForPGbenchCompletion(t *testing.T, svc *testenv.Service, timeout time.Duration) pgbenchLoadSummary {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := svc.Exec(t, "/bin/sh", "-lc", "test -f "+demoPGbenchDonePath)
		if done.ExitCode == 0 {
			return pgbenchLoadSummary{
				TotalSuccess: readServiceCounter(t, svc, demoPGbenchSuccessPath),
				AfterSuccess: readServiceCounter(t, svc, demoPGbenchAfterPath),
				Failures:     readServiceCounter(t, svc, demoPGbenchFailurePath),
				Log:          serviceLogTail(t, svc, demoPGbenchLogPath),
			}
		}

		time.Sleep(time.Second)
	}

	t.Fatalf("pgbench load did not complete on %q; log:\n%s", svc.Name(), serviceLogTail(t, svc, demoPGbenchLogPath))
	return pgbenchLoadSummary{}
}

func readServiceCounter(t *testing.T, svc *testenv.Service, path string) int {
	t.Helper()

	result := svc.RequireExec(t, "/bin/sh", "-lc", "cat "+path+" 2>/dev/null || printf '0\\n'")
	value, err := strconv.Atoi(strings.TrimSpace(result))
	if err != nil {
		t.Fatalf("parse counter %q from %q: %v (output=%q)", path, svc.Name(), err, result)
	}

	return value
}

func triggerSwitchoverViaPacmanctl(t *testing.T, svc *testenv.Service) {
	t.Helper()

	output := svc.RequireExec(
		t,
		"/bin/bash", "-lc",
		"PACMANCTL_API_URL="+demoPacmandAPIURL+
			" PACMANCTL_API_TOKEN="+demoAdminToken+
			" pacmanctl cluster switchover -candidate alpha-2 -reason demo-switchover -requested-by install-integration -force",
	)

	if !strings.Contains(output, "alpha-2") {
		t.Fatalf("unexpected pacmanctl switchover output: %q", output)
	}
}

func waitForVIPWritablePrimary(t *testing.T, svc *testenv.Service, vipAddress string) {
	t.Helper()

	command := fmt.Sprintf(
		"PGPASSWORD=%q /usr/pgsql-17/bin/psql -h %q -p 5432 -U postgres --dbname=postgres -tAc %q",
		demoPostgresPassword,
		vipAddress,
		"CREATE TABLE IF NOT EXISTS vip_writable_marker (id integer PRIMARY KEY, payload text NOT NULL); "+
			"INSERT INTO vip_writable_marker (id, payload) VALUES (1, 'switched') ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload; "+
			"SELECT CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END;",
	)

	result := svc.RequireExec(t, "/bin/bash", "-lc", command)
	if !strings.HasSuffix(strings.TrimSpace(result), "primary") {
		t.Fatalf("expected VIP %s to route to writable primary, got %q", vipAddress, result)
	}
}

func newPACMANAPIClient(baseURL, token string) pacmanAPIClient {
	return pacmanAPIClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		client:  &http.Client{Timeout: 3 * time.Second},
	}
}

func (c pacmanAPIClient) members(t *testing.T) (nativeapi.MembersResponse, error) {
	t.Helper()

	var response nativeapi.MembersResponse
	err := c.getJSON(t, "/api/v1/members", &response)
	return response, err
}

func (c pacmanAPIClient) history(t *testing.T) (nativeapi.HistoryResponse, error) {
	t.Helper()

	var response nativeapi.HistoryResponse
	err := c.getJSON(t, "/api/v1/history", &response)
	return response, err
}

func (c pacmanAPIClient) getJSON(t *testing.T, path string, target any) error {
	t.Helper()

	request, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}

	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)

	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d for %s", response.StatusCode, path)
	}

	return json.NewDecoder(response.Body).Decode(target)
}

func waitForMembers(t *testing.T, client pacmanAPIClient, predicate func(nativeapi.MembersResponse) bool, timeout time.Duration) nativeapi.MembersResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last nativeapi.MembersResponse
	var lastErr error

	for time.Now().Before(deadline) {
		response, err := client.members(t)
		if err == nil {
			last = response
			if predicate(response) {
				return response
			}
		} else {
			lastErr = err
		}

		time.Sleep(time.Second)
	}

	if lastErr != nil {
		t.Fatalf("members API did not satisfy predicate within %s: %v; last payload: %+v", timeout, lastErr, last)
	}

	t.Fatalf("members API did not satisfy predicate within %s; last payload: %+v", timeout, last)
	return nativeapi.MembersResponse{}
}

func waitForHistoryEntry(t *testing.T, client pacmanAPIClient, predicate func(nativeapi.HistoryEntry) bool, timeout time.Duration) nativeapi.HistoryEntry {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last nativeapi.HistoryResponse
	var lastErr error

	for time.Now().Before(deadline) {
		response, err := client.history(t)
		if err == nil {
			last = response
			for _, entry := range response.Items {
				if predicate(entry) {
					return entry
				}
			}
		} else {
			lastErr = err
		}

		time.Sleep(time.Second)
	}

	if lastErr != nil {
		t.Fatalf("history API did not produce matching entry within %s: %v; last payload: %+v", timeout, lastErr, last)
	}

	t.Fatalf("history API did not produce matching entry within %s; last payload: %+v", timeout, last)
	return nativeapi.HistoryEntry{}
}

func memberStatusByName(response nativeapi.MembersResponse, name string) (nativeapi.MemberStatus, bool) {
	for _, item := range response.Items {
		if item.Name == name {
			return item, true
		}
	}

	return nativeapi.MemberStatus{}, false
}

func mustMemberStatus(t *testing.T, response nativeapi.MembersResponse, name string) nativeapi.MemberStatus {
	t.Helper()

	member, ok := memberStatusByName(response, name)
	if !ok {
		t.Fatalf("member %q not present in %+v", name, response)
	}

	return member
}

func waitForPostgresObservation(t *testing.T, address string, predicate func(pgobs.Observation) bool, timeout time.Duration) pgobs.Observation {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last pgobs.Observation
	var lastErr error

	for time.Now().Before(deadline) {
		observation, err := pgobs.QueryObservation(context.Background(), address)
		if err == nil {
			last = observation
			if predicate(observation) {
				return observation
			}
		} else {
			lastErr = err
		}

		time.Sleep(500 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("postgres observation for %s did not satisfy predicate within %s: %v; last observation: %+v", address, timeout, lastErr, last)
	}

	t.Fatalf("postgres observation for %s did not satisfy predicate within %s; last observation: %+v", address, timeout, last)
	return pgobs.Observation{}
}
