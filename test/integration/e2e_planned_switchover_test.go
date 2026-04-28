//go:build integration

package integration_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	nativeapi "github.com/polkiloo/pacman/internal/api/native"
	"github.com/polkiloo/pacman/test/testenv"
)

const (
	e2eSwitchoverClusterName = "alpha"
	e2eSwitchoverSource      = "alpha-1"
	e2eSwitchoverTarget      = "alpha-2"
	e2eSwitchoverPGData      = "/var/lib/postgresql/data"
	e2eSwitchoverPGBin       = "/usr/lib/postgresql/17/bin"
)

type e2eSwitchoverNode struct {
	Name    string
	Base    string
	Client  *http.Client
	Service *testenv.Service
}

type e2eSwitchoverScenario struct {
	Source *e2eSwitchoverNode
	Target *e2eSwitchoverNode
}

func TestEndToEndPlannedSwitchover(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	scenario := startEndToEndSwitchoverScenario(t, "etcd-e2e-switchover-positive")

	execServiceSQL(t, scenario.Source.Service, `
CREATE TABLE IF NOT EXISTS e2e_switchover_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execServiceSQL(t, scenario.Source.Service, `
INSERT INTO e2e_switchover_marker (id, payload)
VALUES (1, 'before-switchover')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForServiceQueryValue(t, scenario.Target.Service, `SELECT payload FROM e2e_switchover_marker WHERE id = 1`, "before-switchover")

	accepted := requestEndToEndSwitchover(t, scenario.Source)
	finalStatus := waitForEndToEndSwitchoverCompletion(t, scenario.Target, accepted.Operation.ID)
	waitForServicePostgresRecovery(t, scenario.Target.Service, false)
	waitForServicePostgresUnavailable(t, scenario.Source.Service)
	execServiceSQL(t, scenario.Target.Service, `
INSERT INTO e2e_switchover_marker (id, payload)
VALUES (2, 'after-switchover')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	historyEntry := waitForEndToEndSwitchoverHistory(t, scenario.Target, accepted.Operation.ID)
	demotedStatus := waitForEndToEndSwitchoverStatus(t, scenario.Target, "former primary demotion", func(status nativeapi.ClusterStatusResponse) bool {
		source := e2eSwitchoverMember(status, e2eSwitchoverSource)
		return source != nil && source.Role != "primary" && !source.Healthy
	})

	positiveCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "positive switchover request is accepted with planned operation metadata",
			run: func(t *testing.T) {
				if accepted.Operation.Kind != "switchover" || accepted.Operation.FromMember != e2eSwitchoverSource || accepted.Operation.ToMember != e2eSwitchoverTarget {
					t.Fatalf("unexpected accepted operation: %+v", accepted.Operation)
				}
				if accepted.Operation.RequestedBy != "e2e-test" || accepted.Operation.Reason != "planned e2e switchover" {
					t.Fatalf("unexpected accepted operation request metadata: %+v", accepted.Operation)
				}
			},
		},
		{
			name: "positive source writes replicate to candidate before handoff",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Target.Service, `SELECT payload FROM e2e_switchover_marker WHERE id = 1`, "before-switchover")
			},
		},
		{
			name: "positive cluster converges on promoted candidate",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, finalStatus, e2eSwitchoverTarget)
				if finalStatus.CurrentPrimary != e2eSwitchoverTarget || finalStatus.ActiveOperation != nil {
					t.Fatalf("unexpected final cluster status: %+v", finalStatus)
				}
				if target.Role != "primary" || !target.Healthy {
					t.Fatalf("unexpected promoted target member: %+v", target)
				}
			},
		},
		{
			name: "positive promoted candidate is writable after switchover",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Target.Service, `SELECT payload FROM e2e_switchover_marker WHERE id = 2`, "after-switchover")
			},
		},
		{
			name: "positive history records success and former primary is demoted",
			run: func(t *testing.T) {
				if historyEntry.Kind != "switchover" || historyEntry.FromMember != e2eSwitchoverSource || historyEntry.ToMember != e2eSwitchoverTarget || historyEntry.Result != "succeeded" {
					t.Fatalf("unexpected switchover history entry: %+v", historyEntry)
				}
				source := requireE2ESwitchoverMember(t, demotedStatus, e2eSwitchoverSource)
				if source.Role == "primary" || source.Healthy {
					t.Fatalf("expected former primary to be unhealthy and non-primary, got %+v", source)
				}
			},
		},
	}

	for _, testCase := range positiveCases {
		t.Run(testCase.name, testCase.run)
	}
}

func TestEndToEndPlannedSwitchoverNegativeCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	scenario := startEndToEndSwitchoverScenario(t, "etcd-e2e-switchover-negative")
	scheduledAt := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)

	negativeCases := []struct {
		name      string
		body      []byte
		headers   map[string]string
		before    func(*testing.T)
		wantCode  int
		wantError string
	}{
		{
			name:      "negative candidate is required",
			body:      []byte(`{"reason":"missing candidate","requestedBy":"e2e-test"}`),
			headers:   map[string]string{topologyContentType: topologyApplicationJSON},
			wantCode:  http.StatusBadRequest,
			wantError: "invalid_switchover_request",
		},
		{
			name:      "negative candidate cannot be current primary",
			body:      []byte(`{"candidate":"alpha-1","reason":"self switchover","requestedBy":"e2e-test"}`),
			headers:   map[string]string{topologyContentType: topologyApplicationJSON},
			wantCode:  http.StatusPreconditionFailed,
			wantError: "switchover_precondition_failed",
		},
		{
			name:      "negative unknown candidate is rejected",
			body:      []byte(`{"candidate":"alpha-3","reason":"unknown candidate","requestedBy":"e2e-test"}`),
			headers:   map[string]string{topologyContentType: topologyApplicationJSON},
			wantCode:  http.StatusPreconditionFailed,
			wantError: "switchover_precondition_failed",
		},
		{
			name:      "negative scheduled switchover is rejected by bootstrap policy",
			body:      []byte(fmt.Sprintf(`{"candidate":"alpha-2","scheduledAt":%q,"reason":"future schedule","requestedBy":"e2e-test"}`, scheduledAt)),
			headers:   map[string]string{topologyContentType: topologyApplicationJSON},
			wantCode:  http.StatusBadRequest,
			wantError: "invalid_switchover_request",
		},
		{
			name: "negative stopped candidate is not eligible",
			body: []byte(`{"candidate":"alpha-2","reason":"stopped candidate","requestedBy":"e2e-test"}`),
			headers: map[string]string{
				topologyContentType: topologyApplicationJSON,
			},
			before: func(t *testing.T) {
				stopServicePostgres(t, scenario.Target.Service)
				waitForEndToEndSwitchoverMember(t, scenario.Source, e2eSwitchoverTarget, func(member nativeapi.MemberStatus) bool {
					return !member.Healthy
				})
			},
			wantCode:  http.StatusPreconditionFailed,
			wantError: "switchover_precondition_failed",
		},
	}

	for _, testCase := range negativeCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.before != nil {
				testCase.before(t)
			}

			resp := performHTTPRequest(t, http.MethodPost, scenario.Source.Base+"/api/v1/operations/switchover", testCase.body, testCase.headers)
			respBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read switchover rejection body: %v", err)
			}

			if resp.StatusCode != testCase.wantCode {
				t.Fatalf("expected status %d, got %d, body: %s", testCase.wantCode, resp.StatusCode, respBody)
			}

			var payload nativeapi.ErrorResponse
			if err := json.Unmarshal(respBody, &payload); err != nil {
				t.Fatalf("decode error payload: %v\nbody: %s", err, respBody)
			}
			if payload.Error != testCase.wantError {
				t.Fatalf("expected error %q, got %+v", testCase.wantError, payload)
			}

			assertNoEndToEndSwitchoverOperation(t, scenario.Source)
		})
	}
}

func startEndToEndSwitchoverScenario(t *testing.T, etcdAlias string) e2eSwitchoverScenario {
	t.Helper()

	env := testenv.New(t)
	startTopologyEtcd(t, env, etcdAlias)

	members := []string{e2eSwitchoverSource, e2eSwitchoverTarget}
	source := startEndToEndSwitchoverPrimary(t, env, etcdAlias, members)
	target := startEndToEndSwitchoverStandby(t, env, etcdAlias, members)

	waitForEndToEndSwitchoverInitialTopology(t, source)
	waitForEndToEndSwitchoverInitialTopology(t, target)
	waitForServicePostgresRecovery(t, target.Service, true)

	return e2eSwitchoverScenario{
		Source: source,
		Target: target,
	}
}

func startEndToEndSwitchoverPrimary(t *testing.T, env *testenv.Environment, etcdAlias string, members []string) *e2eSwitchoverNode {
	t.Helper()

	return startEndToEndSwitchoverService(t, env, e2eSwitchoverSource, etcdAlias, members, []testcontainers.ContainerFile{
		{
			Reader:            strings.NewReader(e2eSwitchoverPrimaryInitScript()),
			ContainerFilePath: "/docker-entrypoint-initdb.d/010-enable-replication.sh",
			FileMode:          0o755,
		},
		{
			Reader:            strings.NewReader(e2eSwitchoverPrimaryEntrypoint()),
			ContainerFilePath: "/usr/local/bin/pacman-e2e-primary.sh",
			FileMode:          0o755,
		},
	}, []string{"/usr/local/bin/pacman-e2e-primary.sh"}, nil)
}

func startEndToEndSwitchoverStandby(t *testing.T, env *testenv.Environment, etcdAlias string, members []string) *e2eSwitchoverNode {
	t.Helper()

	return startEndToEndSwitchoverService(t, env, e2eSwitchoverTarget, etcdAlias, members, []testcontainers.ContainerFile{
		{
			Reader:            strings.NewReader(e2eSwitchoverStandbyEntrypoint()),
			ContainerFilePath: "/usr/local/bin/pacman-e2e-standby.sh",
			FileMode:          0o755,
		},
	}, []string{"/usr/local/bin/pacman-e2e-standby.sh"}, map[string]string{
		"PRIMARY_HOST":          e2eSwitchoverSource,
		"PRIMARY_PORT":          "5432",
		"REPLICATION_USER":      "replicator",
		"REPLICATION_PASSWORD":  "replicator",
		"REPLICATION_SLOT_NAME": "alpha_2_e2e",
		"PGDATA":                e2eSwitchoverPGData,
	})
}

func startEndToEndSwitchoverService(
	t *testing.T,
	env *testenv.Environment,
	nodeName string,
	etcdAlias string,
	members []string,
	files []testcontainers.ContainerFile,
	entrypoint []string,
	extraEnv map[string]string,
) *e2eSwitchoverNode {
	t.Helper()

	testenv.RequireLocalImage(t, pgextTestImage())

	serviceEnv := map[string]string{
		"POSTGRES_DB":       "pacman",
		"POSTGRES_USER":     "pacman",
		"POSTGRES_PASSWORD": "pacman",
		"PGDATABASE":        "pacman",
		"PGUSER":            "pacman",
		"PGPASSWORD":        "pacman",
		"PGSSLMODE":         "disable",
	}
	for key, value := range extraEnv {
		serviceEnv[key] = value
	}

	allFiles := []testcontainers.ContainerFile{writeDaemonConfigFile(t, e2eSwitchoverDaemonConfig(nodeName, etcdAlias, members))}
	allFiles = append(allFiles, files...)

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         nodeName + "-e2e-switchover",
		Image:        pgextTestImage(),
		Aliases:      []string{nodeName},
		Env:          serviceEnv,
		Files:        allFiles,
		Entrypoint:   entrypoint,
		ExposedPorts: []string{"5432/tcp", "8080/tcp"},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(2 * topologyStartupTimeout),
	})

	client := &http.Client{Timeout: 3 * time.Second}
	node := &e2eSwitchoverNode{
		Name:    nodeName,
		Base:    "http://" + service.Address(t, "8080"),
		Client:  client,
		Service: service,
	}
	waitForProbeStatus(t, client, node.Base+"/health", http.StatusOK, topologyStartupTimeout)

	return node
}

func e2eSwitchoverDaemonConfig(nodeName, etcdAlias string, members []string) string {
	return fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: %s
  role: data
  apiAddress: %s:8080
  controlAddress: %s:9090
postgres:
  dataDir: %s
  binDir: %s
  listenAddress: 127.0.0.1
  port: 5432
dcs:
  backend: etcd
  clusterName: %s
  etcd:
    endpoints:
      - http://%s:2379
bootstrap:
  clusterName: %s
  initialPrimary: %s
  seedAddresses:
    - %s:9090
  expectedMembers:
%s
`,
		nodeName,
		nodeName,
		nodeName,
		e2eSwitchoverPGData,
		e2eSwitchoverPGBin,
		e2eSwitchoverClusterName,
		etcdAlias,
		e2eSwitchoverClusterName,
		e2eSwitchoverSource,
		nodeName,
		e2eSwitchoverMembersYAML(members),
	)
}

func e2eSwitchoverMembersYAML(members []string) string {
	var builder strings.Builder
	for _, member := range members {
		fmt.Fprintf(&builder, "    - %s\n", member)
	}
	return strings.TrimRight(builder.String(), "\n")
}

func e2eSwitchoverPrimaryInitScript() string {
	return `#!/bin/sh
set -eu

cat <<'EOF' >> "$PGDATA/pg_hba.conf"
host all all all scram-sha-256
host replication replicator all scram-sha-256
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
ALTER SYSTEM SET wal_level = 'replica';
ALTER SYSTEM SET max_wal_senders = '10';
ALTER SYSTEM SET max_replication_slots = '10';
ALTER SYSTEM SET hot_standby = 'on';
ALTER SYSTEM SET listen_addresses = '*';
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
		CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
	END IF;
END
$$;
SQL
`
}

func e2eSwitchoverPrimaryEntrypoint() string {
	return `#!/bin/bash
set -euo pipefail

run_as_postgres() {
	if command -v gosu >/dev/null 2>&1; then
		gosu postgres "$@"
		return
	fi

	su-exec postgres "$@"
}

pacmand_config="${PACMAND_CONFIG:-/tmp/pacmand.yaml}"
chmod 0644 "$pacmand_config"

docker-entrypoint.sh postgres -c listen_addresses='*' &
postgres_pid=$!

until pg_isready -h 127.0.0.1 -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
	sleep 1
done

run_as_postgres pacmand -config "$pacmand_config" &
pacmand_pid=$!

terminate() {
	kill -TERM "$pacmand_pid" "$postgres_pid" 2>/dev/null || true
	wait "$pacmand_pid" "$postgres_pid" 2>/dev/null || true
}

trap terminate TERM INT
set +e
wait "$pacmand_pid"
status=$?
set -e
terminate
exit "$status"
`
}

func e2eSwitchoverStandbyEntrypoint() string {
	return `#!/bin/bash
set -euo pipefail

run_as_postgres() {
	if command -v gosu >/dev/null 2>&1; then
		gosu postgres "$@"
		return
	fi

	su-exec postgres "$@"
}

pacmand_config="${PACMAND_CONFIG:-/tmp/pacmand.yaml}"
chmod 0644 "$pacmand_config"

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	rm -rf "$PGDATA"/*
	export PGPASSWORD="$REPLICATION_PASSWORD"
	until pg_isready -h "$PRIMARY_HOST" -p "${PRIMARY_PORT:-5432}" -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
		sleep 1
	done
	run_as_postgres pg_basebackup \
		-h "$PRIMARY_HOST" \
		-p "${PRIMARY_PORT:-5432}" \
		-U "$REPLICATION_USER" \
		-D "$PGDATA" \
		-R \
		-X stream \
		-C \
		-S "$REPLICATION_SLOT_NAME"
	printf "primary_slot_name = '%s'\n" "$REPLICATION_SLOT_NAME" >> "$PGDATA/postgresql.auto.conf"
	printf "hot_standby = 'on'\n" >> "$PGDATA/postgresql.auto.conf"
	chown postgres:postgres "$PGDATA/postgresql.auto.conf"
fi

run_as_postgres postgres -D "$PGDATA" -c listen_addresses='*' &
postgres_pid=$!

until pg_isready -h 127.0.0.1 -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
	sleep 1
done

run_as_postgres pacmand -config "$pacmand_config" &
pacmand_pid=$!

terminate() {
	kill -TERM "$pacmand_pid" "$postgres_pid" 2>/dev/null || true
	wait "$pacmand_pid" "$postgres_pid" 2>/dev/null || true
}

trap terminate TERM INT
set +e
wait "$pacmand_pid"
status=$?
set -e
terminate
exit "$status"
`
}

func requestEndToEndSwitchover(t *testing.T, source *e2eSwitchoverNode) nativeapi.OperationAcceptedResponse {
	t.Helper()

	body := []byte(`{"candidate":"alpha-2","reason":"planned e2e switchover","requestedBy":"e2e-test"}`)
	resp := performHTTPRequest(t, http.MethodPost, source.Base+"/api/v1/operations/switchover", body, map[string]string{
		topologyContentType: topologyApplicationJSON,
	})
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read switchover response body: %v", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /api/v1/operations/switchover: got status %d, want %d, body: %s", resp.StatusCode, http.StatusAccepted, respBody)
	}

	var accepted nativeapi.OperationAcceptedResponse
	if err := json.Unmarshal(respBody, &accepted); err != nil {
		t.Fatalf("decode switchover response: %v\nbody: %s", err, respBody)
	}

	return accepted
}

func waitForEndToEndSwitchoverInitialTopology(t *testing.T, node *e2eSwitchoverNode) nativeapi.ClusterStatusResponse {
	t.Helper()

	return waitForEndToEndSwitchoverStatus(t, node, "initial topology", func(status nativeapi.ClusterStatusResponse) bool {
		source := e2eSwitchoverMember(status, e2eSwitchoverSource)
		target := e2eSwitchoverMember(status, e2eSwitchoverTarget)

		return status.ClusterName == e2eSwitchoverClusterName &&
			status.CurrentPrimary == e2eSwitchoverSource &&
			source != nil &&
			source.Role == "primary" &&
			source.Healthy &&
			target != nil &&
			target.Role == "replica" &&
			target.Healthy
	})
}

func waitForEndToEndSwitchoverCompletion(t *testing.T, node *e2eSwitchoverNode, operationID string) nativeapi.ClusterStatusResponse {
	t.Helper()

	return waitForEndToEndSwitchoverStatus(t, node, "switchover completion", func(status nativeapi.ClusterStatusResponse) bool {
		target := e2eSwitchoverMember(status, e2eSwitchoverTarget)
		return status.CurrentPrimary == e2eSwitchoverTarget &&
			status.ActiveOperation == nil &&
			target != nil &&
			target.Role == "primary" &&
			target.Healthy &&
			operationID != ""
	})
}

func waitForEndToEndSwitchoverStatus(
	t *testing.T,
	node *e2eSwitchoverNode,
	description string,
	ready func(nativeapi.ClusterStatusResponse) bool,
) nativeapi.ClusterStatusResponse {
	t.Helper()

	deadline := time.Now().Add(2 * topologyStartupTimeout)
	var (
		lastStatus int
		lastBody   string
		lastErr    error
	)

	for time.Now().Before(deadline) {
		status, code, body, err := fetchEndToEndSwitchoverStatus(t, node)
		lastStatus = code
		lastBody = body
		lastErr = err
		if err == nil && code == http.StatusOK && ready(status) {
			return status
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("%s did not converge on %s; lastStatus=%d lastErr=%v body=%s", node.Name, description, lastStatus, lastErr, lastBody)
	return nativeapi.ClusterStatusResponse{}
}

func fetchEndToEndSwitchoverStatus(t *testing.T, node *e2eSwitchoverNode) (nativeapi.ClusterStatusResponse, int, string, error) {
	t.Helper()

	resp, err := node.Client.Get(node.Base + topologyClusterAPI)
	if err != nil {
		return nativeapi.ClusterStatusResponse{}, 0, "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nativeapi.ClusterStatusResponse{}, resp.StatusCode, "", err
	}

	var status nativeapi.ClusterStatusResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(bodyBytes, &status); err != nil {
			return nativeapi.ClusterStatusResponse{}, resp.StatusCode, string(bodyBytes), err
		}
	}

	return status, resp.StatusCode, string(bodyBytes), nil
}

func waitForEndToEndSwitchoverMember(
	t *testing.T,
	node *e2eSwitchoverNode,
	memberName string,
	ready func(nativeapi.MemberStatus) bool,
) nativeapi.MemberStatus {
	t.Helper()

	status := waitForEndToEndSwitchoverStatus(t, node, "member "+memberName, func(status nativeapi.ClusterStatusResponse) bool {
		member := e2eSwitchoverMember(status, memberName)
		return member != nil && ready(*member)
	})

	return requireE2ESwitchoverMember(t, status, memberName)
}

func e2eSwitchoverMember(status nativeapi.ClusterStatusResponse, memberName string) *nativeapi.MemberStatus {
	for index := range status.Members {
		if status.Members[index].Name == memberName {
			return &status.Members[index]
		}
	}
	return nil
}

func requireE2ESwitchoverMember(t *testing.T, status nativeapi.ClusterStatusResponse, memberName string) nativeapi.MemberStatus {
	t.Helper()

	member := e2eSwitchoverMember(status, memberName)
	if member == nil {
		t.Fatalf("expected member %q in status: %+v", memberName, status)
	}
	return *member
}

func waitForEndToEndSwitchoverHistory(t *testing.T, node *e2eSwitchoverNode, operationID string) nativeapi.HistoryEntry {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	var last nativeapi.HistoryResponse
	for time.Now().Before(deadline) {
		clusterJSON(t, node.Client, node.Base+"/api/v1/history", &last)
		for _, entry := range last.Items {
			if entry.OperationID == operationID && entry.Kind == "switchover" && entry.Result == "succeeded" {
				return entry
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("switchover history entry %q did not appear; last history: %+v", operationID, last)
	return nativeapi.HistoryEntry{}
}

func assertNoEndToEndSwitchoverOperation(t *testing.T, node *e2eSwitchoverNode) {
	t.Helper()

	var status nativeapi.ClusterStatusResponse
	clusterJSON(t, node.Client, node.Base+topologyClusterAPI, &status)
	if status.ActiveOperation != nil {
		t.Fatalf("expected rejected switchover not to create active operation, got %+v", status.ActiveOperation)
	}

	var history nativeapi.HistoryResponse
	clusterJSON(t, node.Client, node.Base+"/api/v1/history", &history)
	for _, entry := range history.Items {
		if entry.Kind == "switchover" {
			t.Fatalf("expected rejected switchover not to record history, got %+v", history.Items)
		}
	}
}

func execServiceSQL(t *testing.T, service *testenv.Service, statement string, args ...any) {
	t.Helper()

	db := openServiceDB(t, service)
	defer db.Close()

	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatalf("exec SQL on %q failed: %v", service.Name(), err)
	}
}

func waitForServiceQueryValue(t *testing.T, service *testenv.Service, query string, want string) {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	for time.Now().Before(deadline) {
		db, err := openServiceDBE(t, service)
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var got string
		err = db.QueryRow(query).Scan(&got)
		_ = db.Close()
		if err == nil && got == want {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("query %q on %q did not return %q before deadline", query, service.Name(), want)
}

func assertServiceQueryValue(t *testing.T, service *testenv.Service, query string, want string) {
	t.Helper()

	db := openServiceDB(t, service)
	defer db.Close()

	var got string
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("query %q on %q failed: %v", query, service.Name(), err)
	}
	if got != want {
		t.Fatalf("query %q on %q: got %q, want %q", query, service.Name(), got, want)
	}
}

func waitForServicePostgresRecovery(t *testing.T, service *testenv.Service, wantInRecovery bool) {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	for time.Now().Before(deadline) {
		db, err := openServiceDBE(t, service)
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var inRecovery bool
		err = db.QueryRow(`SELECT pg_is_in_recovery()`).Scan(&inRecovery)
		_ = db.Close()
		if err == nil && inRecovery == wantInRecovery {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("postgres service %q did not report pg_is_in_recovery()=%v before deadline", service.Name(), wantInRecovery)
}

func waitForServicePostgresUnavailable(t *testing.T, service *testenv.Service) {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	for time.Now().Before(deadline) {
		db, err := openServiceDBE(t, service)
		if err != nil {
			return
		}
		_ = db.Close()
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("postgres service %q remained reachable before deadline", service.Name())
}

func stopServicePostgres(t *testing.T, service *testenv.Service) {
	t.Helper()

	result := service.Exec(t, "sh", "-lc", "gosu postgres /usr/lib/postgresql/17/bin/pg_ctl -D /var/lib/postgresql/data -m fast -w stop")
	if result.ExitCode != 0 {
		t.Fatalf("stop postgres in %q returned %d: %s", service.Name(), result.ExitCode, result.Output)
	}
	waitForServicePostgresUnavailable(t, service)
}

func openServiceDB(t *testing.T, service *testenv.Service) *sql.DB {
	t.Helper()

	db, err := openServiceDBE(t, service)
	if err != nil {
		t.Fatalf("open postgres service %q: %v", service.Name(), err)
	}

	return db
}

func openServiceDBE(t *testing.T, service *testenv.Service) (*sql.DB, error) {
	t.Helper()

	return openDB(
		service.Host(t),
		service.Port(t, "5432"),
		"pacman",
		"pacman",
		"pacman",
	)
}
