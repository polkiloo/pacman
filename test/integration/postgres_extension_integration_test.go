//go:build integration

package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestPostgresExtensionStartupPublishesAPIAndInstallsSQLAssets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	node := startPostgresExtensionNode(t, pgextNodeConfig{NodeName: "alpha-ext"})
	waitForProbeStatus(t, node.Client, node.Base+"/health", http.StatusOK, postgresExtensionStartupTimeout)
	waitForProbeStatus(t, node.Client, node.Base+"/primary", http.StatusOK, postgresExtensionStartupTimeout)

	db := openPGExtDB(t, node)
	defer db.Close()

	var preload string
	if err := db.QueryRow(`show shared_preload_libraries`).Scan(&preload); err != nil {
		t.Fatalf("show shared_preload_libraries: %v", err)
	}
	if !strings.Contains(preload, "pacman_agent") {
		t.Fatalf("shared_preload_libraries: got %q, want pacman_agent", preload)
	}

	if _, err := db.Exec(`create extension if not exists pacman_agent`); err != nil {
		t.Fatalf("create extension pacman_agent: %v", err)
	}

	waitForLogContains(t, node.Service, "started PACMAN helper process", 10*time.Second)
}

func TestPostgresExtensionRestartsPACMANHelperAfterUnexpectedExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	node := startPostgresExtensionNode(t, pgextNodeConfig{NodeName: "alpha-restart"})
	waitForProbeStatus(t, node.Client, node.Base+"/health", http.StatusOK, postgresExtensionStartupTimeout)

	initialPID := waitForHelperPID(t, node.Service, 10*time.Second)
	node.Service.RequireExec(t, "sh", "-lc", "kill -TERM "+strconv.Itoa(initialPID))

	waitForLogContains(t, node.Service, "PACMAN helper process exited", 10*time.Second)
	waitForHelperPIDChange(t, node.Service, initialPID, 20*time.Second)
	waitForProbeStatus(t, node.Client, node.Base+"/health", http.StatusOK, 10*time.Second)
}

func TestPostgresExtensionInvalidConfigKeepsAPIUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	node := startPostgresExtensionNode(t, pgextNodeConfig{
		NodeName: "alpha-invalid",
		NodeRole: "witness",
	})

	waitForHTTPUnavailable(t, node.Client, node.Base+"/health", 10*time.Second)
	waitForLogContains(t, node.Service, "postgres background worker mode requires a postgres-managing node role", 15*time.Second)
}

func TestPostgresExtensionLocalStateObservationWithRealSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	document := loadContractDocument(t)
	node := startPostgresExtensionNode(t, pgextNodeConfig{NodeName: "alpha-observe"})
	waitForProbeStatus(t, node.Client, node.Base+"/health", http.StatusOK, postgresExtensionStartupTimeout)
	waitForProbeStatus(t, node.Client, node.Base+"/api/v1/members", http.StatusOK, postgresExtensionStartupTimeout)

	db := openPGExtDB(t, node)
	defer db.Close()

	for _, statement := range []string{
		`create extension if not exists pacman_agent`,
		`create table if not exists extension_smoke (id integer primary key, note text not null)`,
		`insert into extension_smoke (id, note) values (1, 'background-worker') on conflict (id) do update set note = excluded.note`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec sql %q: %v", statement, err)
		}
	}

	var rows int
	if err := db.QueryRow(`select count(*) from extension_smoke where note = 'background-worker'`).Scan(&rows); err != nil {
		t.Fatalf("verify postgres write/read: %v", err)
	}
	if rows != 1 {
		t.Fatalf("unexpected row count: got %d want 1", rows)
	}

	nodeResp := performHTTPRequest(t, http.MethodGet, node.Base+"/api/v1/nodes/alpha-observe", nil, nil)
	nodeBody, err := io.ReadAll(nodeResp.Body)
	nodeResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/nodes response: %v", err)
	}
	if nodeResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/nodes/alpha-observe: got status %d want %d", nodeResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/nodes/{nodeName}", "get", nodeResp, nodeBody)

	var nodePayload struct {
		NodeName string `json:"nodeName"`
		Role     string `json:"role"`
		State    string `json:"state"`
		Postgres struct {
			Managed bool `json:"managed"`
			Up      bool `json:"up"`
			Details struct {
				ServerVersion    int    `json:"serverVersion"`
				SystemIdentifier string `json:"systemIdentifier"`
			} `json:"details"`
		} `json:"postgres"`
	}
	if err := json.Unmarshal(nodeBody, &nodePayload); err != nil {
		t.Fatalf("decode /api/v1/nodes payload: %v", err)
	}
	if nodePayload.NodeName != "alpha-observe" || nodePayload.Role != "primary" || nodePayload.State != "running" {
		t.Fatalf("unexpected node payload: %+v", nodePayload)
	}
	if !nodePayload.Postgres.Managed || !nodePayload.Postgres.Up {
		t.Fatalf("unexpected postgres payload: %+v", nodePayload.Postgres)
	}
	if nodePayload.Postgres.Details.ServerVersion == 0 || nodePayload.Postgres.Details.SystemIdentifier == "" {
		t.Fatalf("expected postgres details, got %+v", nodePayload.Postgres.Details)
	}
}

func TestPostgresExtensionStopsPACMANHelperWhenPostgresStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	node := startPostgresExtensionNode(t, pgextNodeConfig{NodeName: "alpha-stop"})
	waitForProbeStatus(t, node.Client, node.Base+"/health", http.StatusOK, postgresExtensionStartupTimeout)

	result := node.Service.Exec(t, "sh", "-lc", "gosu postgres /usr/lib/postgresql/17/bin/pg_ctl -D /var/lib/postgresql/data -m fast stop >/tmp/pgctl-stop.log 2>&1 &")
	if result.ExitCode != 0 {
		t.Fatalf("stop postgres: %s", result.Output)
	}

	waitForHTTPUnavailable(t, node.Client, node.Base+"/health", 20*time.Second)
	waitForLogContains(t, node.Service, "stopping PACMAN helper process", 10*time.Second)
}
