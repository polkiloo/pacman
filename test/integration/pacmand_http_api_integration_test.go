//go:build integration

package integration_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/test/testenv"
)

func TestPacmandHTTPAPIServesHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)
	name := sanitizeIntegrationName(t.Name())
	postgres := env.StartPostgres(t, name, name+"-postgres")

	configBody := fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-http
  role: data
  apiAddress: 0.0.0.0:8080
postgres:
  dataDir: /var/lib/postgresql/data
  listenAddress: %s
  port: 5432
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-http
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-http
`, postgres.Alias())

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         name + "-http-api",
		Image:        pacmanTestImage(),
		Aliases:      []string{name + "-http-api"},
		Env:          postgresConnectionEnv(postgres),
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/sh",
			"-lc",
			fmt.Sprintf("pacmand -config %s", daemonConfigPath),
		},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(30 * time.Second),
	})

	endpoint := "http://" + service.Address(t, "8080") + "/health"
	client := &http.Client{Timeout: 2 * time.Second}

	var (
		lastStatus int
		lastBody   string
	)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(endpoint)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		bodyBytes, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatalf("read health response: %v", readErr)
		}

		lastStatus = response.StatusCode
		lastBody = string(bodyBytes)

		if response.StatusCode == http.StatusOK {
			var payload struct {
				State   string `json:"state"`
				Role    string `json:"role"`
				Patroni struct {
					Name  string `json:"name"`
					Scope string `json:"scope"`
				} `json:"patroni"`
			}

			if err := json.Unmarshal(bodyBytes, &payload); err != nil {
				t.Fatalf("decode health payload: %v", err)
			}

			if payload.State != "running" {
				t.Fatalf("unexpected health state: got %q", payload.State)
			}

			if payload.Role != "primary" {
				t.Fatalf("unexpected health role: got %q", payload.Role)
			}

			if payload.Patroni.Name != "alpha-http" {
				t.Fatalf("unexpected patroni name: got %q", payload.Patroni.Name)
			}

			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("health endpoint did not become ready before deadline: status=%d body=%q", lastStatus, lastBody)
}

// TestPacmandPrimaryAndReplicaProbes verifies that the PACMAN daemon exposes
// Patroni-compatible /primary and /replica probe endpoints whose response
// shapes and status codes match the OpenAPI contract.
func TestPacmandPrimaryAndReplicaProbes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)
	name := sanitizeIntegrationName(t.Name())
	postgres := env.StartPostgres(t, name, name+"-postgres")
	document := loadContractDocument(t)

	configBody := fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-primary
  role: data
  apiAddress: 0.0.0.0:8080
postgres:
  dataDir: /var/lib/postgresql/data
  listenAddress: %s
  port: 5432
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-primary
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-primary
`, postgres.Alias())

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         name + "-primary-replica",
		Image:        pacmanTestImage(),
		Aliases:      []string{name + "-primary-replica"},
		Env:          postgresConnectionEnv(postgres),
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/sh",
			"-lc",
			fmt.Sprintf("pacmand -config %s", daemonConfigPath),
		},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(30 * time.Second),
	})

	base := "http://" + service.Address(t, "8080")
	client := &http.Client{Timeout: 2 * time.Second}

	// Wait until postgres is up so the node role is established.
	waitForProbeStatus(t, client, base+"/health", http.StatusOK, 15*time.Second)

	probes := []struct {
		path           string
		method         string
		wantStatus     int
		contractPath   string
		contractMethod string
	}{
		// /primary → 200 when node is the primary
		{path: "/primary", method: http.MethodGet, wantStatus: http.StatusOK, contractPath: "/primary", contractMethod: "get"},
		// /replica → 503 because this node is the primary, not a replica
		{path: "/replica", method: http.MethodGet, wantStatus: http.StatusServiceUnavailable, contractPath: "/replica", contractMethod: "get"},
		// HEAD /primary → 200, no body
		{path: "/primary", method: http.MethodHead, wantStatus: http.StatusOK},
		// HEAD /replica → 503 (primary is not a replica)
		{path: "/replica", method: http.MethodHead, wantStatus: http.StatusServiceUnavailable},
		// OPTIONS /primary → 200 with Allow header
		{path: "/primary", method: http.MethodOptions, wantStatus: http.StatusOK},
		// OPTIONS /replica → 200 with Allow header
		{path: "/replica", method: http.MethodOptions, wantStatus: http.StatusOK},
	}

	for _, probe := range probes {
		req, err := http.NewRequest(probe.method, base+probe.path, nil)
		if err != nil {
			t.Fatalf("build request %s %s: %v", probe.method, probe.path, err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("perform %s %s: %v", probe.method, probe.path, err)
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read body %s %s: %v", probe.method, probe.path, readErr)
		}

		if resp.StatusCode != probe.wantStatus {
			t.Errorf("%s %s: got status %d, want %d (body: %s)",
				probe.method, probe.path, resp.StatusCode, probe.wantStatus, body)
		}

		// Validate OPTIONS Allow header contains the required methods.
		if probe.method == http.MethodOptions {
			allow := resp.Header.Get("Allow")
			for _, m := range []string{"GET", "HEAD", "OPTIONS"} {
				if !strings.Contains(allow, m) {
					t.Errorf("OPTIONS %s Allow header %q missing %s", probe.path, allow, m)
				}
			}
		}

		// Validate GET response shapes against the OpenAPI contract.
		if probe.method == http.MethodGet && probe.contractPath != "" {
			requireResponseMatchesContract(t, document, probe.contractPath, probe.contractMethod, resp, body)
		}
	}

	// Verify the primary probe response has the correct Patroni-compatible fields.
	req, err := http.NewRequest(http.MethodGet, base+"/primary", nil)
	if err != nil {
		t.Fatalf("build primary GET request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /primary: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var payload struct {
		State   string `json:"state"`
		Role    string `json:"role"`
		Patroni struct {
			Name  string `json:"name"`
			Scope string `json:"scope"`
		} `json:"patroni"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode /primary payload: %v", err)
	}

	if payload.State != "running" {
		t.Errorf("/primary state: got %q, want %q", payload.State, "running")
	}

	if payload.Role != "primary" {
		t.Errorf("/primary role: got %q, want %q", payload.Role, "primary")
	}

	if payload.Patroni.Name != "alpha-primary" {
		t.Errorf("/primary patroni.name: got %q, want %q", payload.Patroni.Name, "alpha-primary")
	}
}

func TestPacmandNativeNodeAndMembersAPIWithRealPostgresOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)
	name := "native-api-real-pg"
	postgres := env.StartPostgres(t, name, "native-api-real-pg-postgres")
	document := loadContractDocument(t)

	configBody := fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-api
  role: data
  apiAddress: 0.0.0.0:8080
postgres:
  dataDir: /var/lib/postgresql/data
  listenAddress: %s
  port: 5432
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-api
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-api
`, postgres.Alias())

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         name + "-native-api",
		Image:        pacmanTestImage(),
		Aliases:      []string{name + "-native-api"},
		Env:          postgresConnectionEnv(postgres),
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/sh",
			"-lc",
			fmt.Sprintf("pacmand -config %s", daemonConfigPath),
		},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(30 * time.Second),
	})

	base := "http://" + service.Address(t, "8080")
	client := &http.Client{Timeout: 2 * time.Second}

	waitForProbeStatus(t, client, base+"/health", http.StatusOK, 15*time.Second)
	waitForProbeStatus(t, client, base+"/primary", http.StatusOK, 15*time.Second)
	waitForProbeStatus(t, client, base+"/api/v1/members", http.StatusOK, 15*time.Second)

	db := openIntegrationPostgres(t, postgres)
	defer db.Close()

	execIntegrationSQL(t, db, `
create table if not exists api_smoke (
	id integer primary key,
	note text not null
)`)
	execIntegrationSQL(t, db, `
insert into api_smoke (id, note)
values (1, 'patroni-compatible')
on conflict (id) do update set note = excluded.note`)

	var rows int
	if err := db.QueryRow(`select count(*) from api_smoke where note = 'patroni-compatible'`).Scan(&rows); err != nil {
		t.Fatalf("verify postgres write/read: %v", err)
	}
	if rows != 1 {
		t.Fatalf("unexpected postgres row count: got %d, want 1", rows)
	}

	primaryResponse := performHTTPRequest(t, http.MethodGet, base+"/primary", nil, nil)
	primaryBody, err := io.ReadAll(primaryResponse.Body)
	primaryResponse.Body.Close()
	if err != nil {
		t.Fatalf("read /primary response: %v", err)
	}
	if primaryResponse.StatusCode != http.StatusOK {
		t.Fatalf("unexpected /primary status: got %d want %d", primaryResponse.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/primary", "get", primaryResponse, primaryBody)

	var primaryPayload struct {
		State   string `json:"state"`
		Role    string `json:"role"`
		Patroni struct {
			Name  string `json:"name"`
			Scope string `json:"scope"`
		} `json:"patroni"`
	}
	if err := json.Unmarshal(primaryBody, &primaryPayload); err != nil {
		t.Fatalf("decode /primary payload: %v", err)
	}
	if primaryPayload.Role != "primary" || primaryPayload.State != "running" {
		t.Fatalf("unexpected /primary payload: %+v", primaryPayload)
	}

	nodeResponse := performHTTPRequest(t, http.MethodGet, base+"/api/v1/nodes/alpha-api", nil, nil)
	nodeBody, err := io.ReadAll(nodeResponse.Body)
	nodeResponse.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/nodes response: %v", err)
	}
	if nodeResponse.StatusCode != http.StatusOK {
		t.Fatalf("unexpected /api/v1/nodes status: got %d want %d", nodeResponse.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/nodes/{nodeName}", "get", nodeResponse, nodeBody)

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
	if nodePayload.NodeName != "alpha-api" || nodePayload.Role != "primary" {
		t.Fatalf("unexpected node payload: %+v", nodePayload)
	}
	if !nodePayload.Postgres.Managed || !nodePayload.Postgres.Up {
		t.Fatalf("unexpected postgres payload: %+v", nodePayload.Postgres)
	}
	if nodePayload.Postgres.Details.ServerVersion == 0 || nodePayload.Postgres.Details.SystemIdentifier == "" {
		t.Fatalf("expected postgres details from real postgres observation, got %+v", nodePayload.Postgres.Details)
	}

	membersResponse := performHTTPRequest(t, http.MethodGet, base+"/api/v1/members", nil, nil)
	membersBody, err := io.ReadAll(membersResponse.Body)
	membersResponse.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/members response: %v", err)
	}
	if membersResponse.StatusCode != http.StatusOK {
		t.Fatalf("unexpected /api/v1/members status: got %d want %d", membersResponse.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/members", "get", membersResponse, membersBody)

	var membersPayload struct {
		Items []struct {
			Name    string `json:"name"`
			Role    string `json:"role"`
			State   string `json:"state"`
			Healthy bool   `json:"healthy"`
		} `json:"items"`
	}
	if err := json.Unmarshal(membersBody, &membersPayload); err != nil {
		t.Fatalf("decode /api/v1/members payload: %v", err)
	}
	if len(membersPayload.Items) != 1 {
		t.Fatalf("unexpected member count: got %d want 1", len(membersPayload.Items))
	}
	if membersPayload.Items[0].Name != "alpha-api" || membersPayload.Items[0].Role != "primary" || !membersPayload.Items[0].Healthy {
		t.Fatalf("unexpected member payload: %+v", membersPayload.Items[0])
	}
}

// waitForProbeStatus polls the given URL until it returns wantStatus or the
// deadline is exceeded.
func waitForProbeStatus(t *testing.T, client *http.Client, url string, wantStatus int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var (
		lastStatus int
		lastErr    error
	)

	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		lastStatus = resp.StatusCode

		if resp.StatusCode == wantStatus {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("probe %s did not return %d within %s: last error: %v", url, wantStatus, timeout, lastErr)
	}

	t.Fatalf("probe %s did not return %d within %s: last status %d", url, wantStatus, timeout, lastStatus)
}

func pacmanTestImage() string {
	image := strings.TrimSpace(os.Getenv("PACMAN_TEST_IMAGE"))
	if image == "" {
		return "pacman-test:local"
	}

	return image
}

func postgresConnectionEnv(postgres *testenv.Postgres) map[string]string {
	return map[string]string{
		"PGDATABASE": postgres.Database(),
		"PGUSER":     postgres.Username(),
		"PGPASSWORD": postgres.Password(),
	}
}

func openIntegrationPostgres(t *testing.T, postgres *testenv.Postgres) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		postgres.Host(t),
		postgres.Port(t),
		postgres.Database(),
		postgres.Username(),
		postgres.Password(),
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("ping postgres: %v", err)
	}

	return db
}

func execIntegrationSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec integration sql %q: %v", query, err)
	}
}
