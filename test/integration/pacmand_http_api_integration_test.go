//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

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
