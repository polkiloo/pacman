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
