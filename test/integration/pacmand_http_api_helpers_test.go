//go:build integration

package integration_test

import (
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

const pacmandStartupTimeout = 30 * time.Second

// daemonNodeConfig is the YAML template for a single-node pacmand configuration.
// Arguments: nodeName, postgresListenAddress, nodeName (initialPrimary), nodeName (expectedMember).
const daemonNodeConfig = `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: %s
  role: data
  apiAddress: 0.0.0.0:8080
postgres:
  dataDir: /var/lib/postgresql/data
  listenAddress: %s
  port: 5432
bootstrap:
  clusterName: alpha
  initialPrimary: %s
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - %s
`

// singleNodeDaemon holds runtime handles for a single-node pacmand integration environment.
type singleNodeDaemon struct {
	Base     string
	Postgres *testenv.Postgres
	Client   *http.Client
}

// startSingleNodeDaemon spins up a postgres container and a single-node pacmand daemon.
// The service name prefix is derived from t.Name() so Docker container names remain unique
// across parallel test runs.
func startSingleNodeDaemon(t *testing.T, nodeName string) singleNodeDaemon {
	t.Helper()

	env := testenv.New(t)
	prefix := sanitizeIntegrationName(t.Name())
	pg := env.StartPostgres(t, prefix, prefix+"-postgres")

	configBody := fmt.Sprintf(daemonNodeConfig, nodeName, pg.Alias(), nodeName, nodeName)

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         prefix + "-service",
		Image:        pacmanTestImage(),
		Aliases:      []string{prefix + "-service"},
		Env:          postgresConnectionEnv(pg),
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/sh",
			"-lc",
			fmt.Sprintf("pacmand -config %s", daemonConfigPath),
		},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(pacmandStartupTimeout),
	})

	return singleNodeDaemon{
		Base:     "http://" + service.Address(t, "8080"),
		Postgres: pg,
		Client:   &http.Client{Timeout: 2 * time.Second},
	}
}

// pacmanTestImage returns the Docker image to use for pacmand, defaulting to
// "pacman-test:local" when PACMAN_TEST_IMAGE is not set.
func pacmanTestImage() string {
	if image := strings.TrimSpace(os.Getenv("PACMAN_TEST_IMAGE")); image != "" {
		return image
	}

	return "pacman-test:local"
}

// postgresConnectionEnv builds the PGDATABASE / PGUSER / PGPASSWORD environment
// variables that pacmand expects to connect to postgres.
func postgresConnectionEnv(postgres *testenv.Postgres) map[string]string {
	return map[string]string{
		"PGDATABASE": postgres.Database(),
		"PGUSER":     postgres.Username(),
		"PGPASSWORD": postgres.Password(),
	}
}

// waitForProbeStatus polls url until it returns wantStatus or the timeout expires,
// sleeping 200 ms between attempts.
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
