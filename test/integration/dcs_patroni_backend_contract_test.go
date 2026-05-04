//go:build integration

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/test/testenv"
)

const (
	testConsulImage    = "hashicorp/consul:1.20"
	testZooKeeperImage = "zookeeper:3.9.3"
	testPythonImage    = "python:3.12-alpine"
)

// TestPatroniDCSBackendContractsInTestcontainers covers the Patroni DCS
// backend matrix against live testcontainer fixtures. PACMAN currently runs
// etcd/etcd3 and Raft migrations, while the remaining Patroni DCS backends
// must fail with an explicit unsupported-backend error.
func TestPatroniDCSBackendContractsInTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed Patroni DCS backend contract tests in short mode")
	}

	t.Run("etcd", func(t *testing.T) {
		env := testenv.New(t)
		etcd := startTopologyEtcd(t, env, "patroni-dcs-etcd")
		serviceName := "patroni-dcs-etcd-node"

		node := startEtcdBackedDaemonNode(t, env, serviceName, patroniDCSContractEtcdConfig(
			"etcd",
			etcd.Alias,
			serviceName+topologyPGPostgresSuffix,
		))
		waitForTopologyCurrentPrimary(t, node.Client, node.Base, "postgresql0")
		assertPatroniDCSContractCluster(t, node)
	})

	t.Run("etcd3", func(t *testing.T) {
		env := testenv.New(t)
		etcd := startTopologyEtcd(t, env, "patroni-dcs-etcd3")
		serviceName := "patroni-dcs-etcd3-node"

		node := startEtcdBackedDaemonNode(t, env, serviceName, patroniDCSContractEtcdConfig(
			"etcd3",
			etcd.Alias,
			serviceName+topologyPGPostgresSuffix,
		))
		waitForTopologyCurrentPrimary(t, node.Client, node.Base, "postgresql0")
		assertPatroniDCSContractCluster(t, node)
	})

	t.Run("raft", func(t *testing.T) {
		env := testenv.New(t)
		serviceName := "patroni-dcs-raft-node"

		node := startEtcdBackedDaemonNode(t, env, serviceName, patroniDCSContractRaftConfig(
			serviceName+topologyPGPostgresSuffix,
		))
		waitForTopologyCurrentPrimary(t, node.Client, node.Base, "postgresql0")
		assertPatroniDCSContractCluster(t, node)
	})

	t.Run("consul", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniConsulFixture(t, env, "patroni-dcs-consul")

		assertPatroniUnsupportedDCSBackend(t, env, "consul", `
consul:
  host: patroni-dcs-consul:8500
  register_service: true
`)
	})

	t.Run("zookeeper", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniZooKeeperFixture(t, env, "patroni-dcs-zookeeper")

		assertPatroniUnsupportedDCSBackend(t, env, "zookeeper", `
zookeeper:
  hosts:
    - patroni-dcs-zookeeper:2181
`)
	})

	t.Run("exhibitor", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniHTTPFixture(t, env, "patroni-dcs-exhibitor", patroniExhibitorFixtureSource)

		assertPatroniUnsupportedDCSBackend(t, env, "exhibitor", `
exhibitor:
  hosts:
    - patroni-dcs-exhibitor
  port: 8000
`)
	})

	t.Run("kubernetes", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniHTTPFixture(t, env, "patroni-dcs-kubernetes", patroniKubernetesFixtureSource)

		assertPatroniUnsupportedDCSBackend(t, env, "kubernetes", `
kubernetes:
  namespace: default
  use_endpoints: true
`)
	})
}

func patroniDCSContractEtcdConfig(backend, etcdAlias, postgresAlias string) string {
	return fmt.Sprintf(`
scope: patroni-dcs-contract
name: postgresql0
restapi:
  listen: 0.0.0.0:8080
%s:
  host: %s:2379
bootstrap:
  dcs:
    ttl: 5
    retry_timeout: 5
postgresql:
  listen: %s:5432
  data_dir: /var/lib/postgresql/data
`, backend, etcdAlias, postgresAlias)
}

func patroniDCSContractRaftConfig(postgresAlias string) string {
	return fmt.Sprintf(`
scope: patroni-dcs-contract
name: postgresql0
restapi:
  listen: 0.0.0.0:8080
raft:
  data_dir: /var/lib/pacman/raft
  self_addr: 0.0.0.0:7100
bootstrap:
  dcs:
    ttl: 5
    retry_timeout: 5
postgresql:
  listen: %s:5432
  data_dir: /var/lib/postgresql/data
`, postgresAlias)
}

func assertPatroniDCSContractCluster(t *testing.T, node clusterTopologyNode) {
	t.Helper()

	var cluster struct {
		ClusterName    string `json:"clusterName"`
		CurrentPrimary string `json:"currentPrimary"`
	}
	clusterJSON(t, node.Client, node.Base+topologyClusterAPI, &cluster)

	if cluster.ClusterName != "patroni-dcs-contract" {
		t.Fatalf("unexpected clusterName: got %q, want %q", cluster.ClusterName, "patroni-dcs-contract")
	}
	if cluster.CurrentPrimary != "postgresql0" {
		t.Fatalf("unexpected currentPrimary: got %q, want %q", cluster.CurrentPrimary, "postgresql0")
	}
}

func assertPatroniUnsupportedDCSBackend(t *testing.T, env *testenv.Environment, backend, dcsBlock string) {
	t.Helper()

	testenv.RequireLocalImage(t, pacmanTestImage())

	runner := startDaemonRunner(
		t,
		env,
		"patroni-dcs-"+backend+"-runner",
		patroniUnsupportedDCSConfig(dcsBlock),
		nil,
		nil,
	)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatalf("expected non-zero exit for unsupported Patroni DCS backend %q, got 0", backend)
	}
	if !strings.Contains(result.Output, "Patroni config DCS backend is unsupported") ||
		!strings.Contains(result.Output, backend) {
		t.Fatalf("expected unsupported %s backend error in output, got:\n%s", backend, result.Output)
	}
}

func patroniUnsupportedDCSConfig(dcsBlock string) string {
	return fmt.Sprintf(`
scope: patroni-dcs-contract
name: postgresql0
restapi:
  listen: 127.0.0.1:8008
%s
postgresql:
  listen: 127.0.0.1:5432
  data_dir: data/postgresql0
`, strings.TrimSpace(dcsBlock))
}

func startPatroniConsulFixture(t *testing.T, env *testenv.Environment, alias string) {
	t.Helper()

	env.StartService(t, testenv.ServiceConfig{
		Name:         alias,
		Image:        testConsulImage,
		Aliases:      []string{alias},
		Entrypoint:   []string{"consul"},
		Cmd:          []string{"agent", "-dev", "-client=0.0.0.0", "-bind=0.0.0.0"},
		ExposedPorts: []string{"8500/tcp"},
		WaitStrategy: wait.ForHTTP("/v1/status/leader").
			WithPort("8500/tcp").
			WithStartupTimeout(60 * time.Second),
	})
}

func startPatroniZooKeeperFixture(t *testing.T, env *testenv.Environment, alias string) {
	t.Helper()

	env.StartService(t, testenv.ServiceConfig{
		Name:         alias,
		Image:        testZooKeeperImage,
		Aliases:      []string{alias},
		ExposedPorts: []string{"2181/tcp"},
		WaitStrategy: wait.ForListeningPort("2181/tcp").
			WithStartupTimeout(60 * time.Second),
	})
}

func startPatroniHTTPFixture(t *testing.T, env *testenv.Environment, alias, source string) {
	t.Helper()

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         alias,
		Image:        testPythonImage,
		Aliases:      []string{alias},
		ExposedPorts: []string{"8000/tcp"},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(source),
				ContainerFilePath: "/fixture/server.py",
				FileMode:          0o755,
			},
		},
		Cmd: []string{"python", "-u", "/fixture/server.py"},
		WaitStrategy: wait.ForHTTP("/health").
			WithPort("8000/tcp").
			WithStartupTimeout(60 * time.Second),
	})

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + service.Address(t, "8000") + "/health")
	if err != nil {
		t.Fatalf("GET %s fixture health: %v", alias, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s fixture health returned %d, want 200", alias, resp.StatusCode)
	}
}

const patroniExhibitorFixtureSource = `
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path == "/exhibitor/v1/cluster/list":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"servers": ["patroni-dcs-zookeeper"], "port": 2181}).encode())
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_):
        return

ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
`

const patroniKubernetesFixtureSource = `
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path == "/version":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"major": "1", "minor": "30", "gitVersion": "v1.30.0"}).encode())
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_):
        return

ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
`
