//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/test/testenv"
)

// ---------------------------------------------------------------------------
// Config templates
// ---------------------------------------------------------------------------

// daemonEtcdSingleNodeConfig is a single-member pacmand config using an
// external etcd DCS backend.  Args: nodeName, postgresAlias, etcdAlias,
// initialPrimary, expectedMember.
const daemonEtcdSingleNodeConfig = `
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
dcs:
  backend: etcd
  clusterName: alpha
  etcd:
    endpoints:
      - http://%s:2379
bootstrap:
  clusterName: alpha
  initialPrimary: %s
  expectedMembers:
    - %s
`

// daemonEtcdTwoNodeConfig is a two-member pacmand config using an external
// etcd DCS backend.  Args: nodeName, postgresAlias, etcdAlias, initialPrimary,
// member1, member2.
const daemonEtcdTwoNodeConfig = `
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
dcs:
  backend: etcd
  clusterName: alpha
  etcd:
    endpoints:
      - http://%s:2379
bootstrap:
  clusterName: alpha
  initialPrimary: %s
  expectedMembers:
    - %s
    - %s
`

// ---------------------------------------------------------------------------
// Shared topology types and helpers
// ---------------------------------------------------------------------------

const (
	topologyStartupTimeout = 45 * time.Second

	topologyPGPostgresSuffix = "-pg-postgres"
	topologyClusterAPI       = "/api/v1/cluster"
	topologyMembersAPI       = "/api/v1/members"
	topologyMaintenanceAPI   = "/api/v1/maintenance"
	topologyValidateConfig   = "validate config"
	topologyContentType      = "Content-Type"
	topologyApplicationJSON  = "application/json"
)

// topologyEtcd holds a running etcd service alias for use in daemon configs.
type topologyEtcd struct {
	Alias string
}

// startTopologyEtcd starts a minimal single-node etcd container on the shared
// test network and returns its alias so daemon configs can reference it.
func startTopologyEtcd(t *testing.T, env *testenv.Environment, alias string) topologyEtcd {
	t.Helper()

	env.StartService(t, testenv.ServiceConfig{
		Name:       alias,
		Image:      testEtcdImage,
		Aliases:    []string{alias},
		Entrypoint: []string{"etcd"},
		Cmd: []string{
			"--name=default",
			"--data-dir=/etcd-data",
			"--listen-client-urls=http://0.0.0.0:2379",
			fmt.Sprintf("--advertise-client-urls=http://%s:2379", alias),
			"--listen-peer-urls=http://0.0.0.0:2380",
			fmt.Sprintf("--initial-advertise-peer-urls=http://%s:2380", alias),
			fmt.Sprintf("--initial-cluster=default=http://%s:2380", alias),
		},
		WaitStrategy: wait.ForHTTP("/health").
			WithPort("2379/tcp").
			WithStartupTimeout(60 * time.Second),
	})

	return topologyEtcd{Alias: alias}
}

// clusterTopologyNode holds a running pacmand service and its HTTP base URL.
type clusterTopologyNode struct {
	Base   string
	Client *http.Client
}

// startEtcdBackedDaemonNode starts a pacmand service with the given config and
// waits for its /health probe to return 200.
func startEtcdBackedDaemonNode(t *testing.T, env *testenv.Environment, serviceName, configBody string) clusterTopologyNode {
	t.Helper()

	testenv.RequireLocalImage(t, pacmanTestImage())
	pg := env.StartPostgres(t, serviceName+"-pg", serviceName+"-pg-postgres")

	svc := env.StartService(t, testenv.ServiceConfig{
		Name:         serviceName,
		Image:        pacmanTestImage(),
		Aliases:      []string{serviceName},
		Env:          postgresConnectionEnv(pg),
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/sh", "-lc",
			fmt.Sprintf("pacmand -config %s", daemonConfigPath),
		},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(topologyStartupTimeout),
	})

	base := "http://" + svc.Address(t, "8080")
	client := &http.Client{Timeout: 3 * time.Second}
	waitForProbeStatus(t, client, base+"/health", http.StatusOK, topologyStartupTimeout)

	return clusterTopologyNode{Base: base, Client: client}
}

// clusterJSON GETs url, asserts HTTP 200, and JSON-decodes the body into v.
func clusterJSON(t *testing.T, client *http.Client, url string, v any) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body from GET %s: %v", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, body: %s", url, resp.StatusCode, body)
	}

	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode JSON from GET %s: %v\nbody: %s", url, err, body)
	}
}

// waitForTopologyMemberCount polls /api/v1/members until at least wantCount
// members appear or the 30-second deadline expires.
func waitForTopologyMemberCount(t *testing.T, client *http.Client, base string, wantCount int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/api/v1/members")
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var payload struct {
			Members []struct {
				Member string `json:"member"`
			} `json:"members"`
		}
		if json.Unmarshal(body, &payload) == nil && len(payload.Members) >= wantCount {
			return
		}

		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("did not observe %d member(s) in /api/v1/members before deadline", wantCount)
}

// waitForTopologyMaintenanceState polls /api/v1/maintenance until the enabled
// flag matches wantEnabled or the 20-second deadline expires.
func waitForTopologyMaintenanceState(t *testing.T, client *http.Client, base string, wantEnabled bool) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/api/v1/maintenance")
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var payload struct {
			Enabled bool `json:"enabled"`
		}
		if json.Unmarshal(body, &payload) == nil && payload.Enabled == wantEnabled {
			return
		}

		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("maintenance enabled=%v not observed before deadline", wantEnabled)
}
