//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/test/testenv"
)

// TestDaemonRejectsConfigWithUnknownDCSBackend verifies that pacmand exits
// with a non-zero status and a descriptive error when the DCS backend name is
// not one of the supported values (raft, etcd).
func TestDaemonRejectsConfigWithUnknownDCSBackend(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	cfg := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
dcs:
  backend: postgres
  clusterName: alpha
  etcd:
    endpoints:
      - http://etcd:2379
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-1
  expectedMembers:
    - alpha-1
`
	runner := startDaemonRunner(t, env, "invalid-backend-runner", cfg, nil, nil)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit for unknown DCS backend, got 0")
	}
	if !strings.Contains(result.Output, "dcs: backend is invalid") &&
		!strings.Contains(result.Output, topologyValidateConfig) {
		t.Fatalf("expected DCS backend error in output, got:\n%s", result.Output)
	}
}

// TestDaemonRejectsEtcdConfigWithNoEndpoints verifies that pacmand exits with
// a non-zero status and a descriptive message when the etcd backend is
// selected but no endpoints are provided.
func TestDaemonRejectsEtcdConfigWithNoEndpoints(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	cfg := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
dcs:
  backend: etcd
  clusterName: alpha
  etcd:
    endpoints: []
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-1
  expectedMembers:
    - alpha-1
`
	runner := startDaemonRunner(t, env, "no-etcd-endpoints-runner", cfg, nil, nil)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit for empty etcd endpoints, got 0")
	}
	if !strings.Contains(result.Output, "etcd endpoints") &&
		!strings.Contains(result.Output, topologyValidateConfig) {
		t.Fatalf("expected etcd endpoints error in output, got:\n%s", result.Output)
	}
}

// TestDaemonRejectsEtcdEndpointWithInvalidScheme verifies that a malformed
// endpoint (no URL scheme) causes an immediate startup failure — guarding
// against silent misconfiguration where the host is valid but the transport
// scheme is absent.
func TestDaemonRejectsEtcdEndpointWithInvalidScheme(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	cfg := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
dcs:
  backend: etcd
  clusterName: alpha
  etcd:
    endpoints:
      - etcd:2379
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-1
  expectedMembers:
    - alpha-1
`
	runner := startDaemonRunner(t, env, "bad-etcd-scheme-runner", cfg, nil, nil)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit for invalid etcd endpoint scheme, got 0")
	}
	if !strings.Contains(result.Output, "etcd") &&
		!strings.Contains(result.Output, topologyValidateConfig) {
		t.Fatalf("expected etcd config error in output, got:\n%s", result.Output)
	}
}

// TestSwitchoverApiRejectsRequestWhenNoEligibleStandbyExists verifies that
// POST /api/v1/operations/switchover returns HTTP 412 when the cluster has
// only one member.  A switchover to a non-existent target must never be
// silently accepted.
func TestSwitchoverApiRejectsRequestWhenNoEligibleStandbyExists(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-switchover-reject")

	const nodeName = "epsilon-1"
	cfg := fmt.Sprintf(daemonEtcdSingleNodeConfig,
		nodeName, nodeName+topologyPGPostgresSuffix, etcd.Alias, nodeName, nodeName,
	)
	node := startEtcdBackedDaemonNode(t, env, nodeName+"-svc", cfg)

	body := []byte(`{"candidate":"epsilon-2","reason":"topology-validation-test","requestedBy":"integration"}`)
	resp := performHTTPRequest(t, http.MethodPost, node.Base+"/api/v1/operations/switchover",
		body, map[string]string{topologyContentType: topologyApplicationJSON})

	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 for switchover to unknown member, got %d, body: %s",
			resp.StatusCode, respBody)
	}

	var errPayload struct{ Error string `json:"error"` }
	if err := json.Unmarshal(respBody, &errPayload); err != nil {
		t.Fatalf("decode switchover error payload: %v\nbody: %s", err, respBody)
	}
	if errPayload.Error == "" {
		t.Fatalf("expected non-empty error in switchover rejection, body: %s", respBody)
	}
}

// TestFailoverApiRejectsRequestWhenPrimaryIsHealthy verifies that POST
// /api/v1/operations/failover returns HTTP 412 when the primary is healthy.
// Allowing failover against a healthy primary is a split-brain risk and must
// always be blocked.
func TestFailoverApiRejectsRequestWhenPrimaryIsHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-failover-reject")

	const nodeName = "zeta-1"
	cfg := fmt.Sprintf(daemonEtcdSingleNodeConfig,
		nodeName, nodeName+topologyPGPostgresSuffix, etcd.Alias, nodeName, nodeName,
	)
	node := startEtcdBackedDaemonNode(t, env, nodeName+"-svc", cfg)

	body := []byte(`{"reason":"topology-validation-test","requestedBy":"integration"}`)
	resp := performHTTPRequest(t, http.MethodPost, node.Base+"/api/v1/operations/failover",
		body, map[string]string{topologyContentType: topologyApplicationJSON})

	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 for failover against healthy primary, got %d, body: %s",
			resp.StatusCode, respBody)
	}

	var errPayload struct{ Error string `json:"error"` }
	if err := json.Unmarshal(respBody, &errPayload); err != nil {
		t.Fatalf("decode failover error payload: %v\nbody: %s", err, respBody)
	}
	if errPayload.Error == "" {
		t.Fatalf("expected non-empty error in failover rejection, body: %s", respBody)
	}
}

// TestSwitchoverApiRejectsRequestDuringActiveMaintenance verifies that POST
// /api/v1/operations/switchover is rejected when maintenance mode is enabled.
// Operations must be blocked during maintenance to prevent concurrent
// destabilising changes.
func TestSwitchoverApiRejectsRequestDuringActiveMaintenance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-maintenance-blocks-switchover")

	const nodeName = "eta-1"
	cfg := fmt.Sprintf(daemonEtcdSingleNodeConfig,
		nodeName, nodeName+topologyPGPostgresSuffix, etcd.Alias, nodeName, nodeName,
	)
	node := startEtcdBackedDaemonNode(t, env, nodeName+"-svc", cfg)

	enableResp := performHTTPRequest(t, http.MethodPut, node.Base+"/api/v1/maintenance",
		[]byte(`{"enabled":true,"reason":"maintenance-blocks-test"}`),
		map[string]string{topologyContentType: topologyApplicationJSON})
	io.Copy(io.Discard, enableResp.Body)
	enableResp.Body.Close()

	if enableResp.StatusCode != http.StatusOK {
		t.Fatalf("enable maintenance: got status %d, want 200", enableResp.StatusCode)
	}

	t.Cleanup(func() {
		performHTTPRequest(t, http.MethodPut, node.Base+"/api/v1/maintenance",
			[]byte(`{"enabled":false}`), map[string]string{topologyContentType: topologyApplicationJSON})
	})

	body := []byte(`{"candidate":"eta-2","reason":"should-be-blocked","requestedBy":"integration"}`)
	resp := performHTTPRequest(t, http.MethodPost, node.Base+"/api/v1/operations/switchover",
		body, map[string]string{topologyContentType: topologyApplicationJSON})

	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusAccepted {
		t.Fatalf("expected switchover to be rejected during maintenance, got %d, body: %s",
			resp.StatusCode, respBody)
	}
}
