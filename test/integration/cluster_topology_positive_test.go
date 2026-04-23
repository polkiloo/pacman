//go:build integration

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/test/testenv"
)

// TestEtcdBackedDaemonBootstrapsAndServesHealth verifies that a single pacmand
// node configured with an external etcd DCS backend starts successfully,
// passes its /health probe, and reports the correct cluster name in
// /api/v1/cluster.
func TestEtcdBackedDaemonBootstrapsAndServesHealth(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const nodeName = "alpha-1"

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-single-health")
	serviceName := "alpha-1-health"

	cfg := fmt.Sprintf(daemonEtcdSingleNodeConfig,
		nodeName, serviceName+topologyPGPostgresSuffix, etcd.Alias, nodeName, nodeName,
	)
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	var cluster struct {
		ClusterName string `json:"clusterName"`
		Phase       string `json:"phase"`
	}
	clusterJSON(t, node.Client, node.Base+topologyClusterAPI, &cluster)

	if cluster.ClusterName != "alpha" {
		t.Fatalf("expected clusterName=alpha, got %q", cluster.ClusterName)
	}
	if cluster.Phase == "" {
		t.Fatal("expected non-empty cluster phase")
	}
}

// TestEtcdBackedDaemonReportsCorrectMemberName verifies that the daemon
// registers itself under the configured member name and that /api/v1/members
// returns it with the primary role.
func TestEtcdBackedDaemonReportsCorrectMemberName(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const nodeName = "alpha-member-1"

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-single-member")
	serviceName := nodeName + "-svc"

	cfg := fmt.Sprintf(daemonEtcdSingleNodeConfig,
		nodeName, serviceName+topologyPGPostgresSuffix, etcd.Alias, nodeName, nodeName,
	)
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	waitForTopologyMemberCount(t, node.Client, node.Base, 1)

	var payload struct {
		Items []struct {
			Name string `json:"name"`
			Role string `json:"role"`
		} `json:"items"`
	}
	clusterJSON(t, node.Client, node.Base+topologyMembersAPI, &payload)

	if len(payload.Items) != 1 {
		t.Fatalf("expected 1 member, got %d: %+v", len(payload.Items), payload.Items)
	}
	if payload.Items[0].Name != nodeName {
		t.Fatalf("expected member name %q, got %q", nodeName, payload.Items[0].Name)
	}
	if payload.Items[0].Role != "primary" {
		t.Fatalf("expected primary role for only member, got %q", payload.Items[0].Role)
	}
}

// TestEtcdBackedTwoNodeTopologySharesClusterSpec verifies that two pacmand
// nodes sharing the same etcd backend both report the same cluster name and
// see both expected members — confirming the cluster spec is read from the
// shared DCS rather than per-process memory.
func TestEtcdBackedTwoNodeTopologySharesClusterSpec(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const (
		node1Name = "beta-1"
		node2Name = "beta-2"
		etcdAlias = "etcd-two-node"
	)

	env := testenv.New(t)
	startTopologyEtcd(t, env, etcdAlias)
	service1Name := node1Name + "-svc"
	service2Name := node2Name + "-svc"

	cfg1 := fmt.Sprintf(daemonEtcdTwoNodeConfig,
		node1Name, service1Name+topologyPGPostgresSuffix, etcdAlias, node1Name, node1Name, node2Name,
	)
	cfg2 := fmt.Sprintf(daemonEtcdTwoNodeConfig,
		node2Name, service2Name+topologyPGPostgresSuffix, etcdAlias, node1Name, node1Name, node2Name,
	)

	node1 := startEtcdBackedDaemonNode(t, env, service1Name, cfg1)
	node2 := startEtcdBackedDaemonNode(t, env, service2Name, cfg2)

	waitForTopologyMemberCount(t, node1.Client, node1.Base, 2)
	waitForTopologyMemberCount(t, node2.Client, node2.Base, 2)

	var spec1 struct {
		ClusterName string `json:"clusterName"`
	}
	var spec2 struct {
		ClusterName string `json:"clusterName"`
	}
	clusterJSON(t, node1.Client, node1.Base+topologyClusterAPI, &spec1)
	clusterJSON(t, node2.Client, node2.Base+topologyClusterAPI, &spec2)

	if spec1.ClusterName != "alpha" || spec2.ClusterName != "alpha" {
		t.Fatalf("expected both nodes to report clusterName=alpha, got %q and %q",
			spec1.ClusterName, spec2.ClusterName)
	}
}

// TestEtcdBackedMaintenanceModeIsVisibleAcrossNodes verifies that maintenance
// mode enabled on one etcd-backed node is visible on the other node — the
// shared DCS path that blocks concurrent destabilising operations.
func TestEtcdBackedMaintenanceModeIsVisibleAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const (
		node1Name = "gamma-1"
		node2Name = "gamma-2"
		etcdAlias = "etcd-maintenance"
	)

	env := testenv.New(t)
	startTopologyEtcd(t, env, etcdAlias)
	service1Name := node1Name + "-svc"
	service2Name := node2Name + "-svc"

	cfg1 := fmt.Sprintf(daemonEtcdTwoNodeConfig,
		node1Name, service1Name+topologyPGPostgresSuffix, etcdAlias, node1Name, node1Name, node2Name,
	)
	cfg2 := fmt.Sprintf(daemonEtcdTwoNodeConfig,
		node2Name, service2Name+topologyPGPostgresSuffix, etcdAlias, node1Name, node1Name, node2Name,
	)

	node1 := startEtcdBackedDaemonNode(t, env, service1Name, cfg1)
	node2 := startEtcdBackedDaemonNode(t, env, service2Name, cfg2)

	req := performHTTPRequest(t, http.MethodPut, node1.Base+topologyMaintenanceAPI,
		[]byte(`{"enabled":true,"reason":"cross-node-test"}`),
		map[string]string{topologyContentType: topologyApplicationJSON},
	)
	io.Copy(io.Discard, req.Body)
	req.Body.Close()

	if req.StatusCode != http.StatusOK {
		t.Fatalf("enable maintenance via node-1: got status %d, want 200", req.StatusCode)
	}

	t.Cleanup(func() {
		performHTTPRequest(t, http.MethodPut, node1.Base+topologyMaintenanceAPI,
			[]byte(`{"enabled":false}`),
			map[string]string{topologyContentType: topologyApplicationJSON},
		)
	})

	waitForTopologyMaintenanceState(t, node2.Client, node2.Base, true)
}

// TestEtcdBackedDaemonMetricsReflectClusterState verifies that /metrics on an
// etcd-backed node emits the cluster-level Prometheus metrics populated from
// the shared DCS store.
func TestEtcdBackedDaemonMetricsReflectClusterState(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const nodeName = "delta-1"

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-metrics")
	serviceName := nodeName + "-svc"

	cfg := fmt.Sprintf(daemonEtcdSingleNodeConfig,
		nodeName, serviceName+topologyPGPostgresSuffix, etcd.Alias, nodeName, nodeName,
	)
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	resp, err := node.Client.Get(node.Base + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d, body: %s", resp.StatusCode, body)
	}

	text := string(body)
	for _, want := range []string{
		"pacman_cluster_spec_members_desired",
		"pacman_cluster_members_observed",
		"pacman_cluster_phase",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected /metrics to contain %q\n--- metrics ---\n%s", want, text)
		}
	}
}
