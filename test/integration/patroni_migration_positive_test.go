//go:build integration

package integration_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/test/testenv"
)

// TestPatroniMigratedSingleNodeBootstrapsAndServesHealth verifies that a PACMAN
// config translated from Patroni postgres0.yml (scope→clusterName, etcd.host→
// dcs.etcd.endpoints with scheme, bootstrap.dcs.ttl/retry_timeout) boots
// successfully and passes its /health probe.
func TestPatroniMigratedSingleNodeBootstrapsAndServesHealth(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-compat-health")
	serviceName := patroniCompatNode0Name + "-health-svc"

	cfg := renderPatroniCompatConfig(t, patroniCompatNode0File, patroniCompatRuntimeOptions{
		etcdAlias:     etcd.Alias,
		postgresAlias: serviceName + topologyPGPostgresSuffix,
	})
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	resp, err := node.Client.Get(node.Base + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /health 200 on migrated config, got %d", resp.StatusCode)
	}
}

// TestPatroniMigratedClusterNameMatchesPatroniScope verifies that /api/v1/cluster
// reports clusterName equal to the Patroni scope value ("batman") — confirming
// the scope→dcs.clusterName and scope→bootstrap.clusterName mapping is correct.
func TestPatroniMigratedClusterNameMatchesPatroniScope(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-compat-scope")
	serviceName := patroniCompatNode0Name + "-scope-svc"

	cfg := renderPatroniCompatConfig(t, patroniCompatNode0File, patroniCompatRuntimeOptions{
		etcdAlias:     etcd.Alias,
		postgresAlias: serviceName + topologyPGPostgresSuffix,
	})
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	var cluster struct {
		ClusterName string `json:"clusterName"`
		Phase       string `json:"phase"`
	}
	clusterJSON(t, node.Client, node.Base+topologyClusterAPI, &cluster)

	if cluster.ClusterName != patroniCompatClusterName {
		t.Fatalf("expected clusterName=%q (Patroni scope), got %q",
			patroniCompatClusterName, cluster.ClusterName)
	}
	if cluster.Phase == "" {
		t.Fatal("expected non-empty cluster phase on migrated node")
	}
}

// TestPatroniMigratedMembershipExamplesPreserveInitialPrimaryAndExpectedMembers
// verifies that the shipped postgres0.yml and postgres1.yml compatibility
// examples preserve the same initial primary and expected member set after
// translation.
func TestPatroniMigratedMembershipExamplesPreserveInitialPrimaryAndExpectedMembers(t *testing.T) {
	cfg0 := loadPatroniCompatConfig(t, patroniCompatNode0File)
	cfg1 := loadPatroniCompatConfig(t, patroniCompatNode1File)

	if cfg0.Bootstrap == nil || cfg1.Bootstrap == nil {
		t.Fatal("expected bootstrap blocks in patroni compatibility examples")
	}
	if cfg0.Bootstrap.InitialPrimary != patroniCompatNode0Name || cfg1.Bootstrap.InitialPrimary != patroniCompatNode0Name {
		t.Fatalf("expected both compatibility examples to use %q as initialPrimary, got %q and %q",
			patroniCompatNode0Name, cfg0.Bootstrap.InitialPrimary, cfg1.Bootstrap.InitialPrimary)
	}

	wantMembers := []string{patroniCompatNode0Name, patroniCompatNode1Name, patroniCompatNode2Name}
	if strings.Join(cfg0.Bootstrap.ExpectedMembers, ",") != strings.Join(wantMembers, ",") {
		t.Fatalf("unexpected node0 expectedMembers: got %v, want %v", cfg0.Bootstrap.ExpectedMembers, wantMembers)
	}
	if strings.Join(cfg1.Bootstrap.ExpectedMembers, ",") != strings.Join(wantMembers, ",") {
		t.Fatalf("unexpected node1 expectedMembers: got %v, want %v", cfg1.Bootstrap.ExpectedMembers, wantMembers)
	}
}

// TestPatroniMigratedPrimaryProbeWorks verifies that the primary probe on a
// node started from a migrated config returns HTTP 200 once the node becomes
// primary.
func TestPatroniMigratedPrimaryProbeWorks(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-compat-probes")
	serviceName := patroniCompatNode0Name + "-probe-svc"

	cfg := renderPatroniCompatConfig(t, patroniCompatNode0File, patroniCompatRuntimeOptions{
		etcdAlias:     etcd.Alias,
		postgresAlias: serviceName + topologyPGPostgresSuffix,
	})
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	resp, err := node.Client.Get(node.Base + "/primary")
	if err != nil {
		t.Fatalf("GET /primary on migrated node: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /primary to return 200 on migrated config, got %d", resp.StatusCode)
	}
}

// TestPatroniMigratedMetricsEndpointServesClusterMetrics verifies that /metrics
// on a node started from a migrated config emits the expected cluster-level
// Prometheus metrics — confirming that Patroni DCS parameter translation
// (ttl, retry_timeout) does not break metric emission.
func TestPatroniMigratedMetricsEndpointServesClusterMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-compat-metrics")
	serviceName := patroniCompatNode0Name + "-metrics-svc"

	cfg := renderPatroniCompatConfig(t, patroniCompatNode0File, patroniCompatRuntimeOptions{
		etcdAlias:     etcd.Alias,
		postgresAlias: serviceName + topologyPGPostgresSuffix,
	})
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	resp, err := node.Client.Get(node.Base + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics on migrated node: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
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
			t.Fatalf("expected /metrics to contain %q on migrated config\n--- metrics ---\n%s", want, text)
		}
	}
}

// TestPatroniMigratedNode2RESTAuthVariantRequiresBearerToken verifies that the
// shipped postgres2.yml compatibility example preserves the Patroni REST-auth
// variant by translating it into PACMAN bearer-token authentication.
func TestPatroniMigratedNode2RESTAuthVariantRequiresBearerToken(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const bearerToken = "patroni-compat-token"

	env := testenv.New(t)
	etcd := startTopologyEtcd(t, env, "etcd-compat-auth")
	serviceName := patroniCompatNode2Name + "-auth-svc"

	cfg := renderPatroniCompatConfig(t, patroniCompatNode2File, patroniCompatRuntimeOptions{
		etcdAlias:        etcd.Alias,
		postgresAlias:    serviceName + topologyPGPostgresSuffix,
		adminBearerToken: bearerToken,
	})
	node := startEtcdBackedDaemonNode(t, env, serviceName, cfg)

	unauthorizedResp, err := node.Client.Get(node.Base + topologyClusterAPI)
	if err != nil {
		t.Fatalf("GET /api/v1/cluster without bearer token: %v", err)
	}
	io.Copy(io.Discard, unauthorizedResp.Body)
	unauthorizedResp.Body.Close()

	if unauthorizedResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected /api/v1/cluster without bearer token to return 401, got %d", unauthorizedResp.StatusCode)
	}

	req, err := http.NewRequest(http.MethodGet, node.Base+topologyClusterAPI, nil)
	if err != nil {
		t.Fatalf("build authorized cluster request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+bearerToken)

	authorizedResp, err := node.Client.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/cluster with bearer token: %v", err)
	}
	io.Copy(io.Discard, authorizedResp.Body)
	authorizedResp.Body.Close()

	if authorizedResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /api/v1/cluster with bearer token to return 200, got %d", authorizedResp.StatusCode)
	}
}
