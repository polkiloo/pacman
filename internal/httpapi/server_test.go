package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestServerStartServesAndShutsDown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New("alpha-1", testNodeStatusStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})
	addr := reserveLoopbackAddress(t)

	if err := srv.Start(ctx, addr); err != nil {
		t.Fatalf("start server: %v", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	resp, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		t.Fatalf("call health endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected health status: got %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	cancel()

	if err := srv.Wait(); err != nil {
		t.Fatalf("wait for server shutdown: %v", err)
	}
}

func TestHealthReturnsPrimaryStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	status := agentmodel.NodeStatus{
		NodeName:   "alpha-1",
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		ObservedAt: now,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				ServerVersion:    170002,
				SystemIdentifier: "7599025879359099984",
				Timeline:         1,
			},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus:  status,
		hasNode:     true,
		clusterSpec: cluster.ClusterSpec{ClusterName: "alpha"},
		hasSpec:     true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{}), "/health")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.State != "running" {
		t.Fatalf("unexpected state: got %q", body.State)
	}

	if body.Role != "primary" {
		t.Fatalf("unexpected role: got %q", body.Role)
	}

	if body.Patroni.Name != "alpha-1" {
		t.Fatalf("unexpected patroni name: got %q", body.Patroni.Name)
	}

	if body.Patroni.Scope != "alpha" {
		t.Fatalf("unexpected patroni scope: got %q", body.Patroni.Scope)
	}

	if body.ServerVersion != 170002 {
		t.Fatalf("unexpected server version: got %d", body.ServerVersion)
	}
}

func TestLivenessReturnsServiceUnavailableForStaleHeartbeat(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: agentmodel.NodeStatus{
			NodeName:   "alpha-1",
			ObservedAt: time.Now().Add(-time.Minute),
			Postgres: agentmodel.PostgresStatus{
				Managed: true,
				Up:      true,
			},
		},
		hasNode: true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{LivenessWindow: 5 * time.Second}), "/liveness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status code: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestReadinessHonorsReplicaLagThreshold(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: agentmodel.NodeStatus{
			NodeName:   "alpha-1",
			Role:       cluster.MemberRoleReplica,
			State:      cluster.MemberStateStreaming,
			ObservedAt: time.Now().UTC(),
			Postgres: agentmodel.PostgresStatus{
				Managed: true,
				Up:      true,
				Role:    cluster.MemberRoleReplica,
				Details: agentmodel.PostgresDetails{
					ReplicationLagBytes: 64,
				},
			},
		},
		hasNode: true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})

	response := performRequest(t, srv, "/readiness?lag=32B")
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status code for strict lag: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	response = performRequest(t, srv, "/readiness?lag=128B&mode=apply")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code for relaxed lag: got %d, want %d", response.StatusCode, http.StatusOK)
	}
}

type testNodeStatusStore struct {
	nodeStatus  agentmodel.NodeStatus
	hasNode     bool
	clusterSpec cluster.ClusterSpec
	hasSpec     bool
	maintenance cluster.MaintenanceModeStatus
}

func (store testNodeStatusStore) NodeStatus(nodeName string) (agentmodel.NodeStatus, bool) {
	if !store.hasNode || nodeName != store.nodeStatus.NodeName {
		return agentmodel.NodeStatus{}, false
	}

	return store.nodeStatus.Clone(), true
}

func (store testNodeStatusStore) ClusterSpec() (cluster.ClusterSpec, bool) {
	if !store.hasSpec {
		return cluster.ClusterSpec{}, false
	}

	return store.clusterSpec.Clone(), true
}

func (store testNodeStatusStore) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return store.maintenance
}

func performRequest(t *testing.T, srv *Server, path string) *http.Response {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, path, nil)
	response, err := srv.app.Test(request, int(time.Second.Milliseconds()))
	if err != nil {
		t.Fatalf("perform request %q: %v", path, err)
	}

	return response
}

func decodeJSONResponse(t *testing.T, response *http.Response, target any) {
	t.Helper()
	defer response.Body.Close()

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}

	return address
}
