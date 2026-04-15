package httpapi_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/observability"
)

func TestMetricsEndpointReturnsPrometheusText(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 13, 0, 0, 0, time.UTC)
	store := metricsTestStore{
		nodeStatuses: []agentmodel.NodeStatus{
			{
				NodeName:   "alpha-1",
				MemberName: "alpha-1",
				Role:       cluster.MemberRolePrimary,
				State:      cluster.MemberStateRunning,
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					CheckedAt:     now,
					Up:            true,
					Role:          cluster.MemberRolePrimary,
					RecoveryKnown: true,
					InRecovery:    false,
					Details: agentmodel.PostgresDetails{
						ServerVersion:       170002,
						Timeline:            3,
						ReplicationLagBytes: 0,
					},
				},
				ControlPlane: agentmodel.ControlPlaneStatus{
					ClusterReachable: true,
					Leader:           true,
					LastHeartbeatAt:  now,
					LastDCSSeenAt:    now,
				},
				ObservedAt: now,
			},
		},
		clusterSpec: cluster.ClusterSpec{
			ClusterName: "alpha",
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
			},
		},
		hasSpec: true,
		clusterStatus: cluster.ClusterStatus{
			ClusterName:    "alpha",
			Phase:          cluster.ClusterPhaseHealthy,
			CurrentPrimary: "alpha-1",
			CurrentEpoch:   2,
			Members: []cluster.MemberStatus{
				{
					Name:       "alpha-1",
					Role:       cluster.MemberRolePrimary,
					State:      cluster.MemberStateRunning,
					Healthy:    true,
					Leader:     true,
					Timeline:   3,
					LastSeenAt: now,
				},
			},
			ObservedAt: now,
		},
		hasClusterStatus: true,
		traceCounts: []cluster.OperationTraceCount{
			{Kind: cluster.OperationKindSwitchover, State: cluster.OperationStateAccepted, Count: 1},
			{Kind: cluster.OperationKindSwitchover, State: cluster.OperationStateCompleted, Count: 1},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := reserveLoopbackAddress(t)
	server := httpapi.New("alpha-1", store, slog.New(slog.NewTextHandler(io.Discard, nil)), httpapi.Config{
		Middlewares: []httpapi.Middleware{
			observability.PrometheusExporterMiddleware(store),
		},
	})
	if err := server.Start(ctx, addr); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() {
		cancel()
		if err := server.Wait(); err != nil {
			t.Fatalf("wait server: %v", err)
		}
	}()

	waitForHTTPServer(t, "http://"+addr+"/metrics")

	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain;") {
		t.Fatalf("unexpected metrics content type: got %q", got)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read metrics response: %v", err)
	}

	text := string(body)
	for _, want := range []string{
		"pacman_cluster_current_epoch 2",
		`pacman_cluster_phase{phase="healthy"} 1`,
		`pacman_cluster_primary{member="alpha-1"} 1`,
		`pacman_member_info{member="alpha-1",role="primary",state="running"} 1`,
		`pacman_node_postgres_up{node="alpha-1"} 1`,
		`pacman_controlplane_operation_transitions_total{kind="switchover",state="completed"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected metrics body to contain %q, got:\n%s", want, text)
		}
	}
}

type metricsTestStore struct {
	nodeStatuses      []agentmodel.NodeStatus
	clusterSpec       cluster.ClusterSpec
	hasSpec           bool
	clusterStatus     cluster.ClusterStatus
	hasClusterStatus  bool
	maintenanceStatus cluster.MaintenanceModeStatus
	traceCounts       []cluster.OperationTraceCount
}

func (store metricsTestStore) NodeStatus(nodeName string) (agentmodel.NodeStatus, bool) {
	for _, status := range store.nodeStatuses {
		if status.NodeName == nodeName {
			return status.Clone(), true
		}
	}

	return agentmodel.NodeStatus{}, false
}

func (store metricsTestStore) NodeStatuses() []agentmodel.NodeStatus {
	items := make([]agentmodel.NodeStatus, len(store.nodeStatuses))
	for i, status := range store.nodeStatuses {
		items[i] = status.Clone()
	}

	return items
}

func (store metricsTestStore) ClusterSpec() (cluster.ClusterSpec, bool) {
	if !store.hasSpec {
		return cluster.ClusterSpec{}, false
	}

	return store.clusterSpec.Clone(), true
}

func (store metricsTestStore) ClusterStatus() (cluster.ClusterStatus, bool) {
	if !store.hasClusterStatus {
		return cluster.ClusterStatus{}, false
	}

	return store.clusterStatus.Clone(), true
}

func (store metricsTestStore) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return store.maintenanceStatus
}

func (store metricsTestStore) UpdateMaintenanceMode(context.Context, cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	return cluster.MaintenanceModeStatus{}, nil
}

func (store metricsTestStore) History() []cluster.HistoryEntry {
	return nil
}

func (store metricsTestStore) CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error) {
	return controlplane.SwitchoverIntent{}, nil
}

func (store metricsTestStore) CancelSwitchover(context.Context) (cluster.Operation, error) {
	return cluster.Operation{}, nil
}

func (store metricsTestStore) CreateFailoverIntent(context.Context, controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error) {
	return controlplane.FailoverIntent{}, nil
}

func (store metricsTestStore) OperationTraceCounts() []cluster.OperationTraceCount {
	items := make([]cluster.OperationTraceCount, len(store.traceCounts))
	copy(items, store.traceCounts)
	return items
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

func waitForHTTPServer(t *testing.T, rawURL string) {
	t.Helper()

	client := &http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(rawURL)
		if err == nil {
			response.Body.Close()
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("http server at %q did not become ready", rawURL)
}
