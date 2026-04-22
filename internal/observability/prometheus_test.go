package observability

import (
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestEncodePrometheusTextIncludesClusterNodeAndTraceMetrics(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)
	store := testStateReader{
		spec: cluster.ClusterSpec{
			ClusterName: "alpha",
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2"},
			},
		},
		hasSpec: true,
		status: cluster.ClusterStatus{
			ClusterName:    "alpha",
			Phase:          cluster.ClusterPhaseHealthy,
			CurrentPrimary: "alpha-1",
			CurrentEpoch:   3,
			Maintenance:    cluster.MaintenanceModeStatus{},
			ActiveOperation: &cluster.Operation{
				ID:          "sw-1",
				Kind:        cluster.OperationKindSwitchover,
				State:       cluster.OperationStateRunning,
				RequestedAt: now,
				Result:      cluster.OperationResultPending,
			},
			Members: []cluster.MemberStatus{
				{
					Name:       "alpha-1",
					Role:       cluster.MemberRolePrimary,
					State:      cluster.MemberStateRunning,
					Healthy:    true,
					Leader:     true,
					Timeline:   4,
					LagBytes:   0,
					LastSeenAt: now,
				},
				{
					Name:        "alpha-2",
					Role:        cluster.MemberRoleReplica,
					State:       cluster.MemberStateStreaming,
					Healthy:     true,
					Leader:      false,
					Timeline:    4,
					LagBytes:    128,
					NeedsRejoin: true,
					LastSeenAt:  now,
				},
			},
			ObservedAt: now,
		},
		hasStatus: true,
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
						Timeline:            4,
						DatabaseSizeBytes:   1048576,
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
		traceCounts: []cluster.OperationTraceCount{
			{Kind: cluster.OperationKindSwitchover, State: cluster.OperationStateAccepted, Count: 1},
			{Kind: cluster.OperationKindSwitchover, State: cluster.OperationStateRunning, Count: 1},
			{Kind: cluster.OperationKindSwitchover, State: cluster.OperationStateCompleted, Count: 1},
		},
	}

	body, contentType, err := EncodePrometheusText(NewPrometheusGatherer(store))
	if err != nil {
		t.Fatalf("encode metrics: %v", err)
	}

	if !strings.HasPrefix(contentType, "text/plain;") {
		t.Fatalf("unexpected content type: %q", contentType)
	}

	text := string(body)
	for _, want := range []string{
		"pacman_cluster_spec_members_desired 2",
		"pacman_cluster_members_observed 2",
		`pacman_cluster_phase{phase="healthy"} 1`,
		`pacman_cluster_primary{member="alpha-1"} 1`,
		`pacman_cluster_operation_active{kind="switchover",state="running"} 1`,
		`pacman_member_info{member="alpha-1",role="primary",state="running"} 1`,
		`pacman_member_needs_rejoin{member="alpha-2"} 1`,
		`pacman_node_info{member="alpha-1",node="alpha-1",role="primary",state="running"} 1`,
		`pacman_node_postgres_up{node="alpha-1"} 1`,
		`pacman_node_postgres_database_size_bytes{node="alpha-1"} 1.048576e+06`,
		`pacman_node_controlplane_leader{node="alpha-1"} 1`,
		`pacman_controlplane_operation_transitions_total{kind="switchover",state="completed"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", want, text)
		}
	}
}

type testStateReader struct {
	nodeStatuses []agentmodel.NodeStatus
	spec         cluster.ClusterSpec
	hasSpec      bool
	status       cluster.ClusterStatus
	hasStatus    bool
	maintenance  cluster.MaintenanceModeStatus
	traceCounts  []cluster.OperationTraceCount
}

func (reader testStateReader) NodeStatuses() []agentmodel.NodeStatus {
	items := make([]agentmodel.NodeStatus, len(reader.nodeStatuses))
	for i, status := range reader.nodeStatuses {
		items[i] = status.Clone()
	}

	return items
}

func (reader testStateReader) ClusterSpec() (cluster.ClusterSpec, bool) {
	if !reader.hasSpec {
		return cluster.ClusterSpec{}, false
	}

	return reader.spec.Clone(), true
}

func (reader testStateReader) ClusterStatus() (cluster.ClusterStatus, bool) {
	if !reader.hasStatus {
		return cluster.ClusterStatus{}, false
	}

	return reader.status.Clone(), true
}

func (reader testStateReader) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return reader.maintenance
}

func (reader testStateReader) OperationTraceCounts() []cluster.OperationTraceCount {
	items := make([]cluster.OperationTraceCount, len(reader.traceCounts))
	copy(items, reader.traceCounts)
	return items
}
