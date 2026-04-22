package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreClusterStatusReturnsMissingBeforeDesiredState(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	if _, ok := store.ClusterStatus(); ok {
		t.Fatal("expected cluster status to be missing before desired state exists")
	}

	if got := store.MaintenanceStatus(); got != (cluster.MaintenanceModeStatus{}) {
		t.Fatalf("expected zero maintenance status before desired state exists, got %+v", got)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected active operation to be missing before journaling starts")
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected empty history before journaling starts, got %+v", history)
	}
}

func TestMemoryStateStoreAggregatesObservedClusterStatus(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	now := time.Date(2026, time.March, 27, 11, 0, 0, 0, time.UTC)
	setTestNow(store, func() time.Time { return now })

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{
				Name:       "alpha-1",
				Priority:   100,
				NoFailover: true,
				Tags: map[string]any{
					"zone": "desired-a",
				},
			},
			{
				Name:     "alpha-2",
				Priority: 20,
				Tags: map[string]any{
					"zone": "desired-b",
				},
			},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	for _, registration := range []MemberRegistration{
		{
			NodeName:       "alpha-1",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.10:8080",
			ControlAddress: "10.0.0.10:9090",
			RegisteredAt:   now.Add(-time.Minute),
		},
		{
			NodeName:       "alpha-2",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.11:8080",
			ControlAddress: "10.0.0.11:9090",
			RegisteredAt:   now.Add(-time.Minute),
		},
	} {
		if err := store.RegisterMember(context.Background(), registration); err != nil {
			t.Fatalf("register %q: %v", registration.NodeName, err)
		}
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Role:     cluster.MemberRolePrimary,
		State:    cluster.MemberStateRunning,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline:            7,
				ReplicationLagBytes: 0,
			},
			WAL: agentmodel.WALProgress{
				FlushLSN: "0/7000200",
			},
		},
		ObservedAt: now,
	}); err != nil {
		t.Fatalf("publish primary node status: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-2",
		Role:     cluster.MemberRoleReplica,
		State:    cluster.MemberStateStreaming,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline:            7,
				ReplicationLagBytes: 128,
			},
			WAL: agentmodel.WALProgress{
				ReplayLSN: "0/7000100",
			},
		},
		ObservedAt: now,
	}); err != nil {
		t.Fatalf("publish replica node status: %v", err)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected aggregated cluster status")
	}

	if status.ClusterName != "alpha" {
		t.Fatalf("unexpected cluster name: got %q", status.ClusterName)
	}

	if status.Phase != cluster.ClusterPhaseHealthy {
		t.Fatalf("unexpected cluster phase: got %q", status.Phase)
	}

	if status.CurrentPrimary != "alpha-1" {
		t.Fatalf("unexpected current primary: got %q", status.CurrentPrimary)
	}

	if len(status.Members) != 2 {
		t.Fatalf("unexpected member count: got %d", len(status.Members))
	}

	if status.Members[0].Name != "alpha-1" || status.Members[1].Name != "alpha-2" {
		t.Fatalf("expected sorted members, got %+v", status.Members)
	}

	if !status.Members[0].Leader || !status.Members[0].Healthy {
		t.Fatalf("expected healthy primary leader, got %+v", status.Members[0])
	}

	if status.Members[0].Priority != 100 || !status.Members[0].NoFailover {
		t.Fatalf("expected desired member policy on primary, got %+v", status.Members[0])
	}

	if status.Members[0].Tags["zone"] != "desired-a" {
		t.Fatalf("expected desired tag policy on primary, got %+v", status.Members[0].Tags)
	}

	if status.Members[1].LagBytes != 128 {
		t.Fatalf("unexpected replica lag bytes: got %d", status.Members[1].LagBytes)
	}

	if status.Members[1].Priority != 20 || status.Members[1].NoFailover {
		t.Fatalf("expected desired member policy on replica, got %+v", status.Members[1])
	}

	truth := store.SourceOfTruth()
	if truth.Observed == nil || truth.Observed.CurrentPrimary != "alpha-1" {
		t.Fatalf("expected source of truth to include observed cluster status, got %+v", truth)
	}
}

func TestMemoryStateStoreReconcileReflectsDesiredVsObservedGap(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	now := time.Date(2026, time.March, 27, 12, 0, 0, 0, time.UTC)
	setTestNow(store, func() time.Time { return now })

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Role:     cluster.MemberRolePrimary,
		State:    cluster.MemberStateRunning,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		},
		ObservedAt: now,
	}); err != nil {
		t.Fatalf("publish node status: %v", err)
	}

	truth, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile source of truth: %v", err)
	}

	if truth.Observed == nil {
		t.Fatalf("expected observed state after reconcile, got %+v", truth)
	}

	if truth.Observed.Phase != cluster.ClusterPhaseDegraded {
		t.Fatalf("expected missing desired member to degrade cluster, got %q", truth.Observed.Phase)
	}

	if truth.Observed.CurrentPrimary != "alpha-1" {
		t.Fatalf("unexpected reconciled current primary: got %q", truth.Observed.CurrentPrimary)
	}
}

func TestMemoryStateStoreStoresInitializingObservedClusterStatusWithoutMembers(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	now := time.Date(2026, time.March, 27, 12, 30, 0, 0, time.UTC)
	setTestNow(store, func() time.Time { return now })

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected aggregated cluster status")
	}

	if status.Phase != cluster.ClusterPhaseInitializing {
		t.Fatalf("expected initializing cluster without members, got %q", status.Phase)
	}
}

func TestMemoryStateStoreUpdateMaintenanceModeReconcilesAndJournals(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	now := time.Date(2026, time.March, 27, 13, 0, 0, 0, time.UTC)
	setTestNow(store, func() time.Time { return now })

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Role:     cluster.MemberRolePrimary,
		State:    cluster.MemberStateRunning,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline: 3,
			},
			WAL: agentmodel.WALProgress{
				FlushLSN: "0/3000200",
			},
		},
		ObservedAt: now,
	}); err != nil {
		t.Fatalf("publish primary node status: %v", err)
	}

	status, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     true,
		Reason:      "planned switchover",
		RequestedBy: "operator",
	})
	if err != nil {
		t.Fatalf("update maintenance mode: %v", err)
	}

	if !status.Enabled || status.Reason != "planned switchover" || status.RequestedBy != "operator" {
		t.Fatalf("unexpected maintenance status: %+v", status)
	}

	spec, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected stored cluster spec")
	}

	if !spec.Maintenance.Enabled || spec.Maintenance.DefaultReason != "planned switchover" {
		t.Fatalf("unexpected stored maintenance spec: %+v", spec.Maintenance)
	}

	clusterStatus, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after maintenance update")
	}

	if clusterStatus.Phase != cluster.ClusterPhaseMaintenance {
		t.Fatalf("expected maintenance phase, got %q", clusterStatus.Phase)
	}

	if clusterStatus.Maintenance != status {
		t.Fatalf("expected maintenance status to propagate, got %+v", clusterStatus.Maintenance)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected synchronous maintenance update not to leave an active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one maintenance journal entry, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindMaintenanceChange || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected maintenance history entry: %+v", history[0])
	}

	if history[0].Timeline != 3 || history[0].WALLSN != "0/3000200" {
		t.Fatalf("expected maintenance history entry to capture primary timeline and wal, got %+v", history[0])
	}
}

func TestMemoryStateStoreUpdateMaintenanceModeRequiresDesiredState(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	_, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{Enabled: true})
	if !errors.Is(err, ErrClusterSpecRequired) {
		t.Fatalf("unexpected maintenance update error: got %v", err)
	}
}

func TestMemoryStateStoreUpdateMaintenanceModeKeepsActiveFailoverVisible(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	now := time.Date(2026, time.March, 27, 13, 30, 0, 0, time.UTC)
	setTestNow(store, func() time.Time { return now })

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Role:     cluster.MemberRolePrimary,
		State:    cluster.MemberStateFailed,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
		},
		ObservedAt: now,
	}); err != nil {
		t.Fatalf("publish failed primary node status: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-2",
		Role:     cluster.MemberRoleReplica,
		State:    cluster.MemberStateStreaming,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline: 4,
			},
			WAL: agentmodel.WALProgress{
				ReplayLSN: "0/4000100",
			},
		},
		ObservedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("publish standby node status: %v", err)
	}

	failover, err := store.JournalOperation(context.Background(), cluster.Operation{
		ID:          "failover-1",
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateAccepted,
		RequestedBy: "operator",
		RequestedAt: now.Add(2 * time.Second),
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
	})
	if err != nil {
		t.Fatalf("journal failover operation: %v", err)
	}

	maintenance, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     true,
		Reason:      "operator freeze",
		RequestedBy: "operator",
	})
	if err != nil {
		t.Fatalf("update maintenance mode: %v", err)
	}

	if !maintenance.Enabled || maintenance.Reason != "operator freeze" {
		t.Fatalf("unexpected maintenance status: %+v", maintenance)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected failover to remain active after maintenance change")
	}

	if active.ID != failover.ID || active.Kind != cluster.OperationKindFailover {
		t.Fatalf("unexpected active operation after maintenance change: %+v", active)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after maintenance change")
	}

	if status.Phase != cluster.ClusterPhaseMaintenance {
		t.Fatalf("expected maintenance phase to override failover phase, got %+v", status)
	}

	if status.ActiveOperation == nil || status.ActiveOperation.ID != failover.ID || status.ActiveOperation.Kind != cluster.OperationKindFailover {
		t.Fatalf("expected active failover projection during maintenance, got %+v", status.ActiveOperation)
	}

	history := store.History()
	if len(history) != 1 || history[0].Kind != cluster.OperationKindMaintenanceChange {
		t.Fatalf("expected only maintenance history entry, got %+v", history)
	}
}

func TestMemoryStateStoreJournalOperationTracksActiveAndFinishedState(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	startedAt := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)
	clock := newMutableTestClock(startedAt)
	setTestNow(store, clock.Now)
	setTestLeaseDuration(store, time.Hour)

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Role:     cluster.MemberRolePrimary,
		State:    cluster.MemberStateRunning,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline: 5,
			},
			WAL: agentmodel.WALProgress{
				FlushLSN: "0/5000200",
			},
		},
		ObservedAt: startedAt,
	}); err != nil {
		t.Fatalf("publish alpha-1 node status: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-2",
		Role:     cluster.MemberRoleReplica,
		State:    cluster.MemberStateStreaming,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline: 5,
			},
			WAL: agentmodel.WALProgress{
				ReplayLSN: "0/5000100",
			},
		},
		ObservedAt: startedAt,
	}); err != nil {
		t.Fatalf("publish alpha-2 node status: %v", err)
	}

	running, err := store.JournalOperation(context.Background(), cluster.Operation{
		ID:          "op-1",
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateRunning,
		RequestedBy: "controller",
		RequestedAt: startedAt,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
	})
	if err != nil {
		t.Fatalf("journal running operation: %v", err)
	}

	if running.Result != cluster.OperationResultPending {
		t.Fatalf("expected nonterminal operation result to default to pending, got %q", running.Result)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.ID != "op-1" {
		t.Fatalf("expected active operation to be stored, got ok=%v operation=%+v", ok, active)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status while operation is running")
	}

	if status.Phase != cluster.ClusterPhaseFailingOver {
		t.Fatalf("expected failover phase while operation is active, got %q", status.Phase)
	}

	completedAt := clock.Advance(time.Minute)
	completed, err := store.JournalOperation(context.Background(), cluster.Operation{
		ID:          "op-1",
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateCompleted,
		RequestedBy: "controller",
		RequestedAt: completedAt.Add(-time.Minute),
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		CompletedAt: completedAt,
		Result:      cluster.OperationResultSucceeded,
	})
	if err != nil {
		t.Fatalf("journal completed operation: %v", err)
	}

	if completed.Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected completed result: got %q", completed.Result)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected completed operation to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one history entry, got %+v", history)
	}

	if history[0].OperationID != "op-1" || history[0].Kind != cluster.OperationKindFailover {
		t.Fatalf("unexpected history entry: %+v", history[0])
	}

	if history[0].Timeline != 5 || history[0].WALLSN != "0/5000100" {
		t.Fatalf("expected history entry to use target member timeline and wal, got %+v", history[0])
	}

	status, ok = store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after completing operation")
	}

	if status.Phase != cluster.ClusterPhaseHealthy {
		t.Fatalf("expected cluster to return to healthy phase, got %q", status.Phase)
	}

	history[0].OperationID = "mutated"
	if again := store.History(); again[0].OperationID != "op-1" {
		t.Fatalf("expected history copies to be detached, got %+v", again)
	}
}

func TestMemoryStateStoreJournalOperationRejectsTerminalPendingResult(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	_, err := store.JournalOperation(context.Background(), cluster.Operation{
		ID:          "op-1",
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateCompleted,
		RequestedAt: time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC),
		Result:      cluster.OperationResultPending,
	})
	if !errors.Is(err, cluster.ErrInvalidOperationResult) {
		t.Fatalf("unexpected terminal operation validation error: got %v", err)
	}
}
