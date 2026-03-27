package controlplane

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreFailoverCandidatesApplyEligibilityAndRankingRules(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 28, 10, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:            cluster.FailoverModeAutomatic,
			MaximumLagBytes: 64,
			CheckTimeline:   true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
			{Name: "alpha-3", Priority: 50},
			{Name: "alpha-4", Priority: 200, NoFailover: true},
			{Name: "alpha-5", Priority: 200},
			{Name: "alpha-6", Priority: 300},
			{Name: "alpha-7", Priority: 100},
			{Name: "witness-1", Priority: 10, NoFailover: true},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 7, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(2*time.Second), true, 7, 48),
		failoverNodeStatus("alpha-3", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 7, 16),
		failoverNodeStatus("alpha-4", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(time.Second), true, 7, 8),
		failoverNodeStatus("alpha-5", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 7, 128),
		failoverNodeStatus("alpha-6", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 6, 0),
		failoverNodeStatus("alpha-7", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(3*time.Second), true, 7, 16),
		failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
	})

	candidates, err := store.FailoverCandidates()
	if err != nil {
		t.Fatalf("evaluate failover candidates: %v", err)
	}

	gotNames := make([]string, len(candidates))
	for index, candidate := range candidates {
		gotNames[index] = candidate.Member.Name
	}

	wantNames := []string{
		"alpha-7",
		"alpha-2",
		"alpha-3",
		"alpha-1",
		"alpha-4",
		"alpha-5",
		"alpha-6",
		"witness-1",
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("unexpected candidate order: got %v, want %v", gotNames, wantNames)
	}

	if !candidates[0].Eligible || candidates[0].Rank != 1 {
		t.Fatalf("expected highest-priority low-lag replica to rank first, got %+v", candidates[0])
	}

	if !candidates[1].Eligible || candidates[1].Rank != 2 {
		t.Fatalf("expected second eligible replica to rank second, got %+v", candidates[1])
	}

	if !candidates[2].Eligible || candidates[2].Rank != 3 {
		t.Fatalf("expected lower-priority replica to rank third, got %+v", candidates[2])
	}

	if candidates[3].Eligible || !containsString(candidates[3].Reasons, "member is the current primary") {
		t.Fatalf("expected current primary to be rejected, got %+v", candidates[3])
	}

	if candidates[4].Eligible || !containsString(candidates[4].Reasons, "member is tagged no-failover") {
		t.Fatalf("expected no-failover member to be rejected, got %+v", candidates[4])
	}

	if candidates[5].Eligible || !containsString(candidates[5].Reasons, "member replication lag exceeds failover policy") {
		t.Fatalf("expected lagging member to be rejected, got %+v", candidates[5])
	}

	if candidates[6].Eligible || !containsString(candidates[6].Reasons, "member timeline does not match current primary") {
		t.Fatalf("expected timeline-mismatched member to be rejected, got %+v", candidates[6])
	}

	if candidates[7].Eligible || !containsString(candidates[7].Reasons, "member role is not promotable") {
		t.Fatalf("expected witness to be rejected, got %+v", candidates[7])
	}
}

func TestMemoryStateStoreConfirmPrimaryFailureUsesQuorumPolicy(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 28, 11, 0, 0, 0, time.UTC)

	t.Run("quorum reached", func(t *testing.T) {
		t.Parallel()

		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Failover: cluster.FailoverPolicy{
				Mode:          cluster.FailoverModeAutomatic,
				RequireQuorum: true,
			},
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2", Priority: 100},
				{Name: "witness-1"},
			},
		}, []agentmodel.NodeStatus{
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 9, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 9, 0),
			failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
		})

		confirmation, err := store.ConfirmPrimaryFailure()
		if err != nil {
			t.Fatalf("confirm primary failure: %v", err)
		}

		if !confirmation.Confirmed || !confirmation.QuorumReachable {
			t.Fatalf("expected quorum-confirmed primary failure, got %+v", confirmation)
		}

		if confirmation.CurrentPrimary != "alpha-1" || confirmation.PrimaryHealthy {
			t.Fatalf("unexpected primary confirmation state: %+v", confirmation)
		}

		if confirmation.ReachableVoters != 2 || confirmation.RequiredVoters != 2 || confirmation.TotalVoters != 3 {
			t.Fatalf("unexpected quorum counts: %+v", confirmation)
		}
	})

	t.Run("quorum missing", func(t *testing.T) {
		t.Parallel()

		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Failover: cluster.FailoverPolicy{
				Mode:          cluster.FailoverModeAutomatic,
				RequireQuorum: true,
			},
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2", Priority: 100},
				{Name: "witness-1"},
			},
		}, []agentmodel.NodeStatus{
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 9, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 9, 0),
		})

		confirmation, err := store.ConfirmPrimaryFailure()
		if err != nil {
			t.Fatalf("confirm primary failure: %v", err)
		}

		if confirmation.Confirmed || confirmation.QuorumReachable {
			t.Fatalf("expected quorum to block primary failure confirmation, got %+v", confirmation)
		}

		if confirmation.ReachableVoters != 1 || confirmation.RequiredVoters != 2 || confirmation.TotalVoters != 3 {
			t.Fatalf("unexpected quorum counts without witness: %+v", confirmation)
		}
	})
}

func TestMemoryStateStoreCreateFailoverIntentCreatesAcceptedOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 28, 12, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:          cluster.FailoverModeAutomatic,
			RequireQuorum: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
			{Name: "alpha-3", Priority: 50},
			{Name: "witness-1"},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 11, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(2*time.Second), true, 11, 8),
		failoverNodeStatus("alpha-3", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 11, 32),
		failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
	})
	store.now = func() time.Time { return now.Add(10 * time.Second) }

	intent, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{
		RequestedBy: "controller",
		Reason:      "primary unavailable",
	})
	if err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	if intent.CurrentPrimary != "alpha-1" || intent.Candidate != "alpha-2" {
		t.Fatalf("unexpected failover intent target: %+v", intent)
	}

	if !intent.Confirmation.Confirmed {
		t.Fatalf("expected confirmed failover intent, got %+v", intent.Confirmation)
	}

	if intent.Operation.Kind != cluster.OperationKindFailover ||
		intent.Operation.State != cluster.OperationStateAccepted ||
		intent.Operation.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected failover operation: %+v", intent.Operation)
	}

	if intent.Operation.FromMember != "alpha-1" || intent.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected failover operation members: %+v", intent.Operation)
	}

	if intent.Operation.RequestedBy != "controller" || intent.Operation.Reason != "primary unavailable" {
		t.Fatalf("unexpected failover operation metadata: %+v", intent.Operation)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.ID != intent.Operation.ID {
		t.Fatalf("expected failover intent to journal active operation, got ok=%v operation=%+v", ok, active)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after failover intent")
	}

	if status.Phase != cluster.ClusterPhaseFailingOver {
		t.Fatalf("expected active failover to move cluster phase, got %+v", status)
	}
}

func TestMemoryStateStoreCreateFailoverIntentRejectsBlockedFailover(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 28, 13, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		spec     cluster.ClusterSpec
		statuses []agentmodel.NodeStatus
		prepare  func(t *testing.T, store *MemoryStateStore)
		wantErr  error
	}{
		{
			name: "manual only policy blocks automatic failover",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode: cluster.FailoverModeManualOnly,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 12, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
			},
			wantErr: ErrAutomaticFailoverNotAllowed,
		},
		{
			name: "maintenance blocks failover intent creation",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Maintenance: cluster.MaintenanceDesiredState{
					Enabled: true,
				},
				Failover: cluster.FailoverPolicy{
					Mode: cluster.FailoverModeAutomatic,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 12, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
			},
			wantErr: ErrFailoverMaintenanceEnabled,
		},
		{
			name: "healthy primary blocks failover intent creation",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode: cluster.FailoverModeAutomatic,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 12, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
			},
			wantErr: ErrFailoverPrimaryHealthy,
		},
		{
			name: "missing quorum blocks failover intent creation",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode:          cluster.FailoverModeAutomatic,
					RequireQuorum: true,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100},
					{Name: "witness-1"},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 12, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
			},
			wantErr: ErrFailoverQuorumUnavailable,
		},
		{
			name: "no eligible replicas blocks failover intent creation",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode: cluster.FailoverModeAutomatic,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100, NoFailover: true},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 12, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
			},
			wantErr: ErrFailoverNoEligibleCandidates,
		},
		{
			name: "active operation blocks failover intent creation",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode: cluster.FailoverModeAutomatic,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 12, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()

				if _, err := store.JournalOperation(context.Background(), cluster.Operation{
					ID:          "op-1",
					Kind:        cluster.OperationKindMaintenanceChange,
					State:       cluster.OperationStateRunning,
					RequestedBy: "controller",
					RequestedAt: now,
				}); err != nil {
					t.Fatalf("journal active operation: %v", err)
				}
			},
			wantErr: ErrFailoverOperationInProgress,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, testCase.spec, testCase.statuses)
			if testCase.prepare != nil {
				testCase.prepare(t, store)
			}

			_, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{})
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected failover intent error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func seededFailoverStore(t *testing.T, spec cluster.ClusterSpec, statuses []agentmodel.NodeStatus) *MemoryStateStore {
	t.Helper()

	store := NewMemoryStateStore()
	if _, err := store.StoreClusterSpec(context.Background(), spec); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	for _, status := range statuses {
		if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
			t.Fatalf("publish node status for %q: %v", status.NodeName, err)
		}
	}

	return store
}

func failoverNodeStatus(nodeName string, role cluster.MemberRole, state cluster.MemberState, observedAt time.Time, up bool, timeline int64, lag int64) agentmodel.NodeStatus {
	status := agentmodel.NodeStatus{
		NodeName:   nodeName,
		MemberName: nodeName,
		Role:       role,
		State:      state,
		ObservedAt: observedAt,
	}

	if role == cluster.MemberRoleWitness {
		return status
	}

	status.Postgres = agentmodel.PostgresStatus{
		Managed: true,
		Up:      up,
		Details: agentmodel.PostgresDetails{
			Timeline:            timeline,
			ReplicationLagBytes: lag,
		},
	}

	return status
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}

	return false
}
