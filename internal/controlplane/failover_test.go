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

	if candidates[3].Eligible || !containsString(candidates[3].Reasons, reasonCurrentPrimary) {
		t.Fatalf("expected current primary to be rejected, got %+v", candidates[3])
	}

	if candidates[4].Eligible || !containsString(candidates[4].Reasons, reasonNoFailoverTagged) {
		t.Fatalf("expected no-failover member to be rejected, got %+v", candidates[4])
	}

	if candidates[5].Eligible || !containsString(candidates[5].Reasons, reasonLagExceedsFailoverPolicy) {
		t.Fatalf("expected lagging member to be rejected, got %+v", candidates[5])
	}

	if candidates[6].Eligible || !containsString(candidates[6].Reasons, reasonTimelineMismatch) {
		t.Fatalf("expected timeline-mismatched member to be rejected, got %+v", candidates[6])
	}

	if candidates[7].Eligible || !containsString(candidates[7].Reasons, reasonRoleNotPromotable) {
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

func TestMemoryStateStoreExecuteFailoverRunsFencingPromotionAndAdvancesEpoch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 9, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:            cluster.FailoverModeAutomatic,
			RequireQuorum:   true,
			FencingRequired: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
			{Name: "witness-1"},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 15, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 15, 4),
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

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 4
	store.mu.Unlock()

	fencer := &recordingFencer{}
	promoter := &recordingPromoter{}

	execution, err := store.ExecuteFailover(context.Background(), promoter, fencer)
	if err != nil {
		t.Fatalf("execute failover: %v", err)
	}

	if execution.CurrentPrimary != intent.CurrentPrimary || execution.Candidate != intent.Candidate {
		t.Fatalf("unexpected execution members: %+v", execution)
	}

	if !execution.Fenced || !execution.Promoted {
		t.Fatalf("expected fenced and promoted execution, got %+v", execution)
	}

	if execution.PreviousEpoch != 4 || execution.CurrentEpoch != 5 {
		t.Fatalf("unexpected failover epoch transition: %+v", execution)
	}

	if len(fencer.requests) != 1 || fencer.requests[0].Candidate != "alpha-2" || fencer.requests[0].CurrentEpoch != 4 {
		t.Fatalf("unexpected fencing requests: %+v", fencer.requests)
	}

	if len(promoter.requests) != 1 || promoter.requests[0].Candidate != "alpha-2" || promoter.requests[0].CurrentEpoch != 4 {
		t.Fatalf("unexpected promotion requests: %+v", promoter.requests)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected completed failover to clear active operation")
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after failover execution")
	}

	if status.CurrentPrimary != "alpha-2" || status.CurrentEpoch != 5 {
		t.Fatalf("expected promoted primary and advanced epoch, got %+v", status)
	}

	if status.Phase != cluster.ClusterPhaseDegraded {
		t.Fatalf("expected former primary needs_rejoin to degrade cluster, got %+v", status)
	}

	candidate, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected promoted candidate node status")
	}

	if candidate.Role != cluster.MemberRolePrimary || candidate.State != cluster.MemberStateRunning {
		t.Fatalf("expected promoted candidate to be published as primary, got %+v", candidate)
	}

	if !candidate.Postgres.Up || candidate.Postgres.InRecovery || candidate.Postgres.Role != cluster.MemberRolePrimary {
		t.Fatalf("expected promoted postgres status, got %+v", candidate.Postgres)
	}

	formerPrimary, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status")
	}

	if formerPrimary.Role != cluster.MemberRoleReplica || formerPrimary.State != cluster.MemberStateNeedsRejoin || !formerPrimary.NeedsRejoin {
		t.Fatalf("expected former primary to be marked needs_rejoin, got %+v", formerPrimary)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected failover history entry after execution, got %+v", history)
	}

	if history[0].OperationID != intent.Operation.ID || history[0].Kind != cluster.OperationKindFailover {
		t.Fatalf("unexpected failover history entry: %+v", history[0])
	}

	if history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected failover history members/result: %+v", history[0])
	}

	if execution.Operation.State != cluster.OperationStateCompleted || execution.Operation.Result != cluster.OperationResultSucceeded || execution.Operation.CompletedAt.IsZero() {
		t.Fatalf("expected completed failover execution operation, got %+v", execution.Operation)
	}
}

func TestMemoryStateStoreExecuteFailoverSkipsOptionalFencing(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 10, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode: cluster.FailoverModeAutomatic,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 16, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 16, 0),
	})
	store.now = func() time.Time { return now.Add(5 * time.Second) }

	if _, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{}); err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	promoter := &recordingPromoter{}
	execution, err := store.ExecuteFailover(context.Background(), promoter, nil)
	if err != nil {
		t.Fatalf("execute failover without fencing: %v", err)
	}

	if execution.Fenced {
		t.Fatalf("expected failover without fencing requirement to skip hook, got %+v", execution)
	}

	if len(promoter.requests) != 1 || promoter.requests[0].Candidate != "alpha-2" {
		t.Fatalf("unexpected promotion requests without fencing: %+v", promoter.requests)
	}

	if execution.Operation.State != cluster.OperationStateCompleted || execution.Operation.Result != cluster.OperationResultSucceeded {
		t.Fatalf("expected completed failover execution without fencing, got %+v", execution.Operation)
	}
}

func TestMemoryStateStoreExecuteFailoverRejectsInvalidExecutionPrerequisites(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 11, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		spec     cluster.ClusterSpec
		statuses []agentmodel.NodeStatus
		prepare  func(t *testing.T, store *MemoryStateStore)
		promoter PromotionExecutor
		fencer   FencingHook
		wantErr  error
	}{
		{
			name: "active failover intent is required",
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
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 17, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 17, 0),
			},
			promoter: &recordingPromoter{},
			wantErr:  ErrFailoverIntentRequired,
		},
		{
			name: "promotion executor is required",
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
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 17, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 17, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
				if _, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{}); err != nil {
					t.Fatalf("create failover intent: %v", err)
				}
			},
			wantErr: ErrFailoverPromotionExecutorRequired,
		},
		{
			name: "fencing hook is required when policy demands it",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode:            cluster.FailoverModeAutomatic,
					FencingRequired: true,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2", Priority: 100},
				},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 17, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 17, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
				if _, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{}); err != nil {
					t.Fatalf("create failover intent: %v", err)
				}
			},
			promoter: &recordingPromoter{},
			wantErr:  ErrFailoverFencingHookRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, testCase.spec, testCase.statuses)
			if testCase.prepare != nil {
				testCase.prepare(t, store)
			}

			_, err := store.ExecuteFailover(context.Background(), testCase.promoter, testCase.fencer)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected failover execution error: got %v, want %v", err, testCase.wantErr)
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

type recordingFencer struct {
	requests []FencingRequest
}

func (fencer *recordingFencer) Fence(_ context.Context, request FencingRequest) error {
	fencer.requests = append(fencer.requests, request)
	return nil
}

type recordingPromoter struct {
	requests []PromotionRequest
}

func (promoter *recordingPromoter) Promote(_ context.Context, request PromotionRequest) error {
	promoter.requests = append(promoter.requests, request)
	return nil
}

func TestFailoverIntentClone(t *testing.T) {
	t.Parallel()

	op := cluster.Operation{ID: "fo-1", Kind: "failover"}
	intent := FailoverIntent{
		Operation: op,
		Candidate: "alpha-2",
		Confirmation: PrimaryFailureConfirmation{
			CurrentPrimary: "alpha-1",
			Confirmed:      true,
		},
		Candidates: []FailoverCandidate{
			{
				Member:   cluster.MemberStatus{Name: "alpha-2"},
				Eligible: true,
				Rank:     1,
				Reasons:  []string{"healthy"},
			},
		},
	}

	clone := intent.Clone()

	if clone.Candidate != intent.Candidate {
		t.Fatalf("candidate: got %q, want %q", clone.Candidate, intent.Candidate)
	}
	if clone.Confirmation.CurrentPrimary != intent.Confirmation.CurrentPrimary {
		t.Fatalf("confirmation primary: got %q, want %q", clone.Confirmation.CurrentPrimary, intent.Confirmation.CurrentPrimary)
	}
	if len(clone.Candidates) != len(intent.Candidates) {
		t.Fatalf("candidates count: got %d, want %d", len(clone.Candidates), len(intent.Candidates))
	}

	// Verify clone is independent
	clone.Candidates[0].Reasons[0] = "mutated"
	if intent.Candidates[0].Reasons[0] != "healthy" {
		t.Fatal("expected original reasons to be unaffected by clone mutation")
	}
}

func TestFailoverExecutionClone(t *testing.T) {
	t.Parallel()

	execution := FailoverExecution{
		Operation:      cluster.Operation{ID: "fo-exec-1", Kind: "failover"},
		CurrentPrimary: "alpha-1",
		Candidate:      "alpha-2",
		PreviousEpoch:  3,
		CurrentEpoch:   4,
		Fenced:         true,
		Promoted:       true,
	}

	clone := execution.Clone()

	if clone.CurrentPrimary != execution.CurrentPrimary {
		t.Fatalf("primary: got %q, want %q", clone.CurrentPrimary, execution.CurrentPrimary)
	}
	if clone.Candidate != execution.Candidate {
		t.Fatalf("candidate: got %q, want %q", clone.Candidate, execution.Candidate)
	}
	if clone.CurrentEpoch != execution.CurrentEpoch {
		t.Fatalf("epoch: got %v, want %v", clone.CurrentEpoch, execution.CurrentEpoch)
	}
}
