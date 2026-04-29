package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreValidateSwitchoverAcceptsReadyStandby(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 12, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			MaximumLagBytes: 64,
		},
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 18),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 8),
	})
	setTestNow(store, func() time.Time { return now.Add(2 * time.Minute) })

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 6
	store.mu.Unlock()

	validation, err := store.ValidateSwitchover(context.Background(), SwitchoverRequest{
		RequestedBy: " operator ",
		Reason:      " planned maintenance ",
		Candidate:   " alpha-2 ",
		ScheduledAt: now.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("validate switchover: %v", err)
	}

	if validation.Request.RequestedBy != "operator" || validation.Request.Reason != "planned maintenance" || validation.Request.Candidate != "alpha-2" {
		t.Fatalf("unexpected normalized switchover request: %+v", validation.Request)
	}

	if validation.CurrentPrimary.Name != "alpha-1" || !validation.CurrentPrimary.Healthy {
		t.Fatalf("unexpected current primary: %+v", validation.CurrentPrimary)
	}

	if validation.Target.Member.Name != "alpha-2" || !validation.Target.Ready {
		t.Fatalf("unexpected switchover target validation: %+v", validation.Target)
	}

	if validation.CurrentEpoch != 6 {
		t.Fatalf("unexpected switchover epoch: got %d, want %d", validation.CurrentEpoch, 6)
	}

	if !validation.ValidatedAt.Equal(store.now()) {
		t.Fatalf("unexpected switchover validation time: got %v, want %v", validation.ValidatedAt, store.now())
	}
}

func TestMemoryStateStoreCreateSwitchoverIntentCreatesPlannedTransition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 12, 30, 0, 0, time.UTC)

	t.Run("immediate switchover", func(t *testing.T) {
		t.Parallel()

		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Switchover: cluster.SwitchoverPolicy{
				AllowScheduled: true,
			},
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2"},
			},
		}, []agentmodel.NodeStatus{
			readyPrimaryStatus("alpha-1", now, 18),
			readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
		})
		setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

		intent, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
			RequestedBy: "operator",
			Reason:      "planned maintenance",
			Candidate:   "alpha-2",
		})
		if err != nil {
			t.Fatalf("create switchover intent: %v", err)
		}

		if intent.Operation.Kind != cluster.OperationKindSwitchover || intent.Operation.State != cluster.OperationStateAccepted || intent.Operation.Result != cluster.OperationResultPending {
			t.Fatalf("unexpected switchover operation: %+v", intent.Operation)
		}

		if intent.Operation.FromMember != "alpha-1" || intent.Operation.ToMember != "alpha-2" {
			t.Fatalf("unexpected switchover operation members: %+v", intent.Operation)
		}

		active, ok := store.ActiveOperation()
		if !ok || active.ID != intent.Operation.ID {
			t.Fatalf("expected active switchover operation, got ok=%v operation=%+v", ok, active)
		}

		status, ok := store.ClusterStatus()
		if !ok {
			t.Fatal("expected cluster status after switchover intent")
		}

		if status.Phase != cluster.ClusterPhaseHealthy {
			t.Fatalf("expected accepted switchover to keep healthy phase before execution, got %+v", status)
		}

		if status.ScheduledSwitchover == nil {
			t.Fatal("expected planned switchover projection in cluster status")
		}

		if status.ScheduledSwitchover.At != intent.Operation.RequestedAt || status.ScheduledSwitchover.From != "alpha-1" || status.ScheduledSwitchover.To != "alpha-2" {
			t.Fatalf("unexpected planned switchover projection: %+v", status.ScheduledSwitchover)
		}
	})

	t.Run("scheduled switchover", func(t *testing.T) {
		t.Parallel()

		scheduledAt := now.Add(20 * time.Minute)
		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Switchover: cluster.SwitchoverPolicy{
				AllowScheduled: true,
			},
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2"},
			},
		}, []agentmodel.NodeStatus{
			readyPrimaryStatus("alpha-1", now, 18),
			readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
		})
		setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

		intent, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
			RequestedBy: "operator",
			Reason:      "capacity balancing",
			Candidate:   "alpha-2",
			ScheduledAt: scheduledAt,
		})
		if err != nil {
			t.Fatalf("create scheduled switchover intent: %v", err)
		}

		if intent.Operation.State != cluster.OperationStateScheduled || !intent.Operation.ScheduledAt.Equal(scheduledAt) {
			t.Fatalf("expected scheduled switchover operation, got %+v", intent.Operation)
		}

		status, ok := store.ClusterStatus()
		if !ok || status.ScheduledSwitchover == nil {
			t.Fatalf("expected scheduled switchover status projection, got ok=%v status=%+v", ok, status)
		}

		if !status.ScheduledSwitchover.At.Equal(scheduledAt) || status.ScheduledSwitchover.From != "alpha-1" || status.ScheduledSwitchover.To != "alpha-2" {
			t.Fatalf("unexpected scheduled switchover projection: %+v", status.ScheduledSwitchover)
		}

		if status.Phase != cluster.ClusterPhaseHealthy {
			t.Fatalf("expected scheduled switchover to keep cluster healthy until execution, got %+v", status)
		}
	})
}

func TestMemoryStateStoreCancelSwitchoverCancelsPendingIntent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 12, 45, 0, 0, time.UTC)
	scheduledAt := now.Add(20 * time.Minute)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 18),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
	})
	setTestNow(store, func() time.Time { return now })

	intent, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
		RequestedBy: "operator",
		Reason:      "maintenance",
		Candidate:   "alpha-2",
		ScheduledAt: scheduledAt,
	})
	if err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	cancelledAt := now.Add(5 * time.Minute)
	setTestNow(store, func() time.Time { return cancelledAt })

	cancelled, err := store.CancelSwitchover(context.Background())
	if err != nil {
		t.Fatalf("cancel switchover: %v", err)
	}

	if cancelled.ID != intent.Operation.ID {
		t.Fatalf("cancelled switchover id: got %q, want %q", cancelled.ID, intent.Operation.ID)
	}

	if cancelled.State != cluster.OperationStateCancelled || cancelled.Result != cluster.OperationResultCancelled {
		t.Fatalf("unexpected cancelled switchover operation: %+v", cancelled)
	}

	if !cancelled.CompletedAt.Equal(cancelledAt) {
		t.Fatalf("cancelled switchover completedAt: got %v, want %v", cancelled.CompletedAt, cancelledAt)
	}

	if cancelled.Message != "scheduled switchover cancelled" {
		t.Fatalf("cancelled switchover message: got %q", cancelled.Message)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected cancelled switchover to clear active operation")
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after switchover cancellation")
	}

	if status.ScheduledSwitchover != nil {
		t.Fatalf("expected scheduled switchover projection to be cleared, got %+v", status.ScheduledSwitchover)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one history entry after switchover cancellation, got %+v", history)
	}

	if history[0].OperationID != intent.Operation.ID || history[0].Result != cluster.OperationResultCancelled {
		t.Fatalf("unexpected switchover cancellation history: %+v", history[0])
	}
}

func TestMemoryStateStoreCancelSwitchoverRejectsMissingOrRunningIntent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 13, 10, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		prepare func(t *testing.T, store *MemoryStateStore)
		wantErr error
	}{
		{
			name: "no switchover exists",
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
			},
			wantErr: ErrScheduledSwitchoverNotFound,
		},
		{
			name: "running switchover cannot be cancelled",
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()

				if _, err := store.JournalOperation(context.Background(), cluster.Operation{
					ID:          "sw-1",
					Kind:        cluster.OperationKindSwitchover,
					State:       cluster.OperationStateRunning,
					RequestedBy: "operator",
					RequestedAt: now,
					FromMember:  "alpha-1",
					ToMember:    "alpha-2",
					StartedAt:   now,
					Result:      cluster.OperationResultPending,
				}); err != nil {
					t.Fatalf("journal running switchover: %v", err)
				}
			},
			wantErr: ErrSwitchoverAlreadyRunning,
		},
		{
			name: "non-switchover active operation does not count",
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()

				if _, err := store.JournalOperation(context.Background(), cluster.Operation{
					ID:          "fo-1",
					Kind:        cluster.OperationKindFailover,
					State:       cluster.OperationStateAccepted,
					RequestedBy: "operator",
					RequestedAt: now,
					FromMember:  "alpha-1",
					ToMember:    "alpha-2",
					Result:      cluster.OperationResultPending,
				}); err != nil {
					t.Fatalf("journal failover: %v", err)
				}
			},
			wantErr: ErrScheduledSwitchoverNotFound,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, cluster.ClusterSpec{
				ClusterName: "alpha",
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			}, []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 18),
				readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
			})
			setTestNow(store, func() time.Time { return now })

			testCase.prepare(t, store)

			_, err := store.CancelSwitchover(context.Background())
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected switchover cancel error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreValidateSwitchoverRejectsBlockedRequests(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 13, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		spec     cluster.ClusterSpec
		statuses []agentmodel.NodeStatus
		request  SwitchoverRequest
		prepare  func(t *testing.T, store *MemoryStateStore)
		wantErr  error
	}{
		{
			name: "target is required",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 19),
			},
			request: SwitchoverRequest{},
			wantErr: ErrSwitchoverTargetRequired,
		},
		{
			name: "active operation blocks switchover",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 19),
				readyStandbyStatus("alpha-2", now, 19, 0),
			},
			request: SwitchoverRequest{Candidate: "alpha-2"},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()

				if _, err := store.JournalOperation(context.Background(), cluster.Operation{
					ID:          "op-1",
					Kind:        cluster.OperationKindFailover,
					State:       cluster.OperationStateRunning,
					RequestedBy: "controller",
					RequestedAt: now,
				}); err != nil {
					t.Fatalf("journal active operation: %v", err)
				}
			},
			wantErr: ErrSwitchoverOperationInProgress,
		},
		{
			name: "scheduled switchover requires policy opt-in",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 19),
				readyStandbyStatus("alpha-2", now, 19, 0),
			},
			request: SwitchoverRequest{
				Candidate:   "alpha-2",
				ScheduledAt: now.Add(time.Hour),
			},
			wantErr: ErrSwitchoverSchedulingNotAllowed,
		},
		{
			name: "current primary is required",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
			},
			statuses: []agentmodel.NodeStatus{
				readyStandbyStatus("alpha-2", now, 19, 0),
			},
			request: SwitchoverRequest{Candidate: "alpha-2"},
			wantErr: ErrSwitchoverPrimaryUnknown,
		},
		{
			name: "current primary must be healthy",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 19, 0),
				readyStandbyStatus("alpha-2", now, 19, 0),
			},
			request: SwitchoverRequest{Candidate: "alpha-2"},
			wantErr: ErrSwitchoverPrimaryUnhealthy,
		},
		{
			name: "target must differ from current primary",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 19),
				readyStandbyStatus("alpha-2", now, 19, 0),
			},
			request: SwitchoverRequest{Candidate: "alpha-1"},
			wantErr: ErrSwitchoverTargetIsCurrentPrimary,
		},
		{
			name: "target must be ready",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					MaximumLagBytes: 64,
				},
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 19),
				notReadyStandbyStatus("alpha-2", now, 18, 128),
			},
			request: SwitchoverRequest{Candidate: "alpha-2"},
			wantErr: ErrSwitchoverTargetNotReady,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, testCase.spec, testCase.statuses)
			if testCase.prepare != nil {
				testCase.prepare(t, store)
			}

			_, err := store.ValidateSwitchover(context.Background(), testCase.request)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected switchover validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreSwitchoverTargetReadinessReportsReadinessReasons(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 14, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			MaximumLagBytes: 64,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 20),
		notReadyStandbyStatus("alpha-2", now.Add(time.Second), 19, 128),
	})
	setTestNow(store, func() time.Time { return now.Add(2 * time.Minute) })

	readiness, err := store.SwitchoverTargetReadiness("alpha-2")
	if err != nil {
		t.Fatalf("switchover target readiness: %v", err)
	}

	if readiness.Ready {
		t.Fatalf("expected switchover readiness failure, got %+v", readiness)
	}

	if readiness.Member.Name != "alpha-2" || readiness.CurrentPrimary != "alpha-1" {
		t.Fatalf("unexpected switchover readiness identity: %+v", readiness)
	}

	wantReasons := []string{
		reasonStateNotReadyForSwitchover,
		reasonMemberUnhealthy,
		reasonMemberRequiresRejoin,
		reasonLagExceedsSwitchoverMaximum,
		reasonTimelineMismatch,
		reasonPostgresNotUp,
		reasonRecoveryStateUnknown,
	}
	for _, want := range wantReasons {
		if !containsString(readiness.Reasons, want) {
			t.Fatalf("expected switchover readiness reason %q in %v", want, readiness.Reasons)
		}
	}

	if !readiness.CheckedAt.Equal(store.now()) {
		t.Fatalf("unexpected switchover readiness check time: got %v, want %v", readiness.CheckedAt, store.now())
	}
}

func TestMemoryStateStoreSwitchoverTargetReadinessRejectsUnknownTarget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 15, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 21),
	})

	_, err := store.SwitchoverTargetReadiness("alpha-2")
	if !errors.Is(err, ErrSwitchoverTargetUnknown) {
		t.Fatalf("unexpected switchover target readiness error: got %v, want %v", err, ErrSwitchoverTargetUnknown)
	}
}

func TestMemoryStateStoreExecuteSwitchoverPromotesTargetAndRecordsHistory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 15, 30, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 22),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 22, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })

	if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
		RequestedBy: "operator",
		Reason:      "planned switchover",
		Candidate:   "alpha-2",
	}); err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 9
	store.mu.Unlock()

	demoter := &recordingDemoter{}
	promoter := &recordingPromoter{}
	execution, err := store.ExecuteSwitchover(context.Background(), demoter, promoter)
	if err != nil {
		t.Fatalf("execute switchover: %v", err)
	}

	if execution.CurrentPrimary != "alpha-1" || execution.Candidate != "alpha-2" || execution.PreviousEpoch != 9 || execution.CurrentEpoch != 10 {
		t.Fatalf("unexpected switchover execution members/epoch: %+v", execution)
	}

	if !execution.Demoted || !execution.Promoted {
		t.Fatalf("unexpected switchover execution: %+v", execution)
	}

	if len(demoter.requests) != 1 || demoter.requests[0].CurrentPrimary != "alpha-1" || demoter.requests[0].Candidate != "alpha-2" || demoter.requests[0].CurrentEpoch != 9 {
		t.Fatalf("unexpected demotion requests: %+v", demoter.requests)
	}

	if len(promoter.requests) != 1 || promoter.requests[0].CurrentPrimary != "alpha-1" || promoter.requests[0].Candidate != "alpha-2" || promoter.requests[0].CurrentEpoch != 9 {
		t.Fatalf("unexpected promotion requests: %+v", promoter.requests)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected completed switchover to clear active operation")
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after switchover execution")
	}

	if status.CurrentPrimary != "alpha-2" || status.CurrentEpoch != 10 {
		t.Fatalf("expected promoted primary and advanced epoch, got %+v", status)
	}

	if status.Phase != cluster.ClusterPhaseDegraded {
		t.Fatalf("expected former primary transition to keep cluster degraded until rejoin, got %+v", status)
	}

	if status.ScheduledSwitchover != nil {
		t.Fatalf("expected completed switchover to clear scheduled projection, got %+v", status.ScheduledSwitchover)
	}

	primary, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status after switchover")
	}

	if primary.Role != cluster.MemberRoleReplica || primary.State != cluster.MemberStateStopping || !primary.NeedsRejoin {
		t.Fatalf("expected former primary to be demoted with needs_rejoin=true, got %+v", primary)
	}

	if primary.Postgres.Up || primary.Postgres.Role != cluster.MemberRoleReplica {
		t.Fatalf("expected former primary postgres to be demoted and unavailable, got %+v", primary.Postgres)
	}

	candidate, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected promoted standby node status")
	}

	if candidate.Role != cluster.MemberRolePrimary || candidate.State != cluster.MemberStateRunning {
		t.Fatalf("expected target standby to be promoted, got %+v", candidate)
	}

	if !candidate.Postgres.Up || candidate.Postgres.InRecovery || candidate.Postgres.Role != cluster.MemberRolePrimary {
		t.Fatalf("expected promoted standby postgres status, got %+v", candidate.Postgres)
	}

	if candidate.Postgres.RecoveryKnown {
		t.Fatalf("expected promoted standby recovery state to remain unknown until the next heartbeat, got %+v", candidate.Postgres)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected terminal switchover history, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindSwitchover || history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected switchover history entry: %+v", history[0])
	}

	if execution.Operation.State != cluster.OperationStateCompleted || execution.Operation.Result != cluster.OperationResultSucceeded || execution.Operation.CompletedAt.IsZero() {
		t.Fatalf("expected completed switchover execution operation, got %+v", execution.Operation)
	}
}

func TestMemoryStateStoreExecuteSwitchoverRejectsInvalidExecutionPrerequisites(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 16, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		spec     cluster.ClusterSpec
		statuses []agentmodel.NodeStatus
		prepare  func(t *testing.T, store *MemoryStateStore)
		demoter  DemotionExecutor
		promoter PromotionExecutor
		wantErr  error
	}{
		{
			name: "active switchover intent is required",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 23),
				readyStandbyStatus("alpha-2", now, 23, 0),
			},
			demoter:  &recordingDemoter{},
			promoter: &recordingPromoter{},
			wantErr:  ErrSwitchoverIntentRequired,
		},
		{
			name: "demotion executor is required",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 23),
				readyStandbyStatus("alpha-2", now, 23, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
				if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"}); err != nil {
					t.Fatalf("create switchover intent: %v", err)
				}
			},
			wantErr: ErrSwitchoverDemotionExecutorRequired,
		},
		{
			name: "promotion executor is required",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 23),
				readyStandbyStatus("alpha-2", now, 23, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
				if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"}); err != nil {
					t.Fatalf("create switchover intent: %v", err)
				}
			},
			demoter: &recordingDemoter{},
			wantErr: ErrSwitchoverPromotionExecutorRequired,
		},
		{
			name: "scheduled switchover waits for execution time",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Switchover: cluster.SwitchoverPolicy{
					AllowScheduled: true,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 23),
				readyStandbyStatus("alpha-2", now, 23, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
				setTestNow(store, func() time.Time { return now })
				if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
					Candidate:   "alpha-2",
					ScheduledAt: now.Add(10 * time.Minute),
				}); err != nil {
					t.Fatalf("create scheduled switchover intent: %v", err)
				}
			},
			demoter:  &recordingDemoter{},
			promoter: &recordingPromoter{},
			wantErr:  ErrSwitchoverExecutionNotReady,
		},
		{
			name: "target readiness is revalidated before demotion",
			spec: cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					MaximumLagBytes: 64,
				},
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			},
			statuses: []agentmodel.NodeStatus{
				readyPrimaryStatus("alpha-1", now, 23),
				readyStandbyStatus("alpha-2", now, 23, 0),
			},
			prepare: func(t *testing.T, store *MemoryStateStore) {
				t.Helper()
				if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"}); err != nil {
					t.Fatalf("create switchover intent: %v", err)
				}

				if _, err := store.PublishNodeStatus(context.Background(), notReadyStandbyStatus("alpha-2", now.Add(time.Minute), 22, 128)); err != nil {
					t.Fatalf("publish degraded standby state: %v", err)
				}
			},
			demoter:  &recordingDemoter{},
			promoter: &recordingPromoter{},
			wantErr:  ErrSwitchoverTargetNotReady,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, testCase.spec, testCase.statuses)
			if testCase.prepare != nil {
				testCase.prepare(t, store)
			}

			_, err := store.ExecuteSwitchover(context.Background(), testCase.demoter, testCase.promoter)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected switchover execution error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func readyPrimaryStatus(nodeName string, observedAt time.Time, timeline int64) agentmodel.NodeStatus {
	status := failoverNodeStatus(nodeName, cluster.MemberRolePrimary, cluster.MemberStateRunning, observedAt, true, timeline, 0)
	status.Postgres.Role = cluster.MemberRolePrimary
	status.Postgres.RecoveryKnown = true
	status.Postgres.InRecovery = false

	return status
}

func readyStandbyStatus(nodeName string, observedAt time.Time, timeline int64, lag int64) agentmodel.NodeStatus {
	status := failoverNodeStatus(nodeName, cluster.MemberRoleReplica, cluster.MemberStateStreaming, observedAt, true, timeline, lag)
	status.Postgres.Role = cluster.MemberRoleReplica
	status.Postgres.RecoveryKnown = true
	status.Postgres.InRecovery = true

	return status
}

func notReadyStandbyStatus(nodeName string, observedAt time.Time, timeline int64, lag int64) agentmodel.NodeStatus {
	status := failoverNodeStatus(nodeName, cluster.MemberRoleReplica, cluster.MemberStateStarting, observedAt, false, timeline, lag)
	status.NeedsRejoin = true
	status.Postgres.Role = cluster.MemberRoleReplica
	status.Postgres.RecoveryKnown = false
	status.Postgres.InRecovery = false

	return status
}

type recordingDemoter struct {
	requests []DemotionRequest
}

func (demoter *recordingDemoter) Demote(_ context.Context, request DemotionRequest) error {
	demoter.requests = append(demoter.requests, request)
	return nil
}
