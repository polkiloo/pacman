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
	store.now = func() time.Time { return now.Add(2 * time.Minute) }

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
		store.now = func() time.Time { return now.Add(5 * time.Minute) }

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
		store.now = func() time.Time { return now.Add(5 * time.Minute) }

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
		testCase := testCase

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
	store.now = func() time.Time { return now.Add(2 * time.Minute) }

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

func TestMemoryStateStoreExecuteSwitchoverCoordinatesPrimaryDemotion(t *testing.T) {
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
	store.now = func() time.Time { return now.Add(10 * time.Second) }

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
	execution, err := store.ExecuteSwitchover(context.Background(), demoter)
	if err != nil {
		t.Fatalf("execute switchover: %v", err)
	}

	if execution.CurrentPrimary != "alpha-1" || execution.Candidate != "alpha-2" || execution.PreviousEpoch != 9 || !execution.Demoted {
		t.Fatalf("unexpected switchover execution: %+v", execution)
	}

	if len(demoter.requests) != 1 || demoter.requests[0].CurrentPrimary != "alpha-1" || demoter.requests[0].Candidate != "alpha-2" || demoter.requests[0].CurrentEpoch != 9 {
		t.Fatalf("unexpected demotion requests: %+v", demoter.requests)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected running switchover operation after demotion")
	}

	if active.Kind != cluster.OperationKindSwitchover || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active switchover operation after demotion: %+v", active)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after switchover demotion")
	}

	if status.Phase != cluster.ClusterPhaseSwitchingOver {
		t.Fatalf("expected switching_over phase while switchover is running, got %+v", status)
	}

	if status.ScheduledSwitchover == nil || status.ScheduledSwitchover.From != "alpha-1" || status.ScheduledSwitchover.To != "alpha-2" {
		t.Fatalf("expected running switchover projection in cluster status, got %+v", status.ScheduledSwitchover)
	}

	primary, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected current primary node status after demotion")
	}

	if primary.Role != cluster.MemberRolePrimary || primary.State != cluster.MemberStateStopping {
		t.Fatalf("expected primary to be marked stopping during switchover, got %+v", primary)
	}

	candidate, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected target standby node status")
	}

	if candidate.Role != cluster.MemberRoleReplica || candidate.State != cluster.MemberStateStreaming {
		t.Fatalf("expected target standby to remain replica before promotion, got %+v", candidate)
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected switchover demotion phase not to record terminal history yet, got %+v", history)
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
			demoter: &recordingDemoter{},
			wantErr: ErrSwitchoverIntentRequired,
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
				store.now = func() time.Time { return now }
				if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
					Candidate:   "alpha-2",
					ScheduledAt: now.Add(10 * time.Minute),
				}); err != nil {
					t.Fatalf("create scheduled switchover intent: %v", err)
				}
			},
			demoter: &recordingDemoter{},
			wantErr: ErrSwitchoverExecutionNotReady,
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
			demoter: &recordingDemoter{},
			wantErr: ErrSwitchoverTargetNotReady,
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

			_, err := store.ExecuteSwitchover(context.Background(), testCase.demoter)
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
