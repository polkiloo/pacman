package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestCreateFailoverIntentPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.April, 14, 13, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 11, 0),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 11, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

	wantErr := errors.New("persist failover intent")
	store.dcs = &failingControlPlaneDCS{
		DCS: store.dcs,
		failSetOnCall: map[string]map[int]error{
			store.keyspace.Operation(): {1: wantErr},
		},
	}

	_, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected failover intent persistence error: got %v want %v", err, wantErr)
	}
}

func TestExecuteFailoverPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.April, 14, 13, 30, 0, 0, time.UTC)

	newStore := func(t *testing.T) *MemoryStateStore {
		t.Helper()

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
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 11, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 11, 4),
			failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
		})
		setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })
		if _, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{}); err != nil {
			t.Fatalf("create failover intent: %v", err)
		}
		store.mu.Lock()
		store.clusterStatus.CurrentEpoch = 4
		store.mu.Unlock()

		return store
	}

	t.Run("persist running operation", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist running failover")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.ExecuteFailover(context.Background(), &recordingPromoter{}, nil)
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected failover running-operation error: got %v want %v", err, wantErr)
		}
	})

	t.Run("persist completed history entry", func(t *testing.T) {
		now := time.Date(2026, time.April, 14, 13, 45, 0, 0, time.UTC)
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
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 11, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 11, 4),
			failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
		})

		operation := cluster.Operation{
			ID:          "failover-epoch",
			Kind:        cluster.OperationKindFailover,
			State:       cluster.OperationStateRunning,
			Result:      cluster.OperationResultPending,
			FromMember:  "alpha-1",
			ToMember:    "alpha-2",
			RequestedAt: now.Add(-10 * time.Second),
			StartedAt:   now,
		}
		store.mu.Lock()
		store.activeOperation = &operation
		store.clusterStatus.CurrentEpoch = 4
		store.refreshSourceOfTruthLocked(now)
		store.clusterStatus.CurrentEpoch = 4
		store.mu.Unlock()

		wantErr := errors.New("persist completed failover history")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.History(operation.ID): {1: wantErr},
			},
		}

		_, err := store.publishFailoverEpoch(preparedFailoverExecution{
			spec:          store.clusterSpec.Clone(),
			operation:     operation,
			previousEpoch: 4,
			executedAt:    now,
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected failover history error: got %v want %v", err, wantErr)
		}
	})
}

func TestExecuteSwitchoverPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.April, 14, 14, 0, 0, 0, time.UTC)

	newStore := func(t *testing.T) *MemoryStateStore {
		t.Helper()

		store := NewMemoryStateStore()
		setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })
		setTestLeaseDuration(store, time.Hour)

		if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
			ClusterName: "alpha",
			Switchover:  cluster.SwitchoverPolicy{AllowScheduled: true},
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}); err != nil {
			t.Fatalf("store cluster spec: %v", err)
		}

		for _, status := range []agentmodel.NodeStatus{
			readyPrimaryStatus("alpha-1", now, 18),
			readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
		} {
			if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
				t.Fatalf("publish node status for %q: %v", status.NodeName, err)
			}
		}

		store.mu.Lock()
		store.activeOperation = &cluster.Operation{
			ID:          "switchover-intent",
			Kind:        cluster.OperationKindSwitchover,
			State:       cluster.OperationStateAccepted,
			Result:      cluster.OperationResultPending,
			FromMember:  "alpha-1",
			ToMember:    "alpha-2",
			RequestedAt: now,
			RequestedBy: "operator",
			Reason:      "planned switchover",
		}
		store.clusterStatus.CurrentEpoch = 9
		store.refreshSourceOfTruthLocked(now)
		store.clusterStatus.CurrentEpoch = 9
		store.mu.Unlock()

		return store
	}

	t.Run("persist running operation", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist running switchover")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.ExecuteSwitchover(context.Background(), &recordingDemoter{}, &recordingPromoter{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected switchover running-operation error: got %v want %v", err, wantErr)
		}
	})

	t.Run("persist demoting primary status", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist demoting primary")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Status("alpha-1"): {1: wantErr},
			},
		}

		_, err := store.ExecuteSwitchover(context.Background(), &recordingDemoter{}, &recordingPromoter{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected switchover demotion status error: got %v want %v", err, wantErr)
		}
	})
}

func TestExecuteRejoinRewindPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.March, 30, 20, 0, 0, 0, time.UTC)

	newStore := func(t *testing.T) *MemoryStateStore {
		t.Helper()

		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, []agentmodel.NodeStatus{
			rejoinFormerPrimaryStatus("alpha-1", now.Add(-time.Minute), 10, "sys-alpha"),
			rejoinPrimaryStatus("alpha-2", now.Add(-time.Minute+time.Second), 11, "sys-alpha"),
		})
		store.mu.Lock()
		store.clusterStatus.CurrentEpoch = 7
		store.mu.Unlock()
		setTestNow(store, func() time.Time { return now })

		return store
	}

	t.Run("persist running operation", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist rejoin rewind")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.ExecuteRejoinRewind(context.Background(), RejoinRequest{Member: "alpha-1"}, &recordingRewinder{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected rejoin rewind persistence error: got %v want %v", err, wantErr)
		}
	})
}

func TestPublishSwitchoverCompletionPropagatesHistoryPersistenceError(t *testing.T) {
	now := time.Date(2026, time.April, 14, 16, 0, 0, 0, time.UTC)
	store := seededStoreForCoverage(t, now, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover:  cluster.SwitchoverPolicy{AllowScheduled: true},
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now.Add(-time.Second), 18),
		readyStandbyStatus("alpha-2", now, 18, 0),
	})

	operation := cluster.Operation{
		ID:          "switchover-complete",
		Kind:        cluster.OperationKindSwitchover,
		State:       cluster.OperationStateRunning,
		Result:      cluster.OperationResultPending,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		RequestedAt: now.Add(-20 * time.Second),
		StartedAt:   now.Add(-10 * time.Second),
	}
	store.mu.Lock()
	store.activeOperation = &operation
	store.clusterStatus.CurrentEpoch = 9
	store.refreshSourceOfTruthLocked(now)
	store.clusterStatus.CurrentEpoch = 9
	store.mu.Unlock()

	wantErr := errors.New("persist completed switchover history")
	store.dcs = &failingControlPlaneDCS{
		DCS: store.dcs,
		failSetOnCall: map[string]map[int]error{
			store.keyspace.History(operation.ID): {1: wantErr},
		},
	}

	_, err := store.publishSwitchoverCompletion(preparedSwitchoverExecution{
		operation:     operation,
		previousEpoch: 9,
		executedAt:    now,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected switchover completion history error: got %v want %v", err, wantErr)
	}
}

func TestFailoverExecutionEdgeCases(t *testing.T) {
	t.Run("active failover operation requires candidate", func(t *testing.T) {
		store := NewMemoryStateStore()
		store.mu.Lock()
		store.activeOperation = &cluster.Operation{
			ID:         "failover-missing-candidate",
			Kind:       cluster.OperationKindFailover,
			State:      cluster.OperationStateRunning,
			FromMember: "alpha-1",
		}
		store.mu.Unlock()

		_, err := store.activeFailoverOperationLocked()
		if !errors.Is(err, ErrFailoverCandidateUnknown) {
			t.Fatalf("unexpected active failover error: got %v want %v", err, ErrFailoverCandidateUnknown)
		}
	})

	t.Run("publish failover requires cluster status", func(t *testing.T) {
		now := time.Date(2026, time.April, 14, 16, 30, 0, 0, time.UTC)
		store := seededStoreForCoverage(t, now, cluster.ClusterSpec{
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
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 11, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 11, 4),
			failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
		})

		operation := cluster.Operation{
			ID:         "failover-no-status",
			Kind:       cluster.OperationKindFailover,
			State:      cluster.OperationStateRunning,
			Result:     cluster.OperationResultPending,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		}
		store.mu.Lock()
		store.activeOperation = &operation
		store.clusterStatus = nil
		store.mu.Unlock()

		_, err := store.publishFailoverEpoch(preparedFailoverExecution{
			spec:          store.clusterSpec.Clone(),
			operation:     operation,
			previousEpoch: 4,
			executedAt:    now,
		})
		if !errors.Is(err, ErrFailoverIntentChanged) {
			t.Fatalf("unexpected publish failover error: got %v want %v", err, ErrFailoverIntentChanged)
		}
	})

	t.Run("publish failover requires observed candidate", func(t *testing.T) {
		now := time.Date(2026, time.April, 14, 16, 45, 0, 0, time.UTC)
		store := seededStoreForCoverage(t, now, cluster.ClusterSpec{
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
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 11, 0),
			failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
		})

		operation := cluster.Operation{
			ID:         "failover-missing-candidate-status",
			Kind:       cluster.OperationKindFailover,
			State:      cluster.OperationStateRunning,
			Result:     cluster.OperationResultPending,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		}
		store.mu.Lock()
		store.activeOperation = &operation
		store.clusterStatus.CurrentEpoch = 4
		store.refreshSourceOfTruthLocked(now)
		store.clusterStatus.CurrentEpoch = 4
		store.mu.Unlock()

		_, err := store.publishFailoverEpoch(preparedFailoverExecution{
			spec:          store.clusterSpec.Clone(),
			operation:     operation,
			previousEpoch: 4,
			executedAt:    now,
		})
		if !errors.Is(err, ErrFailoverCandidateUnknown) {
			t.Fatalf("unexpected missing candidate status error: got %v want %v", err, ErrFailoverCandidateUnknown)
		}
	})
}

func TestPublishRejoinRewindPropagatesMemberPersistenceError(t *testing.T) {
	now := time.Date(2026, time.April, 14, 17, 0, 0, 0, time.UTC)
	memberStatus := rejoinFormerPrimaryStatus("alpha-1", now.Add(-time.Minute), 10, "sys-alpha")
	currentPrimary := rejoinPrimaryStatus("alpha-2", now.Add(-time.Minute+time.Second), 11, "sys-alpha")
	store := seededStoreForCoverage(t, now, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{memberStatus, currentPrimary})
	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 7
	store.mu.Unlock()

	prepared := preparedRejoinExecution{
		decision: RejoinStrategyDecision{
			State:          cluster.RejoinStateSelectingStrategy,
			CurrentPrimary: cluster.MemberStatus{Name: "alpha-2", Healthy: true},
			Member:         cluster.MemberStatus{Name: "alpha-1", NeedsRejoin: true},
			Strategy:       cluster.RejoinStrategyRewind,
			Decided:        true,
		},
		memberNode:         memberStatus,
		currentPrimaryNode: currentPrimary,
		operation: cluster.Operation{
			ID:         "rejoin-publish",
			Kind:       cluster.OperationKindRejoin,
			State:      cluster.OperationStateRunning,
			Result:     cluster.OperationResultPending,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		},
		currentEpoch: 7,
		executedAt:   now,
	}

	wantErr := errors.New("persist rewound member status")
	store.dcs = &failingControlPlaneDCS{
		DCS: store.dcs,
		failSetOnCall: map[string]map[int]error{
			store.keyspace.Status("alpha-1"): {1: wantErr},
		},
	}

	_, err := store.publishRejoinRewind(prepared)
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected rejoin rewind member-status error: got %v want %v", err, wantErr)
	}
}

func TestFailRejoinExecutionKeepsFailedHistoryWhenPersistenceFails(t *testing.T) {
	now := time.Date(2026, time.April, 14, 17, 15, 0, 0, time.UTC)
	store := seededStoreForCoverage(t, now, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now.Add(-time.Minute), 10, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(-time.Minute+time.Second), 11, "sys-alpha"),
	})

	prepared := preparedRejoinExecution{
		operation: cluster.Operation{
			ID:          "rejoin-failed",
			Kind:        cluster.OperationKindRejoin,
			State:       cluster.OperationStateRunning,
			Result:      cluster.OperationResultPending,
			FromMember:  "alpha-1",
			ToMember:    "alpha-2",
			RequestedAt: now.Add(-15 * time.Second),
			StartedAt:   now.Add(-10 * time.Second),
		},
		executedAt: now,
	}
	store.mu.Lock()
	store.activeOperation = &prepared.operation
	store.clusterStatus.CurrentEpoch = 7
	store.refreshSourceOfTruthLocked(now)
	store.clusterStatus.CurrentEpoch = 7
	store.mu.Unlock()

	store.dcs = &failingControlPlaneDCS{
		DCS: store.dcs,
		failSetOnCall: map[string]map[int]error{
			store.keyspace.History(prepared.operation.ID): {1: errors.New("persist failed rejoin history")},
		},
	}

	store.failRejoinExecution(prepared, "pg_rewind failed for alpha-1 against alpha-2")

	history := store.History()
	if len(history) != 1 || history[0].OperationID != prepared.operation.ID || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("expected failed rejoin history entry to remain in memory, got %+v", history)
	}
}

func TestReconcileAndMaintenancePersistenceErrors(t *testing.T) {
	t.Run("reconcile requires source of truth state", func(t *testing.T) {
		store := NewMemoryStateStore()

		_, err := store.Reconcile(context.Background())
		if !errors.Is(err, ErrSourceOfTruthStateRequired) {
			t.Fatalf("unexpected reconcile error: got %v want %v", err, ErrSourceOfTruthStateRequired)
		}
	})

	t.Run("update maintenance propagates config compare-and-set error", func(t *testing.T) {
		now := time.Date(2026, time.April, 14, 15, 0, 0, 0, time.UTC)
		store := NewMemoryStateStore()
		setTestNow(store, func() time.Time { return now })

		if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}},
		}); err != nil {
			t.Fatalf("store cluster spec: %v", err)
		}

		wantErr := errors.New("persist maintenance config")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failCASOnCall: map[string]map[int]error{
				store.keyspace.Config(): {1: wantErr},
			},
		}

		_, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
			Enabled:     true,
			RequestedBy: "operator",
			Reason:      "maintenance window",
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected maintenance update error: got %v want %v", err, wantErr)
		}
	})

	t.Run("journal operation propagates persistence error", func(t *testing.T) {
		now := time.Date(2026, time.April, 14, 15, 30, 0, 0, time.UTC)
		store := NewMemoryStateStore()
		setTestNow(store, func() time.Time { return now })

		if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}},
		}); err != nil {
			t.Fatalf("store cluster spec: %v", err)
		}

		wantErr := errors.New("persist journaled operation")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.JournalOperation(context.Background(), cluster.Operation{
			ID:          "manual-op",
			Kind:        cluster.OperationKindMaintenanceChange,
			State:       cluster.OperationStateRunning,
			RequestedBy: "operator",
			RequestedAt: now,
			Result:      cluster.OperationResultPending,
			Message:     "manual operation running",
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected journal operation error: got %v want %v", err, wantErr)
		}
	})
}

func seededStoreForCoverage(t *testing.T, now time.Time, spec cluster.ClusterSpec, statuses []agentmodel.NodeStatus) *MemoryStateStore {
	t.Helper()

	store := NewMemoryStateStore()
	setTestNow(store, func() time.Time { return now })
	setTestLeaseDuration(store, time.Hour)

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
