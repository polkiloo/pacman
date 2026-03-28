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
		"member state is not ready for switchover",
		"member is not healthy",
		"member requires rejoin",
		"member replication lag exceeds configured maximum",
		"member timeline does not match current primary",
		"member postgres is not up",
		"member recovery state is unknown",
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
