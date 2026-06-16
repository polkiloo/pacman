package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreCreateReinitIntentCreatesDistinctOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 16, 9, 0, 0, 0, time.UTC)
	store := seededReinitStore(t, now, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
		failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, true, 0, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

	intent, err := store.CreateReinitIntent(context.Background(), ReinitRequest{
		Member:      " alpha-2 ",
		RequestedBy: " ops ",
		Reason:      " reclone from WAL-G ",
	})
	if err != nil {
		t.Fatalf("create reinit intent: %v", err)
	}

	if intent.Operation.Kind != cluster.OperationKindReinit || intent.Operation.Kind == cluster.OperationKindRejoin {
		t.Fatalf("unexpected reinit operation kind: %+v", intent.Operation)
	}

	if intent.Operation.State != cluster.OperationStateAccepted || intent.Operation.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected reinit operation lifecycle: %+v", intent.Operation)
	}

	if intent.Operation.FromMember != "alpha-1" || intent.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected reinit operation members: %+v", intent.Operation)
	}

	if intent.Operation.RequestedBy != "ops" || intent.Operation.Reason != "reclone from WAL-G" {
		t.Fatalf("unexpected normalized reinit operation metadata: %+v", intent.Operation)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.ID != intent.Operation.ID || active.Kind != cluster.OperationKindReinit {
		t.Fatalf("expected active reinit operation, got ok=%v operation=%+v", ok, active)
	}
}

func TestMemoryStateStoreValidateReinitRejectsIneligibleTargets(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 16, 9, 30, 0, 0, time.UTC)

	cases := []struct {
		name    string
		member  string
		wantErr error
	}{
		{name: "missing member", member: "", wantErr: ErrReinitTargetRequired},
		{name: "unknown member", member: "missing", wantErr: ErrReinitTargetUnknown},
		{name: "current primary", member: "alpha-1", wantErr: ErrReinitTargetIsCurrentPrimary},
		{name: "witness", member: "witness-1", wantErr: ErrReinitTargetIsWitness},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := seededReinitStore(t, now, []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
				failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, true, 0, 0),
			})

			_, err := store.ValidateReinit(context.Background(), ReinitRequest{Member: tc.member})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("validate reinit error: got %v want %v", err, tc.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreValidateReinitRequiresHealthyPrimaryAndNoActiveOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 16, 10, 0, 0, 0, time.UTC)

	t.Run("source primary unhealthy", func(t *testing.T) {
		t.Parallel()

		store := seededReinitStore(t, now, []agentmodel.NodeStatus{
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 21, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
		})

		_, err := store.ValidateReinit(context.Background(), ReinitRequest{Member: "alpha-2"})
		if !errors.Is(err, ErrReinitSourcePrimaryUnhealthy) {
			t.Fatalf("validate reinit error: got %v want %v", err, ErrReinitSourcePrimaryUnhealthy)
		}
	})

	t.Run("operation in progress", func(t *testing.T) {
		t.Parallel()

		store := seededReinitStore(t, now, []agentmodel.NodeStatus{
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
		})

		_, err := store.JournalOperation(context.Background(), cluster.Operation{
			ID:          "switchover-active",
			Kind:        cluster.OperationKindSwitchover,
			State:       cluster.OperationStateRunning,
			RequestedAt: now,
			Result:      cluster.OperationResultPending,
		})
		if err != nil {
			t.Fatalf("journal active operation: %v", err)
		}

		_, err = store.ValidateReinit(context.Background(), ReinitRequest{Member: "alpha-2"})
		if !errors.Is(err, ErrReinitOperationInProgress) {
			t.Fatalf("validate reinit error: got %v want %v", err, ErrReinitOperationInProgress)
		}
	})
}

func seededReinitStore(t *testing.T, now time.Time, statuses []agentmodel.NodeStatus) *MemoryStateStore {
	t.Helper()

	return seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
			{Name: "witness-1"},
		},
	}, statuses)
}
