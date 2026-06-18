package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreExecuteReinitRestartAsStandbyTransitionsToStarting(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 15, 0, 0, 0, time.UTC)
	store := seededRecoveryConfiguredReinitStore(t, now)

	restarter := &recordingReinitStandbyRestarter{}
	execution, err := store.ExecuteReinitRestartAsStandby(context.Background(), " alpha-2 ", restarter)
	if err != nil {
		t.Fatalf("execute reinit restart as standby: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || !execution.WALGRestored || !execution.RecoveryConfig || !execution.RestartedAsStandby {
		t.Fatalf("unexpected reinit restart execution: %+v", execution)
	}
	if len(restarter.requests) != 1 {
		t.Fatalf("expected one reinit standby restart request, got %+v", restarter.requests)
	}
	request := restarter.requests[0]
	if request.Operation.Kind != cluster.OperationKindReinit || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected reinit standby restart request: %+v", request)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after standby restart")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active operation after standby restart: %+v", active)
	}
	if active.Message != reinitStandbyRestartCompletedMessage("alpha-2", "alpha-1") {
		t.Fatalf("unexpected standby restart operation message: %+v", active)
	}

	target, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected reinit target node status")
	}
	if target.Role != cluster.MemberRoleReplica || target.State != cluster.MemberStateStarting || target.PendingRestart {
		t.Fatalf("expected reinit target to be starting without pending restart, got %+v", target)
	}
	if !target.Postgres.Up || target.Postgres.Role != cluster.MemberRoleReplica || !target.Postgres.RecoveryKnown || !target.Postgres.InRecovery || target.Postgres.Details.PendingRestart {
		t.Fatalf("expected reinit target postgres to be starting in recovery, got %+v", target.Postgres)
	}
}

func TestMemoryStateStoreExecuteReinitRestartAsStandbyRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 15, 30, 0, 0, time.UTC)

	testCases := []struct {
		name      string
		prepare   func(t *testing.T) *MemoryStateStore
		member    string
		restarter ReinitStandbyRestartExecutor
		wantErr   error
	}{
		{
			name: "restarter is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededRecoveryConfiguredReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitStandbyRestartExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededRecoveryConfiguredReinitStore(t, now)
			},
			restarter: &recordingReinitStandbyRestarter{},
			wantErr:   ErrReinitTargetRequired,
		},
		{
			name: "recovery config must be completed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededWALGRestoredReinitStore(t, now)
			},
			member:    "alpha-2",
			restarter: &recordingReinitStandbyRestarter{},
			wantErr:   ErrReinitExecutionChanged,
		},
		{
			name: "pending restart must be set",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededRecoveryConfiguredReinitStore(t, now)
				status, ok := store.NodeStatus("alpha-2")
				if !ok {
					t.Fatal("expected alpha-2 status")
				}
				status.PendingRestart = false
				status.Postgres.Details.PendingRestart = false
				store.nodeStatuses["alpha-2"] = status
				return store
			},
			member:    "alpha-2",
			restarter: &recordingReinitStandbyRestarter{},
			wantErr:   ErrReinitRecoveryConfigRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitRestartAsStandby(context.Background(), testCase.member, testCase.restarter)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit standby restart error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitRestartAsStandbyRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 16, 0, 0, 0, time.UTC)
	store := seededRecoveryConfiguredReinitStore(t, now)

	restartErr := errors.New("pg_ctl start failed")
	_, err := store.ExecuteReinitRestartAsStandby(context.Background(), "alpha-2", &recordingReinitStandbyRestarter{err: restartErr})
	if !errors.Is(err, restartErr) {
		t.Fatalf("execute reinit standby restart error: got %v want %v", err, restartErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed standby restart to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}
