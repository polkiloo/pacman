package controlplane

import (
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestReinitOperationPhaseTransitions(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, time.June, 21, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name          string
		seed          cluster.Operation
		begin         func(cluster.Operation, time.Time) cluster.Operation
		canBegin      func(cluster.Operation) bool
		wantMessage   string
		wrongPrevious cluster.Operation
		terminalRetry cluster.Operation
	}{
		{
			name:          "stop postgres",
			seed:          reinitTransitionOperation(cluster.OperationStateAccepted, ""),
			begin:         beginReinitPostgresStop,
			canBegin:      canBeginReinitPostgresStop,
			wantMessage:   reinitPostgresStopRunningMessage("alpha-2"),
			wrongPrevious: reinitTransitionOperation(cluster.OperationStateRunning, reinitDataDirArchiveCompletedMessage("alpha-2")),
			terminalRetry: reinitTransitionOperation(cluster.OperationStateFailed, reinitPostgresStopRunningMessage("alpha-2")),
		},
		{
			name:          "archive data dir",
			seed:          reinitTransitionOperation(cluster.OperationStateRunning, reinitPostgresStopCompletedMessage("alpha-2")),
			begin:         beginReinitDataDirArchive,
			canBegin:      canBeginReinitDataDirArchive,
			wantMessage:   reinitDataDirArchiveRunningMessage("alpha-2"),
			wrongPrevious: reinitTransitionOperation(cluster.OperationStateAccepted, reinitPostgresStopCompletedMessage("alpha-2")),
			terminalRetry: reinitTransitionOperation(cluster.OperationStateFailed, reinitDataDirArchiveRunningMessage("alpha-2")),
		},
		{
			name:          "wal-g restore",
			seed:          reinitTransitionOperation(cluster.OperationStateRunning, reinitDataDirArchiveCompletedMessage("alpha-2")),
			begin:         beginReinitWALGRestore,
			canBegin:      canBeginReinitWALGRestore,
			wantMessage:   reinitWALGRestoreRunningMessage("alpha-2"),
			wrongPrevious: reinitTransitionOperation(cluster.OperationStateRunning, reinitPostgresStopCompletedMessage("alpha-2")),
			terminalRetry: reinitTransitionOperation(cluster.OperationStateFailed, reinitWALGRestoreRunningMessage("alpha-2")),
		},
		{
			name:          "recovery config",
			seed:          reinitTransitionOperation(cluster.OperationStateRunning, reinitWALGRestoreCompletedMessage("alpha-2")),
			begin:         beginReinitRecoveryConfig,
			canBegin:      canBeginReinitRecoveryConfig,
			wantMessage:   reinitRecoveryConfigRunningMessage("alpha-2"),
			wrongPrevious: reinitTransitionOperation(cluster.OperationStateRunning, reinitDataDirArchiveCompletedMessage("alpha-2")),
			terminalRetry: reinitTransitionOperation(cluster.OperationStateFailed, reinitRecoveryConfigRunningMessage("alpha-2")),
		},
		{
			name:          "standby restart",
			seed:          reinitTransitionOperation(cluster.OperationStateRunning, reinitRecoveryConfigCompletedMessage("alpha-2")),
			begin:         beginReinitStandbyRestart,
			canBegin:      canBeginReinitStandbyRestart,
			wantMessage:   reinitStandbyRestartRunningMessage("alpha-2", "alpha-1"),
			wrongPrevious: reinitTransitionOperation(cluster.OperationStateRunning, reinitWALGRestoreCompletedMessage("alpha-2")),
			terminalRetry: reinitTransitionOperation(cluster.OperationStateFailed, reinitStandbyRestartRunningMessage("alpha-2", "alpha-1")),
		},
		{
			name:          "replication verification",
			seed:          reinitTransitionOperation(cluster.OperationStateRunning, reinitStandbyRestartCompletedMessage("alpha-2", "alpha-1")),
			begin:         beginReinitReplicationVerification,
			canBegin:      canBeginReinitReplicationVerification,
			wantMessage:   reinitReplicationVerificationRunningMessage("alpha-2", "alpha-1"),
			wrongPrevious: reinitTransitionOperation(cluster.OperationStateRunning, reinitRecoveryConfigCompletedMessage("alpha-2")),
			terminalRetry: reinitTransitionOperation(cluster.OperationStateFailed, reinitReplicationVerificationRunningMessage("alpha-2", "alpha-1")),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if !testCase.canBegin(testCase.seed) {
				t.Fatalf("expected phase to begin from prerequisite operation: %+v", testCase.seed)
			}
			if testCase.canBegin(testCase.wrongPrevious) {
				t.Fatalf("expected phase to reject wrong prerequisite operation: %+v", testCase.wrongPrevious)
			}
			if testCase.canBegin(testCase.terminalRetry) {
				t.Fatalf("expected phase to reject terminal retry operation: %+v", testCase.terminalRetry)
			}

			updated := testCase.begin(testCase.seed, startedAt)
			if updated.State != cluster.OperationStateRunning || updated.Result != cluster.OperationResultPending {
				t.Fatalf("unexpected updated lifecycle: %+v", updated)
			}
			if !updated.StartedAt.Equal(startedAt) {
				t.Fatalf("startedAt: got %s want %s", updated.StartedAt, startedAt)
			}
			if updated.Message != testCase.wantMessage {
				t.Fatalf("message: got %q want %q", updated.Message, testCase.wantMessage)
			}
			if !testCase.canBegin(updated) {
				t.Fatalf("expected phase to be idempotently begin-able from its running state: %+v", updated)
			}

			alreadyStarted := testCase.seed
			alreadyStarted.StartedAt = startedAt.Add(-time.Minute)
			updated = testCase.begin(alreadyStarted, startedAt)
			if !updated.StartedAt.Equal(alreadyStarted.StartedAt) {
				t.Fatalf("expected existing start time to be preserved, got %s want %s", updated.StartedAt, alreadyStarted.StartedAt)
			}
		})
	}
}

func TestReinitCompletionTransitionMarksSucceededTerminalOperation(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, time.June, 21, 13, 0, 0, 0, time.UTC)
	operation := reinitTransitionOperation(
		cluster.OperationStateRunning,
		reinitReplicationVerificationRunningMessage("alpha-2", "alpha-1"),
	)
	operation.Result = cluster.OperationResultPending
	operation.StartedAt = completedAt.Add(-time.Minute)

	completed := completeReinitExecution(operation, completedAt, "alpha-2", "alpha-1")
	if completed.State != cluster.OperationStateCompleted || completed.Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected completed lifecycle: %+v", completed)
	}
	if !completed.CompletedAt.Equal(completedAt) || !completed.StartedAt.Equal(operation.StartedAt) {
		t.Fatalf("unexpected completed timestamps: %+v", completed)
	}
	if completed.Message != reinitCompletedMessage("alpha-2", "alpha-1") {
		t.Fatalf("completed message: got %q", completed.Message)
	}
}

func TestActiveReinitOperationLockedValidatesActiveOperationShape(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 21, 14, 0, 0, 0, time.UTC)

	testCases := []struct {
		name      string
		operation *cluster.Operation
		wantErr   error
	}{
		{name: "missing", wantErr: ErrReinitExecutionRequired},
		{
			name: "wrong kind",
			operation: &cluster.Operation{
				ID:          "switchover-1",
				Kind:        cluster.OperationKindSwitchover,
				State:       cluster.OperationStateRunning,
				RequestedAt: now,
				Result:      cluster.OperationResultPending,
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
			},
			wantErr: ErrReinitExecutionRequired,
		},
		{
			name:      "terminal",
			operation: reinitTransitionPointer(cluster.OperationStateCompleted, reinitCompletedMessage("alpha-2", "alpha-1")),
			wantErr:   ErrReinitExecutionRequired,
		},
		{
			name:      "missing from member",
			operation: reinitTransitionPointer(cluster.OperationStateRunning, reinitPostgresStopRunningMessage("alpha-2")),
			wantErr:   ErrReinitExecutionRequired,
		},
		{
			name:      "missing to member",
			operation: reinitTransitionPointer(cluster.OperationStateRunning, reinitPostgresStopRunningMessage("alpha-2")),
			wantErr:   ErrReinitExecutionRequired,
		},
		{
			name:      "accepted reinit",
			operation: reinitTransitionPointer(cluster.OperationStateAccepted, reinitOperationMessage("alpha-1", "alpha-2")),
		},
	}

	testCases[3].operation.FromMember = ""
	testCases[4].operation.ToMember = ""

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := &MemoryStateStore{activeOperation: testCase.operation}
			operation, err := store.activeReinitOperationLocked()
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("active reinit operation error: got %v want %v", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("active reinit operation: %v", err)
			}
			if operation.ID != testCase.operation.ID || operation.Kind != cluster.OperationKindReinit {
				t.Fatalf("unexpected active reinit operation: %+v", operation)
			}

			operation.Message = "mutated"
			if store.activeOperation.Message == "mutated" {
				t.Fatal("expected active operation clone to be detached from store")
			}
		})
	}
}

func TestReinitOperationForPublicationLockedRequiresSameActiveOperation(t *testing.T) {
	t.Parallel()

	active := reinitTransitionOperation(cluster.OperationStateRunning, reinitPostgresStopRunningMessage("alpha-2"))
	store := &MemoryStateStore{activeOperation: &active}

	if _, err := store.reinitOperationForPublicationLocked(reinitTransitionOperation(cluster.OperationStateRunning, reinitPostgresStopRunningMessage("alpha-2"))); err != nil {
		t.Fatalf("expected matching active operation to publish: %v", err)
	}

	mismatchedID := active
	mismatchedID.ID = "reinit-other"
	if _, err := store.reinitOperationForPublicationLocked(mismatchedID); !errors.Is(err, ErrReinitExecutionChanged) {
		t.Fatalf("mismatched id error: got %v want %v", err, ErrReinitExecutionChanged)
	}

	wrongKind := active
	wrongKind.Kind = cluster.OperationKindSwitchover
	store.activeOperation = &wrongKind
	if _, err := store.reinitOperationForPublicationLocked(active); !errors.Is(err, ErrReinitExecutionChanged) {
		t.Fatalf("wrong kind error: got %v want %v", err, ErrReinitExecutionChanged)
	}

	store.activeOperation = nil
	if _, err := store.reinitOperationForPublicationLocked(active); !errors.Is(err, ErrReinitExecutionChanged) {
		t.Fatalf("missing active operation error: got %v want %v", err, ErrReinitExecutionChanged)
	}
}

func reinitTransitionOperation(state cluster.OperationState, message string) cluster.Operation {
	return cluster.Operation{
		ID:          "reinit-1",
		Kind:        cluster.OperationKindReinit,
		State:       state,
		RequestedAt: time.Date(2026, time.June, 21, 11, 30, 0, 0, time.UTC),
		Result:      cluster.OperationResultPending,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		Message:     message,
	}
}

func reinitTransitionPointer(state cluster.OperationState, message string) *cluster.Operation {
	operation := reinitTransitionOperation(state, message)
	return &operation
}
