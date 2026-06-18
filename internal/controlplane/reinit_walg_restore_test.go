package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreExecuteReinitWALGRestoreRestoresAfterArchive(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 12, 0, 0, 0, time.UTC)
	store := seededArchivedReinitStore(t, now)

	restorer := &recordingReinitWALGRestorer{
		result: ReinitWALGRestoreResult{
			DataDir:    "/var/lib/postgresql/data",
			BackupName: "LATEST",
		},
	}
	execution, err := store.ExecuteReinitWALGRestore(context.Background(), " alpha-2 ", restorer)
	if err != nil {
		t.Fatalf("execute reinit WAL-G restore: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || !execution.WALGRestored || execution.WALGBackupName != "LATEST" {
		t.Fatalf("unexpected reinit WAL-G restore execution: %+v", execution)
	}
	if len(restorer.requests) != 1 {
		t.Fatalf("expected one WAL-G restore request, got %+v", restorer.requests)
	}
	request := restorer.requests[0]
	if request.Operation.Kind != cluster.OperationKindReinit || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected WAL-G restore request: %+v", request)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after WAL-G restore")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active operation after WAL-G restore: %+v", active)
	}
	if active.Message != reinitWALGRestoreCompletedMessage("alpha-2") {
		t.Fatalf("unexpected WAL-G restore operation message: %+v", active)
	}
}

func TestMemoryStateStoreExecuteReinitWALGRestoreRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 12, 30, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		prepare  func(t *testing.T) *MemoryStateStore
		member   string
		restorer ReinitWALGRestoreExecutor
		wantErr  error
	}{
		{
			name: "restorer is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededArchivedReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitWALGRestoreExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededArchivedReinitStore(t, now)
			},
			restorer: &recordingReinitWALGRestorer{},
			wantErr:  ErrReinitTargetRequired,
		},
		{
			name: "archive must be completed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededStoppedReinitStore(t, now)
			},
			member:   "alpha-2",
			restorer: &recordingReinitWALGRestorer{},
			wantErr:  ErrReinitExecutionChanged,
		},
		{
			name: "already restored blocks repeat",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededArchivedReinitStore(t, now)
				if _, err := store.ExecuteReinitWALGRestore(context.Background(), "alpha-2", &recordingReinitWALGRestorer{}); err != nil {
					t.Fatalf("restore from WAL-G: %v", err)
				}
				return store
			},
			member:   "alpha-2",
			restorer: &recordingReinitWALGRestorer{},
			wantErr:  ErrReinitExecutionChanged,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitWALGRestore(context.Background(), testCase.member, testCase.restorer)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit WAL-G restore error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitWALGRestoreRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 13, 0, 0, 0, time.UTC)
	store := seededArchivedReinitStore(t, now)

	restoreErr := errors.New("wal-g backup-fetch failed")
	_, err := store.ExecuteReinitWALGRestore(context.Background(), "alpha-2", &recordingReinitWALGRestorer{err: restoreErr})
	if !errors.Is(err, restoreErr) {
		t.Fatalf("execute reinit WAL-G restore error: got %v want %v", err, restoreErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed WAL-G restore to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}
