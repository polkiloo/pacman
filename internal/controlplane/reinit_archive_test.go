package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreExecuteReinitArchiveDataDirArchivesAfterStop(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 10, 30, 0, 0, time.UTC)
	store := seededStoppedReinitStore(t, now)

	archiver := &recordingReinitDataDirArchiver{
		result: ReinitDataDirArchiveResult{
			DataDir:     "/var/lib/postgresql/data",
			ArchivePath: "/var/lib/postgresql/.pacman-reinit-archive/data-op-1",
			Archived:    true,
		},
	}
	execution, err := store.ExecuteReinitArchiveDataDir(context.Background(), " alpha-2 ", archiver)
	if err != nil {
		t.Fatalf("execute reinit data dir archive: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || execution.ArchivePath != archiver.result.ArchivePath {
		t.Fatalf("unexpected reinit archive execution: %+v", execution)
	}
	if len(archiver.requests) != 1 {
		t.Fatalf("expected one data dir archive request, got %+v", archiver.requests)
	}
	request := archiver.requests[0]
	if request.Operation.Kind != cluster.OperationKindReinit || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected archive request: %+v", request)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after archive")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active operation after archive: %+v", active)
	}
	if active.Message != reinitDataDirArchiveCompletedMessage("alpha-2") {
		t.Fatalf("unexpected archive operation message: %+v", active)
	}
}

func TestMemoryStateStoreExecuteReinitArchiveDataDirRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 11, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		prepare  func(t *testing.T) *MemoryStateStore
		member   string
		archiver ReinitDataDirArchiveExecutor
		wantErr  error
	}{
		{
			name: "archiver is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededStoppedReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitDataDirArchiveExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededStoppedReinitStore(t, now)
			},
			archiver: &recordingReinitDataDirArchiver{},
			wantErr:  ErrReinitTargetRequired,
		},
		{
			name: "postgres must be stopped",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededReinitIntentStore(t, now)
			},
			member:   "alpha-2",
			archiver: &recordingReinitDataDirArchiver{},
			wantErr:  ErrReinitExecutionChanged,
		},
		{
			name: "already archived blocks repeat",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededStoppedReinitStore(t, now)
				if _, err := store.ExecuteReinitArchiveDataDir(context.Background(), "alpha-2", &recordingReinitDataDirArchiver{}); err != nil {
					t.Fatalf("archive data dir: %v", err)
				}
				return store
			},
			member:   "alpha-2",
			archiver: &recordingReinitDataDirArchiver{},
			wantErr:  ErrReinitExecutionChanged,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitArchiveDataDir(context.Background(), testCase.member, testCase.archiver)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit archive error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitArchiveDataDirRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 11, 30, 0, 0, time.UTC)
	store := seededStoppedReinitStore(t, now)

	archiveErr := errors.New("rename failed")
	_, err := store.ExecuteReinitArchiveDataDir(context.Background(), "alpha-2", &recordingReinitDataDirArchiver{err: archiveErr})
	if !errors.Is(err, archiveErr) {
		t.Fatalf("execute reinit archive error: got %v want %v", err, archiveErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed archive to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}
