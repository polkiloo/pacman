package controlplane

import (
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestReinitStatusFromActiveOperationMapsLifecyclePhases(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, time.June, 19, 9, 0, 0, 0, time.UTC)
	startedAt := requestedAt.Add(time.Minute)
	completedAt := requestedAt.Add(2 * time.Minute)

	testCases := []struct {
		name    string
		mutate  func(cluster.Operation) cluster.Operation
		want    cluster.ReinitState
		updated time.Time
	}{
		{
			name: "accepted",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.State = cluster.OperationStateAccepted
				operation.StartedAt = time.Time{}
				return operation
			},
			want:    cluster.ReinitStateAccepted,
			updated: requestedAt,
		},
		{
			name: "stopping postgres",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.Message = reinitPostgresStopRunningMessage(operation.ToMember)
				return operation
			},
			want:    cluster.ReinitStateStoppingPostgres,
			updated: startedAt,
		},
		{
			name: "archiving data dir",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.Message = reinitDataDirArchiveCompletedMessage(operation.ToMember)
				return operation
			},
			want:    cluster.ReinitStateArchivingDataDir,
			updated: startedAt,
		},
		{
			name: "restoring backup",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.Message = reinitWALGRestoreRunningMessage(operation.ToMember)
				return operation
			},
			want:    cluster.ReinitStateRestoringBackup,
			updated: startedAt,
		},
		{
			name: "rendering recovery config",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.Message = reinitRecoveryConfigCompletedMessage(operation.ToMember)
				return operation
			},
			want:    cluster.ReinitStateRenderingRecoveryConfig,
			updated: startedAt,
		},
		{
			name: "restarting standby",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.Message = reinitStandbyRestartRunningMessage(operation.ToMember, operation.FromMember)
				return operation
			},
			want:    cluster.ReinitStateRestartingStandby,
			updated: startedAt,
		},
		{
			name: "verifying replication",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.Message = reinitReplicationVerificationRunningMessage(operation.ToMember, operation.FromMember)
				return operation
			},
			want:    cluster.ReinitStateVerifyingReplication,
			updated: startedAt,
		},
		{
			name: "completed",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.State = cluster.OperationStateCompleted
				operation.Result = cluster.OperationResultSucceeded
				operation.CompletedAt = completedAt
				return operation
			},
			want:    cluster.ReinitStateCompleted,
			updated: completedAt,
		},
		{
			name: "failed",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.State = cluster.OperationStateFailed
				operation.Result = cluster.OperationResultFailed
				operation.CompletedAt = completedAt
				return operation
			},
			want:    cluster.ReinitStateFailed,
			updated: completedAt,
		},
		{
			name: "cancelled",
			mutate: func(operation cluster.Operation) cluster.Operation {
				operation.State = cluster.OperationStateCancelled
				operation.Result = cluster.OperationResultCancelled
				operation.CompletedAt = completedAt
				return operation
			},
			want:    cluster.ReinitStateCancelled,
			updated: completedAt,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			operation := testCase.mutate(cluster.Operation{
				ID:          "reinit-1",
				Kind:        cluster.OperationKindReinit,
				State:       cluster.OperationStateRunning,
				RequestedAt: requestedAt,
				StartedAt:   startedAt,
				Result:      cluster.OperationResultPending,
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
			})

			status := reinitStatusFromActiveOperation(&operation)
			if status == nil {
				t.Fatal("expected reinit status")
			}
			if status.State != testCase.want || !status.UpdatedAt.Equal(testCase.updated) {
				t.Fatalf("unexpected reinit status: got %+v want state=%q updated=%s", status, testCase.want, testCase.updated)
			}
		})
	}
}

func TestReinitStatusFromActiveOperationDefaultsPendingAndIgnoresOtherKinds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 19, 10, 0, 0, 0, time.UTC)
	operation := cluster.Operation{
		ID:          "reinit-1",
		Kind:        cluster.OperationKindReinit,
		State:       cluster.OperationStateRunning,
		RequestedAt: now,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		Message:     "unknown running reinit phase",
	}

	status := reinitStatusFromActiveOperation(&operation)
	if status == nil {
		t.Fatal("expected reinit status")
	}
	if status.State != cluster.ReinitStateAccepted || status.LastResult != cluster.OperationResultPending || !status.UpdatedAt.Equal(now) {
		t.Fatalf("unexpected defaulted active reinit status: %+v", status)
	}

	operation.Kind = cluster.OperationKindSwitchover
	if status := reinitStatusFromActiveOperation(&operation); status != nil {
		t.Fatalf("expected non-reinit operation to be ignored, got %+v", status)
	}
}

func TestLatestReinitStatusPrefersActiveOperationOverHistory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 19, 11, 0, 0, 0, time.UTC)
	store := &MemoryStateStore{
		activeOperation: &cluster.Operation{
			ID:          "reinit-active",
			Kind:        cluster.OperationKindReinit,
			State:       cluster.OperationStateRunning,
			RequestedAt: now,
			StartedAt:   now.Add(time.Minute),
			Result:      cluster.OperationResultPending,
			FromMember:  "alpha-1",
			ToMember:    "alpha-3",
			Message:     reinitWALGRestoreRunningMessage("alpha-3"),
		},
		history: []cluster.HistoryEntry{
			{
				OperationID: "reinit-history",
				Kind:        cluster.OperationKindReinit,
				Result:      cluster.OperationResultSucceeded,
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				FinishedAt:  now.Add(-time.Hour),
			},
		},
	}

	status := store.latestReinitStatusLocked()
	assertReinitStatus(t, status, "reinit-active", cluster.ReinitStateRestoringBackup, cluster.OperationResultPending, "alpha-1", "alpha-3")
}

func TestLatestReinitStatusUsesNewestReinitHistoryEntry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 19, 12, 0, 0, 0, time.UTC)
	store := &MemoryStateStore{
		history: []cluster.HistoryEntry{
			{
				OperationID: "reinit-old",
				Kind:        cluster.OperationKindReinit,
				Result:      cluster.OperationResultSucceeded,
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				FinishedAt:  now.Add(-2 * time.Hour),
			},
			{
				OperationID: "failover-newer",
				Kind:        cluster.OperationKindFailover,
				Result:      cluster.OperationResultSucceeded,
				FromMember:  "alpha-1",
				ToMember:    "alpha-3",
				FinishedAt:  now.Add(-time.Hour),
			},
			{
				OperationID: "reinit-new",
				Kind:        cluster.OperationKindReinit,
				Result:      cluster.OperationResultFailed,
				FromMember:  "alpha-1",
				ToMember:    "alpha-3",
				FinishedAt:  now,
			},
		},
	}

	status := store.latestReinitStatusLocked()
	assertReinitStatus(t, status, "reinit-new", cluster.ReinitStateFailed, cluster.OperationResultFailed, "alpha-1", "alpha-3")
}

func TestReinitStatusForMemberScopesActiveAndHistoryByTarget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 19, 13, 0, 0, 0, time.UTC)
	store := &MemoryStateStore{
		activeOperation: &cluster.Operation{
			ID:          "reinit-active",
			Kind:        cluster.OperationKindReinit,
			State:       cluster.OperationStateRunning,
			RequestedAt: now,
			StartedAt:   now.Add(time.Minute),
			Result:      cluster.OperationResultPending,
			FromMember:  "alpha-1",
			ToMember:    "alpha-3",
			Message:     reinitPostgresStopCompletedMessage("alpha-3"),
		},
		history: []cluster.HistoryEntry{
			{
				OperationID: "reinit-alpha-2",
				Kind:        cluster.OperationKindReinit,
				Result:      cluster.OperationResultSucceeded,
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				FinishedAt:  now.Add(-time.Hour),
			},
		},
	}

	activeTargetStatus := store.reinitStatusForMemberLocked(" alpha-3 ")
	assertReinitStatus(t, activeTargetStatus, "reinit-active", cluster.ReinitStateStoppingPostgres, cluster.OperationResultPending, "alpha-1", "alpha-3")

	historyTargetStatus := store.reinitStatusForMemberLocked("alpha-2")
	assertReinitStatus(t, historyTargetStatus, "reinit-alpha-2", cluster.ReinitStateCompleted, cluster.OperationResultSucceeded, "alpha-1", "alpha-2")

	if status := store.reinitStatusForMemberLocked("alpha-4"); status != nil {
		t.Fatalf("expected no reinit status for unrelated member, got %+v", status)
	}
	if status := store.reinitStatusForMemberLocked(" "); status != nil {
		t.Fatalf("expected no reinit status for blank member, got %+v", status)
	}
}

func TestReinitStateFromHistoryEntryMapsTerminalResults(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		result cluster.OperationResult
		want   cluster.ReinitState
	}{
		{name: "succeeded", result: cluster.OperationResultSucceeded, want: cluster.ReinitStateCompleted},
		{name: "failed", result: cluster.OperationResultFailed, want: cluster.ReinitStateFailed},
		{name: "cancelled", result: cluster.OperationResultCancelled, want: cluster.ReinitStateCancelled},
		{name: "pending fallback", result: cluster.OperationResultPending, want: cluster.ReinitStateFailed},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			entry := cluster.HistoryEntry{Result: testCase.result}
			if got := reinitStateFromHistoryEntry(entry); got != testCase.want {
				t.Fatalf("unexpected reinit history state: got %q want %q", got, testCase.want)
			}
		})
	}
}
