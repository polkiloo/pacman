package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreExecuteReinitVerifyReplicationKeepsOperationRunning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 18, 10, 0, 0, 0, time.UTC)
	store := seededRestartedReinitStore(t, now)
	publishVerifiedReinitReplica(t, store, now.Add(30*time.Second))

	verifier := &recordingReinitReplicationVerifier{
		result: ReinitReplicationVerificationResult{
			SystemIdentifier:  "sys-alpha",
			Timeline:          21,
			BackupName:        "LATEST",
			PrimarySlotName:   "alpha_2",
			WALReceiverStatus: "streaming",
			InRecovery:        true,
		},
	}

	execution, err := store.ExecuteReinitVerifyReplication(context.Background(), " alpha-2 ", verifier)
	if err != nil {
		t.Fatalf("verify reinit replication: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || !execution.WALGRestored || !execution.RecoveryConfig || !execution.RestartedAsStandby || !execution.ReplicationVerified {
		t.Fatalf("unexpected reinit verification execution: %+v", execution)
	}
	if execution.WALGBackupName != "LATEST" || execution.PrimarySlotName != "alpha_2" || execution.WALReceiverStatus != "streaming" || execution.SystemIdentifier != "sys-alpha" || execution.Timeline != 21 {
		t.Fatalf("unexpected verification metadata: %+v", execution)
	}
	if len(verifier.requests) != 1 {
		t.Fatalf("expected one reinit verification request, got %+v", verifier.requests)
	}
	request := verifier.requests[0]
	if request.ExpectedPrimarySlotName != "alpha_2" || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected reinit verification request: %+v", request)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after verification")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active reinit operation after verification: %+v", active)
	}
	if active.Message != reinitReplicationVerificationCompletedMessage("alpha-2", "alpha-1") {
		t.Fatalf("unexpected reinit verification message: %+v", active)
	}
	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected verified reinit to keep operation active without history, got %+v", history)
	}
}

func TestMemoryStateStoreExecuteReinitVerifyReplicationRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 18, 10, 30, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		prepare  func(t *testing.T) *MemoryStateStore
		member   string
		verifier ReinitReplicationVerifier
		wantErr  error
	}{
		{
			name: "verifier is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededRestartedReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitReplicationVerifierRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededRestartedReinitStore(t, now)
			},
			verifier: &recordingReinitReplicationVerifier{},
			wantErr:  ErrReinitTargetRequired,
		},
		{
			name: "restart must be completed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededRecoveryConfiguredReinitStore(t, now)
			},
			member:   "alpha-2",
			verifier: &recordingReinitReplicationVerifier{},
			wantErr:  ErrReinitExecutionChanged,
		},
		{
			name: "streaming state is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededRestartedReinitStore(t, now)
			},
			member: "alpha-2",
			verifier: &recordingReinitReplicationVerifier{
				result: ReinitReplicationVerificationResult{
					SystemIdentifier:  "sys-alpha",
					Timeline:          21,
					BackupName:        "LATEST",
					PrimarySlotName:   "alpha_2",
					WALReceiverStatus: "streaming",
					InRecovery:        true,
				},
			},
			wantErr: ErrReinitReplicationNotHealthy,
		},
		{
			name: "slot attachment is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededRestartedReinitStore(t, now)
				publishVerifiedReinitReplica(t, store, now.Add(30*time.Second))
				return store
			},
			member: "alpha-2",
			verifier: &recordingReinitReplicationVerifier{
				result: ReinitReplicationVerificationResult{
					SystemIdentifier:  "sys-alpha",
					Timeline:          21,
					BackupName:        "LATEST",
					PrimarySlotName:   "wrong_slot",
					WALReceiverStatus: "streaming",
					InRecovery:        true,
				},
			},
			wantErr: ErrReinitReplicationNotHealthy,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitVerifyReplication(context.Background(), testCase.member, testCase.verifier)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit verification error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}
