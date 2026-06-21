package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreExecuteReinitVerifyReplicationRecordsHistory(t *testing.T) {
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
	if execution.Operation.State != cluster.OperationStateCompleted || execution.Operation.Result != cluster.OperationResultSucceeded || execution.Operation.CompletedAt.IsZero() {
		t.Fatalf("expected completed reinit operation, got %+v", execution.Operation)
	}
	if len(verifier.requests) != 1 {
		t.Fatalf("expected one reinit verification request, got %+v", verifier.requests)
	}
	request := verifier.requests[0]
	if request.ExpectedPrimarySlotName != "alpha_2" || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected reinit verification request: %+v", request)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected completed reinit to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one completed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected completed reinit history entry: %+v", history[0])
	}
	if history[0].Timeline != 21 {
		t.Fatalf("unexpected completed reinit history timeline: %+v", history[0])
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after completed reinit")
	}
	assertReinitStatus(t, status.Reinit, execution.Operation.ID, cluster.ReinitStateCompleted, cluster.OperationResultSucceeded, "alpha-1", "alpha-2")
	target := memberStatusByName(t, status.Members, "alpha-2")
	assertReinitStatus(t, target.Reinit, execution.Operation.ID, cluster.ReinitStateCompleted, cluster.OperationResultSucceeded, "alpha-1", "alpha-2")
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

func assertReinitStatus(t *testing.T, status *cluster.ReinitStatus, operationID string, state cluster.ReinitState, result cluster.OperationResult, fromMember, toMember string) {
	t.Helper()

	if status == nil {
		t.Fatal("expected reinit status")
	}
	if status.OperationID != operationID || status.State != state || status.LastResult != result || status.FromMember != fromMember || status.ToMember != toMember || status.UpdatedAt.IsZero() {
		t.Fatalf("unexpected reinit status: %+v", status)
	}
}

func memberStatusByName(t *testing.T, members []cluster.MemberStatus, name string) cluster.MemberStatus {
	t.Helper()

	for _, member := range members {
		if member.Name == name {
			return member
		}
	}
	t.Fatalf("member %q not found in %+v", name, members)
	return cluster.MemberStatus{}
}
