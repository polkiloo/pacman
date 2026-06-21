package controlplane

import (
	"context"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func (store *MemoryStateStore) ExecuteReinitVerifyReplication(ctx context.Context, member string, verifier ReinitReplicationVerifier) (ReinitExecution, error) {
	if err := ctx.Err(); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitExecution{}, err
	}

	prepared, err := store.prepareReinitReplicationVerification(member, verifier)
	if err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitExecution{}, err
	}

	result, err := verifier.VerifyReinitReplication(ctx, buildReinitReplicationVerificationRequest(prepared))
	if err != nil {
		return ReinitExecution{}, err
	}
	prepared.verification = result

	if reasons := assessReinitReplicationVerificationReasons(prepared); len(reasons) > 0 {
		return ReinitExecution{}, ErrReinitReplicationNotHealthy
	}

	return store.publishReinitReplicationVerified(prepared)
}

func (store *MemoryStateStore) prepareReinitReplicationVerification(member string, verifier ReinitReplicationVerifier) (preparedReinitExecution, error) {
	if verifier == nil {
		return preparedReinitExecution{}, ErrReinitReplicationVerifierRequired
	}

	targetName := strings.TrimSpace(member)
	if targetName == "" {
		return preparedReinitExecution{}, ErrReinitTargetRequired
	}

	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	operation, err := store.activeReinitOperationLocked()
	if err != nil {
		return preparedReinitExecution{}, err
	}

	if operation.ToMember != targetName {
		return preparedReinitExecution{}, ErrReinitExecutionChanged
	}
	if !canBeginReinitReplicationVerification(operation) {
		return preparedReinitExecution{}, ErrReinitExecutionChanged
	}

	_, status, err := store.reinitInputsLocked()
	if err != nil {
		return preparedReinitExecution{}, err
	}

	request := ReinitRequest{
		Member:      operation.ToMember,
		RequestedBy: operation.RequestedBy,
		Reason:      operation.Reason,
	}
	validation, err := evaluateReinitRequest(status, request, nil, executedAt)
	if err != nil {
		return preparedReinitExecution{}, err
	}
	if validation.CurrentPrimary.Name != operation.FromMember || validation.Target.Name != operation.ToMember {
		return preparedReinitExecution{}, ErrReinitExecutionChanged
	}

	targetNode, hasTargetNode := store.nodeStatuses[operation.ToMember]
	currentPrimaryNode, hasCurrentPrimaryNode := store.nodeStatuses[operation.FromMember]
	if !hasTargetNode || !hasCurrentPrimaryNode {
		return preparedReinitExecution{}, ErrReinitExecutionChanged
	}

	updated := beginReinitReplicationVerification(operation, executedAt)
	store.journalOperationLocked(updated, executedAt)
	store.refreshSourceOfTruthLocked(executedAt)

	return preparedReinitExecution{
		validation:         validation.Clone(),
		targetNode:         targetNode.Clone(),
		currentPrimaryNode: currentPrimaryNode.Clone(),
		operation:          updated.Clone(),
		currentEpoch:       status.CurrentEpoch,
		executedAt:         executedAt,
	}, nil
}

func beginReinitReplicationVerification(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = reinitReplicationVerificationRunningMessage(updated.ToMember, updated.FromMember)

	return updated
}

func canBeginReinitReplicationVerification(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitStandbyRestartCompletedMessage(operation.ToMember, operation.FromMember) ||
			operation.Message == reinitReplicationVerificationRunningMessage(operation.ToMember, operation.FromMember))
}

func buildReinitReplicationVerificationRequest(prepared preparedReinitExecution) ReinitReplicationVerificationRequest {
	return ReinitReplicationVerificationRequest{
		Operation:               prepared.operation.Clone(),
		Validation:              prepared.validation.Clone(),
		TargetNode:              prepared.targetNode.Clone(),
		CurrentPrimaryNode:      prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:            prepared.currentEpoch,
		ExpectedPrimarySlotName: rejoinPrimarySlotName(prepared.validation.Target.Name),
	}
}

func (store *MemoryStateStore) publishReinitReplicationVerified(prepared preparedReinitExecution) (ReinitExecution, error) {
	store.mu.Lock()
	running, err := store.reinitOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return ReinitExecution{}, err
	}

	updatedOperation := completeReinitExecution(running, prepared.executedAt, prepared.validation.Target.Name, prepared.validation.CurrentPrimary.Name)
	store.journalOperationLocked(updatedOperation, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	store.mu.Unlock()

	if err := store.persistJournaledOperation(context.Background(), updatedOperation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return ReinitExecution{}, err
	}

	return ReinitExecution{
		Operation:           updatedOperation.Clone(),
		Validation:          prepared.validation.Clone(),
		CurrentEpoch:        prepared.currentEpoch,
		PostgresStopped:     true,
		DataDirArchived:     true,
		WALGRestored:        true,
		WALGBackupName:      prepared.verification.BackupName,
		RecoveryConfig:      true,
		RestartedAsStandby:  true,
		ReplicationVerified: true,
		SystemIdentifier:    prepared.verification.SystemIdentifier,
		Timeline:            prepared.verification.Timeline,
		PrimarySlotName:     prepared.verification.PrimarySlotName,
		WALReceiverStatus:   prepared.verification.WALReceiverStatus,
		ExecutedAt:          prepared.executedAt,
	}.Clone(), nil
}

func completeReinitExecution(operation cluster.Operation, completedAt time.Time, member, currentPrimary string) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateCompleted
	updated.CompletedAt = completedAt
	updated.Result = cluster.OperationResultSucceeded
	updated.Message = reinitCompletedMessage(member, currentPrimary)

	return updated
}

func assessReinitReplicationVerificationReasons(prepared preparedReinitExecution) []string {
	var reasons []string

	if prepared.targetNode.PendingRestart || prepared.targetNode.Postgres.Details.PendingRestart {
		reasons = append(reasons, reasonRejoinRestartPending)
	}
	if prepared.targetNode.Role != cluster.MemberRoleReplica {
		reasons = append(reasons, reasonRoleNotStandby)
	}
	if prepared.targetNode.State != cluster.MemberStateStreaming {
		reasons = append(reasons, reasonReinitWALReceiverNotStreaming)
	}
	if !prepared.targetNode.Postgres.Up {
		reasons = append(reasons, reasonPostgresNotUp)
	}
	if !prepared.targetNode.Postgres.RecoveryKnown {
		reasons = append(reasons, reasonRecoveryStateUnknown)
	}
	if !prepared.targetNode.Postgres.InRecovery || !prepared.verification.InRecovery {
		reasons = append(reasons, reasonNotInRecovery)
	}
	if prepared.targetNode.Postgres.Role != cluster.MemberRoleReplica {
		reasons = append(reasons, reasonPostgresRoleNotStandby)
	}

	targetSystemIdentifier := strings.TrimSpace(prepared.targetNode.Postgres.Details.SystemIdentifier)
	currentPrimarySystemIdentifier := strings.TrimSpace(prepared.currentPrimaryNode.Postgres.Details.SystemIdentifier)
	verifiedSystemIdentifier := strings.TrimSpace(prepared.verification.SystemIdentifier)
	switch {
	case targetSystemIdentifier == "" || verifiedSystemIdentifier == "":
		reasons = append(reasons, reasonMemberSystemIdentifierUnknown)
	case currentPrimarySystemIdentifier == "":
		reasons = append(reasons, reasonCurrentPrimarySystemIdentifierUnknown)
	case targetSystemIdentifier != currentPrimarySystemIdentifier || verifiedSystemIdentifier != currentPrimarySystemIdentifier:
		reasons = append(reasons, reasonSystemIdentifierMismatch)
	}

	switch {
	case prepared.targetNode.Postgres.Details.Timeline == 0 || prepared.verification.Timeline == 0:
		reasons = append(reasons, reasonMemberTimelineUnknown)
	case prepared.currentPrimaryNode.Postgres.Details.Timeline == 0:
		reasons = append(reasons, reasonCurrentPrimaryTimelineUnknown)
	case prepared.targetNode.Postgres.Details.Timeline != prepared.currentPrimaryNode.Postgres.Details.Timeline ||
		prepared.verification.Timeline != prepared.currentPrimaryNode.Postgres.Details.Timeline:
		reasons = append(reasons, reasonTimelineMismatch)
	}

	if strings.TrimSpace(prepared.verification.BackupName) == "" {
		reasons = append(reasons, reasonReinitBackupMetadataUnknown)
	}
	if prepared.verification.PrimarySlotName != rejoinPrimarySlotName(prepared.validation.Target.Name) {
		reasons = append(reasons, reasonReinitPrimarySlotMismatch)
	}
	if prepared.verification.WALReceiverStatus != "streaming" {
		reasons = append(reasons, reasonReinitWALReceiverNotStreaming)
	}

	return reasons
}

func reinitReplicationVerificationRunningMessage(member, currentPrimary string) string {
	return "verifying reinit target " + member + " is streaming from " + currentPrimary
}

func reinitCompletedMessage(member, currentPrimary string) string {
	return "reinit completed for " + member + " following " + currentPrimary
}
