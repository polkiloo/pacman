package controlplane

import (
	"context"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func (store *MemoryStateStore) ExecuteReinitWALGRestore(ctx context.Context, member string, restorer ReinitWALGRestoreExecutor) (ReinitExecution, error) {
	if err := ctx.Err(); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitExecution{}, err
	}

	prepared, err := store.prepareReinitWALGRestore(member, restorer)
	if err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitExecution{}, err
	}

	result, err := restorer.RestoreFromWALG(ctx, buildReinitWALGRestoreRequest(prepared))
	if err != nil {
		store.failReinitExecution(prepared, reinitWALGRestoreFailedMessage(prepared.validation.Target.Name))
		return ReinitExecution{}, err
	}
	prepared.walgBackupName = result.BackupName

	return store.publishReinitWALGRestored(prepared)
}

func (store *MemoryStateStore) prepareReinitWALGRestore(member string, restorer ReinitWALGRestoreExecutor) (preparedReinitExecution, error) {
	if restorer == nil {
		return preparedReinitExecution{}, ErrReinitWALGRestoreExecutorRequired
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
	if !canBeginReinitWALGRestore(operation) {
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
	validation, err := evaluateReinitExecutionRequest(status, request, executedAt)
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
	if targetNode.Postgres.Managed && targetNode.Postgres.Up {
		return preparedReinitExecution{}, ErrReinitPostgresStopRequired
	}

	updated := beginReinitWALGRestore(operation, executedAt)
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

// ExecuteReinitRecoveryConfig renders PostgreSQL recovery settings, including
// WAL-G restore_command, into the restored data directory before PostgreSQL is
// allowed to start.

func beginReinitWALGRestore(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = reinitWALGRestoreRunningMessage(updated.ToMember)

	return updated
}

func canBeginReinitWALGRestore(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitDataDirArchiveCompletedMessage(operation.ToMember) ||
			operation.Message == reinitWALGRestoreRunningMessage(operation.ToMember))
}

func buildReinitWALGRestoreRequest(prepared preparedReinitExecution) ReinitWALGRestoreRequest {
	return ReinitWALGRestoreRequest{
		Operation:          prepared.operation.Clone(),
		Validation:         prepared.validation.Clone(),
		TargetNode:         prepared.targetNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
	}
}

func (store *MemoryStateStore) publishReinitWALGRestored(prepared preparedReinitExecution) (ReinitExecution, error) {
	store.mu.Lock()
	running, err := store.reinitOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return ReinitExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = reinitWALGRestoreCompletedMessage(prepared.validation.Target.Name)
	store.activeOperation = &updatedOperation
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	store.mu.Unlock()

	if err := store.persistActiveOperation(context.Background(), updatedOperation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return ReinitExecution{}, err
	}

	return ReinitExecution{
		Operation:       updatedOperation.Clone(),
		Validation:      prepared.validation.Clone(),
		CurrentEpoch:    prepared.currentEpoch,
		PostgresStopped: true,
		DataDirArchived: true,
		WALGRestored:    true,
		WALGBackupName:  prepared.walgBackupName,
		ExecutedAt:      prepared.executedAt,
	}.Clone(), nil
}

func reinitWALGRestoreRunningMessage(member string) string {
	return "restoring PostgreSQL data directory from WAL-G on reinit target " + member
}

func reinitWALGRestoreCompletedMessage(member string) string {
	return "PostgreSQL data directory restored from WAL-G on reinit target " + member + "; recovery configuration is pending"
}

func reinitWALGRestoreFailedMessage(member string) string {
	return "failed to restore PostgreSQL data directory from WAL-G on reinit target " + member
}
