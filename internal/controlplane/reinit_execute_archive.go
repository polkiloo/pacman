package controlplane

import (
	"context"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func (store *MemoryStateStore) ExecuteReinitArchiveDataDir(ctx context.Context, member string, archiver ReinitDataDirArchiveExecutor) (ReinitExecution, error) {
	if err := ctx.Err(); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitExecution{}, err
	}

	prepared, err := store.prepareReinitDataDirArchive(member, archiver)
	if err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitExecution{}, err
	}

	result, err := archiver.ArchiveDataDir(ctx, buildReinitDataDirArchiveRequest(prepared))
	if err != nil {
		store.failReinitExecution(prepared, reinitDataDirArchiveFailedMessage(prepared.validation.Target.Name))
		return ReinitExecution{}, err
	}
	prepared.archivePath = result.ArchivePath

	return store.publishReinitDataDirArchived(prepared)
}

func (store *MemoryStateStore) prepareReinitDataDirArchive(member string, archiver ReinitDataDirArchiveExecutor) (preparedReinitExecution, error) {
	if archiver == nil {
		return preparedReinitExecution{}, ErrReinitDataDirArchiveExecutorRequired
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
	if !canBeginReinitDataDirArchive(operation) {
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
	if targetNode.Postgres.Managed && targetNode.Postgres.Up {
		return preparedReinitExecution{}, ErrReinitPostgresStopRequired
	}

	updated := beginReinitDataDirArchive(operation, executedAt)
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

// ExecuteReinitWALGRestore restores the selected WAL-G base backup into the
// archived target data directory and leaves the active operation running for
// later recovery configuration and replication verification phases.

func beginReinitDataDirArchive(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = reinitDataDirArchiveRunningMessage(updated.ToMember)

	return updated
}

func canBeginReinitDataDirArchive(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitPostgresStopCompletedMessage(operation.ToMember) ||
			operation.Message == reinitDataDirArchiveRunningMessage(operation.ToMember))
}

func buildReinitDataDirArchiveRequest(prepared preparedReinitExecution) ReinitDataDirArchiveRequest {
	return ReinitDataDirArchiveRequest{
		Operation:          prepared.operation.Clone(),
		Validation:         prepared.validation.Clone(),
		TargetNode:         prepared.targetNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
	}
}

func (store *MemoryStateStore) publishReinitDataDirArchived(prepared preparedReinitExecution) (ReinitExecution, error) {
	store.mu.Lock()
	running, err := store.reinitOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return ReinitExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = reinitDataDirArchiveCompletedMessage(prepared.validation.Target.Name)
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
		ArchivePath:     prepared.archivePath,
		ExecutedAt:      prepared.executedAt,
	}.Clone(), nil
}

func reinitDataDirArchiveRunningMessage(member string) string {
	return "archiving PostgreSQL data directory on reinit target " + member
}

func reinitDataDirArchiveCompletedMessage(member string) string {
	return "PostgreSQL data directory archived on reinit target " + member + "; WAL-G restore is pending"
}

func reinitDataDirArchiveFailedMessage(member string) string {
	return "failed to archive PostgreSQL data directory on reinit target " + member
}
