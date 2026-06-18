package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

type preparedReinitExecution struct {
	validation         ReinitValidation
	targetNode         agentmodel.NodeStatus
	currentPrimaryNode agentmodel.NodeStatus
	operation          cluster.Operation
	currentEpoch       cluster.Epoch
	archivePath        string
	walgBackupName     string
	restoreCommand     string
	standby            postgres.StandbyConfig
	executedAt         time.Time
}

// ExecuteReinitStopPostgres stops PostgreSQL on the reinit target and leaves
// the active operation running for the later destructive restore phases.
func (store *MemoryStateStore) ExecuteReinitStopPostgres(ctx context.Context, member string, stopper ReinitPostgresStopExecutor) (ReinitExecution, error) {
	if err := ctx.Err(); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitExecution{}, err
	}

	prepared, err := store.prepareReinitPostgresStop(member, stopper)
	if err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitExecution{}, err
	}

	if err := stopper.StopPostgres(ctx, buildReinitPostgresStopRequest(prepared)); err != nil {
		store.failReinitExecution(prepared, reinitPostgresStopFailedMessage(prepared.validation.Target.Name))
		return ReinitExecution{}, err
	}

	return store.publishReinitPostgresStopped(prepared)
}

func (store *MemoryStateStore) prepareReinitPostgresStop(member string, stopper ReinitPostgresStopExecutor) (preparedReinitExecution, error) {
	if stopper == nil {
		return preparedReinitExecution{}, ErrReinitPostgresStopExecutorRequired
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
	if !canBeginReinitPostgresStop(operation) {
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

	updated := beginReinitPostgresStop(operation, executedAt)
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

// ExecuteReinitArchiveDataDir archives the stopped target's data directory and
// leaves the active operation running for the later WAL-G restore phase.
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
func (store *MemoryStateStore) ExecuteReinitRecoveryConfig(ctx context.Context, member string, configurator ReinitRecoveryConfigExecutor) (ReinitExecution, error) {
	if err := ctx.Err(); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitExecution{}, err
	}

	prepared, err := store.prepareReinitRecoveryConfig(member, configurator)
	if err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitExecution{}, err
	}

	result, err := configurator.ConfigureReinitRecovery(ctx, buildReinitRecoveryConfigRequest(prepared))
	if err != nil {
		store.failReinitExecution(prepared, reinitRecoveryConfigFailedMessage(prepared.validation.Target.Name))
		return ReinitExecution{}, err
	}
	prepared.restoreCommand = result.RestoreCommand

	return store.publishReinitRecoveryConfigured(prepared)
}

func (store *MemoryStateStore) prepareReinitRecoveryConfig(member string, configurator ReinitRecoveryConfigExecutor) (preparedReinitExecution, error) {
	if configurator == nil {
		return preparedReinitExecution{}, ErrReinitRecoveryConfigExecutorRequired
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
	if !canBeginReinitRecoveryConfig(operation) {
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

	standby, err := buildReinitRecoveryStandbyConfig(currentPrimaryNode.Postgres.Address, operation.ToMember)
	if err != nil {
		return preparedReinitExecution{}, err
	}

	updated := beginReinitRecoveryConfig(operation, executedAt)
	store.journalOperationLocked(updated, executedAt)
	store.refreshSourceOfTruthLocked(executedAt)

	return preparedReinitExecution{
		validation:         validation.Clone(),
		targetNode:         targetNode.Clone(),
		currentPrimaryNode: currentPrimaryNode.Clone(),
		operation:          updated.Clone(),
		currentEpoch:       status.CurrentEpoch,
		standby:            standby,
		executedAt:         executedAt,
	}, nil
}

func (store *MemoryStateStore) activeReinitOperationLocked() (cluster.Operation, error) {
	if store.activeOperation == nil {
		return cluster.Operation{}, ErrReinitExecutionRequired
	}

	operation := store.activeOperation.Clone()
	if operation.Kind != cluster.OperationKindReinit || operation.State.IsTerminal() {
		return cluster.Operation{}, ErrReinitExecutionRequired
	}

	if strings.TrimSpace(operation.FromMember) == "" || strings.TrimSpace(operation.ToMember) == "" {
		return cluster.Operation{}, ErrReinitExecutionRequired
	}

	return operation, nil
}

func (store *MemoryStateStore) reinitOperationForPublicationLocked(expected cluster.Operation) (cluster.Operation, error) {
	if store.activeOperation == nil || store.activeOperation.ID != expected.ID || store.activeOperation.Kind != cluster.OperationKindReinit {
		return cluster.Operation{}, ErrReinitExecutionChanged
	}

	return store.activeOperation.Clone(), nil
}

func beginReinitPostgresStop(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = reinitPostgresStopRunningMessage(updated.ToMember)

	return updated
}

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

func beginReinitRecoveryConfig(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = reinitRecoveryConfigRunningMessage(updated.ToMember)

	return updated
}

func canBeginReinitPostgresStop(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateAccepted ||
		operation.Message == reinitPostgresStopRunningMessage(operation.ToMember)
}

func canBeginReinitDataDirArchive(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitPostgresStopCompletedMessage(operation.ToMember) ||
			operation.Message == reinitDataDirArchiveRunningMessage(operation.ToMember))
}

func canBeginReinitWALGRestore(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitDataDirArchiveCompletedMessage(operation.ToMember) ||
			operation.Message == reinitWALGRestoreRunningMessage(operation.ToMember))
}

func canBeginReinitRecoveryConfig(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitWALGRestoreCompletedMessage(operation.ToMember) ||
			operation.Message == reinitRecoveryConfigRunningMessage(operation.ToMember))
}

func buildReinitPostgresStopRequest(prepared preparedReinitExecution) ReinitPostgresStopRequest {
	return ReinitPostgresStopRequest{
		Operation:          prepared.operation.Clone(),
		Validation:         prepared.validation.Clone(),
		TargetNode:         prepared.targetNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
	}
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

func buildReinitWALGRestoreRequest(prepared preparedReinitExecution) ReinitWALGRestoreRequest {
	return ReinitWALGRestoreRequest{
		Operation:          prepared.operation.Clone(),
		Validation:         prepared.validation.Clone(),
		TargetNode:         prepared.targetNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
	}
}

func buildReinitRecoveryConfigRequest(prepared preparedReinitExecution) ReinitRecoveryConfigRequest {
	return ReinitRecoveryConfigRequest{
		Operation:          prepared.operation.Clone(),
		Validation:         prepared.validation.Clone(),
		TargetNode:         prepared.targetNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
		Standby:            prepared.standby.WithDefaults(),
	}
}

func (store *MemoryStateStore) publishReinitPostgresStopped(prepared preparedReinitExecution) (ReinitExecution, error) {
	store.mu.Lock()
	running, err := store.reinitOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return ReinitExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = reinitPostgresStopCompletedMessage(prepared.validation.Target.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.validation.Target.Name] = reinitPostgresStoppedStatus(prepared.targetNode, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	target := store.nodeStatuses[prepared.validation.Target.Name].Clone()
	store.mu.Unlock()

	if err := store.persistActiveOperation(context.Background(), updatedOperation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistNodeStatus(context.Background(), target); err != nil {
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
		ExecutedAt:      prepared.executedAt,
	}.Clone(), nil
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

func (store *MemoryStateStore) publishReinitRecoveryConfigured(prepared preparedReinitExecution) (ReinitExecution, error) {
	store.mu.Lock()
	running, err := store.reinitOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return ReinitExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = reinitRecoveryConfigCompletedMessage(prepared.validation.Target.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.validation.Target.Name] = reinitRecoveryConfiguredStatus(prepared.targetNode, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	target := store.nodeStatuses[prepared.validation.Target.Name].Clone()
	store.mu.Unlock()

	if err := store.persistActiveOperation(context.Background(), updatedOperation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistNodeStatus(context.Background(), target); err != nil {
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
		RecoveryConfig:  true,
		RestoreCommand:  prepared.restoreCommand,
		ExecutedAt:      prepared.executedAt,
	}.Clone(), nil
}

func (store *MemoryStateStore) failReinitExecution(prepared preparedReinitExecution, message string) {
	store.mu.Lock()
	failed := prepared.operation.Clone()
	failed.State = cluster.OperationStateFailed
	failed.Result = cluster.OperationResultFailed
	failed.CompletedAt = prepared.executedAt
	failed.Message = message
	store.journalOperationLocked(failed, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	store.mu.Unlock()

	if err := store.persistJournaledOperation(context.Background(), failed); err != nil {
		store.logger.Error("failed to persist failed reinit operation", "operation_id", failed.ID, "error", err)
	}

	if err := store.refreshCache(context.Background()); err != nil {
		store.logger.Error("failed to refresh cache after reinit failure", "operation_id", failed.ID, "error", err)
	}
}

func reinitPostgresStoppedStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.State = cluster.MemberStateStopping
	updated.ObservedAt = observedAt

	if updated.Postgres.Managed {
		updated.Postgres.Up = false
		updated.Postgres.CheckedAt = observedAt
		updated.Postgres.RecoveryKnown = false
		updated.Postgres.InRecovery = false
	}

	return updated
}

func buildReinitRecoveryStandbyConfig(currentPrimaryAddress, targetMember string) (postgres.StandbyConfig, error) {
	if strings.TrimSpace(currentPrimaryAddress) == "" {
		return postgres.StandbyConfig{}, ErrReinitCurrentPrimaryAddressRequired
	}

	connInfo, err := rejoinPrimaryConnInfo(currentPrimaryAddress, targetMember)
	if err != nil {
		return postgres.StandbyConfig{}, err
	}

	return (postgres.StandbyConfig{
		PrimaryConnInfo:        connInfo,
		RecoveryTargetTimeline: postgres.DefaultRecoveryTargetTimeline,
	}).WithDefaults(), nil
}

func reinitRecoveryConfiguredStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.State = cluster.MemberStateStopping
	updated.PendingRestart = true
	updated.ObservedAt = observedAt

	if updated.Postgres.Managed {
		updated.Postgres.Up = false
		updated.Postgres.CheckedAt = observedAt
		updated.Postgres.Details.PendingRestart = true
	}

	return updated
}

func reinitPostgresStopRunningMessage(member string) string {
	return "stopping PostgreSQL on reinit target " + member
}

func reinitPostgresStopCompletedMessage(member string) string {
	return "PostgreSQL stopped on reinit target " + member + "; destructive restore is pending"
}

func reinitPostgresStopFailedMessage(member string) string {
	return "failed to stop PostgreSQL on reinit target " + member
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

func reinitWALGRestoreRunningMessage(member string) string {
	return "restoring PostgreSQL data directory from WAL-G on reinit target " + member
}

func reinitWALGRestoreCompletedMessage(member string) string {
	return "PostgreSQL data directory restored from WAL-G on reinit target " + member + "; recovery configuration is pending"
}

func reinitWALGRestoreFailedMessage(member string) string {
	return "failed to restore PostgreSQL data directory from WAL-G on reinit target " + member
}

func reinitRecoveryConfigRunningMessage(member string) string {
	return "rendering PostgreSQL recovery configuration on reinit target " + member
}

func reinitRecoveryConfigCompletedMessage(member string) string {
	return "PostgreSQL recovery configuration rendered on reinit target " + member + "; standby start is pending"
}

func reinitRecoveryConfigFailedMessage(member string) string {
	return "failed to render PostgreSQL recovery configuration on reinit target " + member
}
