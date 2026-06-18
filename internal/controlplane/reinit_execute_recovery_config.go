package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

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

// ExecuteReinitRestartAsStandby starts the restored reinit target in standby
// mode after WAL-G restore and recovery configuration have completed.

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

func canBeginReinitRecoveryConfig(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitWALGRestoreCompletedMessage(operation.ToMember) ||
			operation.Message == reinitRecoveryConfigRunningMessage(operation.ToMember))
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
		PrimarySlotName:        rejoinPrimarySlotName(targetMember),
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

func reinitRecoveryConfigRunningMessage(member string) string {
	return "rendering PostgreSQL recovery configuration on reinit target " + member
}

func reinitRecoveryConfigCompletedMessage(member string) string {
	return "PostgreSQL recovery configuration rendered on reinit target " + member + "; standby start is pending"
}

func reinitRecoveryConfigFailedMessage(member string) string {
	return "failed to render PostgreSQL recovery configuration on reinit target " + member
}
