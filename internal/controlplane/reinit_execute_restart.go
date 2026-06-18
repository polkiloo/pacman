package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func (store *MemoryStateStore) ExecuteReinitRestartAsStandby(ctx context.Context, member string, restarter ReinitStandbyRestartExecutor) (ReinitExecution, error) {
	if err := ctx.Err(); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitExecution{}, err
	}

	prepared, err := store.prepareReinitStandbyRestart(member, restarter)
	if err != nil {
		return ReinitExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return ReinitExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitExecution{}, err
	}

	if err := restarter.RestartReinitStandby(ctx, buildReinitStandbyRestartRequest(prepared)); err != nil {
		store.failReinitExecution(prepared, reinitStandbyRestartFailedMessage(prepared.validation.Target.Name, prepared.validation.CurrentPrimary.Name))
		return ReinitExecution{}, err
	}

	return store.publishReinitStandbyRestart(prepared)
}

// ExecuteReinitVerifyReplication verifies that the restarted reinit target is
// attached to the expected replication slot, streaming from the current
// primary, and on the same PostgreSQL system identifier and timeline.

func (store *MemoryStateStore) prepareReinitStandbyRestart(member string, restarter ReinitStandbyRestartExecutor) (preparedReinitExecution, error) {
	if restarter == nil {
		return preparedReinitExecution{}, ErrReinitStandbyRestartExecutorRequired
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
	if !canBeginReinitStandbyRestart(operation) {
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
	if !targetNode.PendingRestart && !targetNode.Postgres.Details.PendingRestart {
		return preparedReinitExecution{}, ErrReinitRecoveryConfigRequired
	}

	updated := beginReinitStandbyRestart(operation, executedAt)
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

func beginReinitStandbyRestart(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = reinitStandbyRestartRunningMessage(updated.ToMember, updated.FromMember)

	return updated
}

func canBeginReinitStandbyRestart(operation cluster.Operation) bool {
	return operation.State == cluster.OperationStateRunning &&
		(operation.Message == reinitRecoveryConfigCompletedMessage(operation.ToMember) ||
			operation.Message == reinitStandbyRestartRunningMessage(operation.ToMember, operation.FromMember))
}

func buildReinitStandbyRestartRequest(prepared preparedReinitExecution) ReinitStandbyRestartRequest {
	return ReinitStandbyRestartRequest{
		Operation:          prepared.operation.Clone(),
		Validation:         prepared.validation.Clone(),
		TargetNode:         prepared.targetNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
	}
}

func (store *MemoryStateStore) publishReinitStandbyRestart(prepared preparedReinitExecution) (ReinitExecution, error) {
	store.mu.Lock()
	running, err := store.reinitOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return ReinitExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = reinitStandbyRestartCompletedMessage(prepared.validation.Target.Name, prepared.validation.CurrentPrimary.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.validation.Target.Name] = restartingReinitStandbyStatus(prepared.targetNode, prepared.executedAt)
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
		Operation:          updatedOperation.Clone(),
		Validation:         prepared.validation.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
		PostgresStopped:    true,
		DataDirArchived:    true,
		WALGRestored:       true,
		RecoveryConfig:     true,
		RestartedAsStandby: true,
		ExecutedAt:         prepared.executedAt,
	}.Clone(), nil
}

func restartingReinitStandbyStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRoleReplica
	updated.State = cluster.MemberStateStarting
	updated.PendingRestart = false
	updated.ObservedAt = observedAt

	if updated.Postgres.Managed {
		updated.Postgres.Up = true
		updated.Postgres.CheckedAt = observedAt
		updated.Postgres.Role = cluster.MemberRoleReplica
		updated.Postgres.RecoveryKnown = true
		updated.Postgres.InRecovery = true
		updated.Postgres.Details.PendingRestart = false
	}

	return updated
}

func reinitStandbyRestartRunningMessage(member, currentPrimary string) string {
	return "restarting reinit target " + member + " as a standby following " + currentPrimary
}

func reinitStandbyRestartCompletedMessage(member, currentPrimary string) string {
	return "reinit target " + member + " restarted as a standby following " + currentPrimary + "; replication verification is still pending"
}

func reinitStandbyRestartFailedMessage(member, currentPrimary string) string {
	return "failed to restart reinit target " + member + " as a standby following " + currentPrimary
}
