package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

type preparedSwitchoverExecution struct {
	operation     cluster.Operation
	previousEpoch cluster.Epoch
	executedAt    time.Time
}

// ExecuteSwitchover demotes the current primary, promotes the chosen standby,
// publishes the next epoch, and records the completed topology transition.
func (store *MemoryStateStore) ExecuteSwitchover(ctx context.Context, demoter DemotionExecutor, promoter PromotionExecutor) (SwitchoverExecution, error) {
	if err := ctx.Err(); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return SwitchoverExecution{}, err
	}

	prepared, err := store.prepareSwitchoverExecution(demoter, promoter)
	if err != nil {
		return SwitchoverExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := runSwitchoverDemotion(ctx, prepared, demoter); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := store.publishSwitchoverDemotion(prepared); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := runSwitchoverPromotion(ctx, prepared, promoter); err != nil {
		return SwitchoverExecution{}, err
	}

	return store.publishSwitchoverCompletion(prepared)
}

func (store *MemoryStateStore) prepareSwitchoverExecution(demoter DemotionExecutor, promoter PromotionExecutor) (preparedSwitchoverExecution, error) {
	if err := validateSwitchoverDemoter(demoter); err != nil {
		return preparedSwitchoverExecution{}, err
	}

	if err := validateSwitchoverPromoter(promoter); err != nil {
		return preparedSwitchoverExecution{}, err
	}

	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	spec, status, operation, err := store.loadSwitchoverExecutionInputsLocked()
	if err != nil {
		return preparedSwitchoverExecution{}, err
	}

	if err := validateSwitchoverExecutionSchedule(operation, executedAt); err != nil {
		return preparedSwitchoverExecution{}, err
	}

	validation, err := store.validateSwitchoverExecutionIntentLocked(spec, status, operation, executedAt)
	if err != nil {
		return preparedSwitchoverExecution{}, err
	}

	if err := validateSwitchoverExecutionIntent(operation, validation); err != nil {
		return preparedSwitchoverExecution{}, err
	}

	operation = store.startSwitchoverExecutionLocked(operation, executedAt)
	return buildPreparedSwitchoverExecution(operation, status.CurrentEpoch, executedAt), nil
}

func validateSwitchoverDemoter(demoter DemotionExecutor) error {
	if demoter == nil {
		return ErrSwitchoverDemotionExecutorRequired
	}

	return nil
}

func validateSwitchoverPromoter(promoter PromotionExecutor) error {
	if promoter == nil {
		return ErrSwitchoverPromotionExecutorRequired
	}

	return nil
}

func (store *MemoryStateStore) loadSwitchoverExecutionInputsLocked() (cluster.ClusterSpec, cluster.ClusterStatus, cluster.Operation, error) {
	spec, status, err := store.switchoverInputsLocked()
	if err != nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, cluster.Operation{}, err
	}

	operation, err := store.activeSwitchoverOperationLocked()
	if err != nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, cluster.Operation{}, err
	}

	return spec, status, operation, nil
}

func validateSwitchoverExecutionSchedule(operation cluster.Operation, executedAt time.Time) error {
	if operation.ScheduledAt.After(executedAt) {
		return ErrSwitchoverExecutionNotReady
	}

	return nil
}

func (store *MemoryStateStore) validateSwitchoverExecutionIntentLocked(spec cluster.ClusterSpec, status cluster.ClusterStatus, operation cluster.Operation, executedAt time.Time) (SwitchoverValidation, error) {
	return store.evaluateSwitchoverRequestLocked(spec, status, switchoverRequestForOperation(operation), nil, executedAt)
}

func switchoverRequestForOperation(operation cluster.Operation) SwitchoverRequest {
	return SwitchoverRequest{
		RequestedBy: operation.RequestedBy,
		Reason:      operation.Reason,
		Candidate:   operation.ToMember,
		ScheduledAt: operation.ScheduledAt,
	}
}

func validateSwitchoverExecutionIntent(operation cluster.Operation, validation SwitchoverValidation) error {
	if validation.CurrentPrimary.Name != operation.FromMember || validation.Target.Member.Name != operation.ToMember {
		return ErrSwitchoverIntentChanged
	}

	return nil
}

func (store *MemoryStateStore) startSwitchoverExecutionLocked(operation cluster.Operation, executedAt time.Time) cluster.Operation {
	updated := beginSwitchoverExecution(operation, executedAt)
	store.journalOperationLocked(updated, executedAt)
	store.refreshSourceOfTruthLocked(executedAt)

	return updated.Clone()
}

func buildPreparedSwitchoverExecution(operation cluster.Operation, previousEpoch cluster.Epoch, executedAt time.Time) preparedSwitchoverExecution {
	return preparedSwitchoverExecution{
		operation:     operation.Clone(),
		previousEpoch: previousEpoch,
		executedAt:    executedAt,
	}
}

func runSwitchoverDemotion(ctx context.Context, prepared preparedSwitchoverExecution, demoter DemotionExecutor) error {
	return demoter.Demote(ctx, DemotionRequest{
		Operation:      prepared.operation.Clone(),
		CurrentPrimary: prepared.operation.FromMember,
		Candidate:      prepared.operation.ToMember,
		CurrentEpoch:   prepared.previousEpoch,
	})
}

func runSwitchoverPromotion(ctx context.Context, prepared preparedSwitchoverExecution, promoter PromotionExecutor) error {
	return promoter.Promote(ctx, PromotionRequest{
		Operation:      prepared.operation.Clone(),
		CurrentPrimary: prepared.operation.FromMember,
		Candidate:      prepared.operation.ToMember,
		CurrentEpoch:   prepared.previousEpoch,
	})
}

func (store *MemoryStateStore) publishSwitchoverDemotion(prepared preparedSwitchoverExecution) error {
	store.mu.Lock()
	if _, err := store.switchoverOperationForPublicationLocked(prepared.operation); err != nil {
		store.mu.Unlock()
		return err
	}

	primaryStatus := store.memberNodeStatusLocked(prepared.operation.FromMember, prepared.executedAt)
	store.nodeStatuses[prepared.operation.FromMember] = demotingPrimaryNodeStatus(primaryStatus, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	primary := store.nodeStatuses[prepared.operation.FromMember].Clone()
	store.mu.Unlock()

	if err := store.persistNodeStatus(context.Background(), primary); err != nil {
		return err
	}

	return store.refreshCache(context.Background())
}

func (store *MemoryStateStore) publishSwitchoverCompletion(prepared preparedSwitchoverExecution) (SwitchoverExecution, error) {
	store.mu.Lock()
	runningOperation, err := store.switchoverOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return SwitchoverExecution{}, err
	}

	targetStatus, err := store.switchoverTargetStatusLocked(prepared.operation.ToMember)
	if err != nil {
		store.mu.Unlock()
		return SwitchoverExecution{}, err
	}

	if store.clusterStatus == nil {
		store.mu.Unlock()
		return SwitchoverExecution{}, ErrSwitchoverIntentChanged
	}

	formerPrimaryStatus := store.formerPrimaryNodeStatusLocked(prepared.operation.FromMember, prepared.executedAt)
	nextEpoch := nextClusterEpoch(prepared.previousEpoch)
	store.nodeStatuses[prepared.operation.ToMember] = promotedNodeStatus(targetStatus, prepared.executedAt)
	store.nodeStatuses[prepared.operation.FromMember] = demotedFormerPrimaryStatus(formerPrimaryStatus, prepared.executedAt)
	store.clusterStatus.CurrentEpoch = nextEpoch

	completedOperation := completeSwitchoverExecution(runningOperation, prepared.executedAt, prepared.operation.ToMember, nextEpoch)
	store.journalOperationLocked(completedOperation, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	target := store.nodeStatuses[prepared.operation.ToMember].Clone()
	formerPrimary := store.nodeStatuses[prepared.operation.FromMember].Clone()
	store.mu.Unlock()

	if err := store.persistNodeStatuses(context.Background(), target, formerPrimary); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := store.persistJournaledOperation(context.Background(), completedOperation); err != nil {
		return SwitchoverExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return SwitchoverExecution{}, err
	}

	return buildSwitchoverExecution(prepared, completedOperation, nextEpoch), nil
}

func (store *MemoryStateStore) activeSwitchoverOperationLocked() (cluster.Operation, error) {
	if store.activeOperation == nil {
		return cluster.Operation{}, ErrSwitchoverIntentRequired
	}

	operation := store.activeOperation.Clone()
	if operation.Kind != cluster.OperationKindSwitchover || operation.State.IsTerminal() {
		return cluster.Operation{}, ErrSwitchoverIntentRequired
	}

	if strings.TrimSpace(operation.FromMember) == "" || strings.TrimSpace(operation.ToMember) == "" {
		return cluster.Operation{}, ErrSwitchoverIntentRequired
	}

	return operation, nil
}

func (store *MemoryStateStore) switchoverOperationForPublicationLocked(expected cluster.Operation) (cluster.Operation, error) {
	if store.activeOperation == nil || store.activeOperation.ID != expected.ID || store.activeOperation.Kind != cluster.OperationKindSwitchover {
		return cluster.Operation{}, ErrSwitchoverIntentChanged
	}

	return store.activeOperation.Clone(), nil
}

func beginSwitchoverExecution(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = "demoting current primary " + updated.FromMember + " before promoting " + updated.ToMember

	return updated
}

func completeSwitchoverExecution(operation cluster.Operation, completedAt time.Time, candidate string, epoch cluster.Epoch) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateCompleted
	updated.CompletedAt = completedAt
	updated.Result = cluster.OperationResultSucceeded
	updated.Message = switchoverCompletedMessage(candidate, epoch)

	return updated
}

func switchoverCompletedMessage(candidate string, epoch cluster.Epoch) string {
	return "planned switchover completed on " + candidate + " at epoch " + epoch.String()
}

func (store *MemoryStateStore) switchoverTargetStatusLocked(target string) (agentmodel.NodeStatus, error) {
	status, ok := store.nodeStatuses[target]
	if !ok {
		return agentmodel.NodeStatus{}, ErrSwitchoverTargetUnknown
	}

	return status.Clone(), nil
}

func (store *MemoryStateStore) memberNodeStatusLocked(nodeName string, observedAt time.Time) agentmodel.NodeStatus {
	status, ok := store.nodeStatuses[nodeName]
	if ok {
		return status.Clone()
	}

	return agentmodel.NodeStatus{
		NodeName:   nodeName,
		MemberName: nodeName,
		ObservedAt: observedAt,
	}
}

func demotingPrimaryNodeStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRolePrimary
	updated.State = cluster.MemberStateStopping
	updated.ObservedAt = observedAt
	updated.NeedsRejoin = false

	if updated.Postgres.Managed {
		updated.Postgres.Up = true
		updated.Postgres.CheckedAt = observedAt
		updated.Postgres.Role = cluster.MemberRolePrimary
	}

	return updated
}

func demotedFormerPrimaryStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRoleReplica
	updated.State = cluster.MemberStateStopping
	updated.ObservedAt = observedAt
	updated.NeedsRejoin = false

	if updated.Postgres.Managed {
		updated.Postgres.Up = false
		updated.Postgres.CheckedAt = observedAt
		updated.Postgres.Role = cluster.MemberRoleReplica
		updated.Postgres.RecoveryKnown = false
		updated.Postgres.InRecovery = false
	}

	return updated
}

func buildSwitchoverExecution(prepared preparedSwitchoverExecution, operation cluster.Operation, currentEpoch cluster.Epoch) SwitchoverExecution {
	return SwitchoverExecution{
		Operation:      operation.Clone(),
		CurrentPrimary: prepared.operation.FromMember,
		Candidate:      prepared.operation.ToMember,
		PreviousEpoch:  prepared.previousEpoch,
		CurrentEpoch:   currentEpoch,
		Demoted:        true,
		Promoted:       true,
		ExecutedAt:     prepared.executedAt,
	}.Clone()
}
