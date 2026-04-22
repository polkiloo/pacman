package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

type preparedRejoinExecution struct {
	request            RejoinRequest
	decision           RejoinStrategyDecision
	memberNode         agentmodel.NodeStatus
	currentPrimaryNode agentmodel.NodeStatus
	rewindSourceServer string
	operation          cluster.Operation
	currentEpoch       cluster.Epoch
	standby            postgres.StandbyConfig
	executedAt         time.Time
}

// ExecuteRejoinRewind runs the pg_rewind phase of a former-primary rejoin and
// leaves the cluster in the recovering phase until the later standby steps
// complete.
func (store *MemoryStateStore) ExecuteRejoinRewind(ctx context.Context, request RejoinRequest, rewinder RewindExecutor) (RejoinExecution, error) {
	if err := ctx.Err(); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return RejoinExecution{}, err
	}

	prepared, err := store.prepareRejoinRewindExecution(request, rewinder)
	if err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return RejoinExecution{}, err
	}

	if err := rewinder.Rewind(ctx, buildRewindRequest(prepared)); err != nil {
		store.failRejoinExecution(prepared, rejoinRewindFailedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name))
		return RejoinExecution{}, err
	}

	return store.publishRejoinRewind(prepared)
}

func (store *MemoryStateStore) prepareRejoinRewindExecution(request RejoinRequest, rewinder RewindExecutor) (preparedRejoinExecution, error) {
	if rewinder == nil {
		return preparedRejoinExecution{}, ErrRejoinRewindExecutorRequired
	}

	normalized := normalizeRejoinRequest(request)
	if normalized.Member == "" {
		return preparedRejoinExecution{}, ErrRejoinTargetRequired
	}
	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.activeOperation != nil {
		return preparedRejoinExecution{}, ErrRejoinOperationInProgress
	}

	inputs, err := store.rejoinInputsLocked(normalized.Member)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	decision := buildRejoinStrategyDecision(buildRejoinDivergenceAssessment(inputs))
	if err := validateRejoinRewindDecision(decision); err != nil {
		return preparedRejoinExecution{}, err
	}

	primaryAddress := store.primaryPostgresAddressLocked(decision.CurrentPrimary.Name, inputs.currentPrimaryNode.Postgres.Address)
	rewindSourceServer, err := rejoinPrimaryConnInfo(primaryAddress, "")
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	operation, err := buildRejoinOperation(normalized, decision, executedAt)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	store.journalOperationLocked(operation, executedAt)
	store.refreshSourceOfTruthLocked(executedAt)

	return preparedRejoinExecution{
		request:            normalized,
		decision:           decision.Clone(),
		memberNode:         inputs.memberNode.Clone(),
		currentPrimaryNode: inputs.currentPrimaryNode.Clone(),
		rewindSourceServer: rewindSourceServer,
		operation:          operation.Clone(),
		currentEpoch:       inputs.currentEpoch,
		executedAt:         executedAt,
	}, nil
}

func validateRejoinRewindDecision(decision RejoinStrategyDecision) error {
	switch {
	case decision.Strategy == cluster.RejoinStrategyReclone:
		return ErrRejoinRecloneRequired
	case !decision.Decided && !decision.DirectRejoinPossible && len(decision.Reasons) > 0:
		return ErrRejoinStrategyUndetermined
	default:
		return nil
	}
}

// validateRejoinContinuationDecision accepts both direct rejoin and post-rewind
// paths for the standby config and restart continuation phases.
func validateRejoinContinuationDecision(decision RejoinStrategyDecision) error {
	if decision.DirectRejoinPossible {
		return nil
	}
	if !decision.Decided {
		return ErrRejoinStrategyUndetermined
	}
	if decision.Strategy != cluster.RejoinStrategyRewind {
		return ErrRejoinRecloneRequired
	}
	return nil
}

// ExecuteRejoinDirect creates a rejoin operation for a former primary that does
// not require pg_rewind (same timeline, no divergence) and transitions it into
// the standby configuration phase.
func (store *MemoryStateStore) ExecuteRejoinDirect(ctx context.Context, request RejoinRequest) (RejoinExecution, error) {
	if err := ctx.Err(); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return RejoinExecution{}, err
	}

	prepared, err := store.prepareRejoinDirectExecution(request)
	if err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return RejoinExecution{}, err
	}

	return store.publishRejoinDirect(prepared)
}

func (store *MemoryStateStore) prepareRejoinDirectExecution(request RejoinRequest) (preparedRejoinExecution, error) {
	normalized := normalizeDirectRejoinRequest(request)
	if normalized.Member == "" {
		return preparedRejoinExecution{}, ErrRejoinTargetRequired
	}

	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.activeOperation != nil {
		return preparedRejoinExecution{}, ErrRejoinOperationInProgress
	}

	inputs, err := store.rejoinInputsLocked(normalized.Member)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	decision := buildRejoinStrategyDecision(buildRejoinDivergenceAssessment(inputs))
	if !decision.DirectRejoinPossible {
		return preparedRejoinExecution{}, ErrRejoinStrategyUndetermined
	}

	operation, err := buildRejoinDirectOperation(normalized, decision, executedAt)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	store.journalOperationLocked(operation, executedAt)
	store.refreshSourceOfTruthLocked(executedAt)

	return preparedRejoinExecution{
		request:            normalized,
		decision:           decision.Clone(),
		memberNode:         inputs.memberNode.Clone(),
		currentPrimaryNode: inputs.currentPrimaryNode.Clone(),
		operation:          operation.Clone(),
		currentEpoch:       inputs.currentEpoch,
		executedAt:         executedAt,
	}, nil
}

func (store *MemoryStateStore) publishRejoinDirect(prepared preparedRejoinExecution) (RejoinExecution, error) {
	store.mu.Lock()
	updatedOperation := prepared.operation.Clone()
	updatedOperation.Message = rejoinDirectReadyMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.decision.Member.Name] = rewoundFormerPrimaryStatus(prepared.memberNode, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	member := store.nodeStatuses[prepared.decision.Member.Name].Clone()
	store.mu.Unlock()

	if err := store.persistActiveOperation(context.Background(), updatedOperation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistNodeStatus(context.Background(), member); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return RejoinExecution{}, err
	}

	return RejoinExecution{
		Operation:    updatedOperation.Clone(),
		Decision:     prepared.decision.Clone(),
		CurrentEpoch: prepared.currentEpoch,
		State:        cluster.RejoinStateConfiguringStandby,
		ExecutedAt:   prepared.executedAt,
	}.Clone(), nil
}

func buildRejoinDirectOperation(request RejoinRequest, decision RejoinStrategyDecision, executedAt time.Time) (cluster.Operation, error) {
	operation := cluster.Operation{
		ID:          rejoinOperationID(executedAt),
		Kind:        cluster.OperationKindRejoin,
		State:       cluster.OperationStateRunning,
		RequestedBy: request.RequestedBy,
		RequestedAt: executedAt,
		Reason:      request.Reason,
		FromMember:  decision.Member.Name,
		ToMember:    decision.CurrentPrimary.Name,
		StartedAt:   executedAt,
		Result:      cluster.OperationResultPending,
		Message:     rejoinDirectRunningMessage(decision.Member.Name, decision.CurrentPrimary.Name),
	}

	if err := operation.Validate(); err != nil {
		return cluster.Operation{}, err
	}

	return operation, nil
}

func normalizeDirectRejoinRequest(request RejoinRequest) RejoinRequest {
	normalized := request
	normalized.Member = strings.TrimSpace(normalized.Member)
	normalized.RequestedBy = strings.TrimSpace(normalized.RequestedBy)
	normalized.Reason = strings.TrimSpace(normalized.Reason)

	if normalized.RequestedBy == "" {
		normalized.RequestedBy = "controlplane"
	}

	if normalized.Reason == "" {
		normalized.Reason = "direct rejoin of former primary without pg_rewind"
	}

	return normalized
}

func rejoinDirectRunningMessage(member, currentPrimary string) string {
	return "preparing direct rejoin for " + member + " to follow " + currentPrimary
}

func rejoinDirectReadyMessage(member, currentPrimary string) string {
	return "direct rejoin ready for " + member + " to follow " + currentPrimary + "; standby configuration is pending"
}

func buildRejoinOperation(request RejoinRequest, decision RejoinStrategyDecision, executedAt time.Time) (cluster.Operation, error) {
	operation := cluster.Operation{
		ID:          rejoinOperationID(executedAt),
		Kind:        cluster.OperationKindRejoin,
		State:       cluster.OperationStateRunning,
		RequestedBy: request.RequestedBy,
		RequestedAt: executedAt,
		Reason:      request.Reason,
		FromMember:  decision.Member.Name,
		ToMember:    decision.CurrentPrimary.Name,
		StartedAt:   executedAt,
		Result:      cluster.OperationResultPending,
		Message:     rejoinRewindRunningMessage(decision.Member.Name, decision.CurrentPrimary.Name),
	}

	if err := operation.Validate(); err != nil {
		return cluster.Operation{}, err
	}

	return operation, nil
}

func buildRewindRequest(prepared preparedRejoinExecution) RewindRequest {
	return RewindRequest{
		Operation:          prepared.operation.Clone(),
		Decision:           prepared.decision.Clone(),
		MemberNode:         prepared.memberNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
		SourceServer:       prepared.rewindSourceServer,
	}
}

func (store *MemoryStateStore) publishRejoinRewind(prepared preparedRejoinExecution) (RejoinExecution, error) {
	store.mu.Lock()
	updatedOperation := prepared.operation.Clone()
	updatedOperation.Message = rejoinRewindCompletedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.decision.Member.Name] = rewoundFormerPrimaryStatus(prepared.memberNode, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	member := store.nodeStatuses[prepared.decision.Member.Name].Clone()
	store.mu.Unlock()

	if err := store.persistActiveOperation(context.Background(), updatedOperation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistNodeStatus(context.Background(), member); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return RejoinExecution{}, err
	}

	return RejoinExecution{
		Operation:    updatedOperation.Clone(),
		Decision:     prepared.decision.Clone(),
		CurrentEpoch: prepared.currentEpoch,
		State:        cluster.RejoinStateRewinding,
		Rewound:      true,
		ExecutedAt:   prepared.executedAt,
	}.Clone(), nil
}

func (store *MemoryStateStore) failRejoinExecution(prepared preparedRejoinExecution, message string) {
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
		store.logger.Error("failed to persist failed rejoin operation", "operation_id", failed.ID, "error", err)
	}

	if err := store.refreshCache(context.Background()); err != nil {
		store.logger.Error("failed to refresh cache after rejoin failure", "operation_id", failed.ID, "error", err)
	}
}

func normalizeRejoinRequest(request RejoinRequest) RejoinRequest {
	normalized := request
	normalized.Member = strings.TrimSpace(normalized.Member)
	normalized.RequestedBy = strings.TrimSpace(normalized.RequestedBy)
	normalized.Reason = strings.TrimSpace(normalized.Reason)

	if normalized.RequestedBy == "" {
		normalized.RequestedBy = "controlplane"
	}

	if normalized.Reason == "" {
		normalized.Reason = "repair former primary using pg_rewind"
	}

	return normalized
}

func rejoinOperationID(now time.Time) string {
	return "rejoin-" + now.UTC().Format("20060102T150405.000000000Z07:00")
}

func rejoinRewindRunningMessage(member, currentPrimary string) string {
	return "executing pg_rewind for " + member + " against " + currentPrimary
}

func rejoinRewindCompletedMessage(member, currentPrimary string) string {
	return "pg_rewind completed for " + member + " against " + currentPrimary + "; standby configuration is still pending"
}

func rejoinRewindFailedMessage(member, currentPrimary string) string {
	return "pg_rewind failed for " + member + " against " + currentPrimary
}

func rewoundFormerPrimaryStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRoleReplica
	updated.State = cluster.MemberStateNeedsRejoin
	updated.NeedsRejoin = true
	updated.ObservedAt = observedAt
	updated.Postgres.Managed = true
	updated.Postgres.Up = false
	updated.Postgres.CheckedAt = observedAt
	updated.Postgres.Role = cluster.MemberRoleReplica
	updated.Postgres.RecoveryKnown = false
	updated.Postgres.InRecovery = false

	return updated
}
