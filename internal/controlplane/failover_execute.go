package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

type preparedFailoverExecution struct {
	spec          cluster.ClusterSpec
	operation     cluster.Operation
	previousEpoch cluster.Epoch
	executedAt    time.Time
}

// ExecuteFailover runs the active failover intent through optional fencing,
// candidate promotion, and epoch publication. It intentionally leaves final
// cleanup such as former-primary rejoin marking and history recording to later
// orchestration steps.
func (store *MemoryStateStore) ExecuteFailover(ctx context.Context, promoter PromotionExecutor, fencer FencingHook) (FailoverExecution, error) {
	if err := ctx.Err(); err != nil {
		return FailoverExecution{}, err
	}

	prepared, err := store.prepareFailoverExecution(promoter, fencer)
	if err != nil {
		return FailoverExecution{}, err
	}

	if err := runFailoverFencing(ctx, prepared, fencer); err != nil {
		return FailoverExecution{}, err
	}

	if err := runFailoverPromotion(ctx, prepared, promoter); err != nil {
		return FailoverExecution{}, err
	}

	return store.publishFailoverEpoch(prepared)
}

func (store *MemoryStateStore) prepareFailoverExecution(promoter PromotionExecutor, fencer FencingHook) (preparedFailoverExecution, error) {
	if promoter == nil {
		return preparedFailoverExecution{}, ErrFailoverPromotionExecutorRequired
	}

	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	spec, status, err := store.failoverInputsLocked()
	if err != nil {
		return preparedFailoverExecution{}, err
	}

	operation, err := store.activeFailoverOperationLocked()
	if err != nil {
		return preparedFailoverExecution{}, err
	}

	if spec.Failover.FencingRequired && fencer == nil {
		return preparedFailoverExecution{}, ErrFailoverFencingHookRequired
	}

	operation = beginFailoverExecution(operation, executedAt)
	store.activeOperation = &operation
	store.refreshSourceOfTruthLocked(executedAt)

	return preparedFailoverExecution{
		spec:          spec,
		operation:     operation,
		previousEpoch: status.CurrentEpoch,
		executedAt:    executedAt,
	}, nil
}

func runFailoverFencing(ctx context.Context, prepared preparedFailoverExecution, fencer FencingHook) error {
	if !prepared.spec.Failover.FencingRequired {
		return nil
	}

	return fencer.Fence(ctx, FencingRequest{
		Operation:      prepared.operation.Clone(),
		CurrentPrimary: prepared.operation.FromMember,
		Candidate:      prepared.operation.ToMember,
		CurrentEpoch:   prepared.previousEpoch,
	})
}

func runFailoverPromotion(ctx context.Context, prepared preparedFailoverExecution, promoter PromotionExecutor) error {
	return promoter.Promote(ctx, PromotionRequest{
		Operation:      prepared.operation.Clone(),
		CurrentPrimary: prepared.operation.FromMember,
		Candidate:      prepared.operation.ToMember,
		CurrentEpoch:   prepared.previousEpoch,
	})
}

func (store *MemoryStateStore) publishFailoverEpoch(prepared preparedFailoverExecution) (FailoverExecution, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	updatedOperation, err := store.failoverOperationForPublicationLocked(prepared.operation)
	if err != nil {
		return FailoverExecution{}, err
	}

	candidateStatus, err := store.failoverCandidateStatusLocked(prepared.operation.ToMember)
	if err != nil {
		return FailoverExecution{}, err
	}

	nextEpoch := nextClusterEpoch(prepared.previousEpoch)
	store.nodeStatuses[prepared.operation.ToMember] = promotedNodeStatus(candidateStatus, prepared.executedAt)
	store.clusterStatus.CurrentEpoch = nextEpoch

	updatedOperation.Message = failoverEpochPublishedMessage(prepared.operation.ToMember, nextEpoch)
	store.activeOperation = &updatedOperation
	store.refreshSourceOfTruthLocked(prepared.executedAt)

	return buildFailoverExecution(prepared, updatedOperation, nextEpoch), nil
}

func (store *MemoryStateStore) activeFailoverOperationLocked() (cluster.Operation, error) {
	if store.activeOperation == nil {
		return cluster.Operation{}, ErrFailoverIntentRequired
	}

	operation := store.activeOperation.Clone()
	if operation.Kind != cluster.OperationKindFailover || operation.State.IsTerminal() {
		return cluster.Operation{}, ErrFailoverIntentRequired
	}

	if strings.TrimSpace(operation.ToMember) == "" {
		return cluster.Operation{}, ErrFailoverCandidateUnknown
	}

	return operation, nil
}

func (store *MemoryStateStore) failoverOperationForPublicationLocked(expected cluster.Operation) (cluster.Operation, error) {
	if store.activeOperation == nil || store.activeOperation.ID != expected.ID || store.activeOperation.Kind != cluster.OperationKindFailover {
		return cluster.Operation{}, ErrFailoverIntentChanged
	}

	return store.activeOperation.Clone(), nil
}

func (store *MemoryStateStore) failoverCandidateStatusLocked(candidate string) (agentmodel.NodeStatus, error) {
	candidateStatus, ok := store.nodeStatuses[candidate]
	if !ok {
		return agentmodel.NodeStatus{}, ErrFailoverCandidateUnknown
	}

	return candidateStatus.Clone(), nil
}

func beginFailoverExecution(operation cluster.Operation, startedAt time.Time) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = startedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = "executing automatic failover for " + updated.ToMember

	return updated
}

func failoverEpochPublishedMessage(candidate string, epoch cluster.Epoch) string {
	return "automatic failover promoted " + candidate + " and published epoch " + epoch.String()
}

func nextClusterEpoch(current cluster.Epoch) cluster.Epoch {
	return current + 1
}

func promotedNodeStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRolePrimary
	updated.State = cluster.MemberStateRunning
	updated.NeedsRejoin = false
	updated.ObservedAt = observedAt

	updated.Postgres.Managed = true
	updated.Postgres.Up = true
	updated.Postgres.CheckedAt = observedAt
	updated.Postgres.Role = cluster.MemberRolePrimary
	updated.Postgres.RecoveryKnown = true
	updated.Postgres.InRecovery = false

	return updated
}

func buildFailoverExecution(prepared preparedFailoverExecution, operation cluster.Operation, currentEpoch cluster.Epoch) FailoverExecution {
	return FailoverExecution{
		Operation:      operation.Clone(),
		CurrentPrimary: prepared.operation.FromMember,
		Candidate:      prepared.operation.ToMember,
		PreviousEpoch:  prepared.previousEpoch,
		CurrentEpoch:   currentEpoch,
		Fenced:         prepared.spec.Failover.FencingRequired,
		Promoted:       true,
		ExecutedAt:     prepared.executedAt,
	}
}
