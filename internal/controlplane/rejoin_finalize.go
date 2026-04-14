package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// VerifyRejoinReplication confirms that the restarted former primary is now
// streaming from the current primary and leaves the rejoin operation active
// until the control plane marks the member healthy again.
func (store *MemoryStateStore) VerifyRejoinReplication(ctx context.Context) (RejoinExecution, error) {
	if err := ctx.Err(); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return RejoinExecution{}, err
	}

	prepared, err := store.prepareRejoinReplicationVerification()
	if err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return RejoinExecution{}, err
	}

	return store.publishRejoinReplicationVerification(prepared)
}

// CompleteRejoin clears the synthetic rejoin flags from a verified standby and
// records the completed rejoin operation in history.
func (store *MemoryStateStore) CompleteRejoin(ctx context.Context) (RejoinExecution, error) {
	if err := ctx.Err(); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return RejoinExecution{}, err
	}

	prepared, err := store.prepareRejoinCompletion()
	if err != nil {
		return RejoinExecution{}, err
	}

	return store.publishRejoinCompletion(prepared)
}

func (store *MemoryStateStore) prepareRejoinReplicationVerification() (preparedRejoinExecution, error) {
	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	prepared, err := store.prepareVerifiedRejoinExecutionLocked(executedAt)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	prepared = store.startRejoinContinuationLocked(
		prepared,
		rejoinVerificationRunningMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name),
	)

	return prepared, nil
}

func (store *MemoryStateStore) prepareRejoinCompletion() (preparedRejoinExecution, error) {
	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	return store.prepareVerifiedRejoinExecutionLocked(executedAt)
}

func (store *MemoryStateStore) prepareVerifiedRejoinExecutionLocked(executedAt time.Time) (preparedRejoinExecution, error) {
	operation, err := store.activeRejoinOperationLocked()
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	inputs, err := store.rejoinInputsLocked(operation.FromMember)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	if !inputs.hasMemberNode || !inputs.hasCurrentPrimary || !inputs.hasCurrentPrimaryNode {
		return preparedRejoinExecution{}, ErrRejoinExecutionChanged
	}

	if inputs.member.Name != operation.FromMember || inputs.currentPrimary.Name != operation.ToMember {
		return preparedRejoinExecution{}, ErrRejoinExecutionChanged
	}

	if reasons := assessRejoinReplicationVerificationReasons(inputs); len(reasons) > 0 {
		return preparedRejoinExecution{}, ErrRejoinReplicationNotHealthy
	}

	return preparedRejoinExecution{
		decision:           buildRejoinStrategyDecision(buildRejoinDivergenceAssessment(inputs)).Clone(),
		memberNode:         inputs.memberNode.Clone(),
		currentPrimaryNode: inputs.currentPrimaryNode.Clone(),
		operation:          operation.Clone(),
		currentEpoch:       inputs.currentEpoch,
		executedAt:         executedAt,
	}, nil
}

func assessRejoinReplicationVerificationReasons(inputs rejoinInputs) []string {
	reasons := assessRejoinMemberReasons(inputs)
	if !inputs.hasMemberNode {
		return reasons
	}

	if !inputs.hasCurrentPrimaryNode {
		reasons = append(reasons, reasonCurrentPrimaryStateNotObserved)
		return reasons
	}

	if inputs.memberNode.PendingRestart || inputs.memberNode.Postgres.Details.PendingRestart {
		reasons = append(reasons, reasonRejoinRestartPending)
	}

	if inputs.memberNode.Role != cluster.MemberRoleReplica {
		reasons = append(reasons, reasonRoleNotStandby)
	}

	if inputs.memberNode.State != cluster.MemberStateStreaming {
		reasons = append(reasons, reasonRejoinReplicationNotStreaming)
	}

	if !inputs.memberNode.Postgres.Up {
		reasons = append(reasons, reasonPostgresNotUp)
	}

	if !inputs.memberNode.Postgres.RecoveryKnown {
		reasons = append(reasons, reasonRecoveryStateUnknown)
	}

	if !inputs.memberNode.Postgres.InRecovery {
		reasons = append(reasons, reasonNotInRecovery)
	}

	if inputs.memberNode.Postgres.Role != cluster.MemberRoleReplica {
		reasons = append(reasons, reasonPostgresRoleNotStandby)
	}

	memberSystemIdentifier := strings.TrimSpace(inputs.memberNode.Postgres.Details.SystemIdentifier)
	currentPrimarySystemIdentifier := strings.TrimSpace(inputs.currentPrimaryNode.Postgres.Details.SystemIdentifier)
	switch {
	case memberSystemIdentifier == "":
		reasons = append(reasons, reasonMemberSystemIdentifierUnknown)
	case currentPrimarySystemIdentifier == "":
		reasons = append(reasons, reasonCurrentPrimarySystemIdentifierUnknown)
	case memberSystemIdentifier != currentPrimarySystemIdentifier:
		reasons = append(reasons, reasonSystemIdentifierMismatch)
	}

	switch {
	case inputs.member.Timeline == 0:
		reasons = append(reasons, reasonMemberTimelineUnknown)
	case inputs.currentPrimary.Timeline == 0:
		reasons = append(reasons, reasonCurrentPrimaryTimelineUnknown)
	case inputs.member.Timeline != inputs.currentPrimary.Timeline:
		reasons = append(reasons, reasonTimelineMismatch)
	}

	return reasons
}

func (store *MemoryStateStore) publishRejoinReplicationVerification(prepared preparedRejoinExecution) (RejoinExecution, error) {
	store.mu.Lock()
	running, err := store.rejoinOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return RejoinExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = rejoinVerificationCompletedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name)
	store.activeOperation = &updatedOperation
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	store.mu.Unlock()

	if err := store.persistActiveOperation(context.Background(), updatedOperation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return RejoinExecution{}, err
	}

	return RejoinExecution{
		Operation:           updatedOperation.Clone(),
		Decision:            prepared.decision.Clone(),
		CurrentEpoch:        prepared.currentEpoch,
		State:               cluster.RejoinStateVerifyingReplication,
		ReplicationVerified: true,
		ExecutedAt:          prepared.executedAt,
	}.Clone(), nil
}

func (store *MemoryStateStore) publishRejoinCompletion(prepared preparedRejoinExecution) (RejoinExecution, error) {
	store.mu.Lock()
	running, err := store.rejoinOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return RejoinExecution{}, err
	}

	store.nodeStatuses[prepared.decision.Member.Name] = completedRejoinedMemberStatus(prepared.memberNode, prepared.executedAt)
	completedOperation := completeRejoinExecution(running, prepared.executedAt, prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name)
	store.journalOperationLocked(completedOperation, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	member := store.nodeStatuses[prepared.decision.Member.Name].Clone()
	store.mu.Unlock()

	if err := store.persistNodeStatus(context.Background(), member); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistJournaledOperation(context.Background(), completedOperation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(context.Background()); err != nil {
		return RejoinExecution{}, err
	}

	return RejoinExecution{
		Operation:    completedOperation.Clone(),
		Decision:     prepared.decision.Clone(),
		CurrentEpoch: prepared.currentEpoch,
		State:        cluster.RejoinStateCompleted,
		Completed:    true,
		ExecutedAt:   prepared.executedAt,
	}.Clone(), nil
}

func completeRejoinExecution(operation cluster.Operation, completedAt time.Time, member, currentPrimary string) cluster.Operation {
	updated := operation.Clone()
	updated.State = cluster.OperationStateCompleted
	updated.CompletedAt = completedAt
	updated.Result = cluster.OperationResultSucceeded
	updated.Message = rejoinCompletedMessage(member, currentPrimary)

	return updated
}

func completedRejoinedMemberStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRoleReplica
	updated.State = cluster.MemberStateStreaming
	updated.PendingRestart = false
	updated.NeedsRejoin = false
	updated.ObservedAt = observedAt
	updated.Postgres.Managed = true
	updated.Postgres.Up = true
	updated.Postgres.CheckedAt = observedAt
	updated.Postgres.Role = cluster.MemberRoleReplica
	updated.Postgres.RecoveryKnown = true
	updated.Postgres.InRecovery = true
	updated.Postgres.Details.PendingRestart = false

	return updated
}

func rejoinVerificationRunningMessage(member, currentPrimary string) string {
	return "verifying replication health for " + member + " following " + currentPrimary
}

func rejoinVerificationCompletedMessage(member, currentPrimary string) string {
	return "replication verified for " + member + " following " + currentPrimary + "; final completion is still pending"
}

func rejoinCompletedMessage(member, currentPrimary string) string {
	return "rejoin completed for " + member + " following " + currentPrimary
}
