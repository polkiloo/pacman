package controlplane

import (
	"context"
	"net"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

// ExecuteRejoinStandbyConfig renders the local standby configuration for a
// rewound former primary and keeps the rejoin operation active while restart is
// still pending.
func (store *MemoryStateStore) ExecuteRejoinStandbyConfig(ctx context.Context, configurator StandbyConfigExecutor) (RejoinExecution, error) {
	if err := ctx.Err(); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return RejoinExecution{}, err
	}

	prepared, err := store.prepareRejoinStandbyConfig(configurator)
	if err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return RejoinExecution{}, err
	}

	if err := configurator.ConfigureStandby(ctx, buildStandbyConfigRequest(prepared)); err != nil {
		store.failRejoinExecution(prepared, rejoinStandbyConfigFailedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name))
		return RejoinExecution{}, err
	}

	return store.publishRejoinStandbyConfig(prepared)
}

// ExecuteRejoinRestartAsStandby restarts the former primary in standby mode
// after the local standby configuration has been rendered.
func (store *MemoryStateStore) ExecuteRejoinRestartAsStandby(ctx context.Context, restarter StandbyRestartExecutor) (RejoinExecution, error) {
	if err := ctx.Err(); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return RejoinExecution{}, err
	}

	prepared, err := store.prepareRejoinStandbyRestart(restarter)
	if err != nil {
		return RejoinExecution{}, err
	}

	if err := store.persistActiveOperation(ctx, prepared.operation); err != nil {
		return RejoinExecution{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return RejoinExecution{}, err
	}

	if err := restarter.RestartAsStandby(ctx, buildStandbyRestartRequest(prepared)); err != nil {
		store.failRejoinExecution(prepared, rejoinRestartFailedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name))
		return RejoinExecution{}, err
	}

	return store.publishRejoinStandbyRestart(prepared)
}

func (store *MemoryStateStore) prepareRejoinStandbyConfig(configurator StandbyConfigExecutor) (preparedRejoinExecution, error) {
	if configurator == nil {
		return preparedRejoinExecution{}, ErrRejoinStandbyConfigExecutorRequired
	}

	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	prepared, err := store.prepareActiveRejoinContinuationLocked(executedAt)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	standby, err := buildRejoinStandbyConfig(store.clusterSpec.Clone(), prepared.decision.Member.Name, prepared.currentPrimaryNode.Postgres.Address)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	prepared.standby = standby
	prepared = store.startRejoinContinuationLocked(
		prepared,
		rejoinStandbyConfigRunningMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name),
	)

	return prepared, nil
}

func (store *MemoryStateStore) prepareRejoinStandbyRestart(restarter StandbyRestartExecutor) (preparedRejoinExecution, error) {
	if restarter == nil {
		return preparedRejoinExecution{}, ErrRejoinStandbyRestartExecutorRequired
	}

	executedAt := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	prepared, err := store.prepareActiveRejoinContinuationLocked(executedAt)
	if err != nil {
		return preparedRejoinExecution{}, err
	}

	if !prepared.memberNode.PendingRestart && !prepared.memberNode.Postgres.Details.PendingRestart {
		return preparedRejoinExecution{}, ErrRejoinStandbyConfigurationRequired
	}

	prepared = store.startRejoinContinuationLocked(
		prepared,
		rejoinRestartRunningMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name),
	)

	return prepared, nil
}

func (store *MemoryStateStore) prepareActiveRejoinContinuationLocked(executedAt time.Time) (preparedRejoinExecution, error) {
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

	decision := buildRejoinStrategyDecision(buildRejoinDivergenceAssessment(inputs))
	if err := validateRejoinRewindDecision(decision); err != nil {
		return preparedRejoinExecution{}, err
	}

	return preparedRejoinExecution{
		decision:           decision.Clone(),
		memberNode:         inputs.memberNode.Clone(),
		currentPrimaryNode: inputs.currentPrimaryNode.Clone(),
		operation:          operation.Clone(),
		currentEpoch:       inputs.currentEpoch,
		executedAt:         executedAt,
	}, nil
}

func (store *MemoryStateStore) startRejoinContinuationLocked(prepared preparedRejoinExecution, message string) preparedRejoinExecution {
	updated := prepared.operation.Clone()
	updated.State = cluster.OperationStateRunning
	if updated.StartedAt.IsZero() {
		updated.StartedAt = prepared.executedAt
	}
	updated.Result = cluster.OperationResultPending
	updated.Message = message
	store.journalOperationLocked(updated, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)

	prepared.operation = updated.Clone()
	return prepared
}

func (store *MemoryStateStore) publishRejoinStandbyConfig(prepared preparedRejoinExecution) (RejoinExecution, error) {
	store.mu.Lock()
	running, err := store.rejoinOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return RejoinExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = rejoinStandbyConfigCompletedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.decision.Member.Name] = configuredFormerPrimaryStandbyStatus(prepared.memberNode, prepared.executedAt)
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
		Operation:         updatedOperation.Clone(),
		Decision:          prepared.decision.Clone(),
		CurrentEpoch:      prepared.currentEpoch,
		State:             cluster.RejoinStateConfiguringStandby,
		StandbyConfigured: true,
		ExecutedAt:        prepared.executedAt,
	}.Clone(), nil
}

func (store *MemoryStateStore) publishRejoinStandbyRestart(prepared preparedRejoinExecution) (RejoinExecution, error) {
	store.mu.Lock()
	running, err := store.rejoinOperationForPublicationLocked(prepared.operation)
	if err != nil {
		store.mu.Unlock()
		return RejoinExecution{}, err
	}

	updatedOperation := running.Clone()
	updatedOperation.Message = rejoinRestartCompletedMessage(prepared.decision.Member.Name, prepared.decision.CurrentPrimary.Name)
	store.activeOperation = &updatedOperation
	store.nodeStatuses[prepared.decision.Member.Name] = restartingFormerPrimaryStandbyStatus(prepared.memberNode, prepared.executedAt)
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
		Operation:          updatedOperation.Clone(),
		Decision:           prepared.decision.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
		State:              cluster.RejoinStateStartingReplica,
		RestartedAsStandby: true,
		ExecutedAt:         prepared.executedAt,
	}.Clone(), nil
}

func buildStandbyConfigRequest(prepared preparedRejoinExecution) StandbyConfigRequest {
	return StandbyConfigRequest{
		Operation:          prepared.operation.Clone(),
		Decision:           prepared.decision.Clone(),
		MemberNode:         prepared.memberNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
		Standby:            prepared.standby.WithDefaults(),
	}
}

func buildStandbyRestartRequest(prepared preparedRejoinExecution) StandbyRestartRequest {
	return StandbyRestartRequest{
		Operation:          prepared.operation.Clone(),
		Decision:           prepared.decision.Clone(),
		MemberNode:         prepared.memberNode.Clone(),
		CurrentPrimaryNode: prepared.currentPrimaryNode.Clone(),
		CurrentEpoch:       prepared.currentEpoch,
	}
}

func (store *MemoryStateStore) activeRejoinOperationLocked() (cluster.Operation, error) {
	if store.activeOperation == nil {
		return cluster.Operation{}, ErrRejoinExecutionRequired
	}

	operation := store.activeOperation.Clone()
	if operation.Kind != cluster.OperationKindRejoin || operation.State.IsTerminal() {
		return cluster.Operation{}, ErrRejoinExecutionRequired
	}

	if strings.TrimSpace(operation.FromMember) == "" || strings.TrimSpace(operation.ToMember) == "" {
		return cluster.Operation{}, ErrRejoinExecutionRequired
	}

	return operation, nil
}

func (store *MemoryStateStore) rejoinOperationForPublicationLocked(expected cluster.Operation) (cluster.Operation, error) {
	if store.activeOperation == nil || store.activeOperation.ID != expected.ID || store.activeOperation.Kind != cluster.OperationKindRejoin {
		return cluster.Operation{}, ErrRejoinExecutionChanged
	}

	return store.activeOperation.Clone(), nil
}

func buildRejoinStandbyConfig(spec cluster.ClusterSpec, memberName, currentPrimaryAddress string) (postgres.StandbyConfig, error) {
	connInfo, err := rejoinPrimaryConnInfo(currentPrimaryAddress, memberName)
	if err != nil {
		return postgres.StandbyConfig{}, err
	}

	return (postgres.StandbyConfig{
		PrimaryConnInfo:        connInfo,
		PrimarySlotName:        rejoinPrimarySlotName(memberName),
		RestoreCommand:         stringPostgresParameter(spec.Postgres.Parameters, "restore_command"),
		RecoveryTargetTimeline: stringPostgresParameter(spec.Postgres.Parameters, "recovery_target_timeline"),
	}).WithDefaults(), nil
}

func rejoinPrimaryConnInfo(address, memberName string) (string, error) {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return "", ErrRejoinCurrentPrimaryAddressRequired
	}

	host := trimmed
	port := ""
	if parsedHost, parsedPort, err := net.SplitHostPort(trimmed); err == nil {
		host = parsedHost
		port = parsedPort
	}

	parts := []string{"host=" + host}
	if port != "" {
		parts = append(parts, "port="+port)
	}
	if member := strings.TrimSpace(memberName); member != "" {
		parts = append(parts, "application_name="+member)
	}

	return strings.Join(parts, " "), nil
}

func rejoinPrimarySlotName(memberName string) string {
	normalized := strings.ToLower(strings.TrimSpace(memberName))
	if normalized == "" {
		return "pacman_rejoin"
	}

	var builder strings.Builder
	lastUnderscore := false
	for _, character := range normalized {
		switch {
		case character >= 'a' && character <= 'z':
			builder.WriteRune(character)
			lastUnderscore = false
		case character >= '0' && character <= '9':
			builder.WriteRune(character)
			lastUnderscore = false
		case character == '_':
			if !lastUnderscore {
				builder.WriteRune(character)
				lastUnderscore = true
			}
		default:
			if !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		}
	}

	slot := strings.Trim(builder.String(), "_")
	if slot == "" {
		return "pacman_rejoin"
	}
	if len(slot) > 63 {
		slot = strings.TrimRight(slot[:63], "_")
	}
	if slot == "" {
		return "pacman_rejoin"
	}

	return slot
}

func stringPostgresParameter(parameters map[string]any, key string) string {
	if parameters == nil {
		return ""
	}

	value, ok := parameters[key]
	if !ok {
		return ""
	}

	text, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(text)
}

func configuredFormerPrimaryStandbyStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := rewoundFormerPrimaryStatus(status, observedAt)
	updated.PendingRestart = true
	updated.Postgres.Details.PendingRestart = true

	return updated
}

func restartingFormerPrimaryStandbyStatus(status agentmodel.NodeStatus, observedAt time.Time) agentmodel.NodeStatus {
	updated := status.Clone()
	updated.Role = cluster.MemberRoleReplica
	updated.State = cluster.MemberStateStarting
	updated.PendingRestart = false
	updated.NeedsRejoin = true
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

func rejoinStandbyConfigRunningMessage(member, currentPrimary string) string {
	return "rendering standby configuration for " + member + " to follow " + currentPrimary
}

func rejoinStandbyConfigCompletedMessage(member, currentPrimary string) string {
	return "standby configuration rendered for " + member + " to follow " + currentPrimary + "; restart as standby is still pending"
}

func rejoinStandbyConfigFailedMessage(member, currentPrimary string) string {
	return "standby configuration failed for " + member + " against " + currentPrimary
}

func rejoinRestartRunningMessage(member, currentPrimary string) string {
	return "restarting " + member + " as a standby following " + currentPrimary
}

func rejoinRestartCompletedMessage(member, currentPrimary string) string {
	return member + " restarted as a standby following " + currentPrimary + "; replication verification is still pending"
}

func rejoinRestartFailedMessage(member, currentPrimary string) string {
	return "restart as standby failed for " + member + " against " + currentPrimary
}
