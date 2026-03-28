package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// SwitchoverTargetReadiness reports whether the requested standby is healthy
// and observed as a promotable PostgreSQL standby under the current topology.
func (store *MemoryStateStore) SwitchoverTargetReadiness(candidate string) (SwitchoverTargetReadiness, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	spec, status, err := store.switchoverInputsLocked()
	if err != nil {
		return SwitchoverTargetReadiness{}, err
	}

	return store.switchoverTargetReadinessLocked(spec, status, strings.TrimSpace(candidate), store.now().UTC())
}

// ValidateSwitchover applies the planned switchover preflight rules and
// confirms that the requested standby is ready to become primary.
func (store *MemoryStateStore) ValidateSwitchover(ctx context.Context, request SwitchoverRequest) (SwitchoverValidation, error) {
	if err := ctx.Err(); err != nil {
		return SwitchoverValidation{}, err
	}

	now := store.now().UTC()
	normalized := normalizeSwitchoverRequest(request)

	store.mu.RLock()
	defer store.mu.RUnlock()

	spec, status, err := store.switchoverInputsLocked()
	if err != nil {
		return SwitchoverValidation{}, err
	}

	return store.evaluateSwitchoverRequestLocked(spec, status, normalized, store.activeOperation, now)
}

// CreateSwitchoverIntent validates and journals a planned switchover
// operation so the control plane can coordinate a safe topology handoff.
func (store *MemoryStateStore) CreateSwitchoverIntent(ctx context.Context, request SwitchoverRequest) (SwitchoverIntent, error) {
	if err := ctx.Err(); err != nil {
		return SwitchoverIntent{}, err
	}

	now := store.now().UTC()
	normalized := normalizeSwitchoverRequest(request)

	store.mu.Lock()
	defer store.mu.Unlock()

	spec, status, err := store.switchoverInputsLocked()
	if err != nil {
		return SwitchoverIntent{}, err
	}

	validation, err := store.evaluateSwitchoverRequestLocked(spec, status, normalized, store.activeOperation, now)
	if err != nil {
		return SwitchoverIntent{}, err
	}

	operation, err := buildSwitchoverIntentOperation(now, validation)
	if err != nil {
		return SwitchoverIntent{}, err
	}

	store.journalOperationLocked(operation, now)
	store.refreshSourceOfTruthLocked(now)

	return SwitchoverIntent{
		Operation:  operation.Clone(),
		Validation: validation.Clone(),
		CreatedAt:  now,
	}.Clone(), nil
}

func (store *MemoryStateStore) switchoverInputsLocked() (cluster.ClusterSpec, cluster.ClusterStatus, error) {
	if store.clusterSpec == nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, ErrClusterSpecRequired
	}

	if store.clusterStatus == nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, ErrSwitchoverObservedStateRequired
	}

	return store.clusterSpec.Clone(), store.clusterStatus.Clone(), nil
}

func validateSwitchoverRequest(spec cluster.ClusterSpec, request SwitchoverRequest, activeOperation *cluster.Operation) error {
	if request.Candidate == "" {
		return ErrSwitchoverTargetRequired
	}

	if activeOperation != nil {
		return ErrSwitchoverOperationInProgress
	}

	if !request.ScheduledAt.IsZero() && !spec.Switchover.AllowScheduled {
		return ErrSwitchoverSchedulingNotAllowed
	}

	return nil
}

func (store *MemoryStateStore) evaluateSwitchoverRequestLocked(spec cluster.ClusterSpec, status cluster.ClusterStatus, request SwitchoverRequest, activeOperation *cluster.Operation, checkedAt time.Time) (SwitchoverValidation, error) {
	if err := validateSwitchoverRequest(spec, request, activeOperation); err != nil {
		return SwitchoverValidation{}, err
	}

	currentPrimary, ok := switchoverCurrentPrimary(status)
	if !ok {
		return SwitchoverValidation{}, ErrSwitchoverPrimaryUnknown
	}

	if !currentPrimary.Healthy {
		return SwitchoverValidation{}, ErrSwitchoverPrimaryUnhealthy
	}

	if request.Candidate == currentPrimary.Name {
		return SwitchoverValidation{}, ErrSwitchoverTargetIsCurrentPrimary
	}

	target, err := store.switchoverTargetReadinessLocked(spec, status, request.Candidate, checkedAt)
	if err != nil {
		return SwitchoverValidation{}, err
	}

	if !target.Ready {
		return SwitchoverValidation{}, ErrSwitchoverTargetNotReady
	}

	return SwitchoverValidation{
		Request:        request.Clone(),
		CurrentPrimary: currentPrimary,
		Target:         target,
		CurrentEpoch:   status.CurrentEpoch,
		ValidatedAt:    checkedAt,
	}.Clone(), nil
}

func normalizeSwitchoverRequest(request SwitchoverRequest) SwitchoverRequest {
	normalized := request
	normalized.RequestedBy = strings.TrimSpace(normalized.RequestedBy)
	normalized.Reason = strings.TrimSpace(normalized.Reason)
	normalized.Candidate = strings.TrimSpace(normalized.Candidate)
	if normalized.ScheduledAt != (time.Time{}) {
		normalized.ScheduledAt = normalized.ScheduledAt.UTC()
	}

	if normalized.RequestedBy == "" {
		normalized.RequestedBy = "operator"
	}

	if normalized.Reason == "" {
		normalized.Reason = "planned switchover"
	}

	return normalized
}

func buildSwitchoverIntentOperation(now time.Time, validation SwitchoverValidation) (cluster.Operation, error) {
	operation := cluster.Operation{
		ID:          switchoverOperationID(now),
		Kind:        cluster.OperationKindSwitchover,
		State:       switchoverIntentOperationState(now, validation.Request),
		RequestedBy: validation.Request.RequestedBy,
		RequestedAt: now,
		Reason:      validation.Request.Reason,
		FromMember:  validation.CurrentPrimary.Name,
		ToMember:    validation.Target.Member.Name,
		Result:      cluster.OperationResultPending,
		Message:     switchoverOperationMessage(validation),
	}

	if validation.Request.ScheduledAt.After(now) {
		operation.ScheduledAt = validation.Request.ScheduledAt
	}

	if err := operation.Validate(); err != nil {
		return cluster.Operation{}, err
	}

	return operation, nil
}

func switchoverIntentOperationState(now time.Time, request SwitchoverRequest) cluster.OperationState {
	if request.ScheduledAt.After(now) {
		return cluster.OperationStateScheduled
	}

	return cluster.OperationStateAccepted
}

func switchoverOperationID(now time.Time) string {
	return "switchover-" + now.UTC().Format("20060102T150405.000000000Z07:00")
}

func switchoverOperationMessage(validation SwitchoverValidation) string {
	if validation.Request.ScheduledAt.IsZero() || !validation.Request.ScheduledAt.After(validation.ValidatedAt) {
		return "planned switchover from " + validation.CurrentPrimary.Name + " to " + validation.Target.Member.Name + " accepted"
	}

	return "planned switchover from " + validation.CurrentPrimary.Name + " to " + validation.Target.Member.Name + " scheduled"
}

func switchoverCurrentPrimary(status cluster.ClusterStatus) (cluster.MemberStatus, bool) {
	return failoverPrimaryMember(status)
}

func (store *MemoryStateStore) switchoverTargetReadinessLocked(spec cluster.ClusterSpec, status cluster.ClusterStatus, candidate string, checkedAt time.Time) (SwitchoverTargetReadiness, error) {
	target, ok := switchoverTargetMember(status.Members, candidate)
	if !ok {
		return SwitchoverTargetReadiness{}, ErrSwitchoverTargetUnknown
	}

	currentPrimary, _ := switchoverCurrentPrimary(status)
	observed, observedOK := store.nodeStatuses[target.Name]
	readiness := buildSwitchoverTargetReadiness(spec, currentPrimary, target, observed, observedOK, checkedAt)

	return readiness, nil
}

func switchoverTargetMember(members []cluster.MemberStatus, candidate string) (cluster.MemberStatus, bool) {
	for _, member := range members {
		if member.Name == candidate {
			return member.Clone(), true
		}
	}

	return cluster.MemberStatus{}, false
}

func buildSwitchoverTargetReadiness(spec cluster.ClusterSpec, currentPrimary, target cluster.MemberStatus, observed agentmodel.NodeStatus, observedOK bool, checkedAt time.Time) SwitchoverTargetReadiness {
	readiness := SwitchoverTargetReadiness{
		CurrentPrimary: currentPrimary.Name,
		Member:         target.Clone(),
		CheckedAt:      checkedAt,
	}

	readiness.Reasons = switchoverTargetReasons(spec, currentPrimary, target, observed, observedOK)
	readiness.Ready = len(readiness.Reasons) == 0

	return readiness.Clone()
}

func switchoverTargetReasons(spec cluster.ClusterSpec, currentPrimary, target cluster.MemberStatus, observed agentmodel.NodeStatus, observedOK bool) []string {
	reasons := make([]string, 0, 8)

	if !isSwitchoverStandbyRole(target.Role) {
		reasons = append(reasons, reasonRoleNotStandby)
	}

	if target.State != cluster.MemberStateStreaming && target.State != cluster.MemberStateRunning {
		reasons = append(reasons, reasonStateNotReadyForSwitchover)
	}

	if !target.Healthy {
		reasons = append(reasons, reasonMemberUnhealthy)
	}

	if target.NeedsRejoin {
		reasons = append(reasons, reasonMemberRequiresRejoin)
	}

	if spec.Failover.MaximumLagBytes > 0 && target.LagBytes > spec.Failover.MaximumLagBytes {
		reasons = append(reasons, reasonLagExceedsSwitchoverMaximum)
	}

	if currentPrimary.Timeline > 0 && target.Timeline != currentPrimary.Timeline {
		reasons = append(reasons, reasonTimelineMismatch)
	}

	reasons = appendSwitchoverObservedReadinessReasons(reasons, observed, observedOK)

	return reasons
}

func appendSwitchoverObservedReadinessReasons(reasons []string, observed agentmodel.NodeStatus, observedOK bool) []string {
	if !observedOK {
		return append(reasons, reasonNodeStateNotObserved)
	}

	if !observed.Postgres.Managed {
		return append(reasons, reasonPostgresNotManaged)
	}

	if !observed.Postgres.Up {
		reasons = append(reasons, reasonPostgresNotUp)
	}

	if !observed.Postgres.RecoveryKnown {
		reasons = append(reasons, reasonRecoveryStateUnknown)
	} else if !observed.Postgres.InRecovery {
		reasons = append(reasons, reasonNotInRecovery)
	}

	if observed.Postgres.Role != "" && !isSwitchoverStandbyRole(observed.Postgres.Role) {
		reasons = append(reasons, reasonPostgresRoleNotStandby)
	}

	return reasons
}

func isSwitchoverStandbyRole(role cluster.MemberRole) bool {
	switch role {
	case cluster.MemberRoleReplica, cluster.MemberRoleStandbyLeader:
		return true
	default:
		return false
	}
}
