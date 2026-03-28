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

	if err := validateSwitchoverRequest(spec, normalized, store.activeOperation); err != nil {
		return SwitchoverValidation{}, err
	}

	currentPrimary, ok := switchoverCurrentPrimary(status)
	if !ok {
		return SwitchoverValidation{}, ErrSwitchoverPrimaryUnknown
	}

	if !currentPrimary.Healthy {
		return SwitchoverValidation{}, ErrSwitchoverPrimaryUnhealthy
	}

	if normalized.Candidate == currentPrimary.Name {
		return SwitchoverValidation{}, ErrSwitchoverTargetIsCurrentPrimary
	}

	target, err := store.switchoverTargetReadinessLocked(spec, status, normalized.Candidate, now)
	if err != nil {
		return SwitchoverValidation{}, err
	}

	if !target.Ready {
		return SwitchoverValidation{}, ErrSwitchoverTargetNotReady
	}

	return SwitchoverValidation{
		Request:        normalized,
		CurrentPrimary: currentPrimary,
		Target:         target,
		CurrentEpoch:   status.CurrentEpoch,
		ValidatedAt:    now,
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
		reasons = append(reasons, "member role is not a standby")
	}

	if target.State != cluster.MemberStateStreaming && target.State != cluster.MemberStateRunning {
		reasons = append(reasons, "member state is not ready for switchover")
	}

	if !target.Healthy {
		reasons = append(reasons, "member is not healthy")
	}

	if target.NeedsRejoin {
		reasons = append(reasons, "member requires rejoin")
	}

	if spec.Failover.MaximumLagBytes > 0 && target.LagBytes > spec.Failover.MaximumLagBytes {
		reasons = append(reasons, "member replication lag exceeds configured maximum")
	}

	if currentPrimary.Timeline > 0 && target.Timeline != currentPrimary.Timeline {
		reasons = append(reasons, "member timeline does not match current primary")
	}

	reasons = appendSwitchoverObservedReadinessReasons(reasons, observed, observedOK)

	return reasons
}

func appendSwitchoverObservedReadinessReasons(reasons []string, observed agentmodel.NodeStatus, observedOK bool) []string {
	if !observedOK {
		return append(reasons, "member node state has not been observed")
	}

	if !observed.Postgres.Managed {
		return append(reasons, "member postgres is not managed")
	}

	if !observed.Postgres.Up {
		reasons = append(reasons, "member postgres is not up")
	}

	if !observed.Postgres.RecoveryKnown {
		reasons = append(reasons, "member recovery state is unknown")
	} else if !observed.Postgres.InRecovery {
		reasons = append(reasons, "member is not currently in recovery")
	}

	if observed.Postgres.Role != "" && !isSwitchoverStandbyRole(observed.Postgres.Role) {
		reasons = append(reasons, "member postgres role is not a standby")
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
