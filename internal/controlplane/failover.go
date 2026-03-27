package controlplane

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// FailoverEngine exposes automatic failover planning against the replicated
// control-plane state.
type FailoverEngine interface {
	FailoverCandidates() ([]FailoverCandidate, error)
	ConfirmPrimaryFailure() (PrimaryFailureConfirmation, error)
	CreateFailoverIntent(context.Context, FailoverIntentRequest) (FailoverIntent, error)
}

// FailoverCandidate describes a member's promotion eligibility and ranking
// under the current failover policy.
type FailoverCandidate struct {
	Member   cluster.MemberStatus
	Eligible bool
	Rank     int
	Reasons  []string
}

// Clone returns a detached copy of the failover candidate.
func (candidate FailoverCandidate) Clone() FailoverCandidate {
	clone := candidate
	clone.Member = candidate.Member.Clone()
	if candidate.Reasons != nil {
		clone.Reasons = append([]string(nil), candidate.Reasons...)
	}

	return clone
}

// PrimaryFailureConfirmation captures whether the current primary has been
// confirmed failed strongly enough to start automatic failover.
type PrimaryFailureConfirmation struct {
	CurrentPrimary  string
	Confirmed       bool
	PrimaryHealthy  bool
	QuorumRequired  bool
	QuorumReachable bool
	ReachableVoters int
	RequiredVoters  int
	TotalVoters     int
}

// Clone returns a detached copy of the confirmation result.
func (confirmation PrimaryFailureConfirmation) Clone() PrimaryFailureConfirmation {
	return confirmation
}

// FailoverIntentRequest captures operator metadata attached to a newly created
// automatic failover intent.
type FailoverIntentRequest struct {
	RequestedBy string
	Reason      string
}

// FailoverIntent captures the accepted operation and the evaluated state that
// justified it.
type FailoverIntent struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	Confirmation   PrimaryFailureConfirmation
	Candidates     []FailoverCandidate
	CreatedAt      time.Time
}

// Clone returns a detached copy of the failover intent.
func (intent FailoverIntent) Clone() FailoverIntent {
	clone := intent
	clone.Operation = intent.Operation.Clone()
	clone.Confirmation = intent.Confirmation.Clone()
	clone.Candidates = cloneFailoverCandidates(intent.Candidates)

	return clone
}

// FailoverCandidates returns the current ordered promotion candidates together
// with the eligibility reasons applied to every observed member.
func (store *MemoryStateStore) FailoverCandidates() ([]FailoverCandidate, error) {
	store.mu.RLock()
	spec, status, err := store.failoverInputsLocked()
	store.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	return evaluateFailoverCandidates(spec, status), nil
}

// ConfirmPrimaryFailure evaluates the current primary against the observed
// cluster state and quorum policy.
func (store *MemoryStateStore) ConfirmPrimaryFailure() (PrimaryFailureConfirmation, error) {
	store.mu.RLock()
	spec, status, err := store.failoverInputsLocked()
	store.mu.RUnlock()
	if err != nil {
		return PrimaryFailureConfirmation{}, err
	}

	return confirmPrimaryFailure(spec, status), nil
}

// CreateFailoverIntent creates and journals an accepted automatic failover
// operation after the current primary is confirmed failed and a promotion
// candidate is selected.
func (store *MemoryStateStore) CreateFailoverIntent(ctx context.Context, request FailoverIntentRequest) (FailoverIntent, error) {
	if err := ctx.Err(); err != nil {
		return FailoverIntent{}, err
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	spec, status, err := store.failoverInputsLocked()
	if err != nil {
		return FailoverIntent{}, err
	}

	if status.Maintenance.Enabled {
		return FailoverIntent{}, ErrFailoverMaintenanceEnabled
	}

	if store.activeOperation != nil {
		return FailoverIntent{}, ErrFailoverOperationInProgress
	}

	switch spec.Failover.Mode {
	case "", cluster.FailoverModeAutomatic:
	case cluster.FailoverModeManualOnly, cluster.FailoverModeDisabled:
		return FailoverIntent{}, ErrAutomaticFailoverNotAllowed
	default:
		return FailoverIntent{}, ErrAutomaticFailoverNotAllowed
	}

	confirmation := confirmPrimaryFailure(spec, status)
	if confirmation.CurrentPrimary == "" {
		return FailoverIntent{}, ErrFailoverPrimaryUnknown
	}

	if confirmation.PrimaryHealthy {
		return FailoverIntent{}, ErrFailoverPrimaryHealthy
	}

	if confirmation.QuorumRequired && !confirmation.QuorumReachable {
		return FailoverIntent{}, ErrFailoverQuorumUnavailable
	}

	candidates := evaluateFailoverCandidates(spec, status)
	selected, ok := firstEligibleFailoverCandidate(candidates)
	if !ok {
		return FailoverIntent{}, ErrFailoverNoEligibleCandidates
	}

	normalized := normalizeFailoverIntentRequest(request)
	operation := cluster.Operation{
		ID:          failoverOperationID(now),
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateAccepted,
		RequestedBy: normalized.RequestedBy,
		RequestedAt: now,
		Reason:      normalized.Reason,
		FromMember:  confirmation.CurrentPrimary,
		ToMember:    selected.Member.Name,
		Result:      cluster.OperationResultPending,
		Message:     failoverOperationMessage(confirmation.CurrentPrimary, selected.Member.Name),
	}

	if err := operation.Validate(); err != nil {
		return FailoverIntent{}, err
	}

	store.journalOperationLocked(operation, now)
	store.refreshSourceOfTruthLocked(now)

	return FailoverIntent{
		Operation:      operation.Clone(),
		CurrentPrimary: confirmation.CurrentPrimary,
		Candidate:      selected.Member.Name,
		Confirmation:   confirmation.Clone(),
		Candidates:     cloneFailoverCandidates(candidates),
		CreatedAt:      now,
	}, nil
}

func (store *MemoryStateStore) failoverInputsLocked() (cluster.ClusterSpec, cluster.ClusterStatus, error) {
	if store.clusterSpec == nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, ErrClusterSpecRequired
	}

	if store.clusterStatus == nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, ErrFailoverObservedStateRequired
	}

	return store.clusterSpec.Clone(), store.clusterStatus.Clone(), nil
}

func evaluateFailoverCandidates(spec cluster.ClusterSpec, status cluster.ClusterStatus) []FailoverCandidate {
	primary, hasPrimary := failoverPrimaryMember(status)
	primaryTimeline := int64(0)
	if hasPrimary {
		primaryTimeline = primary.Timeline
	}

	candidates := make([]FailoverCandidate, 0, len(status.Members))
	for _, member := range status.Members {
		candidate := FailoverCandidate{
			Member: member.Clone(),
		}

		if member.Name == primary.Name && primary.Name != "" {
			candidate.Reasons = append(candidate.Reasons, "member is the current primary")
		}

		switch member.Role {
		case cluster.MemberRoleReplica, cluster.MemberRoleStandbyLeader:
		default:
			candidate.Reasons = append(candidate.Reasons, "member role is not promotable")
		}

		if !member.Healthy {
			candidate.Reasons = append(candidate.Reasons, "member is not healthy")
		}

		if member.NeedsRejoin {
			candidate.Reasons = append(candidate.Reasons, "member requires rejoin")
		}

		if member.NoFailover {
			candidate.Reasons = append(candidate.Reasons, "member is tagged no-failover")
		}

		if spec.Failover.MaximumLagBytes > 0 && member.LagBytes > spec.Failover.MaximumLagBytes {
			candidate.Reasons = append(candidate.Reasons, "member replication lag exceeds failover policy")
		}

		if spec.Failover.CheckTimeline && primaryTimeline > 0 && member.Timeline != primaryTimeline {
			candidate.Reasons = append(candidate.Reasons, "member timeline does not match current primary")
		}

		candidate.Eligible = len(candidate.Reasons) == 0
		candidates = append(candidates, candidate)
	}

	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].Eligible != candidates[right].Eligible {
			return candidates[left].Eligible
		}

		if !candidates[left].Eligible {
			return candidates[left].Member.Name < candidates[right].Member.Name
		}

		if candidates[left].Member.Priority != candidates[right].Member.Priority {
			return candidates[left].Member.Priority > candidates[right].Member.Priority
		}

		if candidates[left].Member.LagBytes != candidates[right].Member.LagBytes {
			return candidates[left].Member.LagBytes < candidates[right].Member.LagBytes
		}

		if candidates[left].Member.Timeline != candidates[right].Member.Timeline {
			return candidates[left].Member.Timeline > candidates[right].Member.Timeline
		}

		if !candidates[left].Member.LastSeenAt.Equal(candidates[right].Member.LastSeenAt) {
			return candidates[left].Member.LastSeenAt.After(candidates[right].Member.LastSeenAt)
		}

		return candidates[left].Member.Name < candidates[right].Member.Name
	})

	rank := 1
	for index := range candidates {
		if !candidates[index].Eligible {
			continue
		}

		candidates[index].Rank = rank
		rank++
	}

	return cloneFailoverCandidates(candidates)
}

func confirmPrimaryFailure(spec cluster.ClusterSpec, status cluster.ClusterStatus) PrimaryFailureConfirmation {
	primary, hasPrimary := failoverPrimaryMember(status)
	totalVoters, reachableVoters := quorumVoteCounts(spec, status)
	requiredVoters := 0
	if totalVoters > 0 {
		requiredVoters = totalVoters/2 + 1
	}

	confirmation := PrimaryFailureConfirmation{
		QuorumRequired:  spec.Failover.RequireQuorum,
		QuorumReachable: requiredVoters == 0 || reachableVoters >= requiredVoters,
		ReachableVoters: reachableVoters,
		RequiredVoters:  requiredVoters,
		TotalVoters:     totalVoters,
	}

	if !hasPrimary {
		return confirmation
	}

	confirmation.CurrentPrimary = primary.Name
	confirmation.PrimaryHealthy = primary.Healthy
	confirmation.Confirmed = !primary.Healthy && (!confirmation.QuorumRequired || confirmation.QuorumReachable)

	return confirmation
}

func failoverPrimaryMember(status cluster.ClusterStatus) (cluster.MemberStatus, bool) {
	if strings.TrimSpace(status.CurrentPrimary) != "" {
		for _, member := range status.Members {
			if member.Name == status.CurrentPrimary {
				return member.Clone(), true
			}
		}
	}

	return currentPrimaryMember(status.Members)
}

func quorumVoteCounts(spec cluster.ClusterSpec, status cluster.ClusterStatus) (int, int) {
	observed := make(map[string]cluster.MemberStatus, len(status.Members))
	for _, member := range status.Members {
		observed[member.Name] = member.Clone()
	}

	if len(spec.Members) > 0 {
		reachable := 0
		for _, member := range spec.Members {
			observedMember, ok := observed[member.Name]
			if ok && observedMember.Healthy {
				reachable++
			}
		}

		return len(spec.Members), reachable
	}

	reachable := 0
	for _, member := range status.Members {
		if member.Healthy {
			reachable++
		}
	}

	return len(status.Members), reachable
}

func firstEligibleFailoverCandidate(candidates []FailoverCandidate) (FailoverCandidate, bool) {
	for _, candidate := range candidates {
		if candidate.Eligible {
			return candidate.Clone(), true
		}
	}

	return FailoverCandidate{}, false
}

func cloneFailoverCandidates(candidates []FailoverCandidate) []FailoverCandidate {
	if candidates == nil {
		return nil
	}

	cloned := make([]FailoverCandidate, len(candidates))
	for index, candidate := range candidates {
		cloned[index] = candidate.Clone()
	}

	return cloned
}

func normalizeFailoverIntentRequest(request FailoverIntentRequest) FailoverIntentRequest {
	normalized := request
	normalized.RequestedBy = strings.TrimSpace(normalized.RequestedBy)
	normalized.Reason = strings.TrimSpace(normalized.Reason)

	if normalized.RequestedBy == "" {
		normalized.RequestedBy = "controlplane"
	}

	if normalized.Reason == "" {
		normalized.Reason = "primary failure confirmed"
	}

	return normalized
}

func failoverOperationID(now time.Time) string {
	return "failover-" + now.UTC().Format("20060102T150405.000000000Z07:00")
}

func failoverOperationMessage(fromMember, toMember string) string {
	if fromMember == "" {
		return "automatic failover accepted"
	}

	return "automatic failover from " + fromMember + " to " + toMember + " accepted"
}
