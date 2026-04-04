package controlplane

import (
	"context"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

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

	if err := validateFailoverIntentCreation(spec, status, store.activeOperation); err != nil {
		return FailoverIntent{}, err
	}

	confirmation := confirmPrimaryFailure(spec, status)
	if err := validateFailoverConfirmation(confirmation); err != nil {
		return FailoverIntent{}, err
	}

	candidates := evaluateFailoverCandidates(spec, status)
	normalizedRequest := normalizeFailoverIntentRequest(request)
	selected, err := selectFailoverCandidate(candidates, normalizedRequest.Candidate)
	if err != nil {
		return FailoverIntent{}, err
	}

	operation, err := buildFailoverIntentOperation(now, normalizedRequest, confirmation, selected)
	if err != nil {
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

func validateFailoverIntentCreation(spec cluster.ClusterSpec, status cluster.ClusterStatus, activeOperation *cluster.Operation) error {
	if status.Maintenance.Enabled {
		return ErrFailoverMaintenanceEnabled
	}

	if activeOperation != nil {
		return ErrFailoverOperationInProgress
	}

	switch spec.Failover.Mode {
	case "", cluster.FailoverModeAutomatic:
		return nil
	case cluster.FailoverModeManualOnly, cluster.FailoverModeDisabled:
		return ErrAutomaticFailoverNotAllowed
	default:
		return ErrAutomaticFailoverNotAllowed
	}
}

func validateFailoverConfirmation(confirmation PrimaryFailureConfirmation) error {
	if confirmation.CurrentPrimary == "" {
		return ErrFailoverPrimaryUnknown
	}

	if confirmation.PrimaryHealthy {
		return ErrFailoverPrimaryHealthy
	}

	if confirmation.QuorumRequired && !confirmation.QuorumReachable {
		return ErrFailoverQuorumUnavailable
	}

	return nil
}

func buildFailoverIntentOperation(now time.Time, request FailoverIntentRequest, confirmation PrimaryFailureConfirmation, candidate FailoverCandidate) (cluster.Operation, error) {
	operation := cluster.Operation{
		ID:          failoverOperationID(now),
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateAccepted,
		RequestedBy: request.RequestedBy,
		RequestedAt: now,
		Reason:      request.Reason,
		FromMember:  confirmation.CurrentPrimary,
		ToMember:    candidate.Member.Name,
		Result:      cluster.OperationResultPending,
		Message:     failoverOperationMessage(confirmation.CurrentPrimary, candidate.Member.Name),
	}

	if err := operation.Validate(); err != nil {
		return cluster.Operation{}, err
	}

	return operation, nil
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

func normalizeFailoverIntentRequest(request FailoverIntentRequest) FailoverIntentRequest {
	normalized := request
	normalized.Candidate = strings.TrimSpace(normalized.Candidate)
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

func selectFailoverCandidate(candidates []FailoverCandidate, requested string) (FailoverCandidate, error) {
	if requested == "" {
		selected, ok := firstEligibleFailoverCandidate(candidates)
		if !ok {
			return FailoverCandidate{}, ErrFailoverNoEligibleCandidates
		}

		return selected, nil
	}

	for _, candidate := range candidates {
		if candidate.Member.Name != requested {
			continue
		}
		if !candidate.Eligible {
			return FailoverCandidate{}, ErrFailoverNoEligibleCandidates
		}

		return candidate.Clone(), nil
	}

	return FailoverCandidate{}, ErrFailoverCandidateUnknown
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
