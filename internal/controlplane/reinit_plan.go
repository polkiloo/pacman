package controlplane

import (
	"context"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// ValidateReinit applies the destructive replica reinitialization preflight
// rules without journaling an operation.
func (store *MemoryStateStore) ValidateReinit(ctx context.Context, request ReinitRequest) (ReinitValidation, error) {
	if err := ctx.Err(); err != nil {
		return ReinitValidation{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitValidation{}, err
	}

	now := store.now().UTC()
	normalized := normalizeReinitRequest(request)

	store.mu.RLock()
	defer store.mu.RUnlock()

	_, status, err := store.reinitInputsLocked()
	if err != nil {
		return ReinitValidation{}, err
	}

	return evaluateReinitRequest(status, normalized, store.activeOperation, now)
}

// CreateReinitIntent validates and journals a destructive replica
// reinitialization operation. Execution is intentionally separate from
// former-primary rejoin and from this intent creation surface.
func (store *MemoryStateStore) CreateReinitIntent(ctx context.Context, request ReinitRequest) (ReinitIntent, error) {
	if err := ctx.Err(); err != nil {
		return ReinitIntent{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return ReinitIntent{}, err
	}

	now := store.now().UTC()
	normalized := normalizeReinitRequest(request)

	store.mu.Lock()
	_, status, err := store.reinitInputsLocked()
	if err != nil {
		store.mu.Unlock()
		return ReinitIntent{}, err
	}

	validation, err := evaluateReinitRequest(status, normalized, store.activeOperation, now)
	if err != nil {
		store.mu.Unlock()
		return ReinitIntent{}, err
	}

	operation, err := buildReinitIntentOperation(now, validation)
	if err != nil {
		store.mu.Unlock()
		return ReinitIntent{}, err
	}

	store.journalOperationLocked(operation, now)
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	if err := store.persistJournaledOperation(ctx, operation); err != nil {
		return ReinitIntent{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return ReinitIntent{}, err
	}

	store.logAudit(ctx, "accepted reinit intent", "reinit.requested", operationLogAttrs(operation)...)

	return ReinitIntent{
		Operation:  operation.Clone(),
		Validation: validation.Clone(),
		CreatedAt:  now,
	}.Clone(), nil
}

func (store *MemoryStateStore) reinitInputsLocked() (cluster.ClusterSpec, cluster.ClusterStatus, error) {
	if store.clusterSpec == nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, ErrClusterSpecRequired
	}

	if store.clusterStatus == nil {
		return cluster.ClusterSpec{}, cluster.ClusterStatus{}, ErrReinitObservedStateRequired
	}

	return store.clusterSpec.Clone(), store.clusterStatus.Clone(), nil
}

func evaluateReinitRequest(status cluster.ClusterStatus, request ReinitRequest, activeOperation *cluster.Operation, checkedAt time.Time) (ReinitValidation, error) {
	return evaluateReinitRequestWithUnknownTarget(status, request, activeOperation, checkedAt, false)
}

func evaluateReinitExecutionRequest(status cluster.ClusterStatus, request ReinitRequest, checkedAt time.Time) (ReinitValidation, error) {
	// A restarted target reports unknown until PostgreSQL is restored, but its
	// accepted operation still proves that it was a replica at request time.
	return evaluateReinitRequestWithUnknownTarget(status, request, nil, checkedAt, true)
}

func evaluateReinitRequestWithUnknownTarget(status cluster.ClusterStatus, request ReinitRequest, activeOperation *cluster.Operation, checkedAt time.Time, allowUnknownTarget bool) (ReinitValidation, error) {
	if request.Member == "" {
		return ReinitValidation{}, ErrReinitTargetRequired
	}

	if activeOperation != nil {
		return ReinitValidation{}, ErrReinitOperationInProgress
	}

	currentPrimary, ok := currentPrimaryMember(status.Members)
	if !ok {
		return ReinitValidation{}, ErrReinitSourcePrimaryUnknown
	}

	if !currentPrimary.Healthy {
		return ReinitValidation{}, ErrReinitSourcePrimaryUnhealthy
	}

	target, ok := memberByName(status.Members, request.Member)
	if !ok {
		return ReinitValidation{}, ErrReinitTargetUnknown
	}

	if target.Name == currentPrimary.Name || target.Role == cluster.MemberRolePrimary {
		return ReinitValidation{}, ErrReinitTargetIsCurrentPrimary
	}

	if !target.Role.IsDataBearing() && (!allowUnknownTarget || target.Role != cluster.MemberRoleUnknown) {
		return ReinitValidation{}, ErrReinitTargetIsWitness
	}

	return ReinitValidation{
		Request:        request.Clone(),
		CurrentPrimary: currentPrimary,
		Target:         target,
		CurrentEpoch:   status.CurrentEpoch,
		ValidatedAt:    checkedAt,
	}.Clone(), nil
}

func normalizeReinitRequest(request ReinitRequest) ReinitRequest {
	normalized := request
	normalized.Member = strings.TrimSpace(normalized.Member)
	normalized.RequestedBy = strings.TrimSpace(normalized.RequestedBy)
	normalized.Reason = strings.TrimSpace(normalized.Reason)

	if normalized.RequestedBy == "" {
		normalized.RequestedBy = "operator"
	}

	if normalized.Reason == "" {
		normalized.Reason = "replica reinitialization requested"
	}

	return normalized
}

func buildReinitIntentOperation(now time.Time, validation ReinitValidation) (cluster.Operation, error) {
	operation := cluster.Operation{
		ID:          reinitOperationID(now),
		Kind:        cluster.OperationKindReinit,
		State:       cluster.OperationStateAccepted,
		RequestedBy: validation.Request.RequestedBy,
		RequestedAt: now,
		Reason:      validation.Request.Reason,
		FromMember:  validation.CurrentPrimary.Name,
		ToMember:    validation.Target.Name,
		Result:      cluster.OperationResultPending,
		Message:     reinitOperationMessage(validation.CurrentPrimary.Name, validation.Target.Name),
	}

	if err := operation.Validate(); err != nil {
		return cluster.Operation{}, err
	}

	return operation, nil
}

func reinitOperationID(now time.Time) string {
	return "reinit-" + now.UTC().Format("20060102T150405.000000000Z07:00")
}

func reinitOperationMessage(sourcePrimary, target string) string {
	return "replica reinitialization for " + target + " from " + sourcePrimary + " accepted"
}

func memberByName(members []cluster.MemberStatus, name string) (cluster.MemberStatus, bool) {
	for _, member := range members {
		if member.Name == name {
			return member.Clone(), true
		}
	}

	return cluster.MemberStatus{}, false
}
