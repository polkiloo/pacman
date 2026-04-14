package controlplane

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// ClusterStatus returns the aggregated cluster-wide observed state derived from
// the replicated member observations.
func (store *MemoryStateStore) ClusterStatus() (cluster.ClusterStatus, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return cluster.ClusterStatus{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if store.clusterStatus == nil {
		return cluster.ClusterStatus{}, false
	}

	return store.clusterStatus.Clone(), true
}

// Reconcile refreshes the source-of-truth snapshot from the desired state and
// the latest observed member information.
func (store *MemoryStateStore) Reconcile(ctx context.Context) (ClusterSourceOfTruth, error) {
	if err := ctx.Err(); err != nil {
		return ClusterSourceOfTruth{}, err
	}

	if err := store.forceRefreshCache(ctx); err != nil {
		return ClusterSourceOfTruth{}, err
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.refreshSourceOfTruthLocked(now)

	truth := store.sourceOfTruthLocked()
	if truth.Desired == nil && truth.Observed == nil {
		return ClusterSourceOfTruth{}, ErrSourceOfTruthStateRequired
	}

	return truth, nil
}

// MaintenanceStatus returns the currently effective maintenance mode.
func (store *MemoryStateStore) MaintenanceStatus() cluster.MaintenanceModeStatus {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return cluster.MaintenanceModeStatus{}
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.maintenance
}

// UpdateMaintenanceMode updates the desired maintenance configuration,
// reconciles the effective maintenance status, and records the change in the
// operation journal.
func (store *MemoryStateStore) UpdateMaintenanceMode(ctx context.Context, request cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	if err := ctx.Err(); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	if err := request.Validate(); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	now := store.now().UTC()

	store.mu.Lock()
	if store.clusterSpec == nil {
		store.mu.Unlock()
		return cluster.MaintenanceModeStatus{}, ErrClusterSpecRequired
	}

	spec := store.clusterSpec.Clone()
	spec.Maintenance.Enabled = request.Enabled
	spec.Maintenance.DefaultReason = request.EffectiveReason(spec.Maintenance.DefaultReason)
	spec = store.storeClusterSpecLocked(spec)
	specRevision := store.clusterSpecRevision

	status := cluster.MaintenanceModeStatus{
		Enabled:     request.Enabled,
		RequestedBy: request.RequestedBy,
		UpdatedAt:   now,
	}

	if request.Enabled {
		status.Reason = spec.Maintenance.EffectiveReason(request.Reason)
	}

	store.maintenance = status
	maintenanceRevision := store.maintenanceRevision

	operation := cluster.Operation{
		ID:          maintenanceOperationID(now),
		Kind:        cluster.OperationKindMaintenanceChange,
		State:       cluster.OperationStateCompleted,
		RequestedBy: request.RequestedBy,
		RequestedAt: now,
		Reason:      strings.TrimSpace(request.Reason),
		CompletedAt: now,
		Result:      cluster.OperationResultSucceeded,
		Message:     maintenanceOperationMessage(request.Enabled),
	}

	if request.Enabled && operation.Reason == "" {
		operation.Reason = status.Reason
	}

	store.journalOperationLocked(operation, now)
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	if err := store.compareAndStoreJSON(ctx, store.keyspace.Config(), specRevision, spec); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	if err := store.compareAndStoreJSON(ctx, store.keyspace.Maintenance(), maintenanceRevision, status); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	if err := store.persistJournaledOperation(ctx, operation); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	return status, nil
}

// ActiveOperation returns the currently active cluster-wide operation tracked
// by the control plane.
func (store *MemoryStateStore) ActiveOperation() (cluster.Operation, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return cluster.Operation{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if store.activeOperation == nil {
		return cluster.Operation{}, false
	}

	return store.activeOperation.Clone(), true
}

// History returns the finished operation history recorded by the control plane.
func (store *MemoryStateStore) History() []cluster.HistoryEntry {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return nil
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	return cloneHistoryEntries(store.history)
}

// JournalOperation records a cluster-wide operation in the active journal or
// the finished history, depending on the operation state.
func (store *MemoryStateStore) JournalOperation(ctx context.Context, operation cluster.Operation) (cluster.Operation, error) {
	if err := ctx.Err(); err != nil {
		return cluster.Operation{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return cluster.Operation{}, err
	}

	now := store.now().UTC()
	cloned := operation.Clone()

	if !cloned.State.IsTerminal() && cloned.Result.IsZero() {
		cloned.Result = cluster.OperationResultPending
	}

	if cloned.State.IsTerminal() && cloned.CompletedAt.IsZero() {
		cloned.CompletedAt = now
	}

	if err := cloned.Validate(); err != nil {
		return cluster.Operation{}, err
	}

	if cloned.State.IsTerminal() && (cloned.Result.IsZero() || cloned.Result == cluster.OperationResultPending) {
		return cluster.Operation{}, cluster.ErrInvalidOperationResult
	}

	store.mu.Lock()
	store.journalOperationLocked(cloned, now)
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	if err := store.persistJournaledOperation(ctx, cloned); err != nil {
		return cluster.Operation{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return cluster.Operation{}, err
	}

	return cloned.Clone(), nil
}

func (store *MemoryStateStore) refreshSourceOfTruthLocked(now time.Time) {
	if store.clusterSpec == nil {
		return
	}

	store.reconcileMaintenanceLocked(now)
	status := store.aggregateClusterStatusLocked(now)
	store.clusterStatus = &status
	store.sourceUpdated = now
}

func (store *MemoryStateStore) sourceOfTruthLocked() ClusterSourceOfTruth {
	truth := ClusterSourceOfTruth{
		UpdatedAt: store.sourceUpdated,
	}

	if store.clusterSpec != nil {
		desired := store.clusterSpec.Clone()
		truth.Desired = &desired
	}

	if store.clusterStatus != nil {
		observed := store.clusterStatus.Clone()
		truth.Observed = &observed
	}

	return truth.Clone()
}

func (store *MemoryStateStore) storeClusterSpecLocked(spec cluster.ClusterSpec) cluster.ClusterSpec {
	cloned := spec.Clone()

	if store.clusterSpec != nil {
		current := store.clusterSpec.Clone()
		switch {
		case sameClusterSpecIgnoringGeneration(current, cloned):
			cloned.Generation = current.Generation
		case cloned.Generation <= current.Generation:
			cloned.Generation = current.Generation + 1
		}
	}

	store.clusterSpec = &cloned

	return cloned.Clone()
}

func (store *MemoryStateStore) reconcileMaintenanceLocked(now time.Time) {
	if store.clusterSpec == nil {
		return
	}

	desired := store.clusterSpec.Maintenance
	maintenance := store.maintenance
	previouslyEnabled := maintenance.Enabled

	maintenance.Enabled = desired.Enabled
	if desired.Enabled {
		maintenance.Reason = desired.EffectiveReason(maintenance.Reason)
		if maintenance.UpdatedAt.IsZero() || !previouslyEnabled {
			maintenance.UpdatedAt = now
		}
	} else if previouslyEnabled {
		maintenance.Reason = ""
		maintenance.UpdatedAt = now
	}

	store.maintenance = maintenance
}

func (store *MemoryStateStore) aggregateClusterStatusLocked(now time.Time) cluster.ClusterStatus {
	members := store.membersLocked()
	currentPrimary, hasPrimary := currentPrimaryMember(members)
	currentEpoch := cluster.Epoch(0)
	if store.clusterStatus != nil {
		currentEpoch = store.clusterStatus.CurrentEpoch
	}

	status := cluster.ClusterStatus{
		ClusterName:         store.clusterSpec.ClusterName,
		Phase:               aggregateClusterPhase(store.clusterSpec.Clone(), store.maintenance, store.activeOperation, members, hasPrimary, currentPrimary.Name),
		CurrentPrimary:      currentPrimary.Name,
		CurrentEpoch:        currentEpoch,
		Maintenance:         store.maintenance,
		ActiveOperation:     cloneOperationValue(store.activeOperation),
		ScheduledSwitchover: scheduledSwitchoverOperation(store.activeOperation),
		Members:             members,
		ObservedAt:          now,
	}

	return status
}

func aggregateClusterPhase(spec cluster.ClusterSpec, maintenance cluster.MaintenanceModeStatus, operation *cluster.Operation, members []cluster.MemberStatus, hasPrimary bool, currentPrimary string) cluster.ClusterPhase {
	if maintenance.Enabled {
		return cluster.ClusterPhaseMaintenance
	}

	if operation != nil {
		switch operation.Kind {
		case cluster.OperationKindFailover:
			return cluster.ClusterPhaseFailingOver
		case cluster.OperationKindSwitchover:
			if operation.State == cluster.OperationStateRunning {
				return cluster.ClusterPhaseSwitchingOver
			}
		case cluster.OperationKindRejoin:
			return cluster.ClusterPhaseRecovering
		case cluster.OperationKindMaintenanceChange:
			if maintenance.Enabled {
				return cluster.ClusterPhaseMaintenance
			}
		}
	}

	if len(members) == 0 {
		return cluster.ClusterPhaseInitializing
	}

	missingExpected := false
	actualMembers := make(map[string]struct{}, len(members))
	hasDataBearing := false
	unhealthy := false
	for _, member := range members {
		actualMembers[member.Name] = struct{}{}
		if member.Role.IsDataBearing() {
			hasDataBearing = true
		}
		if !member.Healthy || member.State == cluster.MemberStateUnknown || member.NeedsRejoin {
			unhealthy = true
		}
	}

	for _, member := range spec.Members {
		if _, ok := actualMembers[member.Name]; !ok {
			missingExpected = true
			break
		}
	}

	if !hasPrimary && hasDataBearing {
		if missingExpected {
			return cluster.ClusterPhaseInitializing
		}

		return cluster.ClusterPhaseDegraded
	}

	if missingExpected || unhealthy {
		if currentPrimary == "" && hasDataBearing {
			return cluster.ClusterPhaseInitializing
		}

		return cluster.ClusterPhaseDegraded
	}

	return cluster.ClusterPhaseHealthy
}

func currentPrimaryMember(members []cluster.MemberStatus) (cluster.MemberStatus, bool) {
	var fallback cluster.MemberStatus
	for _, member := range members {
		if member.Role != cluster.MemberRolePrimary {
			continue
		}

		if member.Healthy {
			return member.Clone(), true
		}

		if fallback.Name == "" {
			fallback = member.Clone()
		}
	}

	if fallback.Name != "" {
		return fallback, true
	}

	return cluster.MemberStatus{}, false
}

func (store *MemoryStateStore) journalOperationLocked(operation cluster.Operation, now time.Time) {
	cloned := operation.Clone()

	if cloned.State.IsTerminal() {
		entry := store.historyEntryForOperationLocked(cloned, now)
		store.history = append(store.history, entry)
		if store.activeOperation != nil && store.activeOperation.ID == cloned.ID {
			store.activeOperation = nil
		}
		return
	}

	store.activeOperation = &cloned
}

func (store *MemoryStateStore) historyEntryForOperationLocked(operation cluster.Operation, now time.Time) cluster.HistoryEntry {
	finishedAt := operation.CompletedAt
	if finishedAt.IsZero() {
		finishedAt = now
	}

	memberName := strings.TrimSpace(operation.ToMember)
	if memberName == "" {
		memberName = strings.TrimSpace(operation.FromMember)
	}
	if memberName == "" {
		memberName = store.currentPrimaryNameLocked()
	}

	entry := cluster.HistoryEntry{
		OperationID: operation.ID,
		Kind:        operation.Kind,
		FromMember:  operation.FromMember,
		ToMember:    operation.ToMember,
		Reason:      operation.Reason,
		Result:      operation.Result,
		FinishedAt:  finishedAt,
	}

	if member, ok := store.memberLocked(memberName); ok {
		entry.Timeline = member.Timeline
		entry.WALLSN = store.memberWALLSNLocked(member.Name)
	}

	return entry
}

func (store *MemoryStateStore) currentPrimaryNameLocked() string {
	if store.clusterStatus != nil && strings.TrimSpace(store.clusterStatus.CurrentPrimary) != "" {
		return store.clusterStatus.CurrentPrimary
	}

	members := store.membersLocked()
	member, ok := currentPrimaryMember(members)
	if !ok {
		return ""
	}

	return member.Name
}

func (store *MemoryStateStore) memberWALLSNLocked(nodeName string) string {
	status, ok := store.nodeStatuses[nodeName]
	if !ok {
		return ""
	}

	switch {
	case status.Postgres.WAL.FlushLSN != "":
		return status.Postgres.WAL.FlushLSN
	case status.Postgres.WAL.ReplayLSN != "":
		return status.Postgres.WAL.ReplayLSN
	case status.Postgres.WAL.WriteLSN != "":
		return status.Postgres.WAL.WriteLSN
	case status.Postgres.WAL.ReceiveLSN != "":
		return status.Postgres.WAL.ReceiveLSN
	default:
		return ""
	}
}

func (store *MemoryStateStore) membersLocked() []cluster.MemberStatus {
	nodeNames := make(map[string]struct{}, len(store.registrations)+len(store.nodeStatuses))
	for nodeName := range store.registrations {
		nodeNames[nodeName] = struct{}{}
	}

	for nodeName := range store.nodeStatuses {
		nodeNames[nodeName] = struct{}{}
	}

	members := make([]cluster.MemberStatus, 0, len(nodeNames))
	for nodeName := range nodeNames {
		member, ok := store.memberLocked(nodeName)
		if ok {
			members = append(members, member.Clone())
		}
	}

	sort.Slice(members, func(left, right int) bool {
		return members[left].Name < members[right].Name
	})

	return members
}

func cloneOperationValue(operation *cluster.Operation) *cluster.Operation {
	if operation == nil {
		return nil
	}

	cloned := operation.Clone()

	return &cloned
}

func scheduledSwitchoverOperation(operation *cluster.Operation) *cluster.ScheduledSwitchover {
	if operation == nil || operation.Kind != cluster.OperationKindSwitchover || operation.State.IsTerminal() {
		return nil
	}

	at := operation.ScheduledAt
	if at.IsZero() {
		at = operation.RequestedAt
	}

	if at.IsZero() || strings.TrimSpace(operation.FromMember) == "" {
		return nil
	}

	scheduled := cluster.ScheduledSwitchover{
		At:   at,
		From: operation.FromMember,
		To:   operation.ToMember,
	}

	return &scheduled
}

func cloneHistoryEntries(entries []cluster.HistoryEntry) []cluster.HistoryEntry {
	if entries == nil {
		return nil
	}

	cloned := make([]cluster.HistoryEntry, len(entries))
	copy(cloned, entries)

	return cloned
}

func (store *MemoryStateStore) persistJournaledOperation(ctx context.Context, operation cluster.Operation) error {
	if operation.State.IsTerminal() {
		store.mu.RLock()
		entry := store.historyEntryForOperationLocked(operation, operation.CompletedAt)
		store.mu.RUnlock()

		if err := store.setJSON(ctx, store.keyspace.History(operation.ID), entry); err != nil {
			return err
		}

		return store.deleteKey(ctx, store.keyspace.Operation())
	}

	return store.setJSON(ctx, store.keyspace.Operation(), operation)
}

func maintenanceOperationID(now time.Time) string {
	return "maintenance-" + now.UTC().Format("20060102T150405.000000000Z07:00")
}

func maintenanceOperationMessage(enabled bool) string {
	if enabled {
		return "maintenance mode enabled"
	}

	return "maintenance mode disabled"
}
