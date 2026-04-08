package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
	dcsmemory "github.com/polkiloo/pacman/internal/dcs/memory"
)

const (
	defaultControlPlaneClusterName = "local"
	defaultCacheMaxAge             = 0
	cacheWatchRetryDelay           = 25 * time.Millisecond
)

// ControlPlane is the DCS-backed HA state engine. MemoryStateStore remains as
// a compatibility alias for the existing unit and integration test surface.
type ControlPlane = MemoryStateStore

// NewControlPlane constructs the HA orchestration layer over the supplied DCS.
func NewControlPlane(store dcs.DCS, clusterName string, logger *slog.Logger) *ControlPlane {
	if store == nil {
		panic("controlplane dcs backend is required")
	}

	resolvedClusterName := strings.TrimSpace(clusterName)
	if resolvedClusterName == "" {
		resolvedClusterName = defaultControlPlaneClusterName
	}

	space, err := dcs.NewKeySpace(resolvedClusterName)
	if err != nil {
		panic(fmt.Sprintf("construct controlplane keyspace: %v", err))
	}

	if err := store.Initialize(context.Background()); err != nil {
		panic(fmt.Sprintf("initialize controlplane dcs: %v", err))
	}

	if logger == nil {
		logger = slog.Default()
	}

	controlPlane := &ControlPlane{
		dcs:                 store,
		keyspace:            space,
		clusterName:         resolvedClusterName,
		logger:              logger,
		registrations:       make(map[string]MemberRegistration),
		nodeStatuses:        make(map[string]agentmodel.NodeStatus),
		now:                 time.Now,
		leaseDuration:       defaultLeaderLeaseDuration,
		cacheMaxAge:         defaultCacheMaxAge,
		clusterSpecRevision: -1,
		maintenanceRevision: -1,
		activeOpRevision:    -1,
		cacheDirty:          true,
	}

	controlPlane.startCacheWatch()
	return controlPlane
}

// NewMemoryStateStore constructs a compatibility ControlPlane over the in-memory DCS backend.
func NewMemoryStateStore() *MemoryStateStore {
	// Use an atomic pointer so the backend's TTLFunc and Now closures can safely
	// read the store reference across the goroutine boundary introduced by
	// NewControlPlane's startCacheWatch call.
	var ref atomic.Pointer[MemoryStateStore]

	backend := dcsmemory.New(dcsmemory.Config{
		TTL: defaultLeaderLeaseDuration,
		TTLFunc: func() time.Duration {
			if s := ref.Load(); s != nil {
				return s.leaseDuration
			}
			return defaultLeaderLeaseDuration
		},
		SweepInterval: 10 * time.Millisecond,
		Now: func() time.Time {
			if s := ref.Load(); s != nil && s.now != nil {
				return s.now()
			}
			return time.Now()
		},
	})

	store := NewControlPlane(backend, defaultControlPlaneClusterName, nil)
	ref.Store(store)
	return store
}

func (store *MemoryStateStore) ensureCacheFresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	now := store.now().UTC()

	store.mu.RLock()
	cacheReady := !store.cacheRefreshedAt.IsZero()
	cacheDirty := store.cacheDirty
	cacheExpired := cacheReady && store.cacheMaxAge > 0 && store.cacheRefreshedAt.Add(store.cacheMaxAge).Before(now)
	store.mu.RUnlock()

	if cacheReady && !cacheDirty && !cacheExpired {
		return nil
	}

	return store.forceRefreshCache(ctx)
}

func (store *MemoryStateStore) refreshCache(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.cacheRefreshedAt = now
	store.cacheDirty = false
	return nil
}

func (store *MemoryStateStore) startCacheWatch() {
	prefix := store.keyspace.Root()
	events, err := store.dcs.Watch(context.Background(), prefix)
	if err != nil {
		store.logger.Warn("initial cache watch failed, retrying in background", "error", err)
		go store.runCacheWatch()
		return
	}

	go store.consumeCacheWatch(prefix, events)
}

func (store *MemoryStateStore) runCacheWatch() {
	prefix := store.keyspace.Root()

	for {
		events, err := store.dcs.Watch(context.Background(), prefix)
		if err != nil {
			if errors.Is(err, dcs.ErrBackendUnavailable) {
				return
			}

			store.invalidateCache()
			time.Sleep(cacheWatchRetryDelay)
			continue
		}

		store.consumeCacheWatch(prefix, events)
		return
	}
}

func (store *MemoryStateStore) consumeCacheWatch(prefix string, events <-chan dcs.WatchEvent) {
	for {
		for event := range events {
			if err := store.applyWatchEvent(event); err != nil {
				store.invalidateCache()
			}
		}

		store.invalidateCache()
		time.Sleep(cacheWatchRetryDelay)

		var err error
		events, err = store.dcs.Watch(context.Background(), prefix)
		if err != nil {
			if errors.Is(err, dcs.ErrBackendUnavailable) {
				return
			}

			time.Sleep(cacheWatchRetryDelay)
			continue
		}
	}
}

func (store *MemoryStateStore) applyWatchEvent(event dcs.WatchEvent) error {
	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.cacheRefreshedAt.IsZero() {
		store.cacheDirty = true
		return nil
	}

	if err := store.applyWatchEventLocked(event); err != nil {
		store.cacheDirty = true
		return err
	}

	store.cacheRefreshedAt = now
	store.cacheDirty = false

	if store.clusterSpec != nil {
		store.refreshSourceOfTruthLocked(now)
	} else {
		store.clusterStatus = nil
		store.sourceUpdated = time.Time{}
	}

	return nil
}

func (store *MemoryStateStore) applyWatchEventLocked(event dcs.WatchEvent) error {
	key := strings.TrimSpace(event.Key)

	switch {
	case key == store.keyspace.Config():
		return store.applyClusterSpecEventLocked(event)
	case key == store.keyspace.Maintenance():
		return store.applyMaintenanceEventLocked(event)
	case key == store.keyspace.Operation():
		return store.applyOperationEventLocked(event)
	case strings.HasPrefix(key, store.keyspace.MembersPrefix()):
		return store.applyMemberRegistrationEventLocked(event)
	case strings.HasPrefix(key, store.keyspace.StatusPrefix()):
		return store.applyNodeStatusEventLocked(event)
	case strings.HasPrefix(key, store.keyspace.HistoryPrefix()):
		return store.applyHistoryEventLocked(event)
	default:
		return nil
	}
}

func (store *MemoryStateStore) applyClusterSpecEventLocked(event dcs.WatchEvent) error {
	if event.Revision > 0 && store.clusterSpecRevision > event.Revision {
		return nil
	}

	if watchEventDeletesKey(event) {
		store.clusterSpec = nil
		store.clusterSpecRevision = -1
		return nil
	}

	var spec cluster.ClusterSpec
	if err := unmarshalWatchValue(event.Key, event.Value, &spec); err != nil {
		return err
	}

	store.clusterSpec = &spec
	store.clusterSpecRevision = event.Revision
	return nil
}

func (store *MemoryStateStore) applyMaintenanceEventLocked(event dcs.WatchEvent) error {
	if event.Revision > 0 && store.maintenanceRevision > event.Revision {
		return nil
	}

	if watchEventDeletesKey(event) {
		store.maintenance = cluster.MaintenanceModeStatus{}
		store.maintenanceRevision = -1
		return nil
	}

	var status cluster.MaintenanceModeStatus
	if err := unmarshalWatchValue(event.Key, event.Value, &status); err != nil {
		return err
	}

	store.maintenance = status
	store.maintenanceRevision = event.Revision
	return nil
}

func (store *MemoryStateStore) applyOperationEventLocked(event dcs.WatchEvent) error {
	if event.Revision > 0 && store.activeOpRevision > event.Revision {
		return nil
	}

	if watchEventDeletesKey(event) {
		store.activeOperation = nil
		store.activeOpRevision = -1
		return nil
	}

	var operation cluster.Operation
	if err := unmarshalWatchValue(event.Key, event.Value, &operation); err != nil {
		return err
	}

	store.activeOperation = &operation
	store.activeOpRevision = event.Revision
	return nil
}

func (store *MemoryStateStore) applyMemberRegistrationEventLocked(event dcs.WatchEvent) error {
	nodeName := strings.TrimPrefix(strings.TrimSpace(event.Key), store.keyspace.MembersPrefix())
	if nodeName == "" {
		return nil
	}

	if watchEventDeletesKey(event) {
		delete(store.registrations, nodeName)
		return nil
	}

	var registration MemberRegistration
	if err := unmarshalWatchValue(event.Key, event.Value, &registration); err != nil {
		return err
	}

	if strings.TrimSpace(registration.NodeName) == "" {
		registration.NodeName = nodeName
	}

	store.registrations[registration.NodeName] = registration
	return nil
}

func (store *MemoryStateStore) applyNodeStatusEventLocked(event dcs.WatchEvent) error {
	nodeName := strings.TrimPrefix(strings.TrimSpace(event.Key), store.keyspace.StatusPrefix())
	if nodeName == "" {
		return nil
	}

	if watchEventDeletesKey(event) {
		delete(store.nodeStatuses, nodeName)
		return nil
	}

	var status agentmodel.NodeStatus
	if err := unmarshalWatchValue(event.Key, event.Value, &status); err != nil {
		return err
	}

	if strings.TrimSpace(status.NodeName) == "" {
		status.NodeName = nodeName
	}

	store.nodeStatuses[status.NodeName] = status
	return nil
}

func (store *MemoryStateStore) applyHistoryEventLocked(event dcs.WatchEvent) error {
	operationID := strings.TrimPrefix(strings.TrimSpace(event.Key), store.keyspace.HistoryPrefix())
	if operationID == "" {
		return nil
	}

	if watchEventDeletesKey(event) {
		store.removeHistoryEntryLocked(operationID)
		return nil
	}

	var entry cluster.HistoryEntry
	if err := unmarshalWatchValue(event.Key, event.Value, &entry); err != nil {
		return err
	}

	if strings.TrimSpace(entry.OperationID) == "" {
		entry.OperationID = operationID
	}

	store.upsertHistoryEntryLocked(entry)
	return nil
}

func (store *MemoryStateStore) upsertHistoryEntryLocked(entry cluster.HistoryEntry) {
	for index, existing := range store.history {
		if existing.OperationID != entry.OperationID {
			continue
		}

		store.history[index] = entry
		sort.Slice(store.history, func(left, right int) bool {
			if store.history[left].FinishedAt.Equal(store.history[right].FinishedAt) {
				return store.history[left].OperationID < store.history[right].OperationID
			}

			return store.history[left].FinishedAt.Before(store.history[right].FinishedAt)
		})
		return
	}

	store.history = append(store.history, entry)
	sort.Slice(store.history, func(left, right int) bool {
		if store.history[left].FinishedAt.Equal(store.history[right].FinishedAt) {
			return store.history[left].OperationID < store.history[right].OperationID
		}

		return store.history[left].FinishedAt.Before(store.history[right].FinishedAt)
	})
}

func (store *MemoryStateStore) removeHistoryEntryLocked(operationID string) {
	filtered := store.history[:0]
	for _, entry := range store.history {
		if entry.OperationID == operationID {
			continue
		}

		filtered = append(filtered, entry)
	}

	store.history = filtered
}

func (store *MemoryStateStore) forceRefreshCache(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	registrations, err := store.loadRegistrations(ctx)
	if err != nil {
		return err
	}

	nodeStatuses, err := store.loadNodeStatuses(ctx)
	if err != nil {
		return err
	}

	spec, specRevision, err := store.loadClusterSpec(ctx)
	if err != nil {
		return err
	}

	maintenance, maintenanceRevision, err := store.loadMaintenance(ctx)
	if err != nil {
		return err
	}

	activeOperation, activeOpRevision, err := store.loadActiveOperation(ctx)
	if err != nil {
		return err
	}

	history, err := store.loadHistory(ctx)
	if err != nil {
		return err
	}

	leaderLease, err := store.loadLeaderLease(ctx)
	if err != nil {
		return err
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.registrations = registrations
	store.nodeStatuses = nodeStatuses
	store.clusterSpec = spec
	store.clusterSpecRevision = specRevision
	store.maintenance = maintenance
	store.maintenanceRevision = maintenanceRevision
	store.activeOperation = activeOperation
	store.activeOpRevision = activeOpRevision
	store.history = history
	store.leaderLease = leaderLease
	store.lastDCSSeenAt = now
	store.cacheRefreshedAt = now
	store.cacheDirty = false

	if store.clusterSpec != nil {
		store.refreshSourceOfTruthLocked(now)
	} else {
		store.clusterStatus = nil
		store.sourceUpdated = time.Time{}
	}

	return nil
}

func (store *MemoryStateStore) loadRegistrations(ctx context.Context) (map[string]MemberRegistration, error) {
	values, err := store.dcs.List(ctx, store.keyspace.MembersPrefix())
	if err != nil {
		return nil, fmt.Errorf("list member registrations: %w", err)
	}

	registrations := make(map[string]MemberRegistration, len(values))
	for _, value := range values {
		var registration MemberRegistration
		if err := json.Unmarshal(value.Value, &registration); err != nil {
			return nil, fmt.Errorf("decode member registration %q: %w", value.Key, err)
		}

		registrations[registration.NodeName] = registration
	}

	return registrations, nil
}

func (store *MemoryStateStore) loadNodeStatuses(ctx context.Context) (map[string]agentmodel.NodeStatus, error) {
	values, err := store.dcs.List(ctx, store.keyspace.StatusPrefix())
	if err != nil {
		return nil, fmt.Errorf("list node statuses: %w", err)
	}

	statuses := make(map[string]agentmodel.NodeStatus, len(values))
	for _, value := range values {
		var status agentmodel.NodeStatus
		if err := json.Unmarshal(value.Value, &status); err != nil {
			return nil, fmt.Errorf("decode node status %q: %w", value.Key, err)
		}

		statuses[status.NodeName] = status
	}

	return statuses, nil
}

func (store *MemoryStateStore) loadClusterSpec(ctx context.Context) (*cluster.ClusterSpec, int64, error) {
	value, ok, err := store.getKey(ctx, store.keyspace.Config())
	if err != nil {
		return nil, -1, err
	}

	if !ok {
		return nil, -1, nil
	}

	var spec cluster.ClusterSpec
	if err := json.Unmarshal(value.Value, &spec); err != nil {
		return nil, -1, fmt.Errorf("decode cluster spec: %w", err)
	}

	return &spec, value.Revision, nil
}

func (store *MemoryStateStore) loadMaintenance(ctx context.Context) (cluster.MaintenanceModeStatus, int64, error) {
	value, ok, err := store.getKey(ctx, store.keyspace.Maintenance())
	if err != nil {
		return cluster.MaintenanceModeStatus{}, -1, err
	}

	if !ok {
		return cluster.MaintenanceModeStatus{}, -1, nil
	}

	var status cluster.MaintenanceModeStatus
	if err := json.Unmarshal(value.Value, &status); err != nil {
		return cluster.MaintenanceModeStatus{}, -1, fmt.Errorf("decode maintenance status: %w", err)
	}

	return status, value.Revision, nil
}

func (store *MemoryStateStore) loadActiveOperation(ctx context.Context) (*cluster.Operation, int64, error) {
	value, ok, err := store.getKey(ctx, store.keyspace.Operation())
	if err != nil {
		return nil, -1, err
	}

	if !ok {
		return nil, -1, nil
	}

	var operation cluster.Operation
	if err := json.Unmarshal(value.Value, &operation); err != nil {
		return nil, -1, fmt.Errorf("decode active operation: %w", err)
	}

	return &operation, value.Revision, nil
}

func (store *MemoryStateStore) loadHistory(ctx context.Context) ([]cluster.HistoryEntry, error) {
	values, err := store.dcs.List(ctx, store.keyspace.HistoryPrefix())
	if err != nil {
		return nil, fmt.Errorf("list operation history: %w", err)
	}

	history := make([]cluster.HistoryEntry, 0, len(values))
	for _, value := range values {
		var entry cluster.HistoryEntry
		if err := json.Unmarshal(value.Value, &entry); err != nil {
			return nil, fmt.Errorf("decode history entry %q: %w", value.Key, err)
		}

		history = append(history, entry)
	}

	sort.Slice(history, func(left, right int) bool {
		if history[left].FinishedAt.Equal(history[right].FinishedAt) {
			return history[left].OperationID < history[right].OperationID
		}

		return history[left].FinishedAt.Before(history[right].FinishedAt)
	})

	return history, nil
}

func (store *MemoryStateStore) loadLeaderLease(ctx context.Context) (LeaderLease, error) {
	lease, ok, err := store.dcs.Leader(ctx)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("load leader lease: %w", err)
	}

	if !ok {
		return LeaderLease{}, nil
	}

	return leaderLeaseFromDCS(lease), nil
}

func (store *MemoryStateStore) syncLeaderLease(ctx context.Context) error {
	lease, err := store.loadLeaderLease(ctx)
	if err != nil {
		return err
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.leaderLease = lease
	store.lastDCSSeenAt = now
	return nil
}

func (store *MemoryStateStore) setJSON(ctx context.Context, key string, value any, options ...dcs.SetOption) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", key, err)
	}

	if err := store.dcs.Set(ctx, key, payload, options...); err != nil {
		return fmt.Errorf("store %s: %w", key, err)
	}

	store.markDCSWritten()
	store.bumpLocalRevision(key)
	return nil
}

func (store *MemoryStateStore) compareAndStoreJSON(ctx context.Context, key string, revision int64, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", key, err)
	}

	if revision < 0 {
		if err := store.dcs.Set(ctx, key, payload); err != nil {
			return fmt.Errorf("store %s: %w", key, err)
		}
	} else {
		if err := store.dcs.CompareAndSet(ctx, key, payload, revision); err != nil {
			return fmt.Errorf("compare-and-store %s: %w", key, err)
		}
	}

	store.markDCSWritten()
	store.setLocalRevision(key, nextRevision(revision))
	return nil
}

func (store *MemoryStateStore) deleteKey(ctx context.Context, key string) error {
	err := store.dcs.Delete(ctx, key)
	if err != nil && !errors.Is(err, dcs.ErrKeyNotFound) {
		return fmt.Errorf("delete %s: %w", key, err)
	}

	store.markDCSWritten()
	store.clearLocalRevision(key)
	return nil
}

func (store *MemoryStateStore) getKey(ctx context.Context, key string) (dcs.KeyValue, bool, error) {
	value, err := store.dcs.Get(ctx, key)
	if errors.Is(err, dcs.ErrKeyNotFound) {
		return dcs.KeyValue{}, false, nil
	}

	if err != nil {
		return dcs.KeyValue{}, false, fmt.Errorf("get %s: %w", key, err)
	}

	return value, true, nil
}

func (store *MemoryStateStore) persistNodeStatus(ctx context.Context, status agentmodel.NodeStatus) error {
	return store.setJSON(ctx, store.keyspace.Status(status.NodeName), status, dcs.WithTTL(store.leaseDuration))
}

func (store *MemoryStateStore) persistNodeStatuses(ctx context.Context, statuses ...agentmodel.NodeStatus) error {
	for _, status := range statuses {
		if err := store.persistNodeStatus(ctx, status); err != nil {
			return err
		}
	}

	return nil
}

func (store *MemoryStateStore) persistActiveOperation(ctx context.Context, operation cluster.Operation) error {
	return store.setJSON(ctx, store.keyspace.Operation(), operation)
}

func (store *MemoryStateStore) invalidateCache() {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.cacheDirty = true
}

func (store *MemoryStateStore) markDCSWritten() {
	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.cacheRefreshedAt = now
	store.cacheDirty = false
}

func nextRevision(revision int64) int64 {
	if revision < 0 {
		return 1
	}

	return revision + 1
}

func (store *MemoryStateStore) bumpLocalRevision(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	switch key {
	case store.keyspace.Operation():
		store.activeOpRevision = nextRevision(store.activeOpRevision)
	case store.keyspace.Config():
		store.clusterSpecRevision = nextRevision(store.clusterSpecRevision)
	case store.keyspace.Maintenance():
		store.maintenanceRevision = nextRevision(store.maintenanceRevision)
	}
}

func (store *MemoryStateStore) setLocalRevision(key string, revision int64) {
	store.mu.Lock()
	defer store.mu.Unlock()

	switch key {
	case store.keyspace.Operation():
		store.activeOpRevision = revision
	case store.keyspace.Config():
		store.clusterSpecRevision = revision
	case store.keyspace.Maintenance():
		store.maintenanceRevision = revision
	}
}

func (store *MemoryStateStore) clearLocalRevision(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	switch key {
	case store.keyspace.Operation():
		// Keep the last seen active-operation revision until the watched delete
		// arrives so delayed put events cannot roll the cache backward.
	case store.keyspace.Config():
		store.clusterSpecRevision = -1
	case store.keyspace.Maintenance():
		store.maintenanceRevision = -1
	}
}

func watchEventDeletesKey(event dcs.WatchEvent) bool {
	return event.Type == dcs.EventDelete || event.Type == dcs.EventExpired
}

func unmarshalWatchValue(key string, value []byte, target any) error {
	if err := json.Unmarshal(value, target); err != nil {
		return fmt.Errorf("decode watched %s: %w", key, err)
	}

	return nil
}

func leaderLeaseFromDCS(lease dcs.LeaderLease) LeaderLease {
	return LeaderLease{
		LeaderNode: lease.Leader,
		Term:       lease.Term,
		AcquiredAt: lease.Acquired,
		RenewedAt:  lease.Renewed,
	}
}

func leaderLeaseToDCS(lease LeaderLease, duration time.Duration) dcs.LeaderLease {
	reference := lease.RenewedAt
	if reference.IsZero() {
		reference = lease.AcquiredAt
	}

	return dcs.LeaderLease{
		Leader:    lease.LeaderNode,
		Term:      lease.Term,
		Acquired:  lease.AcquiredAt,
		Renewed:   lease.RenewedAt,
		ExpiresAt: reference.Add(duration),
	}
}
