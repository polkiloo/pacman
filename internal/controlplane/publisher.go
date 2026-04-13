package controlplane

import (
	"context"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
)

// NodeStatePublisher accepts the latest local node observation from pacmand.
type NodeStatePublisher interface {
	PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error)
}

// MemoryStateStore is an in-memory replicated control-plane state store used
// until the distributed source of truth is implemented.
type MemoryStateStore struct {
	dcs                 dcs.DCS
	keyspace            dcs.KeySpace
	clusterName         string
	logger              *slog.Logger
	mu                  sync.RWMutex
	registrations       map[string]MemberRegistration
	nodeStatuses        map[string]agentmodel.NodeStatus
	nodeStatusRevisions map[string]int64
	clusterSpec         *cluster.ClusterSpec
	clusterSpecRevision int64
	clusterStatus       *cluster.ClusterStatus
	maintenance         cluster.MaintenanceModeStatus
	maintenanceRevision int64
	activeOperation     *cluster.Operation
	activeOpRevision    int64
	history             []cluster.HistoryEntry
	leaderLease         LeaderLease
	now                 func() time.Time
	leaseDuration       time.Duration
	cacheRefreshedAt    time.Time
	cacheMaxAge         time.Duration
	cacheDirty          bool
	sourceUpdated       time.Time
	lastDCSSeenAt       time.Time
}

// RegisterMember stores the static member identity published during daemon
// startup.
func (store *MemoryStateStore) RegisterMember(ctx context.Context, registration MemberRegistration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cloned := registration.Clone()
	if err := cloned.Validate(); err != nil {
		return err
	}

	cloned.RegisteredAt = cloned.RegisteredAt.UTC()

	if err := store.setJSON(ctx, store.keyspace.Member(cloned.NodeName), cloned); err != nil {
		return err
	}

	store.mu.Lock()
	store.registrations[cloned.NodeName] = cloned
	store.mu.Unlock()

	return store.refreshCache(ctx)
}

// CampaignLeader tries to acquire or renew the control-plane leader lease for
// the given registered node.
func (store *MemoryStateStore) CampaignLeader(ctx context.Context, nodeName string) (LeaderLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return LeaderLease{}, false, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return LeaderLease{}, false, err
	}

	candidate := strings.TrimSpace(nodeName)
	if candidate == "" {
		return LeaderLease{}, false, ErrLeaderCandidateRequired
	}

	store.mu.RLock()
	_, ok := store.registrations[candidate]
	store.mu.RUnlock()
	if !ok {
		return LeaderLease{}, false, ErrLeaderCandidateUnknown
	}

	lease, elected, err := store.dcs.Campaign(ctx, candidate)
	if err != nil {
		return LeaderLease{}, false, err
	}

	store.markDCSWritten()
	converted := leaderLeaseFromDCS(lease)

	store.mu.Lock()
	store.leaderLease = converted
	store.mu.Unlock()

	return converted.Clone(), elected, nil
}

// Leader returns the currently active control-plane leader lease.
func (store *MemoryStateStore) Leader() (LeaderLease, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return LeaderLease{}, false
	}

	if err := store.syncLeaderLease(context.Background()); err != nil {
		return LeaderLease{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if !store.leaderLease.isActiveAt(store.now().UTC(), store.leaseDuration) {
		return LeaderLease{}, false
	}

	return store.leaderLease.Clone(), true
}

// PublishNodeStatus stores the latest local node observation.
func (store *MemoryStateStore) PublishNodeStatus(ctx context.Context, status agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	if err := ctx.Err(); err != nil {
		return agentmodel.ControlPlaneStatus{ClusterReachable: false}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return agentmodel.ControlPlaneStatus{ClusterReachable: false}, err
	}

	cloned := status.Clone()

	published := agentmodel.ControlPlaneStatus{
		ClusterReachable: true,
		LastHeartbeatAt:  cloned.ObservedAt,
	}

	lease, ok, err := store.dcs.Leader(ctx)
	if err == nil && ok {
		converted := leaderLeaseFromDCS(lease)
		store.mu.Lock()
		store.leaderLease = converted
		store.mu.Unlock()
		published.Leader = converted.LeaderNode == cloned.NodeName && converted.isActiveAt(cloned.ObservedAt, store.leaseDuration)
	}

	store.mu.RLock()
	published.LastDCSSeenAt = store.lastDCSSeenAt
	if previous, ok := store.nodeStatuses[cloned.NodeName]; ok {
		cloned = mergeControlPlaneManagedNodeFlags(previous, cloned)
	}
	store.mu.RUnlock()

	cloned.ControlPlane = published

	if err := store.setJSON(ctx, store.keyspace.Status(cloned.NodeName), cloned, dcs.WithTTL(store.leaseDuration)); err != nil {
		return agentmodel.ControlPlaneStatus{ClusterReachable: false}, err
	}

	store.mu.Lock()
	if store.nodeStatusRevisions == nil {
		store.nodeStatusRevisions = make(map[string]int64)
	}
	store.nodeStatuses[cloned.NodeName] = cloned
	store.nodeStatusRevisions[cloned.NodeName] = nextRevision(store.nodeStatusRevisions[cloned.NodeName])
	store.refreshSourceOfTruthLocked(cloned.ObservedAt.UTC())
	store.mu.Unlock()

	if err := store.refreshCache(ctx); err != nil {
		return agentmodel.ControlPlaneStatus{ClusterReachable: false}, err
	}

	return published, nil
}

// RegisteredMember returns the stored control-plane registration for the given
// node.
func (store *MemoryStateStore) RegisteredMember(nodeName string) (MemberRegistration, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return MemberRegistration{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	registration, ok := store.registrations[nodeName]
	if !ok {
		return MemberRegistration{}, false
	}

	return registration.Clone(), true
}

// RegisteredMembers returns all registered control-plane members sorted by node
// name.
func (store *MemoryStateStore) RegisteredMembers() []MemberRegistration {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return nil
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	members := make([]MemberRegistration, 0, len(store.registrations))
	for _, registration := range store.registrations {
		members = append(members, registration.Clone())
	}

	sort.Slice(members, func(left, right int) bool {
		return members[left].NodeName < members[right].NodeName
	})

	return members
}

// ClusterSpec returns the desired cluster spec stored in the replicated state
// store.
func (store *MemoryStateStore) ClusterSpec() (cluster.ClusterSpec, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return cluster.ClusterSpec{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if store.clusterSpec == nil {
		return cluster.ClusterSpec{}, false
	}

	return store.clusterSpec.Clone(), true
}

// StoreClusterSpec validates and stores the desired cluster configuration. When
// the effective desired state changes, the generation advances automatically if
// the caller did not already supply a newer one.
func (store *MemoryStateStore) StoreClusterSpec(ctx context.Context, spec cluster.ClusterSpec) (cluster.ClusterSpec, error) {
	if err := ctx.Err(); err != nil {
		return cluster.ClusterSpec{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return cluster.ClusterSpec{}, err
	}

	cloned := spec.Clone()
	if err := cloned.Validate(); err != nil {
		return cluster.ClusterSpec{}, err
	}

	now := store.now().UTC()

	store.mu.Lock()
	cloned = store.storeClusterSpecLocked(cloned)
	revision := store.clusterSpecRevision
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	if err := store.compareAndStoreJSON(ctx, store.keyspace.Config(), revision, cloned); err != nil {
		return cluster.ClusterSpec{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return cluster.ClusterSpec{}, err
	}

	return cloned.Clone(), nil
}

// NodeStatus returns the last published state for the given node.
func (store *MemoryStateStore) NodeStatus(nodeName string) (agentmodel.NodeStatus, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return agentmodel.NodeStatus{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	status, ok := store.nodeStatuses[nodeName]
	if !ok {
		return agentmodel.NodeStatus{}, false
	}

	return status.Clone(), true
}

// NodeStatuses returns all published node states sorted by node name.
func (store *MemoryStateStore) NodeStatuses() []agentmodel.NodeStatus {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return nil
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	nodes := make([]agentmodel.NodeStatus, 0, len(store.nodeStatuses))
	for _, status := range store.nodeStatuses {
		nodes = append(nodes, status.Clone())
	}

	sort.Slice(nodes, func(left, right int) bool {
		return nodes[left].NodeName < nodes[right].NodeName
	})

	return nodes
}

// SourceOfTruth returns the current desired and observed cluster truth held in
// the replicated state store.
func (store *MemoryStateStore) SourceOfTruth() ClusterSourceOfTruth {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return ClusterSourceOfTruth{}
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.sourceOfTruthLocked()
}

// Member returns the discovered cluster member view for the given node.
func (store *MemoryStateStore) Member(nodeName string) (cluster.MemberStatus, bool) {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return cluster.MemberStatus{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	member, ok := store.memberLocked(nodeName)
	if !ok {
		return cluster.MemberStatus{}, false
	}

	return member.Clone(), true
}

// Members returns all discovered cluster members sorted by node name.
func (store *MemoryStateStore) Members() []cluster.MemberStatus {
	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return nil
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.membersLocked()
}

// MarkDCSSeen records the last time the control-plane storage was observed.
func (store *MemoryStateStore) MarkDCSSeen(observedAt time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.lastDCSSeenAt = observedAt.UTC()
}

func (store *MemoryStateStore) memberLocked(nodeName string) (cluster.MemberStatus, bool) {
	registration, registered := store.registrations[nodeName]
	observation, observed := store.nodeStatuses[nodeName]
	if !registered && !observed {
		return cluster.MemberStatus{}, false
	}

	var member cluster.MemberStatus
	if registered {
		member = discoveredMemberFromRegistration(registration)
	} else {
		member = cluster.MemberStatus{
			Name:  nodeName,
			Role:  cluster.MemberRoleUnknown,
			State: cluster.MemberStateUnknown,
		}
	}

	if observed {
		member = mergeObservedMember(member, observation.Clone())
	}

	if spec, ok := store.memberSpecLocked(nodeName); ok {
		member = mergeDesiredMemberPolicy(member, spec)
	}

	if member.Role == "" {
		member.Role = cluster.MemberRoleUnknown
	}

	if member.State == "" {
		member.State = cluster.MemberStateUnknown
	}

	return member, true
}

func mergeObservedMember(member cluster.MemberStatus, observation agentmodel.NodeStatus) cluster.MemberStatus {
	if member.Name == "" {
		member.Name = observation.NodeName
	}

	if observation.Role != "" {
		member.Role = observation.Role
	}

	if observation.State != "" {
		member.State = observation.State
	}

	member.Healthy = observedMemberHealthy(observation)
	member.Leader = observation.Role.IsWritable()
	member.Timeline = observation.Postgres.Details.Timeline
	member.LagBytes = observation.Postgres.Details.ReplicationLagBytes
	member.NeedsRejoin = observation.NeedsRejoin || observation.State == cluster.MemberStateNeedsRejoin
	member.Tags = observation.Tags

	if !observation.ObservedAt.IsZero() {
		member.LastSeenAt = observation.ObservedAt
	}

	return member
}

func mergeDesiredMemberPolicy(member cluster.MemberStatus, spec cluster.MemberSpec) cluster.MemberStatus {
	member.Priority = spec.Priority
	member.NoFailover = spec.NoFailover
	member.Tags = mergeMemberTags(member.Tags, spec.Tags)

	return member
}

func mergeControlPlaneManagedNodeFlags(previous, current agentmodel.NodeStatus) agentmodel.NodeStatus {
	merged := current.Clone()

	if previous.NeedsRejoin && !merged.NeedsRejoin {
		merged.NeedsRejoin = true
	}

	if previous.NeedsRejoin && previous.PendingRestart && !merged.PendingRestart {
		merged.PendingRestart = true
	}

	if merged.PendingRestart {
		merged.Postgres.Details.PendingRestart = true
	}

	return merged
}

func observedMemberHealthy(observation agentmodel.NodeStatus) bool {
	switch observation.State {
	case cluster.MemberStateRunning, cluster.MemberStateStreaming:
	default:
		return false
	}

	if observation.NeedsRejoin || observation.State == cluster.MemberStateNeedsRejoin {
		return false
	}

	if observation.Postgres.Managed {
		return observation.Postgres.Up
	}

	return true
}

func (store *MemoryStateStore) memberSpecLocked(nodeName string) (cluster.MemberSpec, bool) {
	if store.clusterSpec == nil {
		return cluster.MemberSpec{}, false
	}

	for _, member := range store.clusterSpec.Members {
		if member.Name == nodeName {
			return member.Clone(), true
		}
	}

	return cluster.MemberSpec{}, false
}

func mergeMemberTags(observed, desired map[string]any) map[string]any {
	if observed == nil && desired == nil {
		return nil
	}

	merged := make(map[string]any, len(observed)+len(desired))
	for key, value := range observed {
		merged[key] = value
	}

	for key, value := range desired {
		merged[key] = value
	}

	return merged
}

func sameClusterSpecIgnoringGeneration(left, right cluster.ClusterSpec) bool {
	left.Generation = 0
	right.Generation = 0

	return reflect.DeepEqual(left, right)
}
