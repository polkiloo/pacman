package controlplane

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// NodeStatePublisher accepts the latest local node observation from pacmand.
type NodeStatePublisher interface {
	PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error)
}

// MemoryStateStore is an in-memory replicated control-plane state store used
// until the distributed source of truth is implemented.
type MemoryStateStore struct {
	mu              sync.RWMutex
	registrations   map[string]MemberRegistration
	nodeStatuses    map[string]agentmodel.NodeStatus
	clusterSpec     *cluster.ClusterSpec
	clusterStatus   *cluster.ClusterStatus
	maintenance     cluster.MaintenanceModeStatus
	activeOperation *cluster.Operation
	history         []cluster.HistoryEntry
	leaderLease     LeaderLease
	now             func() time.Time
	leaseDuration   time.Duration
	sourceUpdated   time.Time
	lastDCSSeenAt   time.Time
}

// NewMemoryStateStore constructs an in-memory replicated control-plane store.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		registrations: make(map[string]MemberRegistration),
		nodeStatuses:  make(map[string]agentmodel.NodeStatus),
		now:           time.Now,
		leaseDuration: defaultLeaderLeaseDuration,
	}
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

	store.mu.Lock()
	store.registrations[cloned.NodeName] = cloned
	store.mu.Unlock()

	return nil
}

// CampaignLeader tries to acquire or renew the control-plane leader lease for
// the given registered node.
func (store *MemoryStateStore) CampaignLeader(ctx context.Context, nodeName string) (LeaderLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return LeaderLease{}, false, err
	}

	candidate := strings.TrimSpace(nodeName)
	if candidate == "" {
		return LeaderLease{}, false, ErrLeaderCandidateRequired
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.registrations[candidate]; !ok {
		return LeaderLease{}, false, ErrLeaderCandidateUnknown
	}

	if store.leaderLease.isActiveAt(now, store.leaseDuration) {
		if store.leaderLease.LeaderNode == candidate {
			store.leaderLease.RenewedAt = now
			return store.leaderLease.Clone(), true, nil
		}

		return store.leaderLease.Clone(), false, nil
	}

	store.leaderLease = LeaderLease{
		LeaderNode: candidate,
		Term:       store.leaderLease.Term + 1,
		AcquiredAt: now,
		RenewedAt:  now,
	}

	if store.leaderLease.Term == 0 {
		store.leaderLease.Term = 1
	}

	return store.leaderLease.Clone(), true, nil
}

// Leader returns the currently active control-plane leader lease.
func (store *MemoryStateStore) Leader() (LeaderLease, bool) {
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

	cloned := status.Clone()

	published := agentmodel.ControlPlaneStatus{
		ClusterReachable: true,
		LastHeartbeatAt:  cloned.ObservedAt,
	}

	store.mu.Lock()
	published.Leader = store.leaderLease.LeaderNode == cloned.NodeName && store.leaderLease.isActiveAt(cloned.ObservedAt, store.leaseDuration)
	published.LastDCSSeenAt = store.lastDCSSeenAt
	cloned.ControlPlane = published
	store.nodeStatuses[cloned.NodeName] = cloned
	store.refreshSourceOfTruthLocked(cloned.ObservedAt.UTC())
	store.mu.Unlock()

	return published, nil
}

// RegisteredMember returns the stored control-plane registration for the given
// node.
func (store *MemoryStateStore) RegisteredMember(nodeName string) (MemberRegistration, bool) {
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

	cloned := spec.Clone()
	if err := cloned.Validate(); err != nil {
		return cluster.ClusterSpec{}, err
	}

	now := store.now().UTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	cloned = store.storeClusterSpecLocked(cloned)
	store.refreshSourceOfTruthLocked(now)

	return cloned.Clone(), nil
}

// NodeStatus returns the last published state for the given node.
func (store *MemoryStateStore) NodeStatus(nodeName string) (agentmodel.NodeStatus, bool) {
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
	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.sourceOfTruthLocked()
}

// Member returns the discovered cluster member view for the given node.
func (store *MemoryStateStore) Member(nodeName string) (cluster.MemberStatus, bool) {
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

func sameClusterSpecIgnoringGeneration(left, right cluster.ClusterSpec) bool {
	left.Generation = 0
	right.Generation = 0

	return reflect.DeepEqual(left, right)
}
