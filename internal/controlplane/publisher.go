package controlplane

import (
	"context"
	"sort"
	"sync"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// NodeStatePublisher accepts the latest local node observation from pacmand.
type NodeStatePublisher interface {
	PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error)
}

// MemoryStateStore is an in-memory control-plane sink used until the
// distributed source of truth is implemented.
type MemoryStateStore struct {
	mu            sync.RWMutex
	registrations map[string]MemberRegistration
	nodeStatuses  map[string]agentmodel.NodeStatus
	leader        bool
	lastDCSSeenAt time.Time
}

// NewMemoryStateStore constructs an in-memory node state publisher.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		registrations: make(map[string]MemberRegistration),
		nodeStatuses:  make(map[string]agentmodel.NodeStatus),
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
	published.Leader = store.leader
	published.LastDCSSeenAt = store.lastDCSSeenAt
	cloned.ControlPlane = published
	store.nodeStatuses[cloned.NodeName] = cloned
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

// SetLeader updates the local control-plane leadership flag returned on the
// next publish.
func (store *MemoryStateStore) SetLeader(leader bool) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.leader = leader
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
