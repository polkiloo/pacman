package controlplane

import (
	"context"
	"sort"
	"sync"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
)

// NodeStatePublisher accepts the latest local node observation from pacmand.
type NodeStatePublisher interface {
	PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error)
}

// MemoryStateStore is an in-memory control-plane sink used until the
// distributed source of truth is implemented.
type MemoryStateStore struct {
	mu            sync.RWMutex
	nodeStatuses  map[string]agentmodel.NodeStatus
	leader        bool
	lastDCSSeenAt time.Time
}

// NewMemoryStateStore constructs an in-memory node state publisher.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		nodeStatuses: make(map[string]agentmodel.NodeStatus),
	}
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
