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
	dcs                  dcs.DCS
	keyspace             dcs.KeySpace
	clusterName          string
	logger               *slog.Logger
	mu                   sync.RWMutex
	registrations        map[string]MemberRegistration
	nodeStatuses         map[string]agentmodel.NodeStatus
	nodeStatusRevisions  map[string]int64
	clusterSpec          *cluster.ClusterSpec
	clusterSpecRevision  int64
	clusterStatus        *cluster.ClusterStatus
	maintenance          cluster.MaintenanceModeStatus
	maintenanceRevision  int64
	activeOperation      *cluster.Operation
	activeOpRevision     int64
	history              []cluster.HistoryEntry
	operationTraceCounts map[operationTraceKey]uint64
	leaderLease          LeaderLease
	now                  func() time.Time
	leaseDuration        time.Duration
	cacheRefreshedAt     time.Time
	cacheMaxAge          time.Duration
	cacheDirty           bool
	sourceUpdated        time.Time
	lastDCSSeenAt        time.Time
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

	if err := store.refreshCache(ctx); err != nil {
		return err
	}

	store.logLifecycle(
		ctx,
		"registered cluster member",
		slog.String("node", cloned.NodeName),
		slog.String("member", cloned.NodeName),
		slog.String("node_role", string(cloned.NodeRole)),
		slog.String("api_address", cloned.APIAddress),
		slog.String("control_address", cloned.ControlAddress),
	)

	return nil
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
	previousLease := store.leaderLease.Clone()
	store.leaderLease = converted
	store.mu.Unlock()

	if previousLease.LeaderNode != converted.LeaderNode || previousLease.Term != converted.Term {
		store.logTransition(
			ctx,
			"updated control-plane leader lease",
			controlPlaneTransitionLeaderLease,
			slog.String("node", candidate),
			slog.String("leader", converted.LeaderNode),
			slog.Uint64("term", converted.Term),
			slog.Bool("elected", elected),
			slog.String("previous_leader", previousLease.LeaderNode),
			slog.Uint64("previous_term", previousLease.Term),
		)
	}

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
	expectedRevision := nextRevision(store.nodeStatusRevisions[cloned.NodeName])
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
	previousStatus, hadPreviousStatus := store.nodeStatuses[cloned.NodeName]
	previousMember, hadPreviousMember := store.memberLocked(cloned.NodeName)
	if store.nodeStatusRevisions == nil {
		store.nodeStatusRevisions = make(map[string]int64)
	}
	store.nodeStatuses[cloned.NodeName] = cloned
	if store.nodeStatusRevisions[cloned.NodeName] < expectedRevision {
		store.nodeStatusRevisions[cloned.NodeName] = expectedRevision
	}
	store.refreshSourceOfTruthLocked(cloned.ObservedAt.UTC())
	currentStatus := store.nodeStatuses[cloned.NodeName].Clone()
	currentMember, hasCurrentMember := store.memberLocked(cloned.NodeName)
	store.mu.Unlock()

	if err := store.refreshCache(ctx); err != nil {
		return agentmodel.ControlPlaneStatus{ClusterReachable: false}, err
	}

	if !hadPreviousStatus ||
		previousStatus.Role != currentStatus.Role ||
		previousStatus.State != currentStatus.State ||
		previousStatus.NeedsRejoin != currentStatus.NeedsRejoin ||
		previousStatus.PendingRestart != currentStatus.PendingRestart ||
		previousStatus.Postgres.Details.PendingRestart != currentStatus.Postgres.Details.PendingRestart ||
		previousStatus.ControlPlane.Leader != currentStatus.ControlPlane.Leader ||
		observedMemberHealthy(previousStatus) != observedMemberHealthy(currentStatus) {
		attributes := []slog.Attr{
			slog.String("node", currentStatus.NodeName),
			slog.String("member", currentStatus.MemberName),
			slog.String("role", string(currentStatus.Role)),
			slog.String("state", string(currentStatus.State)),
			slog.Bool("healthy", observedMemberHealthy(currentStatus)),
			slog.Bool("needs_rejoin", currentStatus.NeedsRejoin),
			slog.Bool("pending_restart", currentStatus.PendingRestart || currentStatus.Postgres.Details.PendingRestart),
			slog.Bool("leader", currentStatus.ControlPlane.Leader),
		}
		if hasCurrentMember {
			attributes = append(
				attributes,
				slog.Int64("timeline", currentMember.Timeline),
				slog.Int64("lag_bytes", currentMember.LagBytes),
			)
		}
		if hadPreviousStatus {
			attributes = append(
				attributes,
				slog.String("previous_role", string(previousStatus.Role)),
				slog.String("previous_state", string(previousStatus.State)),
				slog.Bool("previous_healthy", observedMemberHealthy(previousStatus)),
				slog.Bool("previous_needs_rejoin", previousStatus.NeedsRejoin),
				slog.Bool("previous_pending_restart", previousStatus.PendingRestart || previousStatus.Postgres.Details.PendingRestart),
				slog.Bool("previous_leader", previousStatus.ControlPlane.Leader),
			)
		}
		if hadPreviousMember {
			attributes = append(
				attributes,
				slog.Int64("previous_timeline", previousMember.Timeline),
				slog.Int64("previous_lag_bytes", previousMember.LagBytes),
			)
		}

		store.logTransition(ctx, "observed member state change", controlPlaneTransitionMemberState, attributes...)
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
	var previousSpec *cluster.ClusterSpec
	if store.clusterSpec != nil {
		clonedPrevious := store.clusterSpec.Clone()
		previousSpec = &clonedPrevious
	}
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

	addedMembers, removedMembers, updatedMembers := clusterSpecTopologyDiff(previousSpec, cloned)
	if previousSpec == nil || len(addedMembers) > 0 || len(removedMembers) > 0 || len(updatedMembers) > 0 {
		attributes := []slog.Attr{
			slog.Int64("generation", int64(cloned.Generation)),
			slog.Int("member_count", len(cloned.Members)),
			slog.Any("added_members", addedMembers),
			slog.Any("removed_members", removedMembers),
			slog.Any("updated_members", updatedMembers),
		}
		if previousSpec != nil {
			attributes = append(
				attributes,
				slog.Int64("previous_generation", int64(previousSpec.Generation)),
				slog.Int("previous_member_count", len(previousSpec.Members)),
			)
		}

		store.logAudit(ctx, "stored cluster topology", "cluster_topology.update", attributes...)
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

	// During shutdown or probe failures the agent can temporarily lose role and
	// timeline visibility before PostgreSQL is fully down. Preserve the last
	// known identity so failover and rejoin planning can still reason about the
	// member across those transitions.
	if shouldPreserveManagedPostgresIdentityLocked(merged) {
		if merged.Role == "" || merged.Role == cluster.MemberRoleUnknown {
			merged.Role = previous.Role
		}

		if merged.Postgres.Role == "" || merged.Postgres.Role == cluster.MemberRoleUnknown {
			merged.Postgres.Role = previous.Postgres.Role
		}

		if merged.Postgres.Details == (agentmodel.PostgresDetails{}) {
			merged.Postgres.Details = previous.Postgres.Details
		}

		if merged.Postgres.WAL == (agentmodel.WALProgress{}) {
			merged.Postgres.WAL = previous.Postgres.WAL
		}
	}

	// Former primaries publish an offline heartbeat while PostgreSQL is stopped
	// during switchover/rejoin. Keep the needs-rejoin state so the control plane
	// can continue the recovery workflow instead of collapsing to generic failure.
	if previous.NeedsRejoin && merged.Postgres.Managed && !merged.Postgres.Up {
		if previous.State == cluster.MemberStateNeedsRejoin {
			merged.State = cluster.MemberStateNeedsRejoin
		}
	}

	if merged.PendingRestart {
		merged.Postgres.Details.PendingRestart = true
	}

	return merged
}

func shouldPreserveManagedPostgresIdentityLocked(current agentmodel.NodeStatus) bool {
	if !current.Postgres.Managed {
		return false
	}

	if !current.Postgres.Up {
		return true
	}

	return current.Role == "" ||
		current.Role == cluster.MemberRoleUnknown ||
		current.Postgres.Role == "" ||
		current.Postgres.Role == cluster.MemberRoleUnknown
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
