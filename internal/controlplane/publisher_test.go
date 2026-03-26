package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
)

func TestMemoryStateStorePublishesNodeStatus(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	observedAt := time.Date(2026, time.March, 23, 9, 0, 0, 0, time.UTC)
	store.MarkDCSSeen(observedAt.Add(-time.Second))
	store.now = func() time.Time { return observedAt }
	store.leaseDuration = time.Minute

	if err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.10:8080",
		ControlAddress: "10.0.0.10:9090",
		RegisteredAt:   observedAt.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("register member: %v", err)
	}

	if _, elected, err := store.CampaignLeader(context.Background(), "alpha-1"); err != nil {
		t.Fatalf("campaign leader: %v", err)
	} else if !elected {
		t.Fatal("expected leader election to succeed")
	}

	status := agentmodel.NodeStatus{
		NodeName:   "alpha-1",
		MemberName: "alpha-1",
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		Tags: map[string]any{
			"zone": "a",
		},
		ObservedAt: observedAt,
	}

	published, err := store.PublishNodeStatus(context.Background(), status)
	if err != nil {
		t.Fatalf("publish node status: %v", err)
	}

	if !published.ClusterReachable {
		t.Fatalf("expected cluster reachable, got %+v", published)
	}

	if !published.Leader {
		t.Fatalf("expected leader flag, got %+v", published)
	}

	if !published.LastHeartbeatAt.Equal(observedAt) {
		t.Fatalf("unexpected last heartbeat time: got %v", published.LastHeartbeatAt)
	}

	if !published.LastDCSSeenAt.Equal(observedAt.Add(-time.Second)) {
		t.Fatalf("unexpected last dcs seen time: got %v", published.LastDCSSeenAt)
	}

	stored, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected stored node status")
	}

	if stored.Role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected stored role: got %q", stored.Role)
	}

	status.Tags["zone"] = "mutated"
	if stored.Tags["zone"] != "a" {
		t.Fatalf("expected stored tags to be detached, got %+v", stored.Tags)
	}
}

func TestMemoryStateStoreRegistersMember(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	registeredAt := time.Date(2026, time.March, 24, 9, 0, 0, 0, time.FixedZone("MSK", 3*60*60))
	registration := MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.10:8080",
		ControlAddress: "10.0.0.10:9090",
		RegisteredAt:   registeredAt,
	}

	if err := store.RegisterMember(context.Background(), registration); err != nil {
		t.Fatalf("register member: %v", err)
	}

	stored, ok := store.RegisteredMember("alpha-1")
	if !ok {
		t.Fatal("expected stored member registration")
	}

	if stored.APIAddress != "10.0.0.10:8080" {
		t.Fatalf("unexpected api address: got %q", stored.APIAddress)
	}

	if stored.ControlAddress != "10.0.0.10:9090" {
		t.Fatalf("unexpected control address: got %q", stored.ControlAddress)
	}

	if stored.NodeRole != cluster.NodeRoleData {
		t.Fatalf("unexpected node role: got %q", stored.NodeRole)
	}

	if !stored.RegisteredAt.Equal(registeredAt.UTC()) {
		t.Fatalf("unexpected registration time: got %v", stored.RegisteredAt)
	}
}

func TestMemoryStateStoreReturnsContextError(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.PublishNodeStatus(ctx, agentmodel.NodeStatus{NodeName: "alpha-1"})
	if err != context.Canceled {
		t.Fatalf("expected canceled context, got %v", err)
	}
}

func TestMemoryStateStoreCampaignLeaderRequiresRegisteredCandidate(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	store.now = func() time.Time {
		return time.Date(2026, time.March, 24, 9, 0, 0, 0, time.UTC)
	}

	if _, _, err := store.CampaignLeader(context.Background(), "alpha-1"); !errors.Is(err, ErrLeaderCandidateUnknown) {
		t.Fatalf("unexpected leader campaign error: got %v", err)
	}

	if _, _, err := store.CampaignLeader(context.Background(), "   "); !errors.Is(err, ErrLeaderCandidateRequired) {
		t.Fatalf("unexpected blank leader campaign error: got %v", err)
	}
}

func TestMemoryStateStoreCampaignLeaderElectsAndRenewsLease(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	current := time.Date(2026, time.March, 24, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }
	store.leaseDuration = 2 * time.Second

	for _, registration := range []MemberRegistration{
		{
			NodeName:       "alpha-1",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.10:8080",
			ControlAddress: "10.0.0.10:9090",
			RegisteredAt:   current.Add(-time.Minute),
		},
		{
			NodeName:       "alpha-2",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.11:8080",
			ControlAddress: "10.0.0.11:9090",
			RegisteredAt:   current.Add(-time.Minute),
		},
	} {
		if err := store.RegisterMember(context.Background(), registration); err != nil {
			t.Fatalf("register member %q: %v", registration.NodeName, err)
		}
	}

	lease, elected, err := store.CampaignLeader(context.Background(), "alpha-1")
	if err != nil {
		t.Fatalf("campaign alpha-1: %v", err)
	}

	if !elected || lease.LeaderNode != "alpha-1" || lease.Term != 1 {
		t.Fatalf("unexpected initial leader lease: elected=%v lease=%+v", elected, lease)
	}

	current = current.Add(time.Second)
	lease, elected, err = store.CampaignLeader(context.Background(), "alpha-1")
	if err != nil {
		t.Fatalf("renew alpha-1: %v", err)
	}

	if !elected || lease.Term != 1 || !lease.RenewedAt.Equal(current) {
		t.Fatalf("unexpected renewed leader lease: elected=%v lease=%+v", elected, lease)
	}

	current = current.Add(3 * time.Second)
	lease, elected, err = store.CampaignLeader(context.Background(), "alpha-2")
	if err != nil {
		t.Fatalf("campaign alpha-2: %v", err)
	}

	if !elected || lease.LeaderNode != "alpha-2" || lease.Term != 2 {
		t.Fatalf("unexpected new leader lease: elected=%v lease=%+v", elected, lease)
	}
}

func TestMemoryStateStoreCampaignLeaderReturnsExistingLeaseWhileActive(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	current := time.Date(2026, time.March, 24, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }
	store.leaseDuration = time.Minute

	for _, registration := range []MemberRegistration{
		{
			NodeName:       "alpha-1",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.10:8080",
			ControlAddress: "10.0.0.10:9090",
			RegisteredAt:   current,
		},
		{
			NodeName:       "alpha-2",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.11:8080",
			ControlAddress: "10.0.0.11:9090",
			RegisteredAt:   current,
		},
	} {
		if err := store.RegisterMember(context.Background(), registration); err != nil {
			t.Fatalf("register member %q: %v", registration.NodeName, err)
		}
	}

	firstLease, elected, err := store.CampaignLeader(context.Background(), "alpha-1")
	if err != nil || !elected {
		t.Fatalf("campaign alpha-1: elected=%v err=%v", elected, err)
	}

	current = current.Add(10 * time.Second)
	secondLease, elected, err := store.CampaignLeader(context.Background(), "alpha-2")
	if err != nil {
		t.Fatalf("campaign alpha-2 while active: %v", err)
	}

	if elected {
		t.Fatalf("expected active lease to reject new leader, got lease %+v", secondLease)
	}

	if secondLease.LeaderNode != firstLease.LeaderNode || secondLease.Term != firstLease.Term {
		t.Fatalf("expected existing lease to be returned, got first=%+v second=%+v", firstLease, secondLease)
	}
}

func TestMemoryStateStoreLeaderReturnsActiveAndExpiredLease(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	current := time.Date(2026, time.March, 24, 11, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }
	store.leaseDuration = 2 * time.Second

	if _, ok := store.Leader(); ok {
		t.Fatal("expected missing leader before election")
	}

	if err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.10:8080",
		ControlAddress: "10.0.0.10:9090",
		RegisteredAt:   current,
	}); err != nil {
		t.Fatalf("register member: %v", err)
	}

	if _, elected, err := store.CampaignLeader(context.Background(), "alpha-1"); err != nil || !elected {
		t.Fatalf("campaign leader: elected=%v err=%v", elected, err)
	}

	lease, ok := store.Leader()
	if !ok || lease.LeaderNode != "alpha-1" {
		t.Fatalf("expected active leader lease, got ok=%v lease=%+v", ok, lease)
	}

	current = current.Add(3 * time.Second)
	if _, ok := store.Leader(); ok {
		t.Fatal("expected leader lease to expire")
	}
}

func TestMemoryStateStoreRegisterMemberReturnsContextError(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.RegisterMember(ctx, MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.10:8080",
		ControlAddress: "10.0.0.10:9090",
		RegisteredAt:   time.Date(2026, time.March, 24, 9, 0, 0, 0, time.UTC),
	})
	if err != context.Canceled {
		t.Fatalf("expected canceled context, got %v", err)
	}
}

func TestMemoryStateStoreRegisterMemberRejectsInvalidRegistration(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "broken",
		ControlAddress: "10.0.0.10:9090",
		RegisteredAt:   time.Date(2026, time.March, 24, 9, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, config.ErrNodeAPIAddressInvalid) {
		t.Fatalf("unexpected registration error: got %v", err)
	}
}

func TestMemoryStateStoreNodeStatusReturnsMissingNode(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	if _, ok := store.NodeStatus("missing"); ok {
		t.Fatal("expected missing node lookup to return false")
	}

	if _, ok := store.Member("missing"); ok {
		t.Fatal("expected missing member lookup to return false")
	}

	if _, ok := store.RegisteredMember("missing"); ok {
		t.Fatal("expected missing registered member lookup to return false")
	}

	if _, ok := store.ClusterSpec(); ok {
		t.Fatal("expected missing cluster spec lookup to return false")
	}
}

func TestMemoryStateStoreNodeStatusesReturnsSortedDetachedCopies(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	first := agentmodel.NodeStatus{
		NodeName: "beta-1",
		Tags: map[string]any{
			"zone": "b",
		},
		ObservedAt: time.Date(2026, time.March, 25, 11, 0, 0, 0, time.UTC),
	}
	second := agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Tags: map[string]any{
			"zone": "a",
		},
		ObservedAt: time.Date(2026, time.March, 25, 11, 1, 0, 0, time.UTC),
	}

	if _, err := store.PublishNodeStatus(context.Background(), first); err != nil {
		t.Fatalf("publish first node status: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), second); err != nil {
		t.Fatalf("publish second node status: %v", err)
	}

	statuses := store.NodeStatuses()
	if len(statuses) != 2 {
		t.Fatalf("unexpected number of node statuses: got %d", len(statuses))
	}

	if statuses[0].NodeName != "alpha-1" || statuses[1].NodeName != "beta-1" {
		t.Fatalf("expected sorted node statuses, got %+v", statuses)
	}

	statuses[0].Tags["zone"] = "mutated"

	stored, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected stored alpha node status")
	}

	if stored.Tags["zone"] != "a" {
		t.Fatalf("expected detached stored tags, got %+v", stored.Tags)
	}
}

func TestMemoryStateStoreMembersReturnsRegisteredMemberWithoutHeartbeat(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	registeredAt := time.Date(2026, time.March, 24, 9, 30, 0, 0, time.UTC)

	if err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "alpha-3",
		NodeRole:       cluster.NodeRoleWitness,
		APIAddress:     "10.0.0.30:8080",
		ControlAddress: "10.0.0.30:9090",
		RegisteredAt:   registeredAt,
	}); err != nil {
		t.Fatalf("register member: %v", err)
	}

	member, ok := store.Member("alpha-3")
	if !ok {
		t.Fatal("expected discovered member")
	}

	if member.APIURL != "http://10.0.0.30:8080" {
		t.Fatalf("unexpected api url: got %q", member.APIURL)
	}

	if member.Host != "10.0.0.30" || member.Port != 8080 {
		t.Fatalf("unexpected endpoint: got %s:%d", member.Host, member.Port)
	}

	if member.Role != cluster.MemberRoleWitness {
		t.Fatalf("unexpected default member role: got %q", member.Role)
	}

	if member.State != cluster.MemberStateUnknown {
		t.Fatalf("unexpected default member state: got %q", member.State)
	}

	if member.Healthy {
		t.Fatalf("expected member without heartbeat to be unhealthy, got %+v", member)
	}

	if !member.LastSeenAt.Equal(registeredAt) {
		t.Fatalf("unexpected last seen time: got %v", member.LastSeenAt)
	}
}

func TestMemoryStateStoreRegisteredMembersReturnsSortedDetachedCopies(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	for _, registration := range []MemberRegistration{
		{
			NodeName:       "beta-1",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.11:8080",
			ControlAddress: "10.0.0.11:9090",
			RegisteredAt:   time.Date(2026, time.March, 24, 8, 0, 0, 0, time.UTC),
		},
		{
			NodeName:       "alpha-1",
			NodeRole:       cluster.NodeRoleWitness,
			APIAddress:     "10.0.0.10:8080",
			ControlAddress: "10.0.0.10:9090",
			RegisteredAt:   time.Date(2026, time.March, 24, 8, 1, 0, 0, time.UTC),
		},
	} {
		if err := store.RegisterMember(context.Background(), registration); err != nil {
			t.Fatalf("register member %q: %v", registration.NodeName, err)
		}
	}

	members := store.RegisteredMembers()
	if len(members) != 2 {
		t.Fatalf("unexpected number of registered members: got %d", len(members))
	}

	if members[0].NodeName != "alpha-1" || members[1].NodeName != "beta-1" {
		t.Fatalf("expected sorted registered members, got %+v", members)
	}

	members[0].APIAddress = "mutated"

	stored, ok := store.RegisteredMember("alpha-1")
	if !ok {
		t.Fatal("expected stored alpha registration")
	}

	if stored.APIAddress != "10.0.0.10:8080" {
		t.Fatalf("expected detached registered member, got %+v", stored)
	}
}

func TestMemoryStateStoreMembersMergeRegistrationAndObservation(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	registeredAt := time.Date(2026, time.March, 24, 8, 0, 0, 0, time.UTC)
	observedAt := time.Date(2026, time.March, 24, 9, 0, 0, 0, time.UTC)

	if err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.10:8080",
		ControlAddress: "10.0.0.10:9090",
		RegisteredAt:   registeredAt,
	}); err != nil {
		t.Fatalf("register member: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName:    "alpha-1",
		Role:        cluster.MemberRolePrimary,
		State:       cluster.MemberStateRunning,
		NeedsRejoin: false,
		Tags: map[string]any{
			"zone": "a",
		},
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline:            4,
				ReplicationLagBytes: 0,
			},
		},
		ObservedAt: observedAt,
	}); err != nil {
		t.Fatalf("publish node status: %v", err)
	}

	member, ok := store.Member("alpha-1")
	if !ok {
		t.Fatal("expected discovered member")
	}

	if member.APIURL != "http://10.0.0.10:8080" {
		t.Fatalf("unexpected api url: got %q", member.APIURL)
	}

	if member.Role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected member role: got %q", member.Role)
	}

	if member.State != cluster.MemberStateRunning {
		t.Fatalf("unexpected member state: got %q", member.State)
	}

	if !member.Healthy {
		t.Fatalf("expected observed primary to be healthy, got %+v", member)
	}

	if !member.Leader {
		t.Fatalf("expected primary member to be leader, got %+v", member)
	}

	if member.Timeline != 4 {
		t.Fatalf("unexpected timeline: got %d", member.Timeline)
	}

	if !member.LastSeenAt.Equal(observedAt) {
		t.Fatalf("unexpected last seen time: got %v", member.LastSeenAt)
	}

	member.Tags["zone"] = "mutated"

	stored, ok := store.Member("alpha-1")
	if !ok {
		t.Fatal("expected stored member status")
	}

	if stored.Tags["zone"] != "a" {
		t.Fatalf("expected detached member tags, got %+v", stored.Tags)
	}
}

func TestMemoryStateStoreMembersReturnsSortedViewsAcrossRegisteredAndObservedNodes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	if err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "charlie-1",
		NodeRole:       cluster.NodeRoleWitness,
		APIAddress:     "10.0.0.30:8080",
		ControlAddress: "10.0.0.30:9090",
		RegisteredAt:   time.Date(2026, time.March, 24, 8, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("register charlie-1: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "alpha-1",
		Role:     cluster.MemberRoleWitness,
		State:    cluster.MemberStateRunning,
		Tags: map[string]any{
			"zone": "a",
		},
		ObservedAt: time.Date(2026, time.March, 24, 9, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("publish alpha-1 node status: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName: "beta-1",
		Role:     cluster.MemberRoleReplica,
		State:    cluster.MemberStateFailed,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		},
		ObservedAt: time.Date(2026, time.March, 24, 9, 1, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("publish beta-1 node status: %v", err)
	}

	members := store.Members()
	if len(members) != 3 {
		t.Fatalf("unexpected member count: got %d", len(members))
	}

	if members[0].Name != "alpha-1" || members[1].Name != "beta-1" || members[2].Name != "charlie-1" {
		t.Fatalf("expected sorted members, got %+v", members)
	}

	if !members[0].Healthy {
		t.Fatalf("expected unmanaged running member to be healthy, got %+v", members[0])
	}

	if members[1].Healthy {
		t.Fatalf("expected failed managed member to be unhealthy, got %+v", members[1])
	}

	members[0].Tags["zone"] = "mutated"

	stored, ok := store.Member("alpha-1")
	if !ok {
		t.Fatal("expected stored alpha member")
	}

	if stored.Tags["zone"] != "a" {
		t.Fatalf("expected detached members list tags, got %+v", stored.Tags)
	}
}

func TestMemoryStateStoreMemberMarksNeedsRejoinUnhealthy(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName:    "alpha-1",
		Role:        cluster.MemberRoleReplica,
		State:       cluster.MemberStateNeedsRejoin,
		NeedsRejoin: true,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		},
		ObservedAt: time.Date(2026, time.March, 24, 9, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("publish node status: %v", err)
	}

	member, ok := store.Member("alpha-1")
	if !ok {
		t.Fatal("expected member view")
	}

	if !member.NeedsRejoin {
		t.Fatalf("expected member to require rejoin, got %+v", member)
	}

	if member.Healthy {
		t.Fatalf("expected needs_rejoin member to be unhealthy, got %+v", member)
	}
}

func TestMemoryStateStoreStoresDesiredStateAndSourceOfTruth(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	updatedAt := time.Date(2026, time.March, 25, 8, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return updatedAt }

	spec, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  0,
		Failover: cluster.FailoverPolicy{
			Mode: cluster.FailoverModeAutomatic,
		},
		Members: []cluster.MemberSpec{
			{
				Name:     "alpha-1",
				Priority: 100,
				Tags: map[string]any{
					"zone": "a",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	if spec.Generation != 0 {
		t.Fatalf("unexpected stored generation: got %d", spec.Generation)
	}

	stored, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected stored cluster spec")
	}

	if stored.ClusterName != "alpha" {
		t.Fatalf("unexpected stored cluster name: got %q", stored.ClusterName)
	}

	truth := store.SourceOfTruth()
	if err := truth.Validate(); err != nil {
		t.Fatalf("validate source of truth: %v", err)
	}

	if truth.Desired == nil || truth.Observed == nil {
		t.Fatalf("unexpected source of truth snapshot: %+v", truth)
	}

	if truth.Observed.Phase != cluster.ClusterPhaseInitializing {
		t.Fatalf("expected observed cluster state to initialize from desired state, got %+v", truth.Observed)
	}

	if !truth.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("unexpected source update time: got %v", truth.UpdatedAt)
	}

	truth.Desired.Members[0].Tags["zone"] = "mutated"

	again, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected stored cluster spec on second read")
	}

	if again.Members[0].Tags["zone"] != "a" {
		t.Fatalf("expected detached desired state tags, got %+v", again.Members[0].Tags)
	}
}

func TestMemoryStateStoreStoreClusterSpecAdvancesGenerationWhenDesiredStateChanges(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	current := time.Date(2026, time.March, 25, 9, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	initial, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  4,
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
		},
	})
	if err != nil {
		t.Fatalf("store initial cluster spec: %v", err)
	}

	if initial.Generation != 4 {
		t.Fatalf("unexpected initial generation: got %d", initial.Generation)
	}

	current = current.Add(time.Minute)
	same, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
		},
	})
	if err != nil {
		t.Fatalf("store unchanged cluster spec: %v", err)
	}

	if same.Generation != 4 {
		t.Fatalf("expected unchanged desired state to keep generation, got %d", same.Generation)
	}

	current = current.Add(time.Minute)
	changed, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	})
	if err != nil {
		t.Fatalf("store changed cluster spec: %v", err)
	}

	if changed.Generation != 5 {
		t.Fatalf("expected changed desired state to advance generation, got %d", changed.Generation)
	}
}

func TestMemoryStateStoreStoreClusterSpecRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.StoreClusterSpec(ctx, cluster.ClusterSpec{ClusterName: "alpha"})
	if err != context.Canceled {
		t.Fatalf("expected canceled context, got %v", err)
	}
}
