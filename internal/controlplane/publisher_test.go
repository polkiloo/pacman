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
	store.SetLeader(true)
	store.MarkDCSSeen(observedAt.Add(-time.Second))

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
