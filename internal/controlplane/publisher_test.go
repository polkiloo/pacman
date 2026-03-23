package controlplane

import (
	"context"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
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
