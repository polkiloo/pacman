package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestPatroniInspiredFailoverExecutorSkipsPromotionWhenFencingFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 1, 12, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			FencingRequired: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now.Add(-time.Minute), false, 4, 0),
		readyStandbyStatus("alpha-2", now, 4, 0),
	})
	setTestNow(store, func() time.Time { return now })

	if _, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{}); err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	promoter := &recordingPromoter{}
	wantErr := errors.New("fencing callback failed")
	_, err := store.ExecuteFailover(context.Background(), promoter, failingFencer{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected failover execution error: got %v, want %v", err, wantErr)
	}

	if len(promoter.requests) != 0 {
		t.Fatalf("expected failed fencing to skip promotion callback, got %+v", promoter.requests)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.State != cluster.OperationStateRunning {
		t.Fatalf("expected failed callback to leave active operation running, ok=%v operation=%+v", ok, active)
	}
}

func TestPatroniInspiredSwitchoverExecutorSkipsPromotionWhenDemotionFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 1, 12, 30, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 12),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 12, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })
	setTestLeaseDuration(store, time.Hour)

	if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
		Candidate: "alpha-2",
	}); err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	promoter := &recordingPromoter{}
	wantErr := errors.New("demotion callback failed")
	_, err := store.ExecuteSwitchover(context.Background(), failingDemoter{err: wantErr}, promoter)
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected switchover execution error: got %v, want %v", err, wantErr)
	}

	if len(promoter.requests) != 0 {
		t.Fatalf("expected failed demotion to skip promotion callback, got %+v", promoter.requests)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.State != cluster.OperationStateRunning {
		t.Fatalf("expected failed callback to leave active operation running, ok=%v operation=%+v", ok, active)
	}
}
