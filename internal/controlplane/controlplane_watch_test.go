package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
	dcsmemory "github.com/polkiloo/pacman/internal/dcs/memory"
)

// testClock is a thread-safe controllable clock for watch tests where
// background goroutines read the current time concurrently with the test
// advancing it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock(t time.Time) *testClock { return &testClock{now: t} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

const watchTestNodeName = "alpha-1"

func TestControlPlaneWatchUpdatesCachedClusterSpecAcrossInstances(t *testing.T) {
	t.Parallel()

	clock := newTestClock(time.Date(2026, time.April, 8, 12, 0, 0, 0, time.UTC))

	backend := dcsmemory.New(dcsmemory.Config{
		TTL:           time.Minute,
		SweepInterval: 5 * time.Millisecond,
		Now:           clock.Now,
	})
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	writer := NewControlPlane(backend, "alpha", nil)
	reader := NewControlPlane(backend, "alpha", nil)
	setTestNow(writer, clock.Now)
	setTestNow(reader, clock.Now)
	setTestCacheMaxAge(writer, time.Hour)
	setTestCacheMaxAge(reader, time.Hour)

	if _, err := writer.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: watchTestNodeName},
		},
	}); err != nil {
		t.Fatalf("store initial cluster spec: %v", err)
	}

	if status, ok := reader.ClusterStatus(); !ok || status.Phase != cluster.ClusterPhaseInitializing {
		t.Fatalf("expected reader to load initial cluster status, got ok=%v status=%+v", ok, status)
	}

	clock.Advance(10 * time.Second)
	if _, err := writer.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: watchTestNodeName},
			{Name: "alpha-2"},
		},
	}); err != nil {
		t.Fatalf("store updated cluster spec: %v", err)
	}

	waitForControlPlaneCondition(t, time.Second, func() bool {
		spec, ok := reader.ClusterSpec()
		return ok && len(spec.Members) == 2
	})

	spec, ok := reader.ClusterSpec()
	if !ok {
		t.Fatal("expected cached cluster spec after watch update")
	}

	if len(spec.Members) != 2 {
		t.Fatalf("expected watched cluster spec update, got %+v", spec)
	}
}

func TestControlPlaneWatchRemovesExpiredNodeStatusAcrossInstances(t *testing.T) {
	t.Parallel()

	clock := newTestClock(time.Date(2026, time.April, 8, 13, 0, 0, 0, time.UTC))

	backend := dcsmemory.New(dcsmemory.Config{
		TTL:           time.Minute,
		SweepInterval: 5 * time.Millisecond,
		Now:           clock.Now,
	})
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	writer := NewControlPlane(backend, "alpha", nil)
	reader := NewControlPlane(backend, "alpha", nil)
	setTestNow(writer, clock.Now)
	setTestNow(reader, clock.Now)
	setTestCacheMaxAge(writer, time.Hour)
	setTestCacheMaxAge(reader, time.Hour)
	setTestLeaseDuration(writer, time.Second)
	setTestLeaseDuration(reader, time.Second)

	if _, err := writer.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: watchTestNodeName},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	if status, ok := reader.ClusterStatus(); !ok || status.Phase != cluster.ClusterPhaseInitializing {
		t.Fatalf("expected initializing cluster before remote heartbeat, got ok=%v status=%+v", ok, status)
	}

	if _, err := writer.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName:   watchTestNodeName,
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		ObservedAt: clock.Now(),
	}); err != nil {
		t.Fatalf("publish remote node status: %v", err)
	}

	waitForControlPlaneCondition(t, time.Second, func() bool {
		status, ok := reader.ClusterStatus()
		return ok && status.Phase == cluster.ClusterPhaseHealthy && status.CurrentPrimary == watchTestNodeName
	})

	if _, ok := reader.NodeStatus(watchTestNodeName); !ok {
		t.Fatal("expected cached node status after watch put event")
	}

	clock.Advance(2 * time.Second)
	waitForControlPlaneCondition(t, time.Second, func() bool {
		_, err := backend.Get(context.Background(), "/pacman/alpha/status/alpha-1")
		return errors.Is(err, dcs.ErrKeyNotFound)
	})

	waitForControlPlaneCondition(t, time.Second, func() bool {
		_, ok := reader.NodeStatus(watchTestNodeName)
		return !ok
	})

	status, ok := reader.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after watched expiration")
	}

	if status.Phase != cluster.ClusterPhaseInitializing {
		t.Fatalf("expected watched status expiration to reinitialize cluster view, got %+v", status)
	}
}

func TestControlPlaneWatchReappliesNodeStatusAfterExpirationAcrossInstances(t *testing.T) {
	t.Parallel()

	clock := newTestClock(time.Date(2026, time.April, 8, 14, 0, 0, 0, time.UTC))

	backend := dcsmemory.New(dcsmemory.Config{
		TTL:           time.Minute,
		SweepInterval: 5 * time.Millisecond,
		Now:           clock.Now,
	})
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	writer := NewControlPlane(backend, "alpha", nil)
	reader := NewControlPlane(backend, "alpha", nil)
	setTestNow(writer, clock.Now)
	setTestNow(reader, clock.Now)
	setTestCacheMaxAge(writer, time.Hour)
	setTestCacheMaxAge(reader, time.Hour)
	setTestLeaseDuration(writer, time.Second)
	setTestLeaseDuration(reader, time.Second)

	if _, err := writer.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: watchTestNodeName},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	publish := func(observedAt time.Time, state cluster.MemberState) {
		t.Helper()

		if _, err := writer.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
			NodeName:   watchTestNodeName,
			Role:       cluster.MemberRolePrimary,
			State:      state,
			ObservedAt: observedAt,
		}); err != nil {
			t.Fatalf("publish remote node status: %v", err)
		}
	}

	publish(clock.Now(), cluster.MemberStateRunning)
	waitForControlPlaneCondition(t, time.Second, func() bool {
		status, ok := reader.NodeStatus(watchTestNodeName)
		return ok && status.State == cluster.MemberStateRunning
	})

	clock.Advance(2 * time.Second)
	waitForControlPlaneCondition(t, time.Second, func() bool {
		_, ok := reader.NodeStatus(watchTestNodeName)
		return !ok
	})

	clock.Advance(time.Second)
	publish(clock.Now(), cluster.MemberStateStreaming)

	waitForControlPlaneCondition(t, time.Second, func() bool {
		status, ok := reader.NodeStatus(watchTestNodeName)
		return ok && status.State == cluster.MemberStateStreaming
	})
}

func waitForControlPlaneCondition(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for control-plane condition after %s", timeout)
}
