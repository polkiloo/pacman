package raft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
)

func TestBackendFollowerMethodsUseLocalFSMView(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 15, 0, 0, 0, time.UTC)
	backend := newFollowerBranchTestBackend(t, now)

	backend.fsm.state.Entries["/pacman/alpha/config"] = keyState{
		Key:      "/pacman/alpha/config",
		Value:    []byte("value"),
		Revision: 2,
	}
	backend.fsm.state.Sessions["alpha-1"] = sessionState{ExpiresAt: now.Add(time.Minute)}
	backend.fsm.state.Leader = leaderState{
		Leader:    "alpha-1",
		Term:      3,
		Acquired:  now.Add(-time.Minute),
		Renewed:   now.Add(-30 * time.Second),
		ExpiresAt: now.Add(time.Minute),
	}

	got, err := backend.Get(context.Background(), "/pacman/alpha/config")
	if err != nil {
		t.Fatalf("get on follower: %v", err)
	}

	if got.Revision != 2 || string(got.Value) != "value" {
		t.Fatalf("unexpected follower get result: %+v", got)
	}

	listed, err := backend.List(context.Background(), "/pacman/alpha/")
	if err != nil {
		t.Fatalf("list on follower: %v", err)
	}

	if len(listed) != 1 || listed[0].Key != "/pacman/alpha/config" {
		t.Fatalf("unexpected follower list result: %+v", listed)
	}

	alive, err := backend.Alive(context.Background(), "alpha-1")
	if err != nil {
		t.Fatalf("alive on follower: %v", err)
	}

	if !alive {
		t.Fatal("expected seeded session to be alive")
	}

	lease, ok, err := backend.Leader(context.Background())
	if err != nil {
		t.Fatalf("leader on follower: %v", err)
	}

	if !ok || lease.Leader != "alpha-1" || lease.Term != 3 {
		t.Fatalf("unexpected follower leader lease: ok=%v lease=%+v", ok, lease)
	}

	campaignLease, held, err := backend.Campaign(context.Background(), "beta-1")
	if err != nil {
		t.Fatalf("campaign on follower: %v", err)
	}

	if held || campaignLease.Leader != "alpha-1" {
		t.Fatalf("expected follower campaign to return existing lease without winning, got held=%v lease=%+v", held, campaignLease)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	events, err := backend.Watch(watchCtx, "/pacman/alpha/")
	if err != nil {
		t.Fatalf("watch on follower: %v", err)
	}
	cancel()

	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("expected watch channel to close after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch close")
	}

	if err := backend.Set(context.Background(), "/pacman/alpha/config", []byte("new")); !errors.Is(err, dcs.ErrNotLeader) {
		t.Fatalf("unexpected follower set error: got %v want %v", err, dcs.ErrNotLeader)
	}
}

func TestBackendRejectsOperationsAfterClose(t *testing.T) {
	t.Parallel()

	backend := newFollowerBranchTestBackend(t, time.Date(2026, time.April, 15, 15, 30, 0, 0, time.UTC))
	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	if err := backend.Close(); err != nil {
		t.Fatalf("close backend second time: %v", err)
	}

	assertUnavailable := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, dcs.ErrBackendUnavailable) {
			t.Fatalf("%s: unexpected error: got %v want %v", name, err, dcs.ErrBackendUnavailable)
		}
	}

	assertUnavailable("initialize", backend.Initialize(context.Background()))
	_, err := backend.Get(context.Background(), "/pacman/alpha/config")
	assertUnavailable("get", err)
	assertUnavailable("set", backend.Set(context.Background(), "/pacman/alpha/config", []byte("value")))
	assertUnavailable("compare and set", backend.CompareAndSet(context.Background(), "/pacman/alpha/config", []byte("value"), 1))
	assertUnavailable("delete", backend.Delete(context.Background(), "/pacman/alpha/config"))
	_, err = backend.List(context.Background(), "/pacman/alpha/")
	assertUnavailable("list", err)
	_, _, err = backend.Campaign(context.Background(), "alpha-1")
	assertUnavailable("campaign", err)
	_, _, err = backend.Leader(context.Background())
	assertUnavailable("leader", err)
	assertUnavailable("resign", backend.Resign(context.Background()))
	assertUnavailable("touch", backend.Touch(context.Background(), "alpha-1"))
	_, err = backend.Alive(context.Background(), "alpha-1")
	assertUnavailable("alive", err)
	_, err = backend.Watch(context.Background(), "/pacman/alpha/")
	assertUnavailable("watch", err)
}

func TestBackendWaitForLocalLeaderHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	backend := newFollowerBranchTestBackend(t, time.Date(2026, time.April, 15, 16, 0, 0, 0, time.UTC))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := backend.waitForLocalLeader(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected waitForLocalLeader error: got %v want %v", err, context.DeadlineExceeded)
	}
}

func TestBackendLeaderVerifyAndApplyPaths(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 16, 15, 0, 0, time.UTC)
	self := reserveTCPAddress(t)
	backend := newBranchTestBackend(t, []string{self}, true, now)

	if err := backend.verifyLeader(); err != nil {
		t.Fatalf("verify leader: %v", err)
	}

	response, err := backend.apply(context.Background(), command{
		Type:      commandCampaign,
		Candidate: "alpha-1",
		TTL:       100 * time.Millisecond,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("apply campaign command: %v", err)
	}

	result, ok := response.(campaignResult)
	if !ok || !result.Held || result.Lease.Leader != "alpha-1" {
		t.Fatalf("unexpected apply campaign result: %#v", response)
	}
}

func TestBackendCampaignReturnsFirstHeldLease(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.April, 15, 16, 20, 0, 0, time.UTC)
	nowValues := []time.Time{
		base,
		base.Add(90 * time.Millisecond),
		base.Add(150 * time.Millisecond),
		base.Add(151 * time.Millisecond),
	}
	var nowMu sync.Mutex
	now := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()

		if len(nowValues) == 0 {
			return base.Add(151 * time.Millisecond)
		}

		current := nowValues[0]
		nowValues = nowValues[1:]
		return current
	}

	self := reserveTCPAddress(t)
	backend, err := New(Config{
		ClusterName:        "alpha",
		TTL:                100 * time.Millisecond,
		RetryTimeout:       time.Second,
		DataDir:            t.TempDir(),
		BindAddress:        self,
		Peers:              []string{self},
		Bootstrap:          true,
		ExpiryInterval:     250 * time.Millisecond,
		HeartbeatTimeout:   75 * time.Millisecond,
		ElectionTimeout:    75 * time.Millisecond,
		LeaderLeaseTimeout: 75 * time.Millisecond,
		Now:                now,
	})
	if err != nil {
		t.Fatalf("create raft backend: %v", err)
	}
	t.Cleanup(func() {
		_ = backend.Close()
	})

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize raft backend: %v", err)
	}

	lease, held, err := backend.Campaign(context.Background(), "alpha-1")
	if err != nil {
		t.Fatalf("campaign leader: %v", err)
	}

	if !held || lease.Leader != "alpha-1" || lease.Term != 1 {
		t.Fatalf("unexpected campaign result: held=%t lease=%+v", held, lease)
	}
}

func TestBackendVerifyLeaderRejectsFollowerAndCloseWithoutResources(t *testing.T) {
	t.Parallel()

	t.Run("verify leader rejects follower", func(t *testing.T) {
		t.Parallel()

		backend := newFollowerBranchTestBackend(t, time.Date(2026, time.April, 15, 16, 30, 0, 0, time.UTC))
		if err := backend.verifyLeader(); !errors.Is(err, dcs.ErrNotLeader) {
			t.Fatalf("unexpected follower verifyLeader error: got %v want %v", err, dcs.ErrNotLeader)
		}
	})

	t.Run("close handles nil resources", func(t *testing.T) {
		t.Parallel()

		stopCh := make(chan struct{})
		doneCh := make(chan struct{})
		close(doneCh)

		backend := &Backend{
			watchers: newWatchBroker(stopCh),
			stopCh:   stopCh,
			doneCh:   doneCh,
		}

		if err := backend.Close(); err != nil {
			t.Fatalf("close nil-resource backend: %v", err)
		}

		if err := backend.Close(); err != nil {
			t.Fatalf("close nil-resource backend second time: %v", err)
		}
	})
}

func TestBackendWaitForLocalLeaderStopsOnCloseAndApplyRejectsFollower(t *testing.T) {
	t.Parallel()

	t.Run("wait for local leader stops on close", func(t *testing.T) {
		t.Parallel()

		backend := newFollowerBranchTestBackend(t, time.Date(2026, time.April, 15, 17, 0, 0, 0, time.UTC))
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = backend.Close()
		}()

		err := backend.waitForLocalLeader(context.Background())
		if !errors.Is(err, dcs.ErrBackendUnavailable) {
			t.Fatalf("unexpected waitForLocalLeader close error: got %v want %v", err, dcs.ErrBackendUnavailable)
		}
	})

	t.Run("apply rejects follower directly", func(t *testing.T) {
		t.Parallel()

		backend := newFollowerBranchTestBackend(t, time.Date(2026, time.April, 15, 17, 15, 0, 0, time.UTC))
		_, err := backend.apply(context.Background(), command{
			Type: commandSet,
			Key:  "/pacman/alpha/config",
			Now:  time.Date(2026, time.April, 15, 17, 15, 0, 0, time.UTC),
		})
		if !errors.Is(err, dcs.ErrNotLeader) {
			t.Fatalf("unexpected follower apply error: got %v want %v", err, dcs.ErrNotLeader)
		}
	})
}

func TestBackendConstructionAndWatchCloseBranches(t *testing.T) {
	t.Parallel()

	t.Run("new rejects invalid config", func(t *testing.T) {
		t.Parallel()

		if _, err := New(Config{}); err == nil {
			t.Fatal("expected invalid config error")
		}
	})

	t.Run("new rejects snapshot mkdir failure", func(t *testing.T) {
		t.Parallel()

		baseDir := filepath.Join(t.TempDir(), "raft")
		if err := os.Mkdir(baseDir, 0o755); err != nil {
			t.Fatalf("mkdir data dir: %v", err)
		}
		if err := os.Chmod(baseDir, 0o500); err != nil {
			t.Fatalf("chmod data dir: %v", err)
		}
		t.Cleanup(func() {
			_ = os.Chmod(baseDir, 0o755)
		})

		address := reserveTCPAddress(t)
		_, err := New(Config{
			ClusterName:        "alpha",
			TTL:                time.Second,
			RetryTimeout:       time.Second,
			DataDir:            baseDir,
			BindAddress:        address,
			Peers:              []string{address},
			ApplyTimeout:       time.Second,
			TransportTimeout:   time.Second,
			HeartbeatTimeout:   100 * time.Millisecond,
			ElectionTimeout:    100 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			ExpiryInterval:     25 * time.Millisecond,
			SnapshotRetention:  1,
		})
		if err == nil || !errors.Is(err, os.ErrPermission) && !strings.Contains(err.Error(), "create raft snapshot dir") {
			t.Fatalf("unexpected snapshot dir error: %v", err)
		}
	})

	t.Run("new rejects bolt store path that is a directory", func(t *testing.T) {
		t.Parallel()

		dataDir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dataDir, "snapshots"), 0o755); err != nil {
			t.Fatalf("mkdir snapshots: %v", err)
		}
		if err := os.Mkdir(filepath.Join(dataDir, "raft.db"), 0o755); err != nil {
			t.Fatalf("mkdir raft.db dir: %v", err)
		}

		address := reserveTCPAddress(t)
		_, err := New(Config{
			ClusterName:        "alpha",
			TTL:                time.Second,
			RetryTimeout:       time.Second,
			DataDir:            dataDir,
			BindAddress:        address,
			Peers:              []string{address},
			ApplyTimeout:       time.Second,
			TransportTimeout:   time.Second,
			HeartbeatTimeout:   100 * time.Millisecond,
			ElectionTimeout:    100 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			ExpiryInterval:     25 * time.Millisecond,
			SnapshotRetention:  1,
		})
		if err == nil || !strings.Contains(err.Error(), "open raft bolt store") {
			t.Fatalf("unexpected bolt store error: %v", err)
		}
	})

	t.Run("close all shuts active watchers down", func(t *testing.T) {
		t.Parallel()

		stopCh := make(chan struct{})
		broker := newWatchBroker(stopCh)
		first, err := broker.watch(context.Background(), "/pacman/alpha/")
		if err != nil {
			t.Fatalf("watch first: %v", err)
		}
		second, err := broker.watch(context.Background(), "/pacman/alpha/")
		if err != nil {
			t.Fatalf("watch second: %v", err)
		}

		broker.closeAll()
		assertRaftChannelClosed(t, first)
		assertRaftChannelClosed(t, second)
	})

	t.Run("checkAvailable returns context error", func(t *testing.T) {
		t.Parallel()

		backend := &Backend{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := backend.checkAvailable(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected checkAvailable error: got %v want %v", err, context.Canceled)
		}
	})
}

func newBranchTestBackend(t *testing.T, peers []string, bootstrap bool, now time.Time) *Backend {
	t.Helper()

	address := peers[0]
	backend, err := New(Config{
		ClusterName:        "alpha",
		TTL:                100 * time.Millisecond,
		RetryTimeout:       time.Second,
		DataDir:            t.TempDir(),
		BindAddress:        address,
		Peers:              peers,
		Bootstrap:          bootstrap,
		ExpiryInterval:     25 * time.Millisecond,
		HeartbeatTimeout:   75 * time.Millisecond,
		ElectionTimeout:    75 * time.Millisecond,
		LeaderLeaseTimeout: 75 * time.Millisecond,
		Now:                func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("create raft backend: %v", err)
	}
	t.Cleanup(func() {
		_ = backend.Close()
	})

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize raft backend: %v", err)
	}

	return backend
}

func newFollowerBranchTestBackend(t *testing.T, now time.Time) *Backend {
	t.Helper()

	self := reserveTCPAddress(t)
	other := reserveTCPAddress(t)
	return newBranchTestBackend(t, []string{self, other}, false, now)
}
