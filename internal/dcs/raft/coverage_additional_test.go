package raft

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/polkiloo/pacman/internal/dcs"
)

func TestConfigValidateRejectsAdditionalInvalidBranches(t *testing.T) {
	t.Parallel()

	newConfig := func() Config {
		return Config{
			ClusterName:  "alpha",
			TTL:          time.Second,
			RetryTimeout: time.Second,
			DataDir:      t.TempDir(),
			BindAddress:  "127.0.0.1:7100",
			Peers:        []string{"127.0.0.1:7100"},
		}.WithDefaults()
	}

	testCases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name: "apply timeout",
			mutate: func(config *Config) {
				config.ApplyTimeout = 0
			},
			want: "apply timeout",
		},
		{
			name: "transport timeout",
			mutate: func(config *Config) {
				config.TransportTimeout = 0
			},
			want: "transport timeout",
		},
		{
			name: "heartbeat timeout",
			mutate: func(config *Config) {
				config.HeartbeatTimeout = 0
			},
			want: "heartbeat timeout",
		},
		{
			name: "election timeout",
			mutate: func(config *Config) {
				config.ElectionTimeout = 0
			},
			want: "election timeout",
		},
		{
			name: "leader lease timeout",
			mutate: func(config *Config) {
				config.LeaderLeaseTimeout = 0
			},
			want: "leader lease timeout",
		},
		{
			name: "expiry interval",
			mutate: func(config *Config) {
				config.ExpiryInterval = 0
			},
			want: "expiry interval",
		},
		{
			name: "snapshot retention",
			mutate: func(config *Config) {
				config.SnapshotRetention = 0
			},
			want: "snapshot retention",
		},
		{
			name: "bind address missing from peers",
			mutate: func(config *Config) {
				config.Peers = []string{"127.0.0.1:7200"}
			},
			want: `peers must include bind address "127.0.0.1:7100"`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			config := newConfig()
			testCase.mutate(&config)

			err := config.Validate()
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("unexpected validation error: got %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestFSMApplyAdditionalBranches(t *testing.T) {
	t.Parallel()

	t.Run("decode and unknown command errors", func(t *testing.T) {
		t.Parallel()

		fsm := newFSM(newWatchBroker(make(chan struct{})))

		if response := fsm.Apply(&hraft.Log{Data: []byte("{")}); response == nil {
			t.Fatal("expected invalid json apply to fail")
		}

		response := applyFSMCommand(t, fsm, command{
			Type: commandType("mystery"),
			Now:  time.Date(2026, time.April, 14, 0, 0, 0, 0, time.UTC),
		})
		if err, ok := response.(error); !ok || !strings.Contains(err.Error(), `unknown raft command type "mystery"`) {
			t.Fatalf("unexpected unknown-command response: %#v", response)
		}
	})

	t.Run("set compare delete and expire branches", func(t *testing.T) {
		t.Parallel()

		stopCh := make(chan struct{})
		broker := newWatchBroker(stopCh)
		fsm := newFSM(broker)

		watchCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		events, err := broker.watch(watchCtx, "/pacman/alpha/")
		if err != nil {
			t.Fatalf("watch broker: %v", err)
		}

		now := time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC)
		key := "/pacman/alpha/config"
		fsm.state.Entries[key] = keyState{
			Key:       key,
			Value:     []byte("stale"),
			Revision:  7,
			TTL:       time.Second,
			ExpiresAt: now.Add(-time.Second),
		}

		if response := applyFSMCommand(t, fsm, command{
			Type:  commandSet,
			Key:   " " + key + " ",
			Value: []byte("fresh"),
			Now:   now,
		}); response != nil {
			t.Fatalf("expected set to succeed, got %#v", response)
		}

		putEvent := waitForRaftEvent(t, events)
		if putEvent.Type != dcs.EventPut || putEvent.Key != key || string(putEvent.Value) != "fresh" || putEvent.Revision != 1 {
			t.Fatalf("unexpected put event: %+v", putEvent)
		}

		current, ok := fsm.Get(key, now)
		if !ok || current.Revision != 1 || string(current.Value) != "fresh" {
			t.Fatalf("unexpected current value after reset set: ok=%t value=%+v", ok, current)
		}

		fsm.state.Entries[key] = keyState{
			Key:       key,
			Value:     []byte("expired"),
			Revision:  3,
			TTL:       time.Second,
			ExpiresAt: now.Add(-time.Second),
		}

		if response := applyFSMCommand(t, fsm, command{
			Type:             commandCompareAndSet,
			Key:              key,
			Value:            []byte("new"),
			ExpectedRevision: 3,
			Now:              now,
		}); !errors.Is(response.(error), dcs.ErrRevisionMismatch) {
			t.Fatalf("unexpected compare-and-set mismatch response: %#v", response)
		}

		if _, ok := fsm.state.Entries[key]; ok {
			t.Fatal("expected expired entry to be removed on compare-and-set mismatch")
		}

		if response := applyFSMCommand(t, fsm, command{
			Type: commandDelete,
			Key:  key,
			Now:  now,
		}); !errors.Is(response.(error), dcs.ErrKeyNotFound) {
			t.Fatalf("unexpected delete-missing response: %#v", response)
		}

		fsm.state.Entries[key] = keyState{
			Key:       key,
			Value:     []byte("ttl"),
			Revision:  4,
			TTL:       time.Second,
			ExpiresAt: now.Add(-time.Second),
		}

		if response := applyFSMCommand(t, fsm, command{
			Type:              commandExpireKey,
			Key:               key,
			ExpectedRevision:  99,
			ExpectedExpiresAt: now.Add(-time.Second),
			Now:               now,
		}); response != nil {
			t.Fatalf("expected mismatched expire-key to be ignored, got %#v", response)
		}

		if _, ok := fsm.state.Entries[key]; !ok {
			t.Fatal("expected mismatched expire-key to preserve entry")
		}

		if response := applyFSMCommand(t, fsm, command{
			Type:              commandExpireKey,
			Key:               key,
			ExpectedRevision:  4,
			ExpectedExpiresAt: now.Add(-time.Second),
			Now:               now,
		}); response != nil {
			t.Fatalf("expected matching expire-key to succeed, got %#v", response)
		}

		expiredEvent := waitForRaftEvent(t, events)
		if expiredEvent.Type != dcs.EventExpired || expiredEvent.Key != key || expiredEvent.Revision != 5 {
			t.Fatalf("unexpected expired event: %+v", expiredEvent)
		}
	})

	t.Run("campaign resign and session expiry branches", func(t *testing.T) {
		t.Parallel()

		fsm := newFSM(newWatchBroker(make(chan struct{})))
		now := time.Date(2026, time.April, 14, 11, 0, 0, 0, time.UTC)

		fsm.state.Leader = leaderState{Term: ^uint64(0)}
		response := applyFSMCommand(t, fsm, command{
			Type:      commandCampaign,
			Candidate: "alpha-1",
			TTL:       time.Second,
			Now:       now,
		})
		result, ok := response.(campaignResult)
		if !ok || !result.Held || result.Lease.Leader != "alpha-1" || result.Lease.Term != 1 {
			t.Fatalf("unexpected initial campaign result: %#v", response)
		}

		response = applyFSMCommand(t, fsm, command{
			Type:      commandCampaign,
			Candidate: "alpha-1",
			TTL:       2 * time.Second,
			Now:       now.Add(500 * time.Millisecond),
		})
		result, ok = response.(campaignResult)
		if !ok || !result.Held || result.Lease.Term != 1 || !result.Lease.ExpiresAt.Equal(now.Add(2500*time.Millisecond)) {
			t.Fatalf("unexpected renewal campaign result: %#v", response)
		}

		response = applyFSMCommand(t, fsm, command{
			Type:      commandCampaign,
			Candidate: "beta-1",
			TTL:       time.Second,
			Now:       now.Add(time.Second),
		})
		result, ok = response.(campaignResult)
		if !ok || result.Held || result.Lease.Leader != "alpha-1" {
			t.Fatalf("unexpected competing campaign result: %#v", response)
		}

		otherFSM := newFSM(newWatchBroker(make(chan struct{})))
		if response := applyFSMCommand(t, otherFSM, command{
			Type: commandResign,
			Now:  now,
		}); !errors.Is(response.(error), dcs.ErrNoLeader) {
			t.Fatalf("unexpected resign-without-leader response: %#v", response)
		}

		otherFSM.state.Leader = leaderState{
			Leader:    "alpha-1",
			Term:      7,
			Acquired:  now.Add(-2 * time.Second),
			Renewed:   now.Add(-1500 * time.Millisecond),
			ExpiresAt: now.Add(-time.Second),
		}
		if response := applyFSMCommand(t, otherFSM, command{
			Type: commandResign,
			Now:  now,
		}); response != nil {
			t.Fatalf("expected resign to clear expired leader record, got %#v", response)
		}
		if lease, ok := otherFSM.Leader(now); ok || lease.Leader != "" {
			t.Fatalf("expected expired leader record to be cleared, got ok=%t lease=%+v", ok, lease)
		}

		otherFSM.state.Sessions["alpha-1"] = sessionState{ExpiresAt: now.Add(-time.Second)}
		if response := applyFSMCommand(t, otherFSM, command{
			Type:              commandExpireSession,
			Member:            "alpha-1",
			ExpectedExpiresAt: now,
			Now:               now,
		}); response != nil {
			t.Fatalf("expected mismatched expire-session to be ignored, got %#v", response)
		}

		if !otherFSM.Alive("alpha-1", now.Add(-1500*time.Millisecond)) {
			t.Fatal("expected session to remain present before explicit expiry")
		}

		if response := applyFSMCommand(t, otherFSM, command{
			Type:              commandExpireSession,
			Member:            "alpha-1",
			ExpectedExpiresAt: now.Add(-time.Second),
			Now:               now,
		}); response != nil {
			t.Fatalf("expected matching expire-session to succeed, got %#v", response)
		}

		if otherFSM.Alive("alpha-1", now) {
			t.Fatal("expected expired session to be removed")
		}
	})

	t.Run("campaign lease usability helper", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, time.April, 14, 12, 0, 0, 0, time.UTC)
		backend := &Backend{
			config: Config{
				TTL:            120 * time.Millisecond,
				ExpiryInterval: 40 * time.Millisecond,
			},
		}

		if got := backend.minimumLeaseRemaining(); got != 30*time.Millisecond {
			t.Fatalf("unexpected minimum lease remaining: got %s want %s", got, 30*time.Millisecond)
		}

		if campaignLeaseUsable(dcs.LeaderLease{}, now, backend.minimumLeaseRemaining()) {
			t.Fatal("expected empty lease to be unusable")
		}

		if campaignLeaseUsable(dcs.LeaderLease{
			Leader:    "alpha-1",
			ExpiresAt: now.Add(20 * time.Millisecond),
		}, now, backend.minimumLeaseRemaining()) {
			t.Fatal("expected near-expiry lease to be unusable")
		}

		if !campaignLeaseUsable(dcs.LeaderLease{
			Leader:    "alpha-1",
			ExpiresAt: now.Add(50 * time.Millisecond),
		}, now, backend.minimumLeaseRemaining()) {
			t.Fatal("expected lease with sufficient remaining time to be usable")
		}
	})
}

func TestSnapshotReleaseAndWatchBrokerLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("snapshot release", func(t *testing.T) {
		t.Parallel()
		(&snapshot{}).Release()
	})

	t.Run("watch broker lifecycle", func(t *testing.T) {
		t.Parallel()

		stopCh := make(chan struct{})
		broker := newWatchBroker(stopCh)

		ctx, cancel := context.WithCancel(context.Background())
		ch, err := broker.watch(ctx, "/pacman/alpha/")
		if err != nil {
			t.Fatalf("watch broker: %v", err)
		}

		broker.broadcast(dcs.WatchEvent{Type: dcs.EventPut, Key: "/pacman/alpha/config", Value: []byte("ok")})
		if event := waitForRaftEvent(t, ch); event.Key != "/pacman/alpha/config" {
			t.Fatalf("unexpected watched event: %+v", event)
		}

		broker.broadcast(dcs.WatchEvent{Type: dcs.EventPut, Key: "/pacman/beta/config"})
		select {
		case event := <-ch:
			t.Fatalf("unexpected mismatched-prefix event: %+v", event)
		default:
		}

		cancel()
		assertRaftChannelClosed(t, ch)

		broker.remove(999)

		closedCtx, closedCancel := context.WithCancel(context.Background())
		defer closedCancel()
		otherCh, err := broker.watch(closedCtx, "/pacman/alpha/")
		if err != nil {
			t.Fatalf("watch broker second watch: %v", err)
		}

		close(stopCh)
		assertRaftChannelClosed(t, otherCh)

		broker.closeAll()
		broker.closeAll()

		if _, err := broker.watch(context.Background(), "/pacman/alpha/"); !errors.Is(err, dcs.ErrBackendUnavailable) {
			t.Fatalf("expected closed broker to reject watches, got %v", err)
		}
	})
}

func TestBackendBootstrapHelpersAndConstructionFailures(t *testing.T) {
	t.Parallel()

	t.Run("bootstrap configuration", func(t *testing.T) {
		t.Parallel()

		backend := &Backend{
			config: Config{
				BindAddress: "127.0.0.1:7100",
				Peers: []string{
					"127.0.0.1:7100",
					"127.0.0.1:7100",
					" ",
					"127.0.0.1:7200",
				},
			},
		}

		configuration := backend.bootstrapConfiguration()
		if len(configuration.Servers) != 2 {
			t.Fatalf("unexpected bootstrap server count: %+v", configuration.Servers)
		}

		backend.config.Peers = []string{"127.0.0.1:7100"}
		if !backend.isSingleNode() {
			t.Fatal("expected single-node helper to accept self-only peer set")
		}

		backend.config.Peers = []string{"127.0.0.1:7100", "127.0.0.1:7200"}
		if backend.isSingleNode() {
			t.Fatal("expected multi-peer helper to reject single-node status")
		}
	})

	t.Run("new rejects data dir file", func(t *testing.T) {
		t.Parallel()

		dataDir := filepath.Join(t.TempDir(), "raft.db")
		address := reserveTCPAddress(t)
		if err := os.WriteFile(dataDir, []byte("file"), 0o644); err != nil {
			t.Fatalf("seed file data dir: %v", err)
		}

		_, err := New(Config{
			ClusterName:  "alpha",
			TTL:          time.Second,
			RetryTimeout: time.Second,
			DataDir:      dataDir,
			BindAddress:  address,
			Peers:        []string{address},
		}.WithDefaults())
		if err == nil || !strings.Contains(err.Error(), "create raft data dir") {
			t.Fatalf("unexpected new error for file data dir: %v", err)
		}
	})

	t.Run("tcp layer tls cloning helpers", func(t *testing.T) {
		t.Parallel()

		serverTLS := &tls.Config{MinVersion: tls.VersionTLS12}
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "db.example"}

		layer, err := newTCPStreamLayer("127.0.0.1:0", serverTLS, clientTLS)
		if err != nil {
			t.Fatalf("new tcp stream layer: %v", err)
		}
		t.Cleanup(func() {
			if err := layer.Close(); err != nil {
				t.Fatalf("close stream layer: %v", err)
			}
		})

		if layer.clientTLS == nil || layer.clientTLS == clientTLS || layer.clientTLS.ServerName != "db.example" {
			t.Fatalf("unexpected client tls clone: %+v", layer.clientTLS)
		}

		cloned := cloneTLSConfig(clientTLS)
		if cloned == nil || cloned == clientTLS || cloned.ServerName != clientTLS.ServerName {
			t.Fatalf("unexpected cloned tls config: %+v", cloned)
		}

		if cloneTLSConfig(nil) != nil {
			t.Fatal("expected nil tls clone for nil input")
		}
	})
}

func applyFSMCommand(t *testing.T, fsm *fsm, cmd command) interface{} {
	t.Helper()

	payload, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal raft command: %v", err)
	}

	return fsm.Apply(&hraft.Log{Data: payload})
}

func waitForRaftEvent(t *testing.T, events <-chan dcs.WatchEvent) dcs.WatchEvent {
	t.Helper()

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("watch channel closed before event")
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for raft watch event")
		return dcs.WatchEvent{}
	}
}

func assertRaftChannelClosed(t *testing.T, events <-chan dcs.WatchEvent) {
	t.Helper()

	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("expected watch channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch channel close")
	}
}
