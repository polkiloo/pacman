package dcstest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
)

// Config defines how the shared DCS conformance suite constructs a backend and
// how long TTL-driven assertions should wait.
type Config struct {
	New func(*testing.T) dcs.DCS
	TTL time.Duration
}

// Run executes the shared DCS conformance suite against a backend
// implementation.
func Run(t *testing.T, config Config) {
	t.Helper()

	if config.New == nil {
		t.Fatal("dcstest config requires a backend constructor")
	}

	if config.TTL <= 0 {
		t.Fatal("dcstest config requires a positive ttl")
	}

	t.Run("GetSetDelete", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		ctx := context.Background()
		key := "/pacman/alpha/config"

		if err := backend.Set(ctx, key, []byte("value")); err != nil {
			t.Fatalf("set key: %v", err)
		}

		got, err := backend.Get(ctx, key)
		if err != nil {
			t.Fatalf("get key: %v", err)
		}

		if got.Key != key {
			t.Fatalf("unexpected key: got %q, want %q", got.Key, key)
		}

		if string(got.Value) != "value" {
			t.Fatalf("unexpected value: got %q, want %q", string(got.Value), "value")
		}

		if got.Revision != 1 {
			t.Fatalf("unexpected revision: got %d, want %d", got.Revision, 1)
		}

		if got.TTL != 0 {
			t.Fatalf("unexpected ttl: got %s, want %s", got.TTL, time.Duration(0))
		}

		if err := backend.Delete(ctx, key); err != nil {
			t.Fatalf("delete key: %v", err)
		}

		_, err = backend.Get(ctx, key)
		if !errors.Is(err, dcs.ErrKeyNotFound) {
			t.Fatalf("unexpected get-after-delete error: got %v, want %v", err, dcs.ErrKeyNotFound)
		}

		err = backend.Delete(ctx, key)
		if !errors.Is(err, dcs.ErrKeyNotFound) {
			t.Fatalf("unexpected delete-missing error: got %v, want %v", err, dcs.ErrKeyNotFound)
		}
	})

	t.Run("CompareAndSet", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		ctx := context.Background()
		key := "/pacman/alpha/config"

		if err := backend.Set(ctx, key, []byte("v1")); err != nil {
			t.Fatalf("seed key: %v", err)
		}

		current, err := backend.Get(ctx, key)
		if err != nil {
			t.Fatalf("get seeded key: %v", err)
		}

		if err := backend.CompareAndSet(ctx, key, []byte("v2"), current.Revision); err != nil {
			t.Fatalf("compare-and-set success: %v", err)
		}

		updated, err := backend.Get(ctx, key)
		if err != nil {
			t.Fatalf("get updated key: %v", err)
		}

		if string(updated.Value) != "v2" {
			t.Fatalf("unexpected updated value: got %q, want %q", string(updated.Value), "v2")
		}

		if updated.Revision != current.Revision+1 {
			t.Fatalf("unexpected updated revision: got %d, want %d", updated.Revision, current.Revision+1)
		}

		err = backend.CompareAndSet(ctx, key, []byte("stale"), current.Revision)
		if !errors.Is(err, dcs.ErrRevisionMismatch) {
			t.Fatalf("unexpected stale revision error: got %v, want %v", err, dcs.ErrRevisionMismatch)
		}
	})

	t.Run("ListPrefix", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		ctx := context.Background()

		if err := backend.Set(ctx, "/pacman/alpha/members/alpha-2", []byte("second")); err != nil {
			t.Fatalf("seed alpha-2: %v", err)
		}

		if err := backend.Set(ctx, "/pacman/alpha/status/alpha-1", []byte("status")); err != nil {
			t.Fatalf("seed status key: %v", err)
		}

		if err := backend.Set(ctx, "/pacman/alpha/members/alpha-1", []byte("first")); err != nil {
			t.Fatalf("seed alpha-1: %v", err)
		}

		listed, err := backend.List(ctx, "/pacman/alpha/members/")
		if err != nil {
			t.Fatalf("list members: %v", err)
		}

		if len(listed) != 2 {
			t.Fatalf("unexpected listed entry count: got %d, want %d", len(listed), 2)
		}

		gotKeys := []string{listed[0].Key, listed[1].Key}
		wantKeys := []string{
			"/pacman/alpha/members/alpha-1",
			"/pacman/alpha/members/alpha-2",
		}
		if !slices.Equal(gotKeys, wantKeys) {
			t.Fatalf("unexpected listed keys: got %v, want %v", gotKeys, wantKeys)
		}

		listed[0].Value[0] = 'X'
		stored, err := backend.Get(ctx, "/pacman/alpha/members/alpha-1")
		if err != nil {
			t.Fatalf("get listed key: %v", err)
		}

		if string(stored.Value) != "first" {
			t.Fatalf("expected list results to be detached copies, got %q", string(stored.Value))
		}
	})

	t.Run("CampaignLeaderResign", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		ctx := context.Background()

		_, ok, err := backend.Leader(ctx)
		if err != nil {
			t.Fatalf("initial leader lookup: %v", err)
		}

		if ok {
			t.Fatal("expected no leader before campaigning")
		}

		lease, held, err := backend.Campaign(ctx, "alpha-1")
		if err != nil {
			t.Fatalf("campaign leader: %v", err)
		}

		if !held {
			t.Fatal("expected first candidate to hold leader lease")
		}

		if lease.Leader != "alpha-1" {
			t.Fatalf("unexpected leader after campaign: got %q, want %q", lease.Leader, "alpha-1")
		}

		if lease.Term != 1 {
			t.Fatalf("unexpected initial term: got %d, want %d", lease.Term, 1)
		}

		current, ok, err := backend.Leader(ctx)
		if err != nil {
			t.Fatalf("leader after campaign: %v", err)
		}

		if !ok || current.Leader != "alpha-1" {
			t.Fatalf("unexpected leader state after campaign: got %+v, ok=%t", current, ok)
		}

		losingLease, held, err := backend.Campaign(ctx, "beta-1")
		if err != nil {
			t.Fatalf("campaign competing leader: %v", err)
		}

		if held {
			t.Fatal("expected competing candidate to lose active lease")
		}

		if losingLease.Leader != "alpha-1" {
			t.Fatalf("unexpected competing lease view: got %q, want %q", losingLease.Leader, "alpha-1")
		}

		time.Sleep(expiryWait(config.TTL))

		lease, held, err = backend.Campaign(ctx, "beta-1")
		if err != nil {
			t.Fatalf("campaign after lease expiry: %v", err)
		}

		if !held {
			t.Fatal("expected candidate to win after lease expiry")
		}

		if lease.Leader != "beta-1" {
			t.Fatalf("unexpected leader after expiry: got %q, want %q", lease.Leader, "beta-1")
		}

		if lease.Term != 2 {
			t.Fatalf("unexpected incremented term: got %d, want %d", lease.Term, 2)
		}

		if err := backend.Resign(ctx); err != nil {
			t.Fatalf("resign leader: %v", err)
		}

		_, ok, err = backend.Leader(ctx)
		if err != nil {
			t.Fatalf("leader after resign: %v", err)
		}

		if ok {
			t.Fatal("expected no leader after resign")
		}
	})

	t.Run("TouchAliveTTL", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		ctx := context.Background()

		alive, err := backend.Alive(ctx, "alpha-1")
		if err != nil {
			t.Fatalf("alive before touch: %v", err)
		}

		if alive {
			t.Fatal("expected untouched member to be reported dead")
		}

		if err := backend.Touch(ctx, "alpha-1"); err != nil {
			t.Fatalf("touch member: %v", err)
		}

		alive, err = backend.Alive(ctx, "alpha-1")
		if err != nil {
			t.Fatalf("alive after touch: %v", err)
		}

		if !alive {
			t.Fatal("expected touched member to be reported alive")
		}

		deadline := time.Now().Add(4 * config.TTL)
		for {
			alive, err = backend.Alive(ctx, "alpha-1")
			if err != nil {
				t.Fatalf("alive during expiry wait: %v", err)
			}

			if !alive {
				break
			}

			if time.Now().After(deadline) {
				t.Fatal("expected touched member session to expire")
			}

			time.Sleep(config.TTL / 4)
		}
	})

	t.Run("WatchEventDelivery", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		ctx := context.Background()
		watchCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		events, err := backend.Watch(watchCtx, "/pacman/alpha/status/")
		if err != nil {
			t.Fatalf("watch status prefix: %v", err)
		}

		if err := backend.Set(ctx, "/pacman/alpha/status/alpha-1", []byte("up")); err != nil {
			t.Fatalf("set watched key: %v", err)
		}

		putEvent := waitForEvent(t, events, 4*config.TTL)
		if putEvent.Type != dcs.EventPut || putEvent.Key != "/pacman/alpha/status/alpha-1" || string(putEvent.Value) != "up" {
			t.Fatalf("unexpected put event: %+v", putEvent)
		}

		if err := backend.Delete(ctx, "/pacman/alpha/status/alpha-1"); err != nil {
			t.Fatalf("delete watched key: %v", err)
		}

		deleteEvent := waitForEvent(t, events, 4*config.TTL)
		if deleteEvent.Type != dcs.EventDelete || deleteEvent.Key != "/pacman/alpha/status/alpha-1" {
			t.Fatalf("unexpected delete event: %+v", deleteEvent)
		}

		if err := backend.Set(ctx, "/pacman/alpha/status/alpha-2", []byte("ttl"), dcs.WithTTL(config.TTL)); err != nil {
			t.Fatalf("set ttl watched key: %v", err)
		}

		ttlPutEvent := waitForEvent(t, events, 4*config.TTL)
		if ttlPutEvent.Type != dcs.EventPut || ttlPutEvent.Key != "/pacman/alpha/status/alpha-2" {
			t.Fatalf("unexpected ttl put event: %+v", ttlPutEvent)
		}

		expiredEvent := waitForEvent(t, events, 6*config.TTL)
		if expiredEvent.Type != dcs.EventExpired || expiredEvent.Key != "/pacman/alpha/status/alpha-2" {
			t.Fatalf("unexpected expired event: %+v", expiredEvent)
		}
	})
}

func newBackend(t *testing.T, config Config) dcs.DCS {
	t.Helper()

	backend := config.New(t)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize backend: %v", err)
	}

	return backend
}

func waitForEvent(t *testing.T, events <-chan dcs.WatchEvent, timeout time.Duration) dcs.WatchEvent {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("watch channel closed before delivering expected event")
		}

		return event
	case <-timer.C:
		t.Fatalf("timed out waiting for watch event after %s", timeout)
		return dcs.WatchEvent{}
	}
}

func expiryWait(ttl time.Duration) time.Duration {
	return ttl + ttl/2
}
