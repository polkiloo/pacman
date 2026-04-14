package dcstest

import (
	"context"
	"errors"
	"fmt"
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

	if err := validateConfig(config); err != nil {
		t.Fatal(err)
	}

	t.Run("GetSetDelete", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		if err := exerciseGetSetDelete(context.Background(), backend, "/pacman/alpha/config"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("CompareAndSet", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		if err := exerciseCompareAndSet(context.Background(), backend, "/pacman/alpha/config"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ListPrefix", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		if err := exerciseListPrefix(context.Background(), backend); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("CampaignLeaderResign", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		if err := exerciseCampaignLeaderResign(context.Background(), backend, config.TTL); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("TouchAliveTTL", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		if err := exerciseTouchAliveTTL(context.Background(), backend, config.TTL); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("WatchEventDelivery", func(t *testing.T) {
		t.Parallel()

		backend := newBackend(t, config)
		if err := exerciseWatchEventDelivery(context.Background(), backend, config.TTL); err != nil {
			t.Fatal(err)
		}
	})
}

func validateConfig(config Config) error {
	if config.New == nil {
		return errors.New("dcstest config requires a backend constructor")
	}

	if config.TTL <= 0 {
		return errors.New("dcstest config requires a positive ttl")
	}

	return nil
}

func exerciseGetSetDelete(ctx context.Context, backend dcs.DCS, key string) error {
	if err := backend.Set(ctx, key, []byte("value")); err != nil {
		return fmt.Errorf("set key: %w", err)
	}

	got, err := backend.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get key: %w", err)
	}

	if got.Key != key {
		return fmt.Errorf("unexpected key: got %q, want %q", got.Key, key)
	}

	if string(got.Value) != "value" {
		return fmt.Errorf("unexpected value: got %q, want %q", string(got.Value), "value")
	}

	if got.Revision != 1 {
		return fmt.Errorf("unexpected revision: got %d, want %d", got.Revision, 1)
	}

	if got.TTL != 0 {
		return fmt.Errorf("unexpected ttl: got %s, want %s", got.TTL, time.Duration(0))
	}

	if err := backend.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	_, err = backend.Get(ctx, key)
	if !errors.Is(err, dcs.ErrKeyNotFound) {
		return fmt.Errorf("unexpected get-after-delete error: got %w, want %w", err, dcs.ErrKeyNotFound)
	}

	err = backend.Delete(ctx, key)
	if !errors.Is(err, dcs.ErrKeyNotFound) {
		return fmt.Errorf("unexpected delete-missing error: got %w, want %w", err, dcs.ErrKeyNotFound)
	}

	return nil
}

func exerciseCompareAndSet(ctx context.Context, backend dcs.DCS, key string) error {
	if err := backend.Set(ctx, key, []byte("v1")); err != nil {
		return fmt.Errorf("seed key: %w", err)
	}

	current, err := backend.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get seeded key: %w", err)
	}

	if err := backend.CompareAndSet(ctx, key, []byte("v2"), current.Revision); err != nil {
		return fmt.Errorf("compare-and-set success: %w", err)
	}

	updated, err := backend.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get updated key: %w", err)
	}

	if string(updated.Value) != "v2" {
		return fmt.Errorf("unexpected updated value: got %q, want %q", string(updated.Value), "v2")
	}

	if updated.Revision != current.Revision+1 {
		return fmt.Errorf("unexpected updated revision: got %d, want %d", updated.Revision, current.Revision+1)
	}

	err = backend.CompareAndSet(ctx, key, []byte("stale"), current.Revision)
	if !errors.Is(err, dcs.ErrRevisionMismatch) {
		return fmt.Errorf("unexpected stale revision error: got %w, want %w", err, dcs.ErrRevisionMismatch)
	}

	return nil
}

func exerciseListPrefix(ctx context.Context, backend dcs.DCS) error {
	if err := backend.Set(ctx, "/pacman/alpha/members/alpha-2", []byte("second")); err != nil {
		return fmt.Errorf("seed alpha-2: %w", err)
	}

	if err := backend.Set(ctx, "/pacman/alpha/status/alpha-1", []byte("status")); err != nil {
		return fmt.Errorf("seed status key: %w", err)
	}

	if err := backend.Set(ctx, "/pacman/alpha/members/alpha-1", []byte("first")); err != nil {
		return fmt.Errorf("seed alpha-1: %w", err)
	}

	listed, err := backend.List(ctx, "/pacman/alpha/members/")
	if err != nil {
		return fmt.Errorf("list members: %w", err)
	}

	if len(listed) != 2 {
		return fmt.Errorf("unexpected listed entry count: got %d, want %d", len(listed), 2)
	}

	gotKeys := []string{listed[0].Key, listed[1].Key}
	wantKeys := []string{
		"/pacman/alpha/members/alpha-1",
		"/pacman/alpha/members/alpha-2",
	}
	if !slices.Equal(gotKeys, wantKeys) {
		return fmt.Errorf("unexpected listed keys: got %v, want %v", gotKeys, wantKeys)
	}

	listed[0].Value[0] = 'X'
	stored, err := backend.Get(ctx, "/pacman/alpha/members/alpha-1")
	if err != nil {
		return fmt.Errorf("get listed key: %w", err)
	}

	if string(stored.Value) != "first" {
		return fmt.Errorf("expected list results to be detached copies, got %q", string(stored.Value))
	}

	return nil
}

func exerciseCampaignLeaderResign(ctx context.Context, backend dcs.DCS, ttl time.Duration) error {
	_, ok, err := backend.Leader(ctx)
	if err != nil {
		return fmt.Errorf("initial leader lookup: %w", err)
	}

	if ok {
		return errors.New("expected no leader before campaigning")
	}

	lease, held, err := backend.Campaign(ctx, "alpha-1")
	if err != nil {
		return fmt.Errorf("campaign leader: %w", err)
	}

	if !held {
		return errors.New("expected first candidate to hold leader lease")
	}

	if lease.Leader != "alpha-1" {
		return fmt.Errorf("unexpected leader after campaign: got %q, want %q", lease.Leader, "alpha-1")
	}

	if lease.Term != 1 {
		return fmt.Errorf("unexpected initial term: got %d, want %d", lease.Term, 1)
	}

	current, ok, err := backend.Leader(ctx)
	if err != nil {
		return fmt.Errorf("leader after campaign: %w", err)
	}

	if !ok || current.Leader != "alpha-1" {
		return fmt.Errorf("unexpected leader state after campaign: got %+v, ok=%t", current, ok)
	}

	losingLease, held, err := backend.Campaign(ctx, "beta-1")
	if err != nil {
		return fmt.Errorf("campaign competing leader: %w", err)
	}

	if held {
		return errors.New("expected competing candidate to lose active lease")
	}

	if losingLease.Leader != "alpha-1" {
		return fmt.Errorf("unexpected competing lease view: got %q, want %q", losingLease.Leader, "alpha-1")
	}

	time.Sleep(expiryWait(ttl))

	lease, held, err = backend.Campaign(ctx, "beta-1")
	if err != nil {
		return fmt.Errorf("campaign after lease expiry: %w", err)
	}

	if !held {
		return errors.New("expected candidate to win after lease expiry")
	}

	if lease.Leader != "beta-1" {
		return fmt.Errorf("unexpected leader after expiry: got %q, want %q", lease.Leader, "beta-1")
	}

	if lease.Term != 2 {
		return fmt.Errorf("unexpected incremented term: got %d, want %d", lease.Term, 2)
	}

	if err := backend.Resign(ctx); err != nil {
		return fmt.Errorf("resign leader: %w", err)
	}

	_, ok, err = backend.Leader(ctx)
	if err != nil {
		return fmt.Errorf("leader after resign: %w", err)
	}

	if ok {
		return errors.New("expected no leader after resign")
	}

	return nil
}

func exerciseTouchAliveTTL(ctx context.Context, backend dcs.DCS, ttl time.Duration) error {
	alive, err := backend.Alive(ctx, "alpha-1")
	if err != nil {
		return fmt.Errorf("alive before touch: %w", err)
	}

	if alive {
		return errors.New("expected untouched member to be reported dead")
	}

	if err := backend.Touch(ctx, "alpha-1"); err != nil {
		return fmt.Errorf("touch member: %w", err)
	}

	alive, err = backend.Alive(ctx, "alpha-1")
	if err != nil {
		return fmt.Errorf("alive after touch: %w", err)
	}

	if !alive {
		return errors.New("expected touched member to be reported alive")
	}

	deadline := time.Now().Add(4 * ttl)
	for {
		alive, err = backend.Alive(ctx, "alpha-1")
		if err != nil {
			return fmt.Errorf("alive during expiry wait: %w", err)
		}

		if !alive {
			break
		}

		if time.Now().After(deadline) {
			return errors.New("expected touched member session to expire")
		}

		time.Sleep(ttl / 4)
	}

	return nil
}

func exerciseWatchEventDelivery(ctx context.Context, backend dcs.DCS, ttl time.Duration) error {
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	events, err := backend.Watch(watchCtx, "/pacman/alpha/status/")
	if err != nil {
		return fmt.Errorf("watch status prefix: %w", err)
	}

	if err := backend.Set(ctx, "/pacman/alpha/status/alpha-1", []byte("up")); err != nil {
		return fmt.Errorf("set watched key: %w", err)
	}

	putEvent, err := waitForEventResult(events, 4*ttl)
	if err != nil {
		return err
	}

	if putEvent.Type != dcs.EventPut || putEvent.Key != "/pacman/alpha/status/alpha-1" || string(putEvent.Value) != "up" {
		return fmt.Errorf("unexpected put event: %+v", putEvent)
	}

	if err := backend.Delete(ctx, "/pacman/alpha/status/alpha-1"); err != nil {
		return fmt.Errorf("delete watched key: %w", err)
	}

	deleteEvent, err := waitForEventResult(events, 4*ttl)
	if err != nil {
		return err
	}

	if deleteEvent.Type != dcs.EventDelete || deleteEvent.Key != "/pacman/alpha/status/alpha-1" {
		return fmt.Errorf("unexpected delete event: %+v", deleteEvent)
	}

	if err := backend.Set(ctx, "/pacman/alpha/status/alpha-2", []byte("ttl"), dcs.WithTTL(ttl)); err != nil {
		return fmt.Errorf("set ttl watched key: %w", err)
	}

	ttlPutEvent, err := waitForEventResult(events, 4*ttl)
	if err != nil {
		return err
	}

	if ttlPutEvent.Type != dcs.EventPut || ttlPutEvent.Key != "/pacman/alpha/status/alpha-2" {
		return fmt.Errorf("unexpected ttl put event: %+v", ttlPutEvent)
	}

	expiredEvent, err := waitForEventResult(events, 6*ttl)
	if err != nil {
		return err
	}

	if expiredEvent.Type != dcs.EventExpired || expiredEvent.Key != "/pacman/alpha/status/alpha-2" {
		return fmt.Errorf("unexpected expired event: %+v", expiredEvent)
	}

	return nil
}

func newBackend(t *testing.T, config Config) dcs.DCS {
	t.Helper()

	backend, err := openBackend(t, config)
	if err != nil {
		t.Fatalf("initialize backend: %v", err)
	}

	return backend
}

func openBackend(t *testing.T, config Config) (dcs.DCS, error) {
	t.Helper()

	backend := config.New(t)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	if err := backend.Initialize(context.Background()); err != nil {
		return nil, err
	}

	return backend, nil
}

func waitForEvent(t *testing.T, events <-chan dcs.WatchEvent, timeout time.Duration) dcs.WatchEvent {
	t.Helper()

	event, err := waitForEventResult(events, timeout)
	if err != nil {
		t.Fatal(err)
	}

	return event
}

func waitForEventResult(events <-chan dcs.WatchEvent, timeout time.Duration) (dcs.WatchEvent, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case event, ok := <-events:
		if !ok {
			return dcs.WatchEvent{}, errors.New("watch channel closed before delivering expected event")
		}

		return event, nil
	case <-timer.C:
		return dcs.WatchEvent{}, fmt.Errorf("timed out waiting for watch event after %s", timeout)
	}
}

func expiryWait(ttl time.Duration) time.Duration {
	return ttl + ttl/2
}
