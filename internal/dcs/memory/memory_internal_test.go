package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
)

func TestNewAppliesDefaults(t *testing.T) {
	t.Parallel()

	backend := New(Config{}).(*store)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	if backend.ttl != DefaultTTL {
		t.Fatalf("unexpected default ttl: got %s, want %s", backend.ttl, DefaultTTL)
	}

	if backend.sweepInterval != DefaultSweepInterval {
		t.Fatalf("unexpected default sweep interval: got %s, want %s", backend.sweepInterval, DefaultSweepInterval)
	}
}

func TestClosedBackendRejectsOperations(t *testing.T) {
	t.Parallel()

	backend := New(Config{TTL: 50 * time.Millisecond}).(*store)
	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	if err := backend.Close(); err != nil {
		t.Fatalf("close backend twice: %v", err)
	}

	ctx := context.Background()

	if err := backend.Initialize(ctx); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("unexpected initialize error after close: got %v, want %v", err, dcs.ErrBackendUnavailable)
	}

	if _, err := backend.Get(ctx, "/pacman/alpha/config"); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("unexpected get error after close: got %v, want %v", err, dcs.ErrBackendUnavailable)
	}

	if err := backend.Set(ctx, "/pacman/alpha/config", []byte("value")); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("unexpected set error after close: got %v, want %v", err, dcs.ErrBackendUnavailable)
	}

	if _, err := backend.Watch(ctx, "/pacman/alpha/"); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("unexpected watch error after close: got %v, want %v", err, dcs.ErrBackendUnavailable)
	}
}

func TestCanceledContextReturnsError(t *testing.T) {
	t.Parallel()

	backend := New(Config{TTL: 50 * time.Millisecond}).(*store)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := backend.Get(ctx, "/pacman/alpha/config"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected get error: got %v, want %v", err, context.Canceled)
	}

	if err := backend.Touch(ctx, "alpha-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected touch error: got %v, want %v", err, context.Canceled)
	}
}

func TestCampaignRenewsExistingLeader(t *testing.T) {
	t.Parallel()

	backend := New(Config{TTL: 50 * time.Millisecond}).(*store)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	ctx := context.Background()

	initial, held, err := backend.Campaign(ctx, "alpha-1")
	if err != nil {
		t.Fatalf("initial campaign: %v", err)
	}

	if !held {
		t.Fatal("expected initial leader campaign to win")
	}

	time.Sleep(10 * time.Millisecond)

	renewed, held, err := backend.Campaign(ctx, "alpha-1")
	if err != nil {
		t.Fatalf("renew campaign: %v", err)
	}

	if !held {
		t.Fatal("expected renew campaign to hold the lease")
	}

	if renewed.Term != initial.Term {
		t.Fatalf("expected renew to preserve term: got %d, want %d", renewed.Term, initial.Term)
	}

	if !renewed.Renewed.After(initial.Renewed) {
		t.Fatalf("expected renew to advance renewed timestamp: initial=%s renewed=%s", initial.Renewed, renewed.Renewed)
	}

	if err := backend.Resign(ctx); err != nil {
		t.Fatalf("resign leader: %v", err)
	}

	if err := backend.Resign(ctx); !errors.Is(err, dcs.ErrNoLeader) {
		t.Fatalf("unexpected resign error without leader: got %v, want %v", err, dcs.ErrNoLeader)
	}
}

func TestWatchCancellationClosesChannel(t *testing.T) {
	t.Parallel()

	backend := New(Config{TTL: 50 * time.Millisecond}).(*store)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	events, err := backend.Watch(ctx, "/pacman/alpha/")
	if err != nil {
		t.Fatalf("watch prefix: %v", err)
	}

	cancel()

	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("expected canceled watch channel to close")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for canceled watch channel to close")
	}
}

func TestBroadcastDropsWhenWatchBufferIsFull(t *testing.T) {
	t.Parallel()

	backend := New(Config{TTL: 50 * time.Millisecond}).(*store)
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	ctx := context.Background()

	mismatchedEvents, err := backend.Watch(ctx, "/pacman/beta/")
	if err != nil {
		t.Fatalf("watch mismatched prefix: %v", err)
	}
	defer drainChannel(mismatchedEvents)

	events, err := backend.Watch(ctx, "/pacman/alpha/")
	if err != nil {
		t.Fatalf("watch matching prefix: %v", err)
	}
	defer drainChannel(events)

	for index := range watchBufferSize + 4 {
		key := "/pacman/alpha/status/node-" + string(rune('a'+index))
		if err := backend.Set(ctx, key, []byte("value")); err != nil {
			t.Fatalf("set buffered watch key %d: %v", index, err)
		}
	}
}

func drainChannel(ch <-chan dcs.WatchEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
