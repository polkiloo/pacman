package dcstest

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/memory"
)

func TestRunWithMemoryBackend(t *testing.T) {
	t.Parallel()

	const ttl = 60 * time.Millisecond

	Run(t, Config{
		New: func(*testing.T) dcs.DCS {
			return memory.New(memory.Config{
				TTL:           ttl,
				SweepInterval: 5 * time.Millisecond,
			})
		},
		TTL: ttl,
	})
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	runFatalHelper(t, "missing_constructor")
	runFatalHelper(t, "non_positive_ttl")
}

func TestNewBackendInitializesAndCloses(t *testing.T) {
	t.Parallel()

	backend := &trackingDCS{}
	t.Run("success", func(t *testing.T) {
		got := newBackend(t, Config{
			TTL: time.Second,
			New: func(*testing.T) dcs.DCS { return backend },
		})
		if got != backend {
			t.Fatalf("unexpected backend instance: got %T want %T", got, backend)
		}
	})

	if !backend.initialized || !backend.closed {
		t.Fatalf("expected backend lifecycle hooks, got initialized=%t closed=%t", backend.initialized, backend.closed)
	}

	runFatalHelper(t, "initialize_failure")
}

func TestWaitForEventFailures(t *testing.T) {
	t.Parallel()

	runFatalHelper(t, "wait_closed")
	runFatalHelper(t, "wait_timeout")
}

func TestConformanceFatalPathsHelper(t *testing.T) {
	caseName := os.Getenv("PACMAN_DCSTEST_HELPER_CASE")
	if caseName == "" {
		return
	}

	switch caseName {
	case "missing_constructor":
		Run(t, Config{TTL: time.Second})
	case "non_positive_ttl":
		Run(t, Config{
			New: func(*testing.T) dcs.DCS { return &trackingDCS{} },
		})
	case "initialize_failure":
		newBackend(t, Config{
			TTL: time.Second,
			New: func(*testing.T) dcs.DCS {
				return &trackingDCS{initializeErr: errors.New("boom")}
			},
		})
	case "wait_closed":
		events := make(chan dcs.WatchEvent)
		close(events)
		waitForEvent(t, events, 10*time.Millisecond)
	case "wait_timeout":
		waitForEvent(t, make(chan dcs.WatchEvent), 10*time.Millisecond)
	default:
		t.Fatalf("unknown helper case %q", caseName)
	}
}

func runFatalHelper(t *testing.T, caseName string) {
	t.Helper()

	command := exec.Command(os.Args[0], "-test.run=^TestConformanceFatalPathsHelper$")
	command.Env = append(os.Environ(), "PACMAN_DCSTEST_HELPER_CASE="+caseName)

	err := command.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected helper case %q to fail test process, got %v", caseName, err)
	}
}

type trackingDCS struct {
	initialized   bool
	closed        bool
	initializeErr error
}

func (backend *trackingDCS) Get(context.Context, string) (dcs.KeyValue, error) {
	return dcs.KeyValue{}, dcs.ErrKeyNotFound
}

func (backend *trackingDCS) Set(context.Context, string, []byte, ...dcs.SetOption) error {
	return nil
}

func (backend *trackingDCS) CompareAndSet(context.Context, string, []byte, int64) error {
	return nil
}

func (backend *trackingDCS) Delete(context.Context, string) error {
	return nil
}

func (backend *trackingDCS) List(context.Context, string) ([]dcs.KeyValue, error) {
	return nil, nil
}

func (backend *trackingDCS) Campaign(context.Context, string) (dcs.LeaderLease, bool, error) {
	return dcs.LeaderLease{}, false, nil
}

func (backend *trackingDCS) Leader(context.Context) (dcs.LeaderLease, bool, error) {
	return dcs.LeaderLease{}, false, nil
}

func (backend *trackingDCS) Resign(context.Context) error {
	return nil
}

func (backend *trackingDCS) Touch(context.Context, string) error {
	return nil
}

func (backend *trackingDCS) Alive(context.Context, string) (bool, error) {
	return false, nil
}

func (backend *trackingDCS) Watch(context.Context, string) (<-chan dcs.WatchEvent, error) {
	events := make(chan dcs.WatchEvent)
	close(events)
	return events, nil
}

func (backend *trackingDCS) Initialize(context.Context) error {
	backend.initialized = true
	return backend.initializeErr
}

func (backend *trackingDCS) Close() error {
	backend.closed = true
	return nil
}
