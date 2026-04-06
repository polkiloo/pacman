package fxrun

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
)

func TestRunReturnsNilAfterSuccessfulCommand(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	app := newCommandApp(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(context.Context) error {
		called.Store(true)
		return nil
	})

	if err := Run(context.Background(), app); err != nil {
		t.Fatalf("run app: %v", err)
	}

	if !called.Load() {
		t.Fatal("expected registered command to run")
	}
}

func TestRunReturnsExitCodeErrorAfterCommandFailure(t *testing.T) {
	t.Parallel()

	app := newCommandApp(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(context.Context) error {
		return errors.New("boom")
	})

	err := Run(context.Background(), app)
	if err == nil {
		t.Fatal("expected run error")
	}

	if !strings.Contains(err.Error(), "application exited with code 1") {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestRunStopsCleanlyWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	app := newCommandApp(ctx, nil, func(runContext context.Context) error {
		close(started)
		<-runContext.Done()
		return runContext.Err()
	})

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, app)
	}()

	// Wait for the command goroutine to signal it has started before
	// triggering the shutdown; avoids relying on a fixed sleep duration.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for command goroutine to start")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run app after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for app shutdown")
	}
}

func newCommandApp(ctx context.Context, logger *slog.Logger, run func(context.Context) error) *fx.App {
	return fx.New(
		fx.NopLogger,
		fx.Provide(func() context.Context { return ctx }),
		fx.Provide(func() *slog.Logger { return logger }),
		fx.Invoke(func(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner, injectedLogger *slog.Logger, baseContext context.Context) {
			RegisterCommand(lifecycle, shutdowner, injectedLogger, baseContext, run)
		}),
	)
}
