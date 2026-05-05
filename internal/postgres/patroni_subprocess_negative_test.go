package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPatroniInspiredExecuteCommandMissingBinary(t *testing.T) {
	t.Parallel()

	result, err := executeCommand(context.Background(), "pacman-command-does-not-exist")
	if err == nil {
		t.Fatal("expected missing command to fail")
	}

	if result.exitCode != -1 {
		t.Fatalf("unexpected missing command exit code: got %d, want -1", result.exitCode)
	}
}

func TestPatroniInspiredExecutePassthroughCommandMissingBinary(t *testing.T) {
	t.Parallel()

	result, err := executePassthroughCommand(context.Background(), "pacman-command-does-not-exist")
	if err == nil {
		t.Fatal("expected missing passthrough command to fail")
	}

	if result.exitCode != -1 {
		t.Fatalf("unexpected missing passthrough command exit code: got %d, want -1", result.exitCode)
	}
}

func TestPatroniInspiredExecuteCommandPreCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := executeCommand(ctx, "sh", "-c", "true")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected pre-canceled command error: got %v, want %v", err, context.Canceled)
	}

	if result.exitCode != -1 {
		t.Fatalf("unexpected pre-canceled command exit code: got %d, want -1", result.exitCode)
	}
}

func TestPatroniInspiredExecutePassthroughCommandPreCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := executePassthroughCommand(ctx, "sh", "-c", "true")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected pre-canceled passthrough command error: got %v, want %v", err, context.Canceled)
	}

	if result.exitCode != -1 {
		t.Fatalf("unexpected pre-canceled passthrough command exit code: got %d, want -1", result.exitCode)
	}
}

func TestPatroniInspiredPGCtlStartCancelsLongRunningPgCtl(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := (PGCtl{
		BinDir:  "/tmp/bin",
		DataDir: "/tmp/pgdata",
		runner:  cancelingCommandRunner(cancel),
	}).Start(ctx)
	if err == nil {
		t.Fatal("expected canceled pg_ctl start to fail")
	}
	if ctx.Err() == nil {
		t.Fatalf("expected pg_ctl start context to be canceled, got error %v", err)
	}
	if !strings.Contains(err.Error(), "start postgres via pg_ctl") {
		t.Fatalf("unexpected pg_ctl start error: %v", err)
	}
}

func TestPatroniInspiredPGCtlStopCancelsLongRunningPgCtl(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := (PGCtl{
		BinDir:  "/tmp/bin",
		DataDir: "/tmp/pgdata",
		runner:  cancelingCommandRunner(cancel),
	}).Stop(ctx, ShutdownModeFast)
	if err == nil {
		t.Fatal("expected canceled pg_ctl stop to fail")
	}
	if ctx.Err() == nil {
		t.Fatalf("expected pg_ctl stop context to be canceled, got error %v", err)
	}
	if !strings.Contains(err.Error(), "stop postgres via pg_ctl") {
		t.Fatalf("unexpected pg_ctl stop error: %v", err)
	}
}

func TestPatroniInspiredPGRewindCancelsLongRunningRewind(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := (PGRewind{
		BinDir:       "/tmp/bin",
		DataDir:      "/tmp/pgdata",
		SourceServer: "host=primary",
		runner:       cancelingCommandRunner(cancel),
	}).Run(ctx)
	if err == nil {
		t.Fatal("expected canceled pg_rewind to fail")
	}
	if ctx.Err() == nil {
		t.Fatalf("expected pg_rewind context to be canceled, got error %v", err)
	}
	if !strings.Contains(err.Error(), "run pg_rewind") {
		t.Fatalf("unexpected pg_rewind error: %v", err)
	}
}

func cancelingCommandRunner(cancel context.CancelFunc) commandRunner {
	return func(ctx context.Context, _ string, _ ...string) (commandResult, error) {
		cancel()
		<-ctx.Done()
		return commandResult{exitCode: -1}, ctx.Err()
	}
}
