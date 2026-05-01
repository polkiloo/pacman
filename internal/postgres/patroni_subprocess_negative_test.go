package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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
	skipWindowsShell(t)

	binDir := t.TempDir()
	writeExecutable(t, binDir, "pg_ctl", "#!/bin/sh\nsleep 5\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := (PGCtl{BinDir: binDir, DataDir: "/tmp/pgdata"}).Start(ctx)
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
	skipWindowsShell(t)

	binDir := t.TempDir()
	writeExecutable(t, binDir, "pg_ctl", "#!/bin/sh\nsleep 5\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := (PGCtl{BinDir: binDir, DataDir: "/tmp/pgdata"}).Stop(ctx, ShutdownModeFast)
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
	skipWindowsShell(t)

	binDir := t.TempDir()
	writeExecutable(t, binDir, "pg_rewind", "#!/bin/sh\nsleep 5\n")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := (PGRewind{
		BinDir:       binDir,
		DataDir:      "/tmp/pgdata",
		SourceServer: "host=primary",
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

func skipWindowsShell(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("shell script execution differs on windows")
	}
}

func writeExecutable(t *testing.T, dir, name, body string) {
	t.Helper()

	path := filepath.Join(dir, name)
	temp, err := os.CreateTemp(dir, "."+name+".tmp-*")
	if err != nil {
		t.Fatalf("create temporary executable for %q: %v", path, err)
	}
	tempPath := temp.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()

	if _, err := temp.WriteString(body); err != nil {
		_ = temp.Close()
		t.Fatalf("write temporary executable %q: %v", tempPath, err)
	}
	if err := temp.Close(); err != nil {
		t.Fatalf("close temporary executable %q: %v", tempPath, err)
	}
	if err := os.Chmod(tempPath, 0o755); err != nil {
		t.Fatalf("chmod temporary executable %q: %v", tempPath, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		t.Fatalf("publish executable %q: %v", path, err)
	}
}
