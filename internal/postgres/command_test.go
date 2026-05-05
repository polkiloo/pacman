package postgres

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestExecuteCommandCapturesOutputAndExitCode(t *testing.T) {
	t.Parallel()

	result, err := executeCommand(context.Background(), "sh", "-c", "printf 'ready'")
	if err != nil {
		t.Fatalf("execute command: %v", err)
	}

	if result.output != "ready" {
		t.Fatalf("unexpected command output: got %q, want %q", result.output, "ready")
	}

	if result.exitCode != 0 {
		t.Fatalf("unexpected command exit code: got %d, want %d", result.exitCode, 0)
	}
}

func TestExecuteCommandCapturesFailureExitCodeAndStderr(t *testing.T) {
	t.Parallel()

	result, err := executeCommand(context.Background(), "sh", "-c", "printf 'boom' >&2; exit 7")
	if err == nil {
		t.Fatal("expected execute command failure")
	}

	if result.exitCode != 7 {
		t.Fatalf("unexpected failed command exit code: got %d, want %d", result.exitCode, 7)
	}

	if !strings.Contains(result.output, "boom") {
		t.Fatalf("expected failed command output to include stderr, got %q", result.output)
	}
}

func TestExecuteCommandCancelsLongRunningProcess(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("shell sleep semantics differ on windows")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	result, err := executeCommand(ctx, "sh", "-c", "sleep 5")
	if err == nil {
		t.Fatal("expected canceled command to fail")
	}

	if ctx.Err() == nil {
		t.Fatalf("expected command context to be canceled, err=%v result=%+v", err, result)
	}

	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("expected command cancellation before sleep completed, elapsed=%s", elapsed)
	}

	if result.exitCode == 0 {
		t.Fatalf("expected non-zero exit code for canceled command, got %+v", result)
	}
}

func TestExecutePassthroughCommandReturnsWithoutWaitingForBackgroundChild(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("shell background process semantics differ on windows")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err := executePassthroughCommand(ctx, "sh", "-c", "sleep 1 &")
	if err != nil {
		t.Fatalf("execute passthrough command: %v", err)
	}

	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("passthrough command blocked for background child: elapsed=%s", elapsed)
	}
}

func TestExecutePassthroughCommandCancelsLongRunningProcess(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("shell sleep semantics differ on windows")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	result, err := executePassthroughCommand(ctx, "sh", "-c", "sleep 5")
	if err == nil {
		t.Fatal("expected canceled passthrough command to fail")
	}

	if ctx.Err() == nil {
		t.Fatalf("expected passthrough command context to be canceled, err=%v result=%+v", err, result)
	}

	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("expected passthrough command cancellation before sleep completed, elapsed=%s", elapsed)
	}

	if result.exitCode == 0 {
		t.Fatalf("expected non-zero exit code for canceled passthrough command, got %+v", result)
	}
}

func TestExecutePassthroughCommandCapturesFailureExitCode(t *testing.T) {
	t.Parallel()

	result, err := executePassthroughCommand(context.Background(), "sh", "-c", "exit 9")
	if err == nil {
		t.Fatal("expected passthrough command failure")
	}

	if result.exitCode != 9 {
		t.Fatalf("unexpected passthrough exit code: got %d, want %d", result.exitCode, 9)
	}
}
