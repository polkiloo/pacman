package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestBinaryPathUsesBinDir(t *testing.T) {
	t.Parallel()

	got := binaryPath("/usr/lib/postgresql/17/bin", "pg_ctl")
	if got != "/usr/lib/postgresql/17/bin/pg_ctl" {
		t.Fatalf("unexpected binary path: got %q", got)
	}
}

func TestBinaryPathFallsBackToBinaryName(t *testing.T) {
	t.Parallel()

	if got := binaryPath("", "pg_ctl"); got != "pg_ctl" {
		t.Fatalf("unexpected binary path fallback: got %q", got)
	}
}

func TestWrapCommandErrorIncludesOutput(t *testing.T) {
	t.Parallel()

	err := wrapCommandError("start postgres", commandResult{output: "boom"}, errors.New("exit status 1"))
	if err == nil {
		t.Fatal("expected wrapped error")
	}

	message := err.Error()
	if !strings.Contains(message, "start postgres") || !strings.Contains(message, "boom") {
		t.Fatalf("unexpected wrapped error: %q", message)
	}
}

func TestExecuteCommandReturnsSuccessOutput(t *testing.T) {
	t.Parallel()

	result, err := executeCommand(context.Background(), "/bin/sh", "-lc", "printf ok")
	if err != nil {
		t.Fatalf("execute command: %v", err)
	}

	if result.output != "ok" {
		t.Fatalf("unexpected output: got %q", result.output)
	}

	if result.exitCode != 0 {
		t.Fatalf("unexpected exit code: got %d", result.exitCode)
	}
}

func TestExecuteCommandReturnsExitCodeOnFailure(t *testing.T) {
	t.Parallel()

	result, err := executeCommand(context.Background(), "/bin/sh", "-lc", "printf boom && exit 7")
	if err == nil {
		t.Fatal("expected command failure")
	}

	if result.output != "boom" {
		t.Fatalf("unexpected output: got %q", result.output)
	}

	if result.exitCode != 7 {
		t.Fatalf("unexpected exit code: got %d", result.exitCode)
	}
}
