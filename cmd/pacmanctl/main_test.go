package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestRunReturnsSuccessForScaffold(t *testing.T) {
	exitCode, stdout, stderr := runWithCapturedIO(t, nil)

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if got, want := stdout, "pacmanctl scaffold: CLI commands are not implemented yet\n"; got != want {
		t.Fatalf("unexpected stdout output: got %q, want %q", got, want)
	}

	if stderr != "" {
		t.Fatalf("expected no stderr output, got %q", stderr)
	}
}

func TestRunReturnsErrorForInvalidFlag(t *testing.T) {
	exitCode, _, stderr := runWithCapturedIO(t, []string{"-invalid"})

	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	assertContains(t, stderr, "flag provided but not defined")
	assertContains(t, stderr, `"msg":"app run failed"`)
}

func runWithCapturedIO(t *testing.T, args []string) (int, string, string) {
	t.Helper()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}

	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stderr pipe: %v", err)
	}

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	os.Args = append([]string{processName}, args...)
	os.Stdout = stdoutW
	os.Stderr = stderrW

	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	exitCode := run()

	if err := stdoutW.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}

	if err := stderrW.Close(); err != nil {
		t.Fatalf("close stderr writer: %v", err)
	}

	stdoutBytes, err := io.ReadAll(stdoutR)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}

	stderrBytes, err := io.ReadAll(stderrR)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}

	if err := stdoutR.Close(); err != nil {
		t.Fatalf("close stdout reader: %v", err)
	}

	if err := stderrR.Close(); err != nil {
		t.Fatalf("close stderr reader: %v", err)
	}

	return exitCode, string(stdoutBytes), string(stderrBytes)
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
