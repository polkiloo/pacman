package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"

	pacmandcmd "github.com/polkiloo/pacman/internal/app/pacmand"
	"github.com/polkiloo/pacman/internal/fxrun"
)

func TestRunReturnsErrorWhenConfigPathIsMissing(t *testing.T) {
	exitCode, stdout, stderr := runWithCapturedIO(t, context.Background(), nil)

	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	if stdout != "" {
		t.Fatalf("expected no stdout output, got %q", stdout)
	}

	assertContains(t, stderr, "pacmand config path is required")
	assertContains(t, stderr, `"msg":"app run failed"`)
}

func TestRunReturnsSuccessForConfiguredDaemonStartup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
  apiAddress: "%s"
postgres:
  dataDir: /var/lib/postgresql/data
`, reserveLoopbackAddress(t))

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	exitCode, stdout, stderr := runWithCapturedIO(t, ctx, []string{"-config", path})

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if stdout != "" {
		t.Fatalf("expected no stdout output, got %q", stdout)
	}

	assertContains(t, stderr, `"msg":"started local agent daemon"`)
	assertContains(t, stderr, `"msg":"observed PostgreSQL unavailability"`)
	assertContains(t, stderr, `"msg":"published local state to control plane"`)
	assertContains(t, stderr, `"postgres_up":false`)
}

func TestRunReturnsErrorForInvalidFlag(t *testing.T) {
	exitCode, _, stderr := runWithCapturedIO(t, context.Background(), []string{"-invalid"})

	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	assertContains(t, stderr, "flag provided but not defined")
	assertContains(t, stderr, `"msg":"app run failed"`)
}

func runWithCapturedIO(t *testing.T, ctx context.Context, args []string) (int, string, string) {
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

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() context.Context { return ctx }),
		pacmandcmd.Module(processName, os.Args[1:], os.Stdout, os.Stderr),
	)
	exitCode := 0
	if err := fxrun.Run(ctx, app); err != nil {
		exitCode = 1
	}

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

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}

	return address
}
