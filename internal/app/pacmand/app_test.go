package pacmand

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/version"
)

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	if err := app.Run(context.Background(), []string{"-version"}); err != nil {
		t.Fatalf("run pacmand version: %v", err)
	}

	if got, want := stdout.String(), version.String()+"\n"; got != want {
		t.Fatalf("unexpected version output: got %q, want %q", got, want)
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}

	if logs.Len() != 0 {
		t.Fatalf("expected no logs for version output, got %q", logs.String())
	}
}

func TestRunRequiresConfigPath(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	err := app.Run(context.Background(), nil)
	if err == nil {
		t.Fatal("expected config path error")
	}

	if stdout.Len() != 0 {
		t.Fatalf("expected no stdout output, got %q", stdout.String())
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}

	if !errors.Is(err, errConfigPathRequired) {
		t.Fatalf("unexpected error: got %v, want %v", err, errConfigPathRequired)
	}

	if logs.Len() != 0 {
		t.Fatalf("expected no logs for missing config path, got %q", logs.String())
	}
}

func TestRunStartsLocalDaemonAndHeartbeatLoopWhenProvided(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
`

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := app.Run(ctx, []string{"-config", path}); err != nil {
		t.Fatalf("run pacmand with config: %v", err)
	}

	assertContains(t, logs.String(), `"msg":"loaded node configuration"`)
	assertContains(t, logs.String(), `"component":"config"`)
	assertContains(t, logs.String(), `"path":"`+path+`"`)
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"role":"data"`)
	assertContains(t, logs.String(), `"msg":"started local agent daemon"`)
	assertContains(t, logs.String(), `"component":"agent"`)
	assertContains(t, logs.String(), `"manages_postgres":true`)
	assertContains(t, logs.String(), `"msg":"observed PostgreSQL unavailability"`)
	assertContains(t, logs.String(), `"msg":"published local state to control plane"`)
	assertContains(t, logs.String(), `"postgres_up":false`)
}

func TestRunReturnsConfigLoadError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	err := app.Run(context.Background(), []string{"-config", filepath.Join(t.TempDir(), "missing.yaml")})
	if err == nil {
		t.Fatal("expected config load error")
	}

	assertContains(t, err.Error(), "open config file")
}

func TestRunReturnsDaemonConstructionError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
`

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	err := app.Run(context.Background(), []string{"-config", path})
	if err == nil {
		t.Fatal("expected daemon construction error")
	}

	assertContains(t, err.Error(), "construct local agent daemon")
	assertContains(t, err.Error(), "agent postgres config is required for data nodes")
}

func TestRunReturnsFlagError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	err := app.Run(context.Background(), []string{"-invalid"})
	if err == nil {
		t.Fatal("expected invalid flag error")
	}

	assertContains(t, err.Error(), "flag provided but not defined")
	assertContains(t, stderr.String(), "flag provided but not defined")
}

func TestRunReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
		Logger: logging.New("pacmand", &bytes.Buffer{}),
	})

	err := app.Run(ctx, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
