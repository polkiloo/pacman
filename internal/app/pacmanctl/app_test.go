package pacmanctl

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/internal/version"
)

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-version"}); err != nil {
		t.Fatalf("run pacmanctl version: %v", err)
	}

	if got, want := stdout.String(), version.String()+"\n"; got != want {
		t.Fatalf("unexpected version output: got %q, want %q", got, want)
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}
}

func TestRunWithoutCommandPrintsScaffold(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), nil); err != nil {
		t.Fatalf("run pacmanctl scaffold: %v", err)
	}

	if got, want := stdout.String(), "pacmanctl scaffold: CLI commands are not implemented yet\n"; got != want {
		t.Fatalf("unexpected scaffold output: got %q, want %q", got, want)
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}
}

func TestRunWithCommandPrintsScaffold(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"cluster", "status"}); err != nil {
		t.Fatalf("run pacmanctl command scaffold: %v", err)
	}

	if got, want := stdout.String(), "pacmanctl scaffold: command support is not implemented yet (cluster status)\n"; got != want {
		t.Fatalf("unexpected command scaffold output: got %q, want %q", got, want)
	}
}

func TestRunReturnsFlagError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
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
	})

	err := app.Run(ctx, nil)
	if err != context.Canceled {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
