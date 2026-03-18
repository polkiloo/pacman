package pacmand

import (
	"bytes"
	"context"
	"strings"
	"testing"

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

func TestRunLogsScaffoldMessage(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: logging.New("pacmand", &logs),
	})

	if err := app.Run(context.Background(), nil); err != nil {
		t.Fatalf("run pacmand scaffold: %v", err)
	}

	if stdout.Len() != 0 {
		t.Fatalf("expected no stdout output, got %q", stdout.String())
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}

	assertContains(t, logs.String(), `"msg":"pacmand scaffold is not implemented yet"`)
	assertContains(t, logs.String(), `"service":"pacmand"`)
	assertContains(t, logs.String(), `"component":"daemon"`)
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
