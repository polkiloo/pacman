package di

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"

	"go.uber.org/dig"
)

type resolvedBaseDeps struct {
	dig.In

	Args   []string  `name:"args"`
	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
	Logger *slog.Logger
}

func TestProvideBaseRegistersDependencies(t *testing.T) {
	t.Parallel()

	container := dig.New()
	args := []string{"-version"}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	if err := ProvideBase(container, "pacmand", args, stdout, stderr); err != nil {
		t.Fatalf("provide base: %v", err)
	}

	var resolved resolvedBaseDeps
	if err := container.Invoke(func(deps resolvedBaseDeps) {
		resolved = deps
	}); err != nil {
		t.Fatalf("resolve base dependencies: %v", err)
	}

	if got, want := resolved.Args, args; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected args: got %v, want %v", got, want)
	}

	if resolved.Stdout != stdout {
		t.Fatal("stdout dependency does not match registered writer")
	}

	if resolved.Stderr != stderr {
		t.Fatal("stderr dependency does not match registered writer")
	}

	resolved.Logger.Info("registered logger")
	if !strings.Contains(stderr.String(), `"service":"pacmand"`) {
		t.Fatalf("expected structured logger output, got %q", stderr.String())
	}
}

func TestProvideBaseReturnsArgsRegistrationError(t *testing.T) {
	t.Parallel()

	container := dig.New()
	mustProvide(t, container, func() []string { return []string{"existing"} }, dig.Name("args"))

	if err := ProvideBase(container, "pacmand", nil, io.Discard, io.Discard); err == nil {
		t.Fatal("expected args registration error")
	}
}

func TestProvideBaseReturnsStdoutRegistrationError(t *testing.T) {
	t.Parallel()

	container := dig.New()
	mustProvide(t, container, func() io.Writer { return io.Discard }, dig.Name("stdout"))

	if err := ProvideBase(container, "pacmand", nil, io.Discard, io.Discard); err == nil {
		t.Fatal("expected stdout registration error")
	}
}

func TestProvideBaseReturnsStderrRegistrationError(t *testing.T) {
	t.Parallel()

	container := dig.New()
	mustProvide(t, container, func() io.Writer { return io.Discard }, dig.Name("stderr"))

	if err := ProvideBase(container, "pacmand", nil, io.Discard, io.Discard); err == nil {
		t.Fatal("expected stderr registration error")
	}
}

func TestProvideBaseReturnsLoggerRegistrationError(t *testing.T) {
	t.Parallel()

	container := dig.New()
	mustProvide(t, container, func() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) })

	if err := ProvideBase(container, "pacmand", nil, io.Discard, io.Discard); err == nil {
		t.Fatal("expected logger registration error")
	}
}

func mustProvide(t *testing.T, container *dig.Container, constructor any, options ...dig.ProvideOption) {
	t.Helper()

	if err := container.Provide(constructor, options...); err != nil {
		t.Fatalf("provide dependency: %v", err)
	}
}
