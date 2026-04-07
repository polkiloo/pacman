package di

import (
	"bytes"
	"io"
	"testing"

	"go.uber.org/fx"
)

type resolvedBaseDeps struct {
	fx.In

	Args   []string  `name:"args"`
	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
}

func TestProvideBaseRegistersDependencies(t *testing.T) {
	t.Parallel()

	args := []string{"-version"}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	var resolved resolvedBaseDeps

	app := fx.New(
		fx.NopLogger,
		ProvideBase(args, stdout, stderr),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
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
}

func TestProvideBaseReturnsArgsRegistrationError(t *testing.T) {
	t.Parallel()

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() duplicateArgsOut {
			return duplicateArgsOut{Args: []string{"existing"}}
		}),
		ProvideBase(nil, io.Discard, io.Discard),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected args registration error")
	}
}

func TestProvideBaseReturnsStdoutRegistrationError(t *testing.T) {
	t.Parallel()

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() duplicateStdoutOut {
			return duplicateStdoutOut{Stdout: io.Discard}
		}),
		ProvideBase(nil, io.Discard, io.Discard),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected stdout registration error")
	}
}

func TestProvideBaseReturnsStderrRegistrationError(t *testing.T) {
	t.Parallel()

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() duplicateStderrOut {
			return duplicateStderrOut{Stderr: io.Discard}
		}),
		ProvideBase(nil, io.Discard, io.Discard),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected stderr registration error")
	}
}

type duplicateArgsOut struct {
	fx.Out

	Args []string `name:"args"`
}

type duplicateStdoutOut struct {
	fx.Out

	Stdout io.Writer `name:"stdout"`
}

type duplicateStderrOut struct {
	fx.Out

	Stderr io.Writer `name:"stderr"`
}
