package pacmanctl

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/fxrun"
	"github.com/polkiloo/pacman/internal/version"
)

func TestModuleBuildsCommandGraph(t *testing.T) {
	t.Parallel()

	type resolved struct {
		fx.In

		App    *App
		Logger *slog.Logger
		Args   []string `name:"args"`
	}

	var deps resolved
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() context.Context { return context.Background() }),
		Module("pacmanctl", []string{"-version"}, &bytes.Buffer{}, &bytes.Buffer{}),
		fx.Populate(&deps),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build pacmanctl fx app: %v", err)
	}

	if deps.App == nil {
		t.Fatal("expected pacmanctl app to be populated")
	}

	if deps.Logger == nil {
		t.Fatal("expected logger to be populated")
	}

	if len(deps.Args) != 1 || deps.Args[0] != "-version" {
		t.Fatalf("unexpected args: got %v", deps.Args)
	}
}

func TestModuleRunnerExecutesCommand(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	ctx := context.Background()
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() context.Context { return ctx }),
		Module("pacmanctl", []string{"-version"}, &stdout, &stderr),
	)

	if err := fxrun.Run(ctx, app); err != nil {
		t.Fatalf("run pacmanctl fx app: %v", err)
	}

	if got, want := stdout.String(), version.String()+"\n"; got != want {
		t.Fatalf("unexpected version output: got %q, want %q", got, want)
	}
}
