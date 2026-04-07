package pacmanctl

import (
	"context"
	"io"
	"log/slog"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/di"
	"github.com/polkiloo/pacman/internal/fxrun"
	"github.com/polkiloo/pacman/internal/logging"
)

// Module wires the pacmanctl command graph and lifecycle runner into Fx.
func Module(processName string, args []string, stdout, stderr io.Writer) fx.Option {
	return fx.Module(
		"pacmanctl",
		di.ProvideBase(args, stdout, stderr),
		logging.Module(processName),
		fx.Provide(New),
		fx.Invoke(registerRunner),
	)
}

type runnerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner
	Logger     *slog.Logger
	Context    context.Context
	App        *App
	Args       []string `name:"args"`
}

func registerRunner(params runnerParams) {
	fxrun.RegisterCommand(params.Lifecycle, params.Shutdowner, params.Logger, params.Context, func(ctx context.Context) error {
		return params.App.Run(ctx, params.Args)
	})
}
