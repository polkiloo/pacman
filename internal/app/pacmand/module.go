package pacmand

import (
	"context"
	"io"
	"log/slog"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/di"
	"github.com/polkiloo/pacman/internal/fxrun"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/observability"
	"github.com/polkiloo/pacman/internal/security"
)

// Module wires the pacmand command graph, including argument parsing and
// runtime config loading, into a single Fx module.
func Module(processName string, args []string, stdout, stderr io.Writer) fx.Option {
	return fx.Module(
		"pacmand",
		di.ProvideBase(args, stdout, stderr),
		logging.Module(processName),
		observability.Module(),
		ConfigModule(),
		security.TLSModule(),
		security.MemberMTLSModule(),
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
}

func registerRunner(params runnerParams) {
	fxrun.RegisterCommand(params.Lifecycle, params.Shutdowner, params.Logger, params.Context, params.App.Run)
}
