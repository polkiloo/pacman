package di

import (
	"io"
	"log/slog"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/logging"
)

type baseDependencies struct {
	fx.Out

	Args   []string  `name:"args"`
	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
}

// ProvideBase registers process-scoped dependencies shared by command entrypoints.
func ProvideBase(processName string, args []string, stdout, stderr io.Writer) fx.Option {
	return fx.Options(
		fx.Provide(func() baseDependencies {
			return baseDependencies{
				Args:   args,
				Stdout: stdout,
				Stderr: stderr,
			}
		}),
		fx.Provide(func() *slog.Logger {
			return logging.New(processName, stderr)
		}),
	)
}
