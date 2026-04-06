package fxrun

import (
	"context"
	"errors"
	"log/slog"

	"go.uber.org/fx"
)

// RegisterCommand hooks a command runner into the Fx lifecycle and drives a
// clean application shutdown when the command finishes.
func RegisterCommand(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner, logger *slog.Logger, baseContext context.Context, run func(context.Context) error) {
	runContext, cancel := context.WithCancel(baseContext)
	done := make(chan struct{})

	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)

				err := run(runContext)
				if err != nil && !(errors.Is(err, context.Canceled) && runContext.Err() != nil) {
					if logger != nil {
						logger.Error("app run failed", slog.Any("error", err))
					}
					_ = shutdowner.Shutdown(fx.ExitCode(1))
					return
				}

				_ = shutdowner.Shutdown()
			}()

			return nil
		},
		OnStop: func(stopContext context.Context) error {
			cancel()

			select {
			case <-done:
				return nil
			case <-stopContext.Done():
				return stopContext.Err()
			}
		},
	})
}
