package fxrun

import (
	"context"
	"errors"
	"log/slog"

	"go.uber.org/fx"
)

// isExpectedShutdown returns true when err is a context cancellation caused by
// the runner's own context being cancelled — i.e. the app is shutting down
// cleanly and the error is not a real failure.
func isExpectedShutdown(err error, runCtx context.Context) bool {
	return errors.Is(err, context.Canceled) && runCtx.Err() != nil
}

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
				if err != nil && !isExpectedShutdown(err, runContext) {
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
