package logging

import (
	"io"
	"log/slog"

	"go.uber.org/fx"
)

type moduleParams struct {
	fx.In

	Stderr      io.Writer    `name:"stderr"`
	Middlewares []Middleware `group:"logging.middleware"`
}

// Module provides the process logger into the Fx graph using the registered
// stderr stream as the sink.
func Module(service string) fx.Option {
	return fx.Module(
		"logging",
		fx.Provide(func(params moduleParams) *slog.Logger {
			return applyMiddleware(New(service, params.Stderr), params.Middlewares)
		}),
	)
}
