package logging

import (
	"log/slog"

	"go.uber.org/fx"
)

// Middleware decorates the process logger before it is exposed to the Fx graph.
type Middleware func(*slog.Logger) *slog.Logger

// ProvideMiddleware registers a logger decorator in the Fx logging middleware
// chain. Middlewares are applied in registration order.
func ProvideMiddleware(middleware Middleware) fx.Option {
	return fx.Provide(
		fx.Annotate(
			func() Middleware {
				return middleware
			},
			fx.ResultTags(`group:"logging.middleware"`),
		),
	)
}

// WithAttrs returns a middleware that enriches the logger with static attrs.
func WithAttrs(attrs ...slog.Attr) Middleware {
	return func(logger *slog.Logger) *slog.Logger {
		if logger == nil || len(attrs) == 0 {
			return logger
		}

		args := make([]any, 0, len(attrs))
		for _, attr := range attrs {
			args = append(args, attr)
		}

		return logger.With(args...)
	}
}

func applyMiddleware(logger *slog.Logger, middlewares []Middleware) *slog.Logger {
	current := logger
	for _, middleware := range middlewares {
		if middleware == nil {
			continue
		}

		current = middleware(current)
	}

	return current
}
