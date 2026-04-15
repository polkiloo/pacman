package observability

import (
	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/agent"
)

// Module contributes the Prometheus exporter as an HTTP API middleware through
// the Fx graph.
func Module() fx.Option {
	return fx.Module(
		"observability",
		fx.Provide(
			fx.Annotate(
				func() agent.Option {
					return agent.WithHTTPAPIMiddlewareFactory(PrometheusExporterMiddleware)
				},
				fx.ResultTags(`group:"agent.option"`),
			),
		),
	)
}
