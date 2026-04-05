package security

import (
	"crypto/tls"
	"fmt"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/config"
)

type apiServerTLSParams struct {
	fx.In

	Config *config.Config `optional:"true"`
}

type apiServerTLSResult struct {
	fx.Out

	TLSConfig *tls.Config `name:"api_server_tls"`
}

// TLSModule wires TLS-related runtime dependencies into the Fx graph.
func TLSModule() fx.Option {
	return fx.Module(
		"security.tls",
		fx.Provide(newAPIServerTLSConfig),
	)
}

func newAPIServerTLSConfig(params apiServerTLSParams) (apiServerTLSResult, error) {
	if params.Config == nil || params.Config.TLS == nil {
		return apiServerTLSResult{}, nil
	}

	tlsConfig, err := LoadServerTLSConfig(*params.Config.TLS, tls.NoClientCert)
	if err != nil {
		return apiServerTLSResult{}, fmt.Errorf("load api server tls config: %w", err)
	}

	return apiServerTLSResult{TLSConfig: tlsConfig}, nil
}
