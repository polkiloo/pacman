package security

import (
	"crypto/tls"
	"fmt"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/config"
)

type memberMTLSParams struct {
	fx.In

	Config *config.Config `optional:"true"`
}

type memberMTLSResult struct {
	fx.Out

	ServerTLSConfig *tls.Config `name:"member_peer_server_tls"`
	ClientTLSConfig *tls.Config `name:"member_peer_client_tls"`
}

// MemberMTLSModule wires peer mTLS server and client TLS dependencies into the
// Fx graph for cluster-member traffic on control-plane listeners.
func MemberMTLSModule() fx.Option {
	return fx.Module(
		"security.member_mtls",
		fx.Provide(newMemberMTLSConfigs),
	)
}

func newMemberMTLSConfigs(params memberMTLSParams) (memberMTLSResult, error) {
	if params.Config == nil || !params.Config.Security.PeerMTLSEnabled() {
		return memberMTLSResult{}, nil
	}

	if params.Config.TLS == nil || !params.Config.TLS.Enabled {
		return memberMTLSResult{}, fmt.Errorf("load member peer tls config: %w", config.ErrSecurityMemberMTLSRequiresTLS)
	}

	serverTLSConfig, err := LoadServerTLSConfig(*params.Config.TLS, tls.RequireAndVerifyClientCert)
	if err != nil {
		return memberMTLSResult{}, fmt.Errorf("load member peer server tls config: %w", err)
	}

	clientTLSConfig, err := LoadClientTLSConfig(*params.Config.TLS)
	if err != nil {
		return memberMTLSResult{}, fmt.Errorf("load member peer client tls config: %w", err)
	}

	return memberMTLSResult{
		ServerTLSConfig: serverTLSConfig,
		ClientTLSConfig: clientTLSConfig,
	}, nil
}
