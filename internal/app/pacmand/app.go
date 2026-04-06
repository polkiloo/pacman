package pacmand

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/app/localagent"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/version"
)

var (
	errConfigPathRequired   = errors.New("pacmand config path is required")
	errConfigSourceConflict = errors.New("pacmand config path and postgres extension environment are mutually exclusive")
)

// App is the pacmand process entrypoint.
type App struct {
	stdout        io.Writer
	logger        *slog.Logger
	options       commandOptions
	runtimeConfig *runtimeConfig
	apiTLSConfig  *tls.Config
	peerServerTLS *tls.Config
	peerClientTLS *tls.Config
}

// Params defines pacmand constructor dependencies.
type Params struct {
	fx.In

	Stdout        io.Writer `name:"stdout"`
	Logger        *slog.Logger
	Options       commandOptions
	RuntimeConfig *runtimeConfig
	APITLSConfig  *tls.Config `name:"api_server_tls" optional:"true"`
	PeerServerTLS *tls.Config `name:"member_peer_server_tls" optional:"true"`
	PeerClientTLS *tls.Config `name:"member_peer_client_tls" optional:"true"`
}

// New constructs a pacmand application.
func New(params Params) *App {
	return &App{
		stdout:        params.Stdout,
		logger:        params.Logger,
		options:       params.Options,
		runtimeConfig: params.RuntimeConfig,
		apiTLSConfig:  params.APITLSConfig,
		peerServerTLS: params.PeerServerTLS,
		peerClientTLS: params.PeerClientTLS,
	}
}

// Run executes the prepared pacmand command plan.
func (a *App) Run(ctx context.Context) error {
	if a.options.ParseErr != nil {
		return a.options.ParseErr
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if a.options.ShowVersion {
		_, err := fmt.Fprintln(a.stdout, version.String())
		return err
	}

	if a.runtimeConfig == nil {
		return errConfigPathRequired
	}

	if a.runtimeConfig.Err != nil {
		return a.runtimeConfig.Err
	}

	a.logLoadedConfig(ctx, a.runtimeConfig.Config, a.runtimeConfig.Source, a.runtimeConfig.Path)

	return localagent.Run(
		ctx,
		a.logger,
		a.runtimeConfig.Config,
		agent.WithAPIServerTLSConfig(a.apiTLSConfig),
		agent.WithPeerServerTLSConfig(a.peerServerTLS),
		agent.WithPeerClientTLSConfig(a.peerClientTLS),
	)
}

func (a *App) logLoadedConfig(ctx context.Context, loadedConfig config.Config, source, path string) {
	attributes := []slog.Attr{
		slog.String("component", "config"),
		slog.String("source", source),
		slog.String("node", loadedConfig.Node.Name),
		slog.String("role", loadedConfig.Node.Role.String()),
		slog.Bool("api_tls_enabled", loadedConfig.TLS != nil && loadedConfig.TLS.Enabled),
		slog.Bool("admin_auth_enabled", loadedConfig.Security.AdminAuthEnabled()),
		slog.Bool("member_mtls_enabled", loadedConfig.Security.PeerMTLSEnabled()),
	}
	if path != "" {
		attributes = append(attributes, slog.String("path", path))
	}

	a.logger.LogAttrs(ctx, slog.LevelInfo, "loaded node configuration", attributes...)
}
