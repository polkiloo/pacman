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
	agentOptions  []agent.Option
	apiTLSConfig  *tls.Config
	peerServerTLS *tls.Config
	peerClientTLS *tls.Config
}

const (
	runtimeModeProcess         = "process"
	runtimeModeEmbeddedWorker  = "embedded_worker"
	failureIsolationHelperProc = "helper_process"
	errorPropagationExitStatus = "structured_stderr_and_exit_status"
)

// Params defines pacmand constructor dependencies.
type Params struct {
	fx.In

	Stdout        io.Writer `name:"stdout"`
	Logger        *slog.Logger
	Options       commandOptions
	RuntimeConfig *runtimeConfig
	AgentOptions  []agent.Option `group:"agent.option"`
	APITLSConfig  *tls.Config    `name:"api_server_tls" optional:"true"`
	PeerServerTLS *tls.Config    `name:"member_peer_server_tls" optional:"true"`
	PeerClientTLS *tls.Config    `name:"member_peer_client_tls" optional:"true"`
}

// New constructs a pacmand application.
func New(params Params) *App {
	return &App{
		stdout:        params.Stdout,
		logger:        params.Logger,
		options:       params.Options,
		runtimeConfig: params.RuntimeConfig,
		agentOptions:  params.AgentOptions,
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
		a.logRuntimeError(ctx, a.runtimeConfig.Source, a.runtimeConfig.Path, a.runtimeConfig.Err)
		return a.runtimeConfig.Err
	}

	a.logLoadedConfig(ctx, a.runtimeConfig.Config, a.runtimeConfig.Source, a.runtimeConfig.Path)
	a.logRuntimeStart(ctx, a.runtimeConfig.Source)

	options := []agent.Option{
		agent.WithAPIServerTLSConfig(a.apiTLSConfig),
		agent.WithPeerServerTLSConfig(a.peerServerTLS),
		agent.WithPeerClientTLSConfig(a.peerClientTLS),
	}
	options = append(options, a.agentOptions...)

	err := localagent.Run(
		ctx,
		a.logger,
		a.runtimeConfig.Config,
		options...,
	)
	if err != nil {
		a.logRuntimeError(ctx, a.runtimeConfig.Source, a.runtimeConfig.Path, err)
		return err
	}

	return nil
}

func (a *App) logLoadedConfig(ctx context.Context, loadedConfig config.Config, source, path string) {
	redactedConfig := loadedConfig.Redacted()
	attributes := []slog.Attr{
		slog.String("component", "config"),
		slog.String("source", source),
		slog.String("node", redactedConfig.Node.Name),
		slog.String("node_role", redactedConfig.Node.Role.String()),
		slog.Bool("api_tls_enabled", redactedConfig.TLS != nil && redactedConfig.TLS.Enabled),
		slog.Bool("admin_auth_enabled", redactedConfig.Security.AdminAuthEnabled()),
		slog.Bool("member_mtls_enabled", redactedConfig.Security.PeerMTLSEnabled()),
	}
	attributes = append(attributes, runtimeModeAttrs(source)...)
	if path != "" {
		attributes = append(attributes, slog.String("path", path))
	}

	a.logger.LogAttrs(ctx, slog.LevelInfo, "loaded node configuration", attributes...)
}

func (a *App) logRuntimeStart(ctx context.Context, source string) {
	if runtimeModeForSource(source) != runtimeModeEmbeddedWorker {
		return
	}

	a.logger.LogAttrs(
		ctx,
		slog.LevelInfo,
		"starting embedded worker runtime",
		append(
			[]slog.Attr{slog.String("component", "embedded_worker")},
			runtimeModeAttrs(source)...,
		)...,
	)
}

func (a *App) logRuntimeError(ctx context.Context, source, path string, err error) {
	if a.logger == nil || err == nil || runtimeModeForSource(source) != runtimeModeEmbeddedWorker {
		return
	}

	attributes := []slog.Attr{
		slog.String("component", "runtime"),
		slog.String("source", source),
		slog.String("error", err.Error()),
	}
	attributes = append(attributes, runtimeModeAttrs(source)...)
	if path != "" {
		attributes = append(attributes, slog.String("path", path))
	}

	a.logger.LogAttrs(ctx, slog.LevelError, "embedded worker runtime failed", attributes...)
}

func runtimeModeForSource(source string) string {
	if source == "pgext-env" {
		return runtimeModeEmbeddedWorker
	}

	return runtimeModeProcess
}

func runtimeModeAttrs(source string) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("runtime_mode", runtimeModeForSource(source)),
	}

	if runtimeModeForSource(source) == runtimeModeEmbeddedWorker {
		attrs = append(
			attrs,
			slog.String("failure_isolation", failureIsolationHelperProc),
			slog.String("error_propagation", errorPropagationExitStatus),
		)
	}

	return attrs
}
