package agent

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/peerapi"
	"github.com/polkiloo/pacman/internal/postgres"
)

const (
	defaultHeartbeatInterval    = 1 * time.Second
	defaultPostgresProbeTimeout = 500 * time.Millisecond
	defaultPeerProbeTimeout     = 1 * time.Second
)

// Daemon runs the node-local PACMAN agent.
type Daemon struct {
	config              config.Config
	logger              *slog.Logger
	now                 func() time.Time
	heartbeatInterval   time.Duration
	postgresProbe       postgresAvailabilityProbe
	postgresStateProbe  postgresStateProbe
	statePublisher      controlplane.NodeStatePublisher
	apiServer           httpServer
	apiTLSConfig        *tls.Config
	apiAuthorizer       httpapi.Authorizer
	apiServerDisabled   bool
	peerServer          httpServer
	peerServerTLSConfig *tls.Config
	peerClientTLSConfig *tls.Config
	probeTimeout        time.Duration
	peerProbeTimeout    time.Duration
	startedFlag         atomic.Bool

	mu         sync.RWMutex
	started    agentmodel.Startup
	heartbeat  agentmodel.Heartbeat
	nodeStatus agentmodel.NodeStatus
	loopWG     sync.WaitGroup
}

type httpServer interface {
	Start(context.Context, string) error
	Wait() error
}

// NewDaemon constructs a local PACMAN daemon from the validated node config.
func NewDaemon(cfg config.Config, logger *slog.Logger, options ...Option) (*Daemon, error) {
	if logger == nil {
		return nil, ErrLoggerRequired
	}

	defaulted := cfg.WithDefaults()
	if err := defaulted.Validate(); err != nil {
		return nil, fmt.Errorf("validate agent config: %w", err)
	}

	if defaulted.Node.Role.HasLocalPostgres() && defaulted.Postgres == nil {
		return nil, ErrPostgresConfigRequired
	}

	store := controlplane.NewMemoryStateStore()
	apiAuthorizer, err := buildAPIAuthorizer(defaulted)
	if err != nil {
		return nil, err
	}

	daemon := &Daemon{
		config:             defaulted,
		logger:             logger,
		now:                time.Now,
		heartbeatInterval:  defaultHeartbeatInterval,
		postgresProbe:      dialPostgresAvailability,
		postgresStateProbe: postgres.QueryObservation,
		statePublisher:     store,
		apiAuthorizer:      apiAuthorizer,
		probeTimeout:       defaultPostgresProbeTimeout,
		peerProbeTimeout:   defaultPeerProbeTimeout,
	}

	for _, option := range options {
		if option != nil {
			option(daemon)
		}
	}

	if !daemon.apiServerDisabled && defaulted.TLS != nil && defaulted.TLS.Enabled && daemon.apiTLSConfig == nil {
		return nil, ErrAPIServerTLSRequired
	}

	if defaulted.Security.PeerMTLSEnabled() {
		if daemon.peerServerTLSConfig == nil {
			return nil, ErrPeerServerTLSRequired
		}

		if daemon.peerClientTLSConfig == nil {
			return nil, ErrPeerClientTLSRequired
		}

		if daemon.peerServer == nil {
			daemon.peerServer = peerapi.New(defaulted.Node.Name, logger, peerapi.Config{
				TLSConfig:    daemon.peerServerTLSConfig,
				AllowedPeers: memberPeerSubjects(defaulted),
			})
		}
	}

	if !daemon.apiServerDisabled && daemon.apiServer == nil {
		daemon.apiServer = httpapi.New(defaulted.Node.Name, store, logger, httpapi.Config{
			TLSConfig:  daemon.apiTLSConfig,
			Authorizer: daemon.apiAuthorizer,
		})
	}

	return daemon, nil
}

func (daemon *Daemon) logArgs(component string, args ...any) []any {
	combined := []any{
		slog.String("component", component),
		slog.String("node", daemon.config.Node.Name),
		slog.String("node_role", daemon.config.Node.Role.String()),
	}

	return append(combined, args...)
}

func buildAPIAuthorizer(cfg config.Config) (httpapi.Authorizer, error) {
	if cfg.Security == nil || !cfg.Security.AdminAuthEnabled() {
		return nil, nil
	}

	token, err := cfg.Security.ResolveAdminBearerToken(nil)
	if err != nil {
		return nil, fmt.Errorf("resolve api admin bearer token: %w", err)
	}

	return httpapi.NewAdminBearerTokenAuthorizer(token), nil
}

// Start records daemon startup state and emits the first lifecycle log entry.
func (daemon *Daemon) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer func() {
		if daemon.Startup().StartedAt.IsZero() {
			cancel()
		}
	}()

	startup, err := daemon.beginStart()
	if err != nil {
		return err
	}

	if err := daemon.bootstrapClusterSpec(runCtx); err != nil {
		cancel()
		daemon.rollbackStart()
		return fmt.Errorf("store bootstrap cluster spec: %w", err)
	}

	if err := daemon.startAPIServer(runCtx); err != nil {
		cancel()
		daemon.rollbackStart()
		return fmt.Errorf("start http api server: %w", err)
	}

	if err := daemon.startPeerServer(runCtx); err != nil {
		cancel()
		daemon.rollbackStart()
		return fmt.Errorf("start peer api server: %w", err)
	}

	daemon.logStartup(ctx, startup)
	daemon.registerMember(ctx, startup)
	daemon.recordHeartbeat(ctx)
	daemon.loopWG.Add(1)
	go daemon.runHeartbeatLoop(runCtx)
	go daemon.probeSeedPeers(runCtx)

	return nil
}

func (daemon *Daemon) beginStart() (agentmodel.Startup, error) {
	if !daemon.startedFlag.CompareAndSwap(false, true) {
		return agentmodel.Startup{}, ErrDaemonAlreadyStarted
	}

	startup := agentmodel.Startup{
		NodeName:        daemon.config.Node.Name,
		NodeRole:        daemon.config.Node.Role,
		APIAddress:      daemon.config.Node.APIAddress,
		ControlAddress:  daemon.config.Node.ControlAddress,
		ManagesPostgres: daemon.config.Node.Role.HasLocalPostgres(),
		StartedAt:       daemon.now().UTC(),
	}

	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	daemon.started = startup
	return startup, nil
}

func (daemon *Daemon) rollbackStart() {
	daemon.startedFlag.Store(false)

	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	daemon.started = agentmodel.Startup{}
}

func (daemon *Daemon) logStartup(ctx context.Context, startup agentmodel.Startup) {
	daemon.logger.InfoContext(
		ctx,
		"started local agent daemon",
		daemon.logArgs(
			"agent",
			slog.Bool("manages_postgres", startup.ManagesPostgres),
			slog.Bool("api_tls_enabled", daemon.config.TLS != nil && daemon.config.TLS.Enabled),
			slog.Bool("member_mtls_enabled", daemon.config.Security.PeerMTLSEnabled()),
			slog.String("api_address", startup.APIAddress),
			slog.String("control_address", startup.ControlAddress),
		)...,
	)
}

func (daemon *Daemon) registerMember(ctx context.Context, startup agentmodel.Startup) {
	registrar, ok := daemon.statePublisher.(controlplane.MemberRegistrar)
	if !ok {
		return
	}

	registration := controlplane.MemberRegistration{
		NodeName:       startup.NodeName,
		NodeRole:       startup.NodeRole,
		APIAddress:     startup.APIAddress,
		ControlAddress: startup.ControlAddress,
		RegisteredAt:   startup.StartedAt,
	}

	if err := registrar.RegisterMember(ctx, registration); err != nil {
		daemon.logger.WarnContext(
			ctx,
			"failed to register local member in control plane",
			slog.String("component", "controlplane"),
			slog.String("node", startup.NodeName),
			slog.String("node_role", startup.NodeRole.String()),
			slog.String("api_address", startup.APIAddress),
			slog.String("control_address", startup.ControlAddress),
			slog.String("register_error", err.Error()),
		)
		return
	}

	daemon.logger.InfoContext(
		ctx,
		"registered local member in control plane",
		slog.String("component", "controlplane"),
		slog.String("node", startup.NodeName),
		slog.String("node_role", startup.NodeRole.String()),
		slog.String("api_address", startup.APIAddress),
		slog.String("control_address", startup.ControlAddress),
	)
}

// Startup returns the daemon startup state collected during Start.
func (daemon *Daemon) Startup() agentmodel.Startup {
	daemon.mu.RLock()
	defer daemon.mu.RUnlock()

	return daemon.started
}

// Heartbeat returns the latest local heartbeat observation collected by the
// daemon.
func (daemon *Daemon) Heartbeat() agentmodel.Heartbeat {
	daemon.mu.RLock()
	defer daemon.mu.RUnlock()

	return daemon.heartbeat
}

// NodeStatus returns the latest local node observation published by the agent.
func (daemon *Daemon) NodeStatus() agentmodel.NodeStatus {
	daemon.mu.RLock()
	defer daemon.mu.RUnlock()

	return daemon.nodeStatus.Clone()
}

// Wait blocks until the heartbeat loop stops after the daemon context is
// cancelled.
func (daemon *Daemon) Wait() {
	daemon.loopWG.Wait()

	if daemon.peerServer != nil {
		if err := daemon.peerServer.Wait(); err != nil {
			daemon.logger.Error(
				"peer api server stopped unexpectedly",
				daemon.logArgs("peerapi", slog.String("error", err.Error()))...,
			)
		}
	}

	if daemon.apiServer == nil {
		return
	}

	if err := daemon.apiServer.Wait(); err != nil {
		daemon.logger.Error(
			"http api server stopped unexpectedly",
			daemon.logArgs("httpapi", slog.String("error", err.Error()))...,
		)
	}
}

func (daemon *Daemon) startAPIServer(ctx context.Context) error {
	if daemon.apiServer == nil {
		return nil
	}

	return daemon.apiServer.Start(ctx, daemon.config.Node.APIAddress)
}

func (daemon *Daemon) startPeerServer(ctx context.Context) error {
	if daemon.peerServer == nil {
		return nil
	}

	return daemon.peerServer.Start(ctx, daemon.config.Node.ControlAddress)
}

func (daemon *Daemon) probeSeedPeers(ctx context.Context) {
	if daemon.peerClientTLSConfig == nil || daemon.config.Bootstrap == nil {
		return
	}

	client := &http.Client{
		Timeout: daemon.peerProbeTimeout,
		Transport: &http.Transport{
			TLSClientConfig: daemon.peerClientTLSConfig.Clone(),
		},
	}

	for _, seedAddress := range daemon.config.Bootstrap.SeedAddresses {
		if !shouldProbeSeedAddress(seedAddress, daemon.config.Node.ControlAddress) {
			continue
		}

		daemon.probeSeedPeer(ctx, client, seedAddress)
	}
}

func (daemon *Daemon) probeSeedPeer(ctx context.Context, client *http.Client, seedAddress string) {
	probeCtx, cancel := context.WithTimeout(ctx, daemon.peerProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "https://"+seedAddress+"/peer/v1/identity", nil)
	if err != nil {
		daemon.logger.WarnContext(
			ctx,
			"failed to build peer probe request",
			daemon.logArgs(
				"peerapi",
				slog.String("seed_address", seedAddress),
				slog.String("error", err.Error()),
			)...,
		)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		daemon.logger.WarnContext(
			ctx,
			"failed to probe peer over mTLS",
			daemon.logArgs(
				"peerapi",
				slog.String("seed_address", seedAddress),
				slog.String("error", err.Error()),
			)...,
		)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		daemon.logger.WarnContext(
			ctx,
			"peer probe returned unexpected status",
			daemon.logArgs(
				"peerapi",
				slog.String("seed_address", seedAddress),
				slog.Int("status", resp.StatusCode),
			)...,
		)
		return
	}

	var identity peerapi.IdentityResponse
	if err := json.NewDecoder(resp.Body).Decode(&identity); err != nil {
		daemon.logger.WarnContext(
			ctx,
			"failed to decode peer probe response",
			daemon.logArgs(
				"peerapi",
				slog.String("seed_address", seedAddress),
				slog.String("error", err.Error()),
			)...,
		)
		return
	}

	daemon.logger.InfoContext(
		ctx,
		"validated peer mTLS connection",
		daemon.logArgs(
			"peerapi",
			slog.String("seed_address", seedAddress),
			slog.String("peer_node", identity.NodeName),
			slog.String("client_subject", identity.Peer.Subject),
		)...,
	)
}

func shouldProbeSeedAddress(seedAddress, localControlAddress string) bool {
	trimmed := strings.TrimSpace(seedAddress)
	if trimmed == "" || trimmed == strings.TrimSpace(localControlAddress) {
		return false
	}

	host, _, err := net.SplitHostPort(trimmed)
	if err != nil {
		return false
	}

	ip := net.ParseIP(strings.TrimSpace(host))
	return ip == nil || !ip.IsUnspecified()
}

func memberPeerSubjects(cfg config.Config) []string {
	if cfg.Bootstrap == nil {
		return nil
	}

	return append([]string(nil), cfg.Bootstrap.ExpectedMembers...)
}
