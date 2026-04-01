package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/postgres"
)

const (
	defaultHeartbeatInterval    = 1 * time.Second
	defaultPostgresProbeTimeout = 500 * time.Millisecond
)

// Daemon runs the node-local PACMAN agent.
type Daemon struct {
	config             config.Config
	logger             *slog.Logger
	now                func() time.Time
	heartbeatInterval  time.Duration
	postgresProbe      postgresAvailabilityProbe
	postgresStateProbe postgresStateProbe
	statePublisher     controlplane.NodeStatePublisher
	apiServer          httpServer
	probeTimeout       time.Duration
	startedFlag        atomic.Bool

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

	daemon := &Daemon{
		config:             defaulted,
		logger:             logger,
		now:                time.Now,
		heartbeatInterval:  defaultHeartbeatInterval,
		postgresProbe:      dialPostgresAvailability,
		postgresStateProbe: postgres.QueryObservation,
		statePublisher:     store,
		apiServer:          httpapi.New(defaulted.Node.Name, store, logger, httpapi.Config{}),
		probeTimeout:       defaultPostgresProbeTimeout,
	}

	for _, option := range options {
		if option != nil {
			option(daemon)
		}
	}

	return daemon, nil
}

// Start records daemon startup state and emits the first lifecycle log entry.
func (daemon *Daemon) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	startup, err := daemon.beginStart()
	if err != nil {
		return err
	}

	if err := daemon.bootstrapClusterSpec(ctx); err != nil {
		daemon.rollbackStart()
		return fmt.Errorf("store bootstrap cluster spec: %w", err)
	}

	if err := daemon.startAPIServer(ctx); err != nil {
		daemon.rollbackStart()
		return fmt.Errorf("start http api server: %w", err)
	}

	daemon.logStartup(ctx, startup)
	daemon.registerMember(ctx, startup)
	daemon.recordHeartbeat(ctx)
	daemon.loopWG.Add(1)
	go daemon.runHeartbeatLoop(ctx)

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
		slog.String("component", "agent"),
		slog.String("node", startup.NodeName),
		slog.String("role", startup.NodeRole.String()),
		slog.Bool("manages_postgres", startup.ManagesPostgres),
		slog.String("api_address", startup.APIAddress),
		slog.String("control_address", startup.ControlAddress),
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
			slog.String("role", startup.NodeRole.String()),
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
		slog.String("role", startup.NodeRole.String()),
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

	if daemon.apiServer == nil {
		return
	}

	if err := daemon.apiServer.Wait(); err != nil {
		daemon.logger.Error(
			"http api server stopped unexpectedly",
			slog.String("component", "httpapi"),
			slog.String("error", err.Error()),
		)
	}
}

func (daemon *Daemon) startAPIServer(ctx context.Context) error {
	if daemon.apiServer == nil {
		return nil
	}

	return daemon.apiServer.Start(ctx, daemon.config.Node.APIAddress)
}
