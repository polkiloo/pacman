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
	probeTimeout       time.Duration
	startedFlag        atomic.Bool

	mu         sync.RWMutex
	started    agentmodel.Startup
	heartbeat  agentmodel.Heartbeat
	nodeStatus agentmodel.NodeStatus
	loopWG     sync.WaitGroup
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

	daemon := &Daemon{
		config:             defaulted,
		logger:             logger,
		now:                time.Now,
		heartbeatInterval:  defaultHeartbeatInterval,
		postgresProbe:      dialPostgresAvailability,
		postgresStateProbe: postgres.QueryObservation,
		statePublisher:     controlplane.NewMemoryStateStore(),
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

	daemon.logStartup(ctx, startup)
	daemon.recordHeartbeat(ctx)
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

	daemon.loopWG.Add(1)
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	daemon.started = startup
	return startup, nil
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
}
