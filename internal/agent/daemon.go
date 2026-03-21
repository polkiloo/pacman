package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/config"
)

const (
	defaultHeartbeatInterval    = 1 * time.Second
	defaultPostgresProbeTimeout = 500 * time.Millisecond
)

// Daemon is the node-local PACMAN agent responsible for PostgreSQL observation
// and lifecycle management.
type Daemon struct {
	config             config.Config
	logger             *slog.Logger
	now                func() time.Time
	heartbeatInterval  time.Duration
	postgresProbe      postgresAvailabilityProbe
	postgresStateProbe postgresStateProbe
	probeTimeout       time.Duration

	mu        sync.RWMutex
	started   agentmodel.Startup
	heartbeat agentmodel.Heartbeat
	loopDone  chan struct{}
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
		postgresStateProbe: queryPostgresRoleAndRecoveryState,
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

	daemon.mu.Lock()
	if !daemon.started.StartedAt.IsZero() {
		daemon.mu.Unlock()
		return ErrDaemonAlreadyStarted
	}

	daemon.started = agentmodel.Startup{
		NodeName:        daemon.config.Node.Name,
		NodeRole:        daemon.config.Node.Role,
		APIAddress:      daemon.config.Node.APIAddress,
		ControlAddress:  daemon.config.Node.ControlAddress,
		ManagesPostgres: daemon.config.Node.Role.HasLocalPostgres(),
		StartedAt:       daemon.now().UTC(),
	}

	daemon.logger.InfoContext(
		ctx,
		"started local agent daemon",
		slog.String("component", "agent"),
		slog.String("node", daemon.started.NodeName),
		slog.String("role", daemon.started.NodeRole.String()),
		slog.Bool("manages_postgres", daemon.started.ManagesPostgres),
		slog.String("api_address", daemon.started.APIAddress),
		slog.String("control_address", daemon.started.ControlAddress),
	)

	loopDone := make(chan struct{})
	daemon.loopDone = loopDone
	daemon.mu.Unlock()

	daemon.recordHeartbeat(ctx)

	go daemon.runHeartbeatLoop(ctx, loopDone)

	return nil
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

// Wait blocks until the heartbeat loop stops after the daemon context is
// cancelled.
func (daemon *Daemon) Wait() {
	daemon.mu.RLock()
	loopDone := daemon.loopDone
	daemon.mu.RUnlock()

	if loopDone == nil {
		return
	}

	<-loopDone
}
