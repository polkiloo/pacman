package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
)

// Startup describes the local daemon identity and startup state after the
// daemon has been initialized successfully.
type Startup struct {
	NodeName        string
	NodeRole        cluster.NodeRole
	APIAddress      string
	ControlAddress  string
	ManagesPostgres bool
	StartedAt       time.Time
}

// Daemon is the node-local PACMAN agent responsible for PostgreSQL observation
// and lifecycle management.
type Daemon struct {
	config config.Config
	logger *slog.Logger
	now    func() time.Time

	mu      sync.RWMutex
	started Startup
}

// Option customizes daemon construction for tests and future runtime hooks.
type Option func(*Daemon)

func withNow(now func() time.Time) Option {
	return func(daemon *Daemon) {
		if now != nil {
			daemon.now = now
		}
	}
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
		config: defaulted,
		logger: logger,
		now:    time.Now,
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
	defer daemon.mu.Unlock()

	if !daemon.started.StartedAt.IsZero() {
		return ErrDaemonAlreadyStarted
	}

	daemon.started = Startup{
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

	return nil
}

// Startup returns the daemon startup state collected during Start.
func (daemon *Daemon) Startup() Startup {
	daemon.mu.RLock()
	defer daemon.mu.RUnlock()

	return daemon.started
}
