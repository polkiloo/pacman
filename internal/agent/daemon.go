package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
)

const (
	defaultHeartbeatInterval    = 1 * time.Second
	defaultPostgresProbeTimeout = 500 * time.Millisecond
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

// PostgresAvailability describes the latest local PostgreSQL reachability
// observation collected by the agent heartbeat loop.
type PostgresAvailability struct {
	Managed   bool
	Up        bool
	Address   string
	CheckedAt time.Time
	Error     string
}

// Heartbeat describes the latest local agent heartbeat.
type Heartbeat struct {
	Sequence   uint64
	ObservedAt time.Time
	Postgres   PostgresAvailability
}

// Daemon is the node-local PACMAN agent responsible for PostgreSQL observation
// and lifecycle management.
type Daemon struct {
	config            config.Config
	logger            *slog.Logger
	now               func() time.Time
	heartbeatInterval time.Duration
	postgresProbe     postgresAvailabilityProbe
	probeTimeout      time.Duration

	mu        sync.RWMutex
	started   Startup
	heartbeat Heartbeat
	loopDone  chan struct{}
}

// Option customizes daemon construction for tests and future runtime hooks.
type Option func(*Daemon)

type postgresAvailabilityProbe func(context.Context, string) error

func withNow(now func() time.Time) Option {
	return func(daemon *Daemon) {
		if now != nil {
			daemon.now = now
		}
	}
}

func withHeartbeatInterval(interval time.Duration) Option {
	return func(daemon *Daemon) {
		if interval > 0 {
			daemon.heartbeatInterval = interval
		}
	}
}

func withPostgresProbe(probe postgresAvailabilityProbe) Option {
	return func(daemon *Daemon) {
		if probe != nil {
			daemon.postgresProbe = probe
		}
	}
}

func withProbeTimeout(timeout time.Duration) Option {
	return func(daemon *Daemon) {
		if timeout > 0 {
			daemon.probeTimeout = timeout
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
		config:            defaulted,
		logger:            logger,
		now:               time.Now,
		heartbeatInterval: defaultHeartbeatInterval,
		postgresProbe:     dialPostgresAvailability,
		probeTimeout:      defaultPostgresProbeTimeout,
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

	loopDone := make(chan struct{})
	daemon.loopDone = loopDone
	daemon.mu.Unlock()

	daemon.recordHeartbeat(ctx)

	go daemon.runHeartbeatLoop(ctx, loopDone)

	return nil
}

// Startup returns the daemon startup state collected during Start.
func (daemon *Daemon) Startup() Startup {
	daemon.mu.RLock()
	defer daemon.mu.RUnlock()

	return daemon.started
}

// Heartbeat returns the latest local heartbeat observation collected by the
// daemon.
func (daemon *Daemon) Heartbeat() Heartbeat {
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

func (daemon *Daemon) runHeartbeatLoop(ctx context.Context, loopDone chan struct{}) {
	defer close(loopDone)

	ticker := time.NewTicker(daemon.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			daemon.recordHeartbeat(ctx)
		}
	}
}

func (daemon *Daemon) recordHeartbeat(ctx context.Context) {
	observedAt := daemon.now().UTC()
	postgres := daemon.detectPostgresAvailability(ctx, observedAt)

	daemon.mu.Lock()
	previous := daemon.heartbeat
	daemon.heartbeat = Heartbeat{
		Sequence:   previous.Sequence + 1,
		ObservedAt: observedAt,
		Postgres:   postgres,
	}
	current := daemon.heartbeat
	daemon.mu.Unlock()

	daemon.logHeartbeat(current, previous)
}

func (daemon *Daemon) detectPostgresAvailability(ctx context.Context, observedAt time.Time) PostgresAvailability {
	if !daemon.config.Node.Role.HasLocalPostgres() || daemon.config.Postgres == nil {
		return PostgresAvailability{
			Managed:   false,
			CheckedAt: observedAt,
		}
	}

	address := localPostgresProbeAddress(*daemon.config.Postgres)
	probeCtx := ctx
	cancel := func() {}
	if daemon.probeTimeout > 0 {
		probeCtx, cancel = context.WithTimeout(ctx, daemon.probeTimeout)
	}
	defer cancel()

	status := PostgresAvailability{
		Managed:   true,
		Address:   address,
		CheckedAt: observedAt,
	}

	if err := daemon.postgresProbe(probeCtx, address); err != nil {
		status.Error = err.Error()
		return status
	}

	status.Up = true
	return status
}

func (daemon *Daemon) logHeartbeat(current, previous Heartbeat) {
	if current.Sequence > 1 &&
		current.Postgres.Managed == previous.Postgres.Managed &&
		current.Postgres.Up == previous.Postgres.Up {
		return
	}

	args := []any{
		slog.String("component", "agent"),
		slog.Uint64("heartbeat_sequence", current.Sequence),
		slog.Bool("postgres_managed", current.Postgres.Managed),
		slog.Bool("postgres_up", current.Postgres.Up),
	}

	if current.Postgres.Address != "" {
		args = append(args, slog.String("postgres_address", current.Postgres.Address))
	}

	if current.Postgres.Error != "" {
		args = append(args, slog.String("postgres_error", current.Postgres.Error))
	}

	if current.Postgres.Managed && current.Postgres.Up {
		daemon.logger.Info("observed PostgreSQL availability", args...)
		return
	}

	if current.Postgres.Managed {
		daemon.logger.Warn("observed PostgreSQL unavailability", args...)
		return
	}

	daemon.logger.Info("observed heartbeat without local PostgreSQL", args...)
}

func localPostgresProbeAddress(cfg config.PostgresLocalConfig) string {
	return net.JoinHostPort(normalizeLocalProbeHost(cfg.ListenAddress), strconv.Itoa(cfg.Port))
}

func normalizeLocalProbeHost(host string) string {
	trimmed := strings.TrimSpace(host)

	switch trimmed {
	case "", "0.0.0.0", "*":
		return "127.0.0.1"
	case "::", "[::]":
		return "::1"
	default:
		return trimmed
	}
}

func dialPostgresAvailability(ctx context.Context, address string) error {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}

	return conn.Close()
}
