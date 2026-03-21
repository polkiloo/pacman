package agent

import (
	"context"
	"log/slog"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
)

func (daemon *Daemon) runHeartbeatLoop(ctx context.Context) {
	defer daemon.loopWG.Done()

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
	postgres := daemon.detectPostgresStatus(ctx, observedAt)

	daemon.mu.Lock()
	previous := daemon.heartbeat
	daemon.heartbeat = agentmodel.Heartbeat{
		Sequence:   previous.Sequence + 1,
		ObservedAt: observedAt,
		Postgres:   postgres,
	}
	current := daemon.heartbeat
	daemon.mu.Unlock()

	daemon.logHeartbeat(current, previous)
}

func (daemon *Daemon) logHeartbeat(current, previous agentmodel.Heartbeat) {
	if current.Sequence > 1 &&
		current.Postgres.Managed == previous.Postgres.Managed &&
		current.Postgres.Up == previous.Postgres.Up &&
		current.Postgres.Role == previous.Postgres.Role &&
		current.Postgres.RecoveryKnown == previous.Postgres.RecoveryKnown &&
		current.Postgres.InRecovery == previous.Postgres.InRecovery &&
		current.Postgres.SystemIdentifier == previous.Postgres.SystemIdentifier &&
		current.Postgres.Timeline == previous.Postgres.Timeline &&
		current.Postgres.AvailabilityError == previous.Postgres.AvailabilityError &&
		current.Postgres.StateError == previous.Postgres.StateError {
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

	if current.Postgres.Role != "" {
		args = append(args, slog.String("member_role", current.Postgres.Role.String()))
	}

	if current.Postgres.RecoveryKnown {
		args = append(args, slog.Bool("in_recovery", current.Postgres.InRecovery))
	}

	if current.Postgres.SystemIdentifier != "" {
		args = append(args, slog.String("system_identifier", current.Postgres.SystemIdentifier))
	}

	if current.Postgres.Timeline > 0 {
		args = append(args, slog.Int64("timeline", current.Postgres.Timeline))
	}

	if current.Postgres.AvailabilityError != "" {
		args = append(args, slog.String("postgres_error", current.Postgres.AvailabilityError))
	}

	if current.Postgres.StateError != "" {
		args = append(args, slog.String("postgres_state_error", current.Postgres.StateError))
	}

	if current.Postgres.Managed && current.Postgres.Up {
		if current.Postgres.StateError != "" {
			daemon.logger.Warn("observed PostgreSQL availability without role state", args...)
			return
		}

		daemon.logger.Info("observed PostgreSQL availability", args...)
		return
	}

	if current.Postgres.Managed {
		daemon.logger.Warn("observed PostgreSQL unavailability", args...)
		return
	}

	daemon.logger.Info("observed heartbeat without local PostgreSQL", args...)
}
