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
	nodeStatus := daemon.buildNodeStatus(observedAt, postgres)
	controlPlane := daemon.publishNodeStatus(ctx, nodeStatus)
	nodeStatus.ControlPlane = controlPlane

	daemon.mu.Lock()
	previous := daemon.heartbeat
	daemon.heartbeat = agentmodel.Heartbeat{
		Sequence:     previous.Sequence + 1,
		ObservedAt:   observedAt,
		Postgres:     postgres,
		ControlPlane: controlPlane,
	}
	daemon.nodeStatus = nodeStatus
	current := daemon.heartbeat
	daemon.mu.Unlock()

	daemon.logHeartbeat(current, previous)
	daemon.logControlPlaneSync(current, previous, nodeStatus)
}

func (daemon *Daemon) logHeartbeat(current, previous agentmodel.Heartbeat) {
	if current.Sequence > 1 && samePostgresStatus(current.Postgres, previous.Postgres) {
		return
	}

	message, warn := heartbeatLogMessage(current.Postgres)
	args := heartbeatLogArgs(current)
	if warn {
		daemon.logger.Warn(message, args...)
		return
	}

	daemon.logger.Info(message, args...)
}

func samePostgresStatus(current, previous agentmodel.PostgresStatus) bool {
	return current.Managed == previous.Managed &&
		current.Up == previous.Up &&
		current.Role == previous.Role &&
		current.RecoveryKnown == previous.RecoveryKnown &&
		current.InRecovery == previous.InRecovery &&
		samePostgresDetails(current.Details, previous.Details) &&
		sameWALProgress(current.WAL, previous.WAL) &&
		current.Errors == previous.Errors
}

func samePostgresDetails(current, previous agentmodel.PostgresDetails) bool {
	return current.ServerVersion == previous.ServerVersion &&
		current.PendingRestart == previous.PendingRestart &&
		current.SystemIdentifier == previous.SystemIdentifier &&
		current.Timeline == previous.Timeline &&
		current.PostmasterStartAt.Equal(previous.PostmasterStartAt) &&
		current.ReplicationLagBytes == previous.ReplicationLagBytes
}

func sameWALProgress(current, previous agentmodel.WALProgress) bool {
	return current.WriteLSN == previous.WriteLSN &&
		current.FlushLSN == previous.FlushLSN &&
		current.ReceiveLSN == previous.ReceiveLSN &&
		current.ReplayLSN == previous.ReplayLSN &&
		current.ReplayTimestamp.Equal(previous.ReplayTimestamp)
}

func heartbeatLogMessage(status agentmodel.PostgresStatus) (string, bool) {
	if !status.Managed {
		return "observed heartbeat without local PostgreSQL", false
	}

	if !status.Up {
		return "observed PostgreSQL unavailability", true
	}

	if status.Errors.State != "" {
		return "observed PostgreSQL availability without role state", true
	}

	return "observed PostgreSQL availability", false
}

func heartbeatLogArgs(heartbeat agentmodel.Heartbeat) []any {
	status := heartbeat.Postgres
	details := status.Details
	wal := status.WAL
	errors := status.Errors

	args := []any{
		slog.String("component", "agent"),
		slog.Uint64("heartbeat_sequence", heartbeat.Sequence),
		slog.Bool("postgres_managed", status.Managed),
		slog.Bool("postgres_up", status.Up),
		slog.Bool("pending_restart", details.PendingRestart),
		slog.Int64("replication_lag_bytes", details.ReplicationLagBytes),
	}

	if status.Address != "" {
		args = append(args, slog.String("postgres_address", status.Address))
	}

	if status.Role != "" {
		args = append(args, slog.String("member_role", status.Role.String()))
	}

	if status.RecoveryKnown {
		args = append(args, slog.Bool("in_recovery", status.InRecovery))
	}

	if details.SystemIdentifier != "" {
		args = append(args, slog.String("system_identifier", details.SystemIdentifier))
	}

	if details.Timeline > 0 {
		args = append(args, slog.Int64("timeline", details.Timeline))
	}

	if details.ServerVersion > 0 {
		args = append(args, slog.Int("server_version", details.ServerVersion))
	}

	if !details.PostmasterStartAt.IsZero() {
		args = append(args, slog.Time("postmaster_start_time", details.PostmasterStartAt))
	}

	if wal.WriteLSN != "" {
		args = append(args, slog.String("write_lsn", wal.WriteLSN))
	}

	if wal.FlushLSN != "" {
		args = append(args, slog.String("flush_lsn", wal.FlushLSN))
	}

	if wal.ReceiveLSN != "" {
		args = append(args, slog.String("receive_lsn", wal.ReceiveLSN))
	}

	if wal.ReplayLSN != "" {
		args = append(args, slog.String("replay_lsn", wal.ReplayLSN))
	}

	if !wal.ReplayTimestamp.IsZero() {
		args = append(args, slog.Time("replay_timestamp", wal.ReplayTimestamp))
	}

	if errors.Availability != "" {
		args = append(args, slog.String("postgres_error", errors.Availability))
	}

	if errors.State != "" {
		args = append(args, slog.String("postgres_state_error", errors.State))
	}

	return args
}
