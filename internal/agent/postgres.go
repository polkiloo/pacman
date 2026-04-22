package agent

import (
	"context"
	"net"
	"strconv"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/postgres"
)

func (daemon *Daemon) detectPostgresStatus(ctx context.Context, observedAt time.Time) agentmodel.PostgresStatus {
	if !daemon.managesLocalPostgres() {
		return daemon.unmanagedPostgresStatus(observedAt)
	}

	status := daemon.newManagedPostgresStatus(observedAt)
	probeCtx, cancel := daemon.newProbeContext(ctx)
	defer cancel()

	if err := daemon.postgresProbe(probeCtx, status.Address); err != nil {
		status.Errors.Availability = err.Error()
		return status
	}

	status.Up = true

	observation, err := daemon.postgresStateProbe(probeCtx, status.Address)
	if err != nil {
		status.Errors.State = err.Error()
		return status
	}

	return observedPostgresStatus(status, observation)
}

func (daemon *Daemon) managesLocalPostgres() bool {
	return daemon.config.Node.Role.HasLocalPostgres() && daemon.config.Postgres != nil
}

func (daemon *Daemon) unmanagedPostgresStatus(observedAt time.Time) agentmodel.PostgresStatus {
	return agentmodel.PostgresStatus{
		CheckedAt: observedAt,
		Role:      localMemberRoleForNodeRole(daemon.config.Node.Role),
	}
}

func (daemon *Daemon) newManagedPostgresStatus(observedAt time.Time) agentmodel.PostgresStatus {
	return agentmodel.PostgresStatus{
		Managed:   true,
		Address:   localPostgresProbeAddress(*daemon.config.Postgres),
		CheckedAt: observedAt,
		Role:      cluster.MemberRoleUnknown,
	}
}

func (daemon *Daemon) newProbeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if daemon.probeTimeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, daemon.probeTimeout)
}

func observedPostgresStatus(status agentmodel.PostgresStatus, observation postgres.Observation) agentmodel.PostgresStatus {
	status.Role = observation.Role
	status.RecoveryKnown = true
	status.InRecovery = observation.InRecovery
	status.Details = agentmodel.PostgresDetails{
		ServerVersion:       observation.Details.ServerVersion,
		PendingRestart:      observation.Details.PendingRestart,
		SystemIdentifier:    observation.Details.SystemIdentifier,
		Timeline:            observation.Details.Timeline,
		PostmasterStartAt:   observation.Details.PostmasterStartAt,
		DatabaseSizeBytes:   observation.Details.DatabaseSizeBytes,
		ReplicationLagBytes: observation.Details.ReplicationLagBytes,
	}
	status.WAL = agentmodel.WALProgress{
		WriteLSN:        observation.WAL.WriteLSN,
		FlushLSN:        observation.WAL.FlushLSN,
		ReceiveLSN:      observation.WAL.ReceiveLSN,
		ReplayLSN:       observation.WAL.ReplayLSN,
		ReplayTimestamp: observation.WAL.ReplayTimestamp,
	}
	return status
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

func localMemberRoleForNodeRole(nodeRole cluster.NodeRole) cluster.MemberRole {
	switch nodeRole {
	case cluster.NodeRoleWitness:
		return cluster.MemberRoleWitness
	case cluster.NodeRoleData:
		return cluster.MemberRoleUnknown
	default:
		return cluster.MemberRoleUnknown
	}
}
