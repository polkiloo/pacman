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
)

func (daemon *Daemon) detectPostgresStatus(ctx context.Context, observedAt time.Time) agentmodel.PostgresStatus {
	if !daemon.config.Node.Role.HasLocalPostgres() || daemon.config.Postgres == nil {
		return agentmodel.PostgresStatus{
			Managed:   false,
			CheckedAt: observedAt,
			Role:      localMemberRoleForNodeRole(daemon.config.Node.Role),
		}
	}

	address := localPostgresProbeAddress(*daemon.config.Postgres)
	probeCtx := ctx
	cancel := func() {}
	if daemon.probeTimeout > 0 {
		probeCtx, cancel = context.WithTimeout(ctx, daemon.probeTimeout)
	}
	defer cancel()

	status := agentmodel.PostgresStatus{
		Managed:   true,
		Address:   address,
		CheckedAt: observedAt,
		Role:      cluster.MemberRoleUnknown,
	}

	if err := daemon.postgresProbe(probeCtx, address); err != nil {
		status.AvailabilityError = err.Error()
		return status
	}

	status.Up = true

	role, inRecovery, err := daemon.postgresStateProbe(probeCtx, address)
	if err != nil {
		status.StateError = err.Error()
		return status
	}

	status.Role = role
	status.RecoveryKnown = true
	status.InRecovery = inRecovery
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
