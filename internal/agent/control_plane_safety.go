package agent

import (
	"context"
	"log/slog"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

func (daemon *Daemon) reconcilePrimaryControlPlaneReachability(ctx context.Context, currentPostgres agentmodel.PostgresStatus, controlPlane agentmodel.ControlPlaneStatus) bool {
	if daemon.pgCtl == nil || controlPlane.ClusterReachable || !primaryRequiresControlPlane(currentPostgres) {
		return false
	}

	if err := daemon.pgCtl.Stop(ctx, postgres.ShutdownModeFast); err != nil {
		daemon.logger.WarnContext(ctx, "primary self-demotion after control-plane loss failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return false
	}

	daemon.markSelfDemotedPrimaryForRejoin()
	daemon.logger.WarnContext(ctx, "primary self-demoted after control-plane loss",
		daemon.logArgs("agent")...)
	return true
}

func primaryRequiresControlPlane(status agentmodel.PostgresStatus) bool {
	return status.Managed &&
		status.Up &&
		status.RecoveryKnown &&
		!status.InRecovery &&
		status.Role == cluster.MemberRolePrimary
}
