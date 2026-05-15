package agent

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/postgres"
)

func (daemon *Daemon) reconcileReplicaFollowPrimary(ctx context.Context, currentPostgres agentmodel.PostgresStatus) {
	if daemon.pgCtl == nil || daemon.stateReader == nil || daemon.config.Postgres == nil {
		return
	}
	if !daemon.config.Node.Role.HasLocalPostgres() || !replicaCanFollowPromotedPrimary(currentPostgres) {
		return
	}

	status, ok := daemon.stateReader.ClusterStatus()
	if !ok || strings.TrimSpace(status.CurrentPrimary) == "" || status.CurrentPrimary == daemon.config.Node.Name {
		return
	}

	local, hasLocal := memberByName(status.Members, daemon.config.Node.Name)
	primary, hasPrimary := memberByName(status.Members, status.CurrentPrimary)
	if !hasLocal || !hasPrimary || !promotedPrimaryAhead(primary, local) {
		return
	}
	if local.NeedsRejoin || local.State == cluster.MemberStateNeedsRejoin {
		return
	}
	if daemon.replicaFollowAlreadyAttempted(primary.Name, primary.Timeline) {
		return
	}

	primaryNode, _ := daemon.stateReader.NodeStatus(primary.Name)
	primaryAddr := replicaFollowPrimaryAddress(primary, primaryNode, daemon.config.Postgres.Port)
	if strings.TrimSpace(primaryAddr) == "" {
		return
	}

	configurator := &localStandbyConfigurator{
		dataDir:             daemon.config.Postgres.DataDir,
		replicationUser:     daemon.config.Postgres.ReplicationUser,
		replicationPassword: daemon.config.Postgres.ReplicationPassword,
	}
	if err := configurator.ConfigureStandby(ctx, controlPlaneStandbyConfig(primaryAddr, daemon.config.Node.Name)); err != nil {
		daemon.logger.WarnContext(ctx, "replica standby reconfiguration failed",
			daemon.logArgs("agent",
				slog.String("primary", primary.Name),
				slog.Int64("primary_timeline", primary.Timeline),
				slog.String("error", err.Error()))...)
		return
	}

	if err := restartReplicaPostgres(ctx, daemon.pgCtl); err != nil {
		daemon.logger.WarnContext(ctx, "replica restart after primary follow reconfiguration failed",
			daemon.logArgs("agent",
				slog.String("primary", primary.Name),
				slog.Int64("primary_timeline", primary.Timeline),
				slog.String("error", err.Error()))...)
		return
	}

	daemon.markReplicaFollowAttempted(primary.Name, primary.Timeline)
	daemon.logger.InfoContext(ctx, "replica standby reconfigured to follow promoted primary",
		daemon.logArgs("agent",
			slog.String("primary", primary.Name),
			slog.Int64("primary_timeline", primary.Timeline))...)
}

func restartReplicaPostgres(ctx context.Context, pgCtl *postgres.PGCtl) error {
	if err := pgCtl.Stop(ctx, postgres.ShutdownModeFast); err != nil {
		return err
	}

	return pgCtl.StartNoWait(ctx)
}

func replicaCanFollowPromotedPrimary(status agentmodel.PostgresStatus) bool {
	return status.Managed &&
		status.Up &&
		status.RecoveryKnown &&
		status.InRecovery &&
		status.Role == cluster.MemberRoleReplica &&
		status.Details.Timeline > 0
}

func promotedPrimaryAhead(primary, local cluster.MemberStatus) bool {
	return primary.Role == cluster.MemberRolePrimary &&
		primary.Healthy &&
		primary.Timeline > 0 &&
		local.Timeline > 0 &&
		local.Timeline < primary.Timeline
}

func memberByName(members []cluster.MemberStatus, name string) (cluster.MemberStatus, bool) {
	for _, member := range members {
		if member.Name == name {
			return member.Clone(), true
		}
	}

	return cluster.MemberStatus{}, false
}

func replicaFollowPrimaryAddress(primary cluster.MemberStatus, primaryNode agentmodel.NodeStatus, fallbackPort int) string {
	host := strings.TrimSpace(primary.Host)
	postgresHost, postgresPort := splitHostPort(primaryNode.Postgres.Address)
	if host == "" {
		host = postgresHost
	}
	if host == "" {
		return ""
	}

	port := postgresPort
	if port == 0 {
		port = fallbackPort
	}
	if port == 0 {
		port = config.DefaultPostgresPort
	}

	return net.JoinHostPort(host, strconv.Itoa(port))
}

func splitHostPort(address string) (string, int) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return "", 0
	}

	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		return "", 0
	}

	return host, parsedPort
}

func controlPlaneStandbyConfig(primaryAddress, applicationName string) controlplane.StandbyConfigRequest {
	host, port, err := net.SplitHostPort(primaryAddress)
	if err != nil {
		return controlplane.StandbyConfigRequest{}
	}

	connInfo := "host=" + formatConnInfoValue(host) +
		" port=" + formatConnInfoValue(port) +
		" application_name=" + formatConnInfoValue(applicationName)

	return controlplane.StandbyConfigRequest{
		Standby: postgres.StandbyConfig{
			PrimaryConnInfo:        connInfo,
			RecoveryTargetTimeline: postgres.DefaultRecoveryTargetTimeline,
		},
	}
}

func (daemon *Daemon) replicaFollowAlreadyAttempted(primaryName string, primaryTimeline int64) bool {
	daemon.mu.RLock()
	defer daemon.mu.RUnlock()

	return daemon.replicaFollowPrimary == primaryName && daemon.replicaFollowTimeline == primaryTimeline
}

func (daemon *Daemon) markReplicaFollowAttempted(primaryName string, primaryTimeline int64) {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()

	daemon.replicaFollowPrimary = primaryName
	daemon.replicaFollowTimeline = primaryTimeline
}
