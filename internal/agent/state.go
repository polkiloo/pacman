package agent

import (
	"context"
	"log/slog"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func (daemon *Daemon) buildNodeStatus(observedAt time.Time, postgres agentmodel.PostgresStatus) agentmodel.NodeStatus {
	return agentmodel.NodeStatus{
		NodeName:       daemon.config.Node.Name,
		MemberName:     daemon.config.Node.Name,
		Role:           postgres.Role,
		State:          localMemberStateForObservation(daemon.config.Node.Role, postgres),
		PendingRestart: postgres.Details.PendingRestart,
		NeedsRejoin:    false,
		Postgres:       postgres,
		ObservedAt:     observedAt,
	}
}

func (daemon *Daemon) publishNodeStatus(ctx context.Context, status agentmodel.NodeStatus) agentmodel.ControlPlaneStatus {
	published, err := daemon.statePublisher.PublishNodeStatus(ctx, status)
	if err != nil {
		published.ClusterReachable = false
		published.PublishError = err.Error()
	}

	return published
}

func (daemon *Daemon) logControlPlaneSync(current, previous agentmodel.Heartbeat, node agentmodel.NodeStatus) {
	if current.Sequence > 1 &&
		current.ControlPlane.ClusterReachable == previous.ControlPlane.ClusterReachable &&
		current.ControlPlane.Leader == previous.ControlPlane.Leader &&
		current.ControlPlane.PublishError == previous.ControlPlane.PublishError {
		return
	}

	args := []any{
		slog.String("component", "controlplane"),
		slog.Uint64("heartbeat_sequence", current.Sequence),
		slog.String("node", node.NodeName),
		slog.String("member_role", node.Role.String()),
		slog.String("member_state", node.State.String()),
		slog.Bool("cluster_reachable", current.ControlPlane.ClusterReachable),
		slog.Bool("control_plane_leader", current.ControlPlane.Leader),
	}

	if !current.ControlPlane.LastHeartbeatAt.IsZero() {
		args = append(args, slog.Time("last_heartbeat_at", current.ControlPlane.LastHeartbeatAt))
	}

	if !current.ControlPlane.LastDCSSeenAt.IsZero() {
		args = append(args, slog.Time("last_dcs_seen_at", current.ControlPlane.LastDCSSeenAt))
	}

	if current.ControlPlane.PublishError != "" {
		args = append(args, slog.String("publish_error", current.ControlPlane.PublishError))
		daemon.logger.Warn("failed to publish local state to control plane", args...)
		return
	}

	daemon.logger.Info("published local state to control plane", args...)
}

func localMemberStateForObservation(nodeRole cluster.NodeRole, postgres agentmodel.PostgresStatus) cluster.MemberState {
	if !nodeRole.HasLocalPostgres() {
		return cluster.MemberStateRunning
	}

	if !postgres.Up {
		return cluster.MemberStateFailed
	}

	if postgres.Role == cluster.MemberRoleReplica && (postgres.WAL.ReceiveLSN != "" || postgres.WAL.ReplayLSN != "") {
		return cluster.MemberStateStreaming
	}

	if postgres.Role == cluster.MemberRoleUnknown {
		return cluster.MemberStateUnknown
	}

	return cluster.MemberStateRunning
}
