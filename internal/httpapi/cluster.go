package httpapi

import (
	"fmt"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// handleClusterStatus returns the current cluster topology and observed state.
func (srv *Server) handleClusterStatus(c *fiber.Ctx) error {
	status, ok := srv.store.ClusterStatus()
	if !ok {
		return writeAPIError(c, fiber.StatusServiceUnavailable, "cluster_status_unavailable", "cluster status unavailable")
	}

	return c.JSON(buildClusterStatusResponse(status))
}

// handleClusterSpec returns the current desired cluster specification.
func (srv *Server) handleClusterSpec(c *fiber.Ctx) error {
	spec, ok := srv.store.ClusterSpec()
	if !ok {
		return writeAPIError(c, fiber.StatusServiceUnavailable, "cluster_spec_unavailable", "cluster spec unavailable")
	}

	return c.JSON(buildClusterSpecResponse(spec))
}

// handleMembers returns the aggregated observed member view.
func (srv *Server) handleMembers(c *fiber.Ctx) error {
	status, ok := srv.store.ClusterStatus()
	if !ok {
		return writeAPIError(c, fiber.StatusServiceUnavailable, "cluster_status_unavailable", "cluster status unavailable")
	}

	return c.JSON(membersResponse{
		Items: buildMemberStatusList(status.Members),
	})
}

// handleNodeStatus returns the detailed node-local status for the requested node.
func (srv *Server) handleNodeStatus(c *fiber.Ctx) error {
	nodeName := c.Params("nodeName")
	node, ok := srv.store.NodeStatus(nodeName)
	if !ok {
		return writeAPIError(c, fiber.StatusNotFound, "node_not_found", fmt.Sprintf("node %q was not found", nodeName))
	}

	return c.JSON(buildNodeStatusResponse(node))
}

func buildClusterStatusResponse(status cluster.ClusterStatus) clusterStatusResponse {
	resp := clusterStatusResponse{
		ClusterName:    status.ClusterName,
		Phase:          string(status.Phase),
		CurrentPrimary: status.CurrentPrimary,
		CurrentEpoch:   int64(status.CurrentEpoch),
		ObservedAt:     status.ObservedAt,
		Maintenance:    buildMaintenanceModeStatusJSON(status.Maintenance),
		Members:        buildMemberStatusList(status.Members),
	}

	if status.ActiveOperation != nil {
		op := buildOperationJSON(*status.ActiveOperation)
		resp.ActiveOperation = &op
	}

	if status.ScheduledSwitchover != nil {
		sw := buildScheduledSwitchoverJSON(*status.ScheduledSwitchover)
		resp.ScheduledSwitchover = &sw
	}

	return resp
}

func buildMemberStatusList(members []cluster.MemberStatus) []memberStatusJSON {
	items := make([]memberStatusJSON, len(members))
	for i, m := range members {
		items[i] = buildMemberStatusJSON(m)
	}

	return items
}

func buildMemberStatusJSON(m cluster.MemberStatus) memberStatusJSON {
	return memberStatusJSON{
		Name:        m.Name,
		APIURL:      m.APIURL,
		Host:        m.Host,
		Port:        m.Port,
		Role:        string(m.Role),
		State:       string(m.State),
		Healthy:     m.Healthy,
		Leader:      m.Leader,
		Timeline:    m.Timeline,
		LagBytes:    m.LagBytes,
		Priority:    m.Priority,
		NoFailover:  m.NoFailover,
		NeedsRejoin: m.NeedsRejoin,
		Tags:        m.Tags,
		LastSeenAt:  m.LastSeenAt,
	}
}

func buildMaintenanceModeStatusJSON(m cluster.MaintenanceModeStatus) maintenanceModeStatusJSON {
	j := maintenanceModeStatusJSON{
		Enabled:     m.Enabled,
		Reason:      m.Reason,
		RequestedBy: m.RequestedBy,
	}

	if !m.UpdatedAt.IsZero() {
		j.UpdatedAt = &m.UpdatedAt
	}

	return j
}

func buildOperationJSON(op cluster.Operation) operationJSON {
	j := operationJSON{
		ID:          op.ID,
		Kind:        string(op.Kind),
		State:       string(op.State),
		RequestedBy: op.RequestedBy,
		RequestedAt: op.RequestedAt,
		Reason:      op.Reason,
		FromMember:  op.FromMember,
		ToMember:    op.ToMember,
		Result:      string(op.Result),
		Message:     op.Message,
	}

	if !op.ScheduledAt.IsZero() {
		j.ScheduledAt = &op.ScheduledAt
	}

	if !op.StartedAt.IsZero() {
		j.StartedAt = &op.StartedAt
	}

	if !op.CompletedAt.IsZero() {
		j.CompletedAt = &op.CompletedAt
	}

	return j
}

func buildScheduledSwitchoverJSON(sw cluster.ScheduledSwitchover) scheduledSwitchoverJSON {
	return scheduledSwitchoverJSON{
		At:   sw.At,
		From: sw.From,
		To:   sw.To,
	}
}

func buildNodeStatusResponse(node agentmodel.NodeStatus) nodeStatusResponse {
	return nodeStatusResponse{
		NodeName:       node.NodeName,
		MemberName:     node.MemberName,
		Role:           string(node.Role),
		State:          string(node.State),
		PendingRestart: node.PendingRestart,
		NeedsRejoin:    node.NeedsRejoin,
		Tags:           node.Tags,
		Postgres:       buildPostgresLocalStatusJSON(node.Postgres),
		ControlPlane:   buildControlPlaneLocalStatusJSON(node.ControlPlane),
		ObservedAt:     node.ObservedAt,
	}
}

func buildPostgresLocalStatusJSON(status agentmodel.PostgresStatus) postgresLocalStatusJSON {
	return postgresLocalStatusJSON{
		Managed:       status.Managed,
		Address:       status.Address,
		CheckedAt:     status.CheckedAt,
		Up:            status.Up,
		Role:          string(status.Role),
		RecoveryKnown: status.RecoveryKnown,
		InRecovery:    status.InRecovery,
		Details:       buildPostgresDetailsJSON(status.Details),
		WAL:           buildWALProgressJSON(status.WAL),
		Errors:        buildPostgresErrorsJSON(status.Errors),
	}
}

func buildPostgresDetailsJSON(details agentmodel.PostgresDetails) postgresDetailsJSON {
	j := postgresDetailsJSON{
		ServerVersion:       details.ServerVersion,
		PendingRestart:      details.PendingRestart,
		SystemIdentifier:    details.SystemIdentifier,
		Timeline:            details.Timeline,
		ReplicationLagBytes: details.ReplicationLagBytes,
	}

	if !details.PostmasterStartAt.IsZero() {
		t := details.PostmasterStartAt
		j.PostmasterStartAt = &t
	}

	return j
}

func buildWALProgressJSON(wal agentmodel.WALProgress) walProgressJSON {
	j := walProgressJSON{
		WriteLSN:   wal.WriteLSN,
		FlushLSN:   wal.FlushLSN,
		ReceiveLSN: wal.ReceiveLSN,
		ReplayLSN:  wal.ReplayLSN,
	}

	if !wal.ReplayTimestamp.IsZero() {
		t := wal.ReplayTimestamp
		j.ReplayTimestamp = &t
	}

	return j
}

func buildControlPlaneLocalStatusJSON(status agentmodel.ControlPlaneStatus) controlPlaneLocalStatusJSON {
	j := controlPlaneLocalStatusJSON{
		ClusterReachable: status.ClusterReachable,
		Leader:           status.Leader,
		PublishError:     status.PublishError,
	}

	if !status.LastHeartbeatAt.IsZero() {
		t := status.LastHeartbeatAt
		j.LastHeartbeatAt = &t
	}

	if !status.LastDCSSeenAt.IsZero() {
		t := status.LastDCSSeenAt
		j.LastDCSSeenAt = &t
	}

	return j
}

func buildPostgresErrorsJSON(errors agentmodel.PostgresErrors) postgresErrorsJSON {
	return postgresErrorsJSON{
		Availability: errors.Availability,
		State:        errors.State,
	}
}

func buildClusterSpecResponse(spec cluster.ClusterSpec) clusterSpecResponse {
	members := make([]memberSpecJSON, len(spec.Members))
	for i, m := range spec.Members {
		members[i] = memberSpecJSON{
			Name:       m.Name,
			Priority:   m.Priority,
			NoFailover: m.NoFailover,
			Tags:       m.Tags,
		}
	}

	return clusterSpecResponse{
		ClusterName: spec.ClusterName,
		Generation:  int64(spec.Generation),
		Maintenance: maintenanceDesiredJSON{
			Enabled:       spec.Maintenance.Enabled,
			DefaultReason: spec.Maintenance.DefaultReason,
		},
		Failover: failoverPolicyJSON{
			Mode:            string(spec.Failover.Mode),
			MaximumLagBytes: spec.Failover.MaximumLagBytes,
			CheckTimeline:   spec.Failover.CheckTimeline,
			RequireQuorum:   spec.Failover.RequireQuorum,
			FencingRequired: spec.Failover.FencingRequired,
		},
		Switchover: switchoverPolicyJSON{
			AllowScheduled: spec.Switchover.AllowScheduled,
			RequireSpecificCandidateDuringMaintenance: spec.Switchover.RequireSpecificCandidateDuringMaintenance,
		},
		Postgres: postgresPolicyJSON{
			SynchronousMode: string(spec.Postgres.SynchronousMode),
			UsePgRewind:     spec.Postgres.UsePgRewind,
			Parameters:      spec.Postgres.Parameters,
		},
		Members: members,
	}
}
