package httpapi

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// clusterStatusResponse is the JSON shape for GET /api/v1/cluster.
type clusterStatusResponse struct {
	ClusterName         string                    `json:"clusterName"`
	Phase               string                    `json:"phase"`
	CurrentPrimary      string                    `json:"currentPrimary,omitempty"`
	CurrentEpoch        int64                     `json:"currentEpoch"`
	ObservedAt          time.Time                 `json:"observedAt"`
	Maintenance         maintenanceModeStatusJSON `json:"maintenance"`
	ActiveOperation     *operationJSON            `json:"activeOperation,omitempty"`
	ScheduledSwitchover *scheduledSwitchoverJSON  `json:"scheduledSwitchover,omitempty"`
	Members             []memberStatusJSON        `json:"members"`
}

type memberStatusJSON struct {
	Name        string         `json:"name"`
	APIURL      string         `json:"apiUrl,omitempty"`
	Host        string         `json:"host,omitempty"`
	Port        int            `json:"port,omitempty"`
	Role        string         `json:"role"`
	State       string         `json:"state"`
	Healthy     bool           `json:"healthy"`
	Leader      bool           `json:"leader,omitempty"`
	Timeline    int64          `json:"timeline,omitempty"`
	LagBytes    int64          `json:"lagBytes,omitempty"`
	Priority    int            `json:"priority,omitempty"`
	NoFailover  bool           `json:"noFailover,omitempty"`
	NeedsRejoin bool           `json:"needsRejoin,omitempty"`
	Tags        map[string]any `json:"tags,omitempty"`
	LastSeenAt  time.Time      `json:"lastSeenAt"`
}

type maintenanceModeStatusJSON struct {
	Enabled     bool       `json:"enabled"`
	Reason      string     `json:"reason,omitempty"`
	RequestedBy string     `json:"requestedBy,omitempty"`
	UpdatedAt   *time.Time `json:"updatedAt,omitempty"`
}

type operationJSON struct {
	ID          string     `json:"id"`
	Kind        string     `json:"kind"`
	State       string     `json:"state"`
	RequestedBy string     `json:"requestedBy,omitempty"`
	RequestedAt time.Time  `json:"requestedAt"`
	Reason      string     `json:"reason,omitempty"`
	FromMember  string     `json:"fromMember,omitempty"`
	ToMember    string     `json:"toMember,omitempty"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Result      string     `json:"result,omitempty"`
	Message     string     `json:"message,omitempty"`
}

type scheduledSwitchoverJSON struct {
	At   time.Time `json:"at"`
	From string    `json:"from"`
	To   string    `json:"to,omitempty"`
}

type membersResponse struct {
	Items []memberStatusJSON `json:"items"`
}

type nodeStatusResponse struct {
	NodeName       string                      `json:"nodeName"`
	MemberName     string                      `json:"memberName,omitempty"`
	Role           string                      `json:"role"`
	State          string                      `json:"state"`
	PendingRestart bool                        `json:"pendingRestart,omitempty"`
	NeedsRejoin    bool                        `json:"needsRejoin,omitempty"`
	Tags           map[string]any              `json:"tags,omitempty"`
	Postgres       postgresLocalStatusJSON     `json:"postgres"`
	ControlPlane   controlPlaneLocalStatusJSON `json:"controlPlane"`
	ObservedAt     time.Time                   `json:"observedAt"`
}

type postgresLocalStatusJSON struct {
	Managed       bool                `json:"managed"`
	Address       string              `json:"address,omitempty"`
	CheckedAt     time.Time           `json:"checkedAt"`
	Up            bool                `json:"up"`
	Role          string              `json:"role"`
	RecoveryKnown bool                `json:"recoveryKnown"`
	InRecovery    bool                `json:"inRecovery"`
	Details       postgresDetailsJSON `json:"details"`
	WAL           walProgressJSON     `json:"wal"`
	Errors        postgresErrorsJSON  `json:"errors"`
}

type postgresDetailsJSON struct {
	ServerVersion       int        `json:"serverVersion,omitempty"`
	PendingRestart      bool       `json:"pendingRestart,omitempty"`
	SystemIdentifier    string     `json:"systemIdentifier,omitempty"`
	Timeline            int64      `json:"timeline,omitempty"`
	PostmasterStartAt   *time.Time `json:"postmasterStartAt,omitempty"`
	ReplicationLagBytes int64      `json:"replicationLagBytes,omitempty"`
}

type walProgressJSON struct {
	WriteLSN        string     `json:"writeLsn,omitempty"`
	FlushLSN        string     `json:"flushLsn,omitempty"`
	ReceiveLSN      string     `json:"receiveLsn,omitempty"`
	ReplayLSN       string     `json:"replayLsn,omitempty"`
	ReplayTimestamp *time.Time `json:"replayTimestamp,omitempty"`
}

type controlPlaneLocalStatusJSON struct {
	ClusterReachable bool       `json:"clusterReachable"`
	Leader           bool       `json:"leader,omitempty"`
	LastHeartbeatAt  *time.Time `json:"lastHeartbeatAt,omitempty"`
	LastDCSSeenAt    *time.Time `json:"lastDcsSeenAt,omitempty"`
	PublishError     string     `json:"publishError,omitempty"`
}

type postgresErrorsJSON struct {
	Availability string `json:"availability,omitempty"`
	State        string `json:"state,omitempty"`
}

type errorResponseJSON struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// clusterSpecResponse is the JSON shape for GET /api/v1/cluster/spec.
type clusterSpecResponse struct {
	ClusterName string                 `json:"clusterName"`
	Generation  int64                  `json:"generation"`
	Maintenance maintenanceDesiredJSON `json:"maintenance"`
	Failover    failoverPolicyJSON     `json:"failover"`
	Switchover  switchoverPolicyJSON   `json:"switchover"`
	Postgres    postgresPolicyJSON     `json:"postgres"`
	Members     []memberSpecJSON       `json:"members,omitempty"`
}

type maintenanceDesiredJSON struct {
	Enabled       bool   `json:"enabled,omitempty"`
	DefaultReason string `json:"defaultReason,omitempty"`
}

type failoverPolicyJSON struct {
	Mode            string `json:"mode,omitempty"`
	MaximumLagBytes int64  `json:"maximumLagBytes,omitempty"`
	CheckTimeline   bool   `json:"checkTimeline,omitempty"`
	RequireQuorum   bool   `json:"requireQuorum,omitempty"`
	FencingRequired bool   `json:"fencingRequired,omitempty"`
}

type switchoverPolicyJSON struct {
	AllowScheduled                            bool `json:"allowScheduled,omitempty"`
	RequireSpecificCandidateDuringMaintenance bool `json:"requireSpecificCandidateDuringMaintenance,omitempty"`
}

type postgresPolicyJSON struct {
	SynchronousMode string         `json:"synchronousMode,omitempty"`
	UsePgRewind     bool           `json:"usePgRewind,omitempty"`
	Parameters      map[string]any `json:"parameters,omitempty"`
}

type memberSpecJSON struct {
	Name       string         `json:"name"`
	Priority   int            `json:"priority,omitempty"`
	NoFailover bool           `json:"noFailover,omitempty"`
	Tags       map[string]any `json:"tags,omitempty"`
}

// handleClusterStatus returns the current cluster topology and observed state.
func (srv *Server) handleClusterStatus(c *fiber.Ctx) error {
	status, ok := srv.store.ClusterStatus()
	if !ok {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "cluster status unavailable",
		})
	}

	return c.JSON(buildClusterStatusResponse(status))
}

// handleClusterSpec returns the current desired cluster specification.
func (srv *Server) handleClusterSpec(c *fiber.Ctx) error {
	spec, ok := srv.store.ClusterSpec()
	if !ok {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "cluster spec unavailable",
		})
	}

	return c.JSON(buildClusterSpecResponse(spec))
}

// handleMembers returns the aggregated observed member view.
func (srv *Server) handleMembers(c *fiber.Ctx) error {
	status, ok := srv.store.ClusterStatus()
	if !ok {
		return c.Status(fiber.StatusServiceUnavailable).JSON(errorResponseJSON{
			Error:   "cluster_status_unavailable",
			Message: "cluster status unavailable",
		})
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
		return c.Status(fiber.StatusNotFound).JSON(errorResponseJSON{
			Error:   "node_not_found",
			Message: fmt.Sprintf("node %q was not found", nodeName),
		})
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
