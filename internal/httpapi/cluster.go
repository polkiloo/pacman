package httpapi

import (
	"time"

	"github.com/gofiber/fiber/v2"

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
	Members             []memberStatusJSON         `json:"members"`
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

// clusterSpecResponse is the JSON shape for GET /api/v1/cluster/spec.
type clusterSpecResponse struct {
	ClusterName string               `json:"clusterName"`
	Generation  int64                `json:"generation"`
	Maintenance maintenanceDesiredJSON `json:"maintenance"`
	Failover    failoverPolicyJSON   `json:"failover"`
	Switchover  switchoverPolicyJSON `json:"switchover"`
	Postgres    postgresPolicyJSON   `json:"postgres"`
	Members     []memberSpecJSON     `json:"members,omitempty"`
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

func buildClusterStatusResponse(status cluster.ClusterStatus) clusterStatusResponse {
	members := make([]memberStatusJSON, len(status.Members))
	for i, m := range status.Members {
		members[i] = buildMemberStatusJSON(m)
	}

	resp := clusterStatusResponse{
		ClusterName:    status.ClusterName,
		Phase:          string(status.Phase),
		CurrentPrimary: status.CurrentPrimary,
		CurrentEpoch:   int64(status.CurrentEpoch),
		ObservedAt:     status.ObservedAt,
		Maintenance:    buildMaintenanceModeStatusJSON(status.Maintenance),
		Members:        members,
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
			AllowScheduled:                            spec.Switchover.AllowScheduled,
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
