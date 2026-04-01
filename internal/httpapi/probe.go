package httpapi

import (
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/version"
)

// patroniNodeStatus is the Patroni-compatible node status response shape.
type patroniNodeStatus struct {
	State                    string             `json:"state"`
	Role                     string             `json:"role"`
	PostmasterStartTime      *time.Time         `json:"postmaster_start_time,omitempty"`
	ServerVersion            int                `json:"server_version,omitempty"`
	Timeline                 int64              `json:"timeline,omitempty"`
	DatabaseSystemIdentifier string             `json:"database_system_identifier,omitempty"`
	Pause                    bool               `json:"pause,omitempty"`
	PendingRestart           bool               `json:"pending_restart,omitempty"`
	DCSLastSeen              int64              `json:"dcs_last_seen,omitempty"`
	XLog                     *patroniXLogStatus `json:"xlog,omitempty"`
	Patroni                  patroniIdentity    `json:"patroni"`
}

type patroniIdentity struct {
	Version string `json:"version"`
	Scope   string `json:"scope"`
	Name    string `json:"name"`
}

type patroniXLogStatus struct {
	Location          int64      `json:"location,omitempty"`
	ReceivedLocation  int64      `json:"received_location,omitempty"`
	ReplayedLocation  int64      `json:"replayed_location,omitempty"`
	ReplayedTimestamp *time.Time `json:"replayed_timestamp,omitempty"`
	Paused            bool       `json:"paused,omitempty"`
}

// handleHealth returns 200 when PostgreSQL is up and running, 503 otherwise.
func (srv *Server) handleHealth(c *fiber.Ctx) error {
	status := srv.buildNodeStatus()
	node, ok := srv.store.NodeStatus(srv.nodeName)

	healthy := ok && node.Postgres.Up

	code := fiber.StatusOK
	if !healthy {
		code = fiber.StatusServiceUnavailable
	}

	return c.Status(code).JSON(status)
}

// handleLiveness returns 200 when the local heartbeat loop is fresh, 503 when
// the last observed heartbeat is older than LivenessWindow.
func (srv *Server) handleLiveness(c *fiber.Ctx) error {
	status := srv.buildNodeStatus()
	node, ok := srv.store.NodeStatus(srv.nodeName)

	alive := ok && time.Since(node.ObservedAt) <= srv.livenessWindow

	code := fiber.StatusOK
	if !alive {
		code = fiber.StatusServiceUnavailable
	}

	return c.Status(code).JSON(status)
}

// handleReadiness returns 200 when the node can serve traffic:
//   - primary: PostgreSQL is up
//   - replica: PostgreSQL is up and replication lag is within the optional ?lag threshold
//
// Accepts optional ?lag=<human-readable-bytes> and ?mode=apply|write query params.
func (srv *Server) handleReadiness(c *fiber.Ctx) error {
	status := srv.buildNodeStatus()
	node, ok := srv.store.NodeStatus(srv.nodeName)

	ready := false
	if ok && node.Postgres.Up {
		if node.Role == cluster.MemberRolePrimary {
			ready = true
		} else {
			lagBytes := node.Postgres.Details.ReplicationLagBytes
			maxLag := parseLagBytes(c.Query("lag"))
			ready = maxLag == 0 || lagBytes <= maxLag
		}
	}

	code := fiber.StatusOK
	if !ready {
		code = fiber.StatusServiceUnavailable
	}

	return c.Status(code).JSON(status)
}

// buildNodeStatus constructs a PatroniNodeStatus response from the latest
// locally stored node observation.
func (srv *Server) buildNodeStatus() patroniNodeStatus {
	scope := ""
	if spec, ok := srv.store.ClusterSpec(); ok {
		scope = spec.ClusterName
	}

	identity := patroniIdentity{
		Version: version.Version,
		Scope:   scope,
		Name:    srv.nodeName,
	}

	maintenance := srv.store.MaintenanceStatus()

	node, ok := srv.store.NodeStatus(srv.nodeName)
	if !ok {
		return patroniNodeStatus{
			State:   "stopped",
			Role:    "unknown",
			Pause:   maintenance.Enabled,
			Patroni: identity,
		}
	}

	status := patroniNodeStatus{
		State:                    postgresState(node),
		Role:                     patroniRole(node.Role),
		ServerVersion:            node.Postgres.Details.ServerVersion,
		Timeline:                 node.Postgres.Details.Timeline,
		DatabaseSystemIdentifier: node.Postgres.Details.SystemIdentifier,
		PendingRestart:           node.Postgres.Details.PendingRestart || node.PendingRestart,
		Pause:                    maintenance.Enabled,
		Patroni:                  identity,
	}

	if !node.Postgres.Details.PostmasterStartAt.IsZero() {
		t := node.Postgres.Details.PostmasterStartAt
		status.PostmasterStartTime = &t
	}

	if !node.ControlPlane.LastDCSSeenAt.IsZero() {
		status.DCSLastSeen = node.ControlPlane.LastDCSSeenAt.Unix()
	}

	if xlog := buildXLogStatus(node.Postgres.WAL); xlog != nil {
		status.XLog = xlog
	}

	return status
}

func buildXLogStatus(wal agentmodel.WALProgress) *patroniXLogStatus {
	xlog := &patroniXLogStatus{}
	empty := true

	if v := parseLSN(wal.WriteLSN); v > 0 {
		xlog.Location = v
		empty = false
	}

	if v := parseLSN(wal.ReceiveLSN); v > 0 {
		xlog.ReceivedLocation = v
		empty = false
	}

	if v := parseLSN(wal.ReplayLSN); v > 0 {
		xlog.ReplayedLocation = v
		empty = false
	}

	if !wal.ReplayTimestamp.IsZero() {
		t := wal.ReplayTimestamp
		xlog.ReplayedTimestamp = &t
		empty = false
	}

	if empty {
		return nil
	}

	return xlog
}

func postgresState(node agentmodel.NodeStatus) string {
	if node.Postgres.Up {
		return "running"
	}

	switch node.State {
	case cluster.MemberStateStarting:
		return "starting"
	case cluster.MemberStateStopping:
		return "stopping"
	default:
		return "stopped"
	}
}

func patroniRole(role cluster.MemberRole) string {
	switch role {
	case cluster.MemberRolePrimary:
		return "primary"
	case cluster.MemberRoleReplica:
		return "replica"
	case cluster.MemberRoleStandbyLeader:
		return "standby_leader"
	default:
		return "unknown"
	}
}

// parseLSN converts a PostgreSQL LSN string (e.g. "0/16B6BB0") to bytes.
func parseLSN(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	hi, lo, found := strings.Cut(s, "/")
	if !found {
		return 0
	}

	high, err := strconv.ParseInt(hi, 16, 64)
	if err != nil {
		return 0
	}

	low, err := strconv.ParseInt(lo, 16, 64)
	if err != nil {
		return 0
	}

	return (high << 32) | low
}

// lagSuffixes lists human-readable byte suffixes in longest-first order so
// "B" does not shadow "kB", "MB", etc.
var lagSuffixes = []struct {
	suffix string
	mult   int64
}{
	{"TB", 1 << 40},
	{"GB", 1 << 30},
	{"MB", 1 << 20},
	{"kB", 1 << 10},
	{"B", 1},
}

// parseLagBytes parses a human-readable lag threshold (e.g. "32MB", "1kB",
// "16B", or a plain integer) into bytes. Returns 0 when s is empty or
// unparseable, which the caller interprets as "no lag limit".
func parseLagBytes(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	for _, entry := range lagSuffixes {
		if strings.HasSuffix(s, entry.suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, entry.suffix))
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil || n < 0 {
				return 0
			}

			return n * entry.mult
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0
	}

	return n
}
