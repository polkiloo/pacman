package httpapi

import (
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func (srv *Server) handleHistory(c *fiber.Ctx) error {
	return c.JSON(historyResponse{
		Items: buildHistoryEntryList(srv.store.History()),
	})
}

func (srv *Server) handleMaintenanceStatus(c *fiber.Ctx) error {
	return c.JSON(buildMaintenanceModeStatusJSON(srv.store.MaintenanceStatus()))
}

func (srv *Server) handleMaintenanceUpdate(c *fiber.Ctx) error {
	var requestBody maintenanceModeUpdateRequestJSON
	if err := c.BodyParser(&requestBody); err != nil {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_maintenance_request", "maintenance request body must be valid JSON")
	}

	updated, err := srv.store.UpdateMaintenanceMode(c.UserContext(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     requestBody.Enabled,
		Reason:      requestBody.Reason,
		RequestedBy: requestBody.RequestedBy,
	})
	if err != nil {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_maintenance_request", err.Error())
	}

	srv.logRequest(
		c,
		slog.LevelInfo,
		"updated maintenance mode",
		append(
			auditLogAttrs("maintenance_mode.update"),
			slog.Bool("maintenance_enabled", updated.Enabled),
			slog.String("reason", updated.Reason),
			slog.String("requested_by", updated.RequestedBy),
		)...,
	)

	return c.JSON(buildMaintenanceModeStatusJSON(updated))
}

func (srv *Server) handleDiagnostics(c *fiber.Ctx) error {
	spec, hasSpec := srv.store.ClusterSpec()
	status, hasStatus := srv.store.ClusterStatus()
	if !hasSpec && !hasStatus {
		return writeAPIError(c, fiber.StatusServiceUnavailable, "diagnostics_unavailable", "diagnostics unavailable")
	}

	includeMembers := queryBoolDefault(c.Query("includeMembers"), true)

	return c.JSON(buildDiagnosticsSummaryResponse(
		spec,
		hasSpec,
		status,
		hasStatus,
		srv.store.NodeStatuses(),
		includeMembers,
		time.Now().UTC(),
	))
}

func buildHistoryEntryList(entries []cluster.HistoryEntry) []historyEntryJSON {
	items := make([]historyEntryJSON, len(entries))
	for i, entry := range entries {
		items[i] = buildHistoryEntryJSON(entry)
	}

	return items
}

func buildHistoryEntryJSON(entry cluster.HistoryEntry) historyEntryJSON {
	return historyEntryJSON{
		OperationID: entry.OperationID,
		Kind:        string(entry.Kind),
		Timeline:    entry.Timeline,
		WALLSN:      entry.WALLSN,
		FromMember:  entry.FromMember,
		ToMember:    entry.ToMember,
		Reason:      entry.Reason,
		Result:      string(entry.Result),
		FinishedAt:  entry.FinishedAt,
	}
}

func buildDiagnosticsSummaryResponse(
	spec cluster.ClusterSpec,
	hasSpec bool,
	status cluster.ClusterStatus,
	hasStatus bool,
	nodeStatuses []agentmodel.NodeStatus,
	includeMembers bool,
	generatedAt time.Time,
) diagnosticsSummaryJSON {
	summary := diagnosticsSummaryJSON{
		ClusterName: diagnosticsClusterName(spec, hasSpec, status, hasStatus),
		GeneratedAt: generatedAt,
		Members:     buildMemberDiagnosticsList(status.Members, includeMembers && hasStatus),
		Warnings:    diagnosticsWarnings(spec, hasSpec, status, hasStatus),
	}

	if leader := controlPlaneLeaderName(nodeStatuses); leader != "" {
		summary.ControlPlaneLeader = leader
	}

	if quorumReachable, ok := diagnosticsQuorumReachable(spec, hasSpec, status, hasStatus); ok {
		summary.QuorumReachable = &quorumReachable
	}

	return summary
}

func buildMemberDiagnosticsList(members []cluster.MemberStatus, includeMembers bool) []memberDiagnosticSummaryJSON {
	if !includeMembers {
		return []memberDiagnosticSummaryJSON{}
	}

	items := make([]memberDiagnosticSummaryJSON, len(members))
	for i, member := range members {
		items[i] = buildMemberDiagnosticSummaryJSON(member)
	}

	return items
}

func buildMemberDiagnosticSummaryJSON(member cluster.MemberStatus) memberDiagnosticSummaryJSON {
	item := memberDiagnosticSummaryJSON{
		Name:        member.Name,
		Role:        string(member.Role),
		State:       string(member.State),
		LagBytes:    member.LagBytes,
		NeedsRejoin: member.NeedsRejoin,
	}

	if !member.LastSeenAt.IsZero() {
		t := member.LastSeenAt
		item.LastSeenAt = &t
	}

	return item
}

func diagnosticsClusterName(spec cluster.ClusterSpec, hasSpec bool, status cluster.ClusterStatus, hasStatus bool) string {
	if hasStatus && strings.TrimSpace(status.ClusterName) != "" {
		return status.ClusterName
	}

	if hasSpec {
		return spec.ClusterName
	}

	return ""
}

func diagnosticsWarnings(spec cluster.ClusterSpec, hasSpec bool, status cluster.ClusterStatus, hasStatus bool) []string {
	if !hasStatus {
		return []string{"cluster status unavailable"}
	}

	warnings := []string{}

	if status.Phase != cluster.ClusterPhaseHealthy {
		warnings = append(warnings, fmt.Sprintf("cluster phase is %s", status.Phase))
	}

	if status.Maintenance.Enabled {
		warnings = append(warnings, "maintenance mode is enabled")
	}

	if status.ActiveOperation != nil {
		warnings = append(warnings, fmt.Sprintf("active operation %s is %s", status.ActiveOperation.Kind, status.ActiveOperation.State))
	}

	if status.CurrentPrimary == "" && len(status.Members) > 0 {
		warnings = append(warnings, "no writable primary observed")
	}

	for _, member := range status.Members {
		if !member.Healthy {
			warnings = append(warnings, fmt.Sprintf("member %s is unhealthy", member.Name))
		}
		if member.NeedsRejoin {
			warnings = append(warnings, fmt.Sprintf("member %s requires rejoin", member.Name))
		}
	}

	if quorumReachable, ok := diagnosticsQuorumReachable(spec, hasSpec, status, hasStatus); ok && !quorumReachable {
		warnings = append(warnings, "quorum is not reachable")
	}

	return warnings
}

func diagnosticsQuorumReachable(spec cluster.ClusterSpec, hasSpec bool, status cluster.ClusterStatus, hasStatus bool) (bool, bool) {
	if !hasStatus {
		return false, false
	}

	totalVoters, reachableVoters := diagnosticsQuorumVoteCounts(spec, hasSpec, status)
	requiredVoters := 0
	if totalVoters > 0 {
		requiredVoters = totalVoters/2 + 1
	}

	return requiredVoters == 0 || reachableVoters >= requiredVoters, true
}

func diagnosticsQuorumVoteCounts(spec cluster.ClusterSpec, hasSpec bool, status cluster.ClusterStatus) (int, int) {
	observed := make(map[string]cluster.MemberStatus, len(status.Members))
	for _, member := range status.Members {
		observed[member.Name] = member.Clone()
	}

	if hasSpec && len(spec.Members) > 0 {
		reachable := 0
		for _, member := range spec.Members {
			if observedMember, ok := observed[member.Name]; ok && observedMember.Healthy {
				reachable++
			}
		}

		return len(spec.Members), reachable
	}

	reachable := 0
	for _, member := range status.Members {
		if member.Healthy {
			reachable++
		}
	}

	return len(status.Members), reachable
}

func controlPlaneLeaderName(nodeStatuses []agentmodel.NodeStatus) string {
	cloned := append([]agentmodel.NodeStatus(nil), nodeStatuses...)
	sort.Slice(cloned, func(left, right int) bool {
		return cloned[left].NodeName < cloned[right].NodeName
	})

	for _, node := range cloned {
		if node.ControlPlane.Leader {
			return node.NodeName
		}
	}

	return ""
}

func queryBoolDefault(value string, defaultValue bool) bool {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}

	return parsed
}
