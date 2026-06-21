package pacmanctl

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"gopkg.in/yaml.v3"
)

func renderOutput[T any](writer io.Writer, format string, payload T, renderText func(io.Writer, T) error) error {
	switch format {
	case outputFormatText, outputFormatPretty:
		return renderText(writer, payload)
	case outputFormatJSON:
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	case outputFormatYAML:
		encoder := yaml.NewEncoder(writer)
		encoder.SetIndent(2)
		defer encoder.Close()
		return encoder.Encode(payload)
	default:
		return fmt.Errorf("%w: %s", errUnsupportedOutputFormat, format)
	}
}

func renderClusterStatusText(writer io.Writer, status clusterStatusResponse) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tab, "Cluster Name:\t%s\n", orDash(status.ClusterName))
	fmt.Fprintf(tab, "Phase:\t%s\n", orDash(status.Phase))
	fmt.Fprintf(tab, "Current Primary:\t%s\n", orDash(status.CurrentPrimary))
	fmt.Fprintf(tab, "Current Epoch:\t%d\n", status.CurrentEpoch)
	fmt.Fprintf(tab, "Observed At:\t%s\n", formatTime(status.ObservedAt))
	fmt.Fprintf(tab, "Maintenance:\t%s\n", formatMaintenance(status.Maintenance))
	fmt.Fprintf(tab, "Active Operation:\t%s\n", formatOperation(status.ActiveOperation))
	fmt.Fprintf(tab, "Scheduled Switchover:\t%s\n", formatScheduledSwitchover(status.ScheduledSwitchover))
	fmt.Fprintf(tab, "Reinit:\t%s\n", formatReinitStatus(status.Reinit))
	fmt.Fprintf(tab, "Members:\t%d\n", len(status.Members))

	if err := tab.Flush(); err != nil {
		return err
	}

	if len(status.Members) == 0 {
		return nil
	}

	if _, err := fmt.Fprintln(writer); err != nil {
		return err
	}

	return writeMembersTable(writer, status.Members)
}

func renderMembersText(writer io.Writer, response membersResponse) error {
	if len(response.Items) == 0 {
		_, err := fmt.Fprintln(writer, "No members.")
		return err
	}

	return writeMembersTable(writer, response.Items)
}

func renderOperationAcceptedText(writer io.Writer, response operationAcceptedResponse) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	topMsg := strings.TrimSpace(response.Message)
	opMsg := strings.TrimSpace(response.Operation.Message)

	if topMsg != "" {
		fmt.Fprintf(tab, "Message:\t%s\n", topMsg)
	}
	fmt.Fprintf(tab, "Operation ID:\t%s\n", orDash(response.Operation.ID))
	fmt.Fprintf(tab, "Kind:\t%s\n", orDash(response.Operation.Kind))
	fmt.Fprintf(tab, "State:\t%s\n", orDash(response.Operation.State))
	fmt.Fprintf(tab, "Requested By:\t%s\n", orDash(response.Operation.RequestedBy))
	fmt.Fprintf(tab, "Requested At:\t%s\n", formatTime(response.Operation.RequestedAt))
	fmt.Fprintf(tab, "Reason:\t%s\n", orDash(response.Operation.Reason))
	fmt.Fprintf(tab, "From Member:\t%s\n", orDash(response.Operation.FromMember))
	fmt.Fprintf(tab, "To Member:\t%s\n", orDash(response.Operation.ToMember))
	fmt.Fprintf(tab, "Scheduled At:\t%s\n", formatOptionalTime(response.Operation.ScheduledAt))
	fmt.Fprintf(tab, "Started At:\t%s\n", formatOptionalTime(response.Operation.StartedAt))
	fmt.Fprintf(tab, "Completed At:\t%s\n", formatOptionalTime(response.Operation.CompletedAt))
	fmt.Fprintf(tab, "Result:\t%s\n", orDash(response.Operation.Result))
	if opMsg != "" && opMsg != topMsg {
		fmt.Fprintf(tab, "Operation Message:\t%s\n", opMsg)
	}

	return tab.Flush()
}

func renderMaintenanceStatusText(writer io.Writer, status maintenanceModeStatusJSON) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tab, "Enabled:\t%t\n", status.Enabled)
	fmt.Fprintf(tab, "Reason:\t%s\n", orDash(status.Reason))
	fmt.Fprintf(tab, "Requested By:\t%s\n", orDash(status.RequestedBy))
	fmt.Fprintf(tab, "Updated At:\t%s\n", formatOptionalTime(status.UpdatedAt))

	return tab.Flush()
}

func renderHistoryText(writer io.Writer, response historyResponse) error {
	if len(response.Items) == 0 {
		_, err := fmt.Fprintln(writer, "No history.")
		return err
	}

	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tab, "OPERATION ID\tKIND\tRESULT\tTIMELINE\tWAL LSN\tFROM\tTO\tFINISHED AT\tREASON"); err != nil {
		return err
	}

	for _, item := range response.Items {
		if _, err := fmt.Fprintf(
			tab,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			orDash(item.OperationID),
			orDash(item.Kind),
			orDash(item.Result),
			formatOptionalInt64(item.Timeline),
			orDash(item.WALLSN),
			orDash(item.FromMember),
			orDash(item.ToMember),
			formatTime(item.FinishedAt),
			orDash(item.Reason),
		); err != nil {
			return err
		}
	}

	return tab.Flush()
}

func renderClusterSpecText(writer io.Writer, spec clusterSpecResponse) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tab, "Cluster Name:\t%s\n", orDash(spec.ClusterName))
	fmt.Fprintf(tab, "Generation:\t%d\n", spec.Generation)
	fmt.Fprintf(tab, "Maintenance Enabled:\t%t\n", spec.Maintenance.Enabled)
	fmt.Fprintf(tab, "Maintenance Default Reason:\t%s\n", orDash(spec.Maintenance.DefaultReason))
	fmt.Fprintf(tab, "Failover Mode:\t%s\n", orDash(spec.Failover.Mode))
	fmt.Fprintf(tab, "Maximum Lag Bytes:\t%s\n", formatOptionalInt64(spec.Failover.MaximumLagBytes))
	fmt.Fprintf(tab, "Check Timeline:\t%t\n", spec.Failover.CheckTimeline)
	fmt.Fprintf(tab, "Require Quorum:\t%t\n", spec.Failover.RequireQuorum)
	fmt.Fprintf(tab, "Fencing Required:\t%t\n", spec.Failover.FencingRequired)
	fmt.Fprintf(tab, "Allow Scheduled:\t%t\n", spec.Switchover.AllowScheduled)
	fmt.Fprintf(tab, "Require Specific Candidate During Maintenance:\t%t\n", spec.Switchover.RequireSpecificCandidateDuringMaintenance)
	fmt.Fprintf(tab, "Synchronous Mode:\t%s\n", orDash(spec.Postgres.SynchronousMode))
	fmt.Fprintf(tab, "Use pgRewind:\t%t\n", spec.Postgres.UsePgRewind)
	fmt.Fprintf(tab, "PostgreSQL Parameters:\t%d\n", len(spec.Postgres.Parameters))
	fmt.Fprintf(tab, "Members:\t%d\n", len(spec.Members))

	if err := tab.Flush(); err != nil {
		return err
	}

	if len(spec.Postgres.Parameters) > 0 {
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer, "PostgreSQL Parameters:"); err != nil {
			return err
		}

		paramTab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(paramTab, "NAME\tVALUE"); err != nil {
			return err
		}
		names := sortedMapKeys(spec.Postgres.Parameters)
		for _, name := range names {
			value := spec.Postgres.Parameters[name]
			if _, err := fmt.Fprintf(paramTab, "%s\t%s\n", name, formatAny(value)); err != nil {
				return err
			}
		}
		if err := paramTab.Flush(); err != nil {
			return err
		}
	}

	if len(spec.Members) == 0 {
		return nil
	}

	if _, err := fmt.Fprintln(writer); err != nil {
		return err
	}

	memberTab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(memberTab, "NAME\tPRIORITY\tNO FAILOVER\tTAGS"); err != nil {
		return err
	}
	for _, member := range spec.Members {
		if _, err := fmt.Fprintf(
			memberTab,
			"%s\t%s\t%t\t%s\n",
			orDash(member.Name),
			formatOptionalInt(member.Priority),
			member.NoFailover,
			formatMap(member.Tags),
		); err != nil {
			return err
		}
	}

	return memberTab.Flush()
}

func renderNodeStatusText(writer io.Writer, status nodeStatusResponse) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tab, "Node Name:\t%s\n", orDash(status.NodeName))
	fmt.Fprintf(tab, "Member Name:\t%s\n", orDash(status.MemberName))
	fmt.Fprintf(tab, "Role:\t%s\n", orDash(status.Role))
	fmt.Fprintf(tab, "State:\t%s\n", orDash(status.State))
	fmt.Fprintf(tab, "Pending Restart:\t%t\n", status.PendingRestart)
	fmt.Fprintf(tab, "Needs Rejoin:\t%t\n", status.NeedsRejoin)
	fmt.Fprintf(tab, "Tags:\t%s\n", formatMap(status.Tags))
	fmt.Fprintf(tab, "Observed At:\t%s\n", formatTime(status.ObservedAt))
	fmt.Fprintf(tab, "Postgres Managed:\t%t\n", status.Postgres.Managed)
	fmt.Fprintf(tab, "Postgres Address:\t%s\n", orDash(status.Postgres.Address))
	fmt.Fprintf(tab, "Postgres Checked At:\t%s\n", formatTime(status.Postgres.CheckedAt))
	fmt.Fprintf(tab, "Postgres Up:\t%t\n", status.Postgres.Up)
	fmt.Fprintf(tab, "Postgres Role:\t%s\n", orDash(status.Postgres.Role))
	fmt.Fprintf(tab, "Postgres Recovery Known:\t%t\n", status.Postgres.RecoveryKnown)
	fmt.Fprintf(tab, "Postgres In Recovery:\t%t\n", status.Postgres.InRecovery)
	fmt.Fprintf(tab, "Server Version:\t%s\n", formatOptionalInt(status.Postgres.Details.ServerVersion))
	fmt.Fprintf(tab, "System Identifier:\t%s\n", orDash(status.Postgres.Details.SystemIdentifier))
	fmt.Fprintf(tab, "Timeline:\t%s\n", formatOptionalInt64(status.Postgres.Details.Timeline))
	fmt.Fprintf(tab, "Postmaster Start:\t%s\n", formatOptionalTime(status.Postgres.Details.PostmasterStartAt))
	fmt.Fprintf(tab, "Replication Lag Bytes:\t%s\n", formatOptionalInt64(status.Postgres.Details.ReplicationLagBytes))
	fmt.Fprintf(tab, "Write LSN:\t%s\n", orDash(status.Postgres.WAL.WriteLSN))
	fmt.Fprintf(tab, "Flush LSN:\t%s\n", orDash(status.Postgres.WAL.FlushLSN))
	fmt.Fprintf(tab, "Receive LSN:\t%s\n", orDash(status.Postgres.WAL.ReceiveLSN))
	fmt.Fprintf(tab, "Replay LSN:\t%s\n", orDash(status.Postgres.WAL.ReplayLSN))
	fmt.Fprintf(tab, "Replay Timestamp:\t%s\n", formatOptionalTime(status.Postgres.WAL.ReplayTimestamp))
	fmt.Fprintf(tab, "Availability Error:\t%s\n", orDash(status.Postgres.Errors.Availability))
	fmt.Fprintf(tab, "State Error:\t%s\n", orDash(status.Postgres.Errors.State))
	fmt.Fprintf(tab, "Cluster Reachable:\t%t\n", status.ControlPlane.ClusterReachable)
	fmt.Fprintf(tab, "Control-Plane Leader:\t%t\n", status.ControlPlane.Leader)
	fmt.Fprintf(tab, "Last Heartbeat At:\t%s\n", formatOptionalTime(status.ControlPlane.LastHeartbeatAt))
	fmt.Fprintf(tab, "Last DCS Seen At:\t%s\n", formatOptionalTime(status.ControlPlane.LastDCSSeenAt))
	fmt.Fprintf(tab, "Publish Error:\t%s\n", orDash(status.ControlPlane.PublishError))

	return tab.Flush()
}

func renderDiagnosticsText(writer io.Writer, summary diagnosticsSummaryJSON) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tab, "Cluster Name:\t%s\n", orDash(summary.ClusterName))
	fmt.Fprintf(tab, "Generated At:\t%s\n", formatTime(summary.GeneratedAt))
	fmt.Fprintf(tab, "Control-Plane Leader:\t%s\n", orDash(summary.ControlPlaneLeader))
	fmt.Fprintf(tab, "Quorum Reachable:\t%s\n", formatOptionalBool(summary.QuorumReachable))
	fmt.Fprintf(tab, "Warnings:\t%d\n", len(summary.Warnings))
	fmt.Fprintf(tab, "Members:\t%d\n", len(summary.Members))

	if err := tab.Flush(); err != nil {
		return err
	}

	if len(summary.Warnings) > 0 {
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer, "Warnings:"); err != nil {
			return err
		}
		for _, warning := range summary.Warnings {
			if _, err := fmt.Fprintf(writer, "- %s\n", warning); err != nil {
				return err
			}
		}
	}

	if len(summary.Members) == 0 {
		return nil
	}

	if _, err := fmt.Fprintln(writer); err != nil {
		return err
	}

	memberTab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(memberTab, "NAME\tROLE\tSTATE\tLAG BYTES\tNEEDS REJOIN\tLAST SEEN"); err != nil {
		return err
	}
	for _, member := range summary.Members {
		if _, err := fmt.Fprintf(
			memberTab,
			"%s\t%s\t%s\t%s\t%t\t%s\n",
			orDash(member.Name),
			orDash(member.Role),
			orDash(member.State),
			formatOptionalInt64(member.LagBytes),
			member.NeedsRejoin,
			formatOptionalTime(member.LastSeenAt),
		); err != nil {
			return err
		}
	}

	return memberTab.Flush()
}

func writeMembersTable(writer io.Writer, members []memberStatusJSON) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tab, "NAME\tROLE\tSTATE\tHEALTHY\tLEADER\tTIMELINE\tLAG BYTES\tREINIT STATE\tREINIT RESULT\tLAST SEEN"); err != nil {
		return err
	}

	for _, member := range members {
		if _, err := fmt.Fprintf(
			tab,
			"%s\t%s\t%s\t%t\t%t\t%s\t%s\t%s\t%s\t%s\n",
			orDash(member.Name),
			orDash(member.Role),
			orDash(member.State),
			member.Healthy,
			member.Leader,
			formatOptionalInt64(member.Timeline),
			formatOptionalInt64(member.LagBytes),
			reinitState(member.Reinit),
			reinitResult(member.Reinit),
			formatTime(member.LastSeenAt),
		); err != nil {
			return err
		}
	}

	return tab.Flush()
}
