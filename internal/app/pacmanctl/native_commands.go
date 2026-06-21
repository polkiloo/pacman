package pacmanctl

import (
	"context"
	"fmt"
	"strings"
)

func (a *App) printCommandHelp() error {
	_, err := fmt.Fprintln(a.stdout, "pacmanctl commands: cluster status, cluster spec show, cluster switchover, cluster failover, cluster reinit, cluster maintenance enable, cluster maintenance disable, members list, history list, node status, diagnostics show, patronictl-compatible: list, topology, history, show-config, pause, resume, switchover, failover")
	return err
}

func (a *App) runCluster(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: cluster")
	}

	switch args[0] {
	case "status":
		format, err := parseOutputFormat("cluster status", args[1:], a.stderr)
		if err != nil {
			return err
		}

		status, err := client.clusterStatus(ctx)
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, format, status, renderClusterStatusText)
	case "spec":
		return a.runClusterSpec(ctx, client, args[1:])
	case "switchover":
		options, err := parseSwitchoverCommandOptions(args[1:], a.stderr)
		if err != nil {
			return err
		}

		response, err := client.switchover(ctx, switchoverRequestJSON{
			Candidate:   options.candidate,
			ScheduledAt: options.scheduledAt,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
			Force:       options.force,
		})
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, response, renderOperationAcceptedText)
	case "failover":
		options, err := parseFailoverCommandOptions(args[1:], a.stderr)
		if err != nil {
			return err
		}

		response, err := client.failover(ctx, failoverRequestJSON{
			Candidate:   options.candidate,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
		})
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, response, renderOperationAcceptedText)
	case "reinit":
		options, err := parseReinitCommandOptions(args[1:], a.stderr)
		if err != nil {
			return err
		}

		response, err := client.reinit(ctx, reinitRequestJSON{
			Member:      options.member,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
		})
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, response, renderOperationAcceptedText)
	case "maintenance":
		return a.runClusterMaintenance(ctx, client, args[1:])
	default:
		return fmt.Errorf("unsupported pacmanctl command: cluster %s", strings.Join(args, " "))
	}
}

func (a *App) runClusterSpec(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: cluster spec")
	}

	switch args[0] {
	case "show":
		format, err := parseOutputFormat("cluster spec show", args[1:], a.stderr)
		if err != nil {
			return err
		}

		spec, err := client.clusterSpec(ctx)
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, format, spec, renderClusterSpecText)
	default:
		return fmt.Errorf("unsupported pacmanctl command: cluster spec %s", strings.Join(args, " "))
	}
}

func (a *App) runMembers(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: members")
	}

	switch args[0] {
	case "list":
		format, err := parseOutputFormat("members list", args[1:], a.stderr)
		if err != nil {
			return err
		}

		members, err := client.members(ctx)
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, format, members, renderMembersText)
	default:
		return fmt.Errorf("unsupported pacmanctl command: members %s", strings.Join(args, " "))
	}
}

func (a *App) runHistory(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: history")
	}

	switch args[0] {
	case "list":
		format, err := parseOutputFormat("history list", args[1:], a.stderr)
		if err != nil {
			return err
		}

		history, err := client.history(ctx)
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, format, history, renderHistoryText)
	default:
		return fmt.Errorf("unsupported pacmanctl command: history %s", strings.Join(args, " "))
	}
}

func (a *App) runNode(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: node")
	}

	switch args[0] {
	case "status":
		options, err := parseNodeStatusOptions(args[1:], a.stderr)
		if err != nil {
			return err
		}

		status, err := client.nodeStatus(ctx, options.nodeName)
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, status, renderNodeStatusText)
	default:
		return fmt.Errorf("unsupported pacmanctl command: node %s", strings.Join(args, " "))
	}
}

func (a *App) runDiagnostics(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: diagnostics")
	}

	switch args[0] {
	case "show":
		options, err := parseDiagnosticsOptions(args[1:], a.stderr)
		if err != nil {
			return err
		}

		summary, err := client.diagnostics(ctx, options.includeMembers)
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, summary, renderDiagnosticsText)
	default:
		return fmt.Errorf("unsupported pacmanctl command: diagnostics %s", strings.Join(args, " "))
	}
}

func (a *App) runClusterMaintenance(ctx context.Context, client *apiClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("unsupported pacmanctl command: cluster maintenance")
	}

	switch args[0] {
	case "enable":
		options, err := parseMaintenanceCommandOptions("cluster maintenance enable", args[1:], a.stderr)
		if err != nil {
			return err
		}

		response, err := client.updateMaintenance(ctx, maintenanceModeUpdateRequestJSON{
			Enabled:     true,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
		})
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, response, renderMaintenanceStatusText)
	case "disable":
		options, err := parseMaintenanceCommandOptions("cluster maintenance disable", args[1:], a.stderr)
		if err != nil {
			return err
		}

		response, err := client.updateMaintenance(ctx, maintenanceModeUpdateRequestJSON{
			Enabled:     false,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
		})
		if err != nil {
			return err
		}

		return renderOutput(a.stdout, options.format, response, renderMaintenanceStatusText)
	default:
		return fmt.Errorf("unsupported pacmanctl command: cluster maintenance %s", strings.Join(args, " "))
	}
}
