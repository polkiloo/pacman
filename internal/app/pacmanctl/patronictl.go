package pacmanctl

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	patronictlWaitTimeout      = 30 * time.Second
	patronictlWaitPollInterval = 250 * time.Millisecond
)

type patronictlListOptions struct {
	scope     string
	format    string
	extended  bool
	timestamp bool
}

type patronictlHistoryOptions struct {
	scope  string
	format string
}

type patronictlShowConfigOptions struct {
	scope  string
	format string
}

type patronictlMaintenanceOptions struct {
	scope       string
	format      string
	reason      string
	requestedBy string
	wait        bool
}

type patronictlSwitchoverOptions struct {
	scope       string
	format      string
	leader      string
	candidate   string
	scheduledAt *time.Time
	reason      string
	requestedBy string
	force       bool
}

type patronictlFailoverOptions struct {
	scope       string
	format      string
	leader      string
	candidate   string
	reason      string
	requestedBy string
	force       bool
}

type patronictlListDocument struct {
	Cluster string               `json:"cluster" yaml:"cluster"`
	Members []patronictlListItem `json:"members" yaml:"members"`
}

type patronictlListItem struct {
	Member      string         `json:"member" yaml:"member"`
	Host        string         `json:"host" yaml:"host"`
	Role        string         `json:"role" yaml:"role"`
	State       string         `json:"state" yaml:"state"`
	Timeline    int64          `json:"timeline,omitempty" yaml:"timeline,omitempty"`
	LagMB       float64        `json:"lagMb,omitempty" yaml:"lagMb,omitempty"`
	Pending     bool           `json:"pendingRestart,omitempty" yaml:"pendingRestart,omitempty"`
	Tags        map[string]any `json:"tags,omitempty" yaml:"tags,omitempty"`
	APIURL      string         `json:"apiUrl,omitempty" yaml:"apiUrl,omitempty"`
	LastSeenAt  *time.Time     `json:"lastSeenAt,omitempty" yaml:"lastSeenAt,omitempty"`
	NeedsRejoin bool           `json:"needsRejoin,omitempty" yaml:"needsRejoin,omitempty"`
}

func (a *App) runPatronictlList(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlListOptions(args)
	if err != nil {
		return err
	}

	status, err := client.clusterStatus(ctx)
	if err != nil {
		return err
	}
	if err := ensurePatronictlScope(options.scope, status.ClusterName); err != nil {
		return err
	}

	switch options.format {
	case outputFormatPretty:
		return renderPatronictlListPretty(a.stdout, status, options)
	case outputFormatTSV:
		return renderPatronictlListTSV(a.stdout, status, options)
	default:
		return renderOutput(a.stdout, options.format, buildPatronictlListDocument(status), func(writer io.Writer, payload patronictlListDocument) error {
			return renderPatronictlListPretty(writer, status, options)
		})
	}
}

func (a *App) runPatronictlHistory(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlHistoryOptions(args)
	if err != nil {
		return err
	}

	history, err := client.history(ctx)
	if err != nil {
		return err
	}
	if options.scope != "" {
		status, err := client.clusterStatus(ctx)
		if err != nil {
			return err
		}
		if err := ensurePatronictlScope(options.scope, status.ClusterName); err != nil {
			return err
		}
	}

	switch options.format {
	case outputFormatPretty:
		return renderHistoryText(a.stdout, history)
	case outputFormatTSV:
		return renderPatronictlHistoryTSV(a.stdout, history)
	default:
		return renderOutput(a.stdout, options.format, history, renderHistoryText)
	}
}

func (a *App) runPatronictlShowConfig(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlShowConfigOptions(args)
	if err != nil {
		return err
	}

	spec, err := client.clusterSpec(ctx)
	if err != nil {
		return err
	}
	if err := ensurePatronictlScope(options.scope, spec.ClusterName); err != nil {
		return err
	}

	config := buildPatronictlDynamicConfig(spec)
	// show-config renders as YAML by default; pretty is an alias for yaml here.
	format := options.format
	if format == outputFormatPretty {
		format = outputFormatYAML
	}
	return renderOutput(a.stdout, format, config, func(writer io.Writer, m map[string]any) error {
		enc := yaml.NewEncoder(writer)
		enc.SetIndent(2)
		defer enc.Close()
		return enc.Encode(m)
	})
}

func (a *App) runPatronictlPause(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlMaintenanceOptions(args)
	if err != nil {
		return err
	}

	if err := validatePatronictlScopeWithClusterStatus(ctx, client, options.scope); err != nil {
		return err
	}

	status, err := client.updateMaintenance(ctx, maintenanceModeUpdateRequestJSON{
		Enabled:     true,
		Reason:      options.reason,
		RequestedBy: options.requestedBy,
	})
	if err != nil {
		return err
	}

	if options.wait {
		status, err = waitForPatronictlMaintenanceState(ctx, client, true)
		if err != nil {
			return err
		}
	}

	return renderPatronictlMaintenanceOutput(a.stdout, options.format, status)
}

func (a *App) runPatronictlResume(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlMaintenanceOptions(args)
	if err != nil {
		return err
	}

	if err := validatePatronictlScopeWithClusterStatus(ctx, client, options.scope); err != nil {
		return err
	}

	status, err := client.updateMaintenance(ctx, maintenanceModeUpdateRequestJSON{
		Enabled:     false,
		Reason:      options.reason,
		RequestedBy: options.requestedBy,
	})
	if err != nil {
		return err
	}

	if options.wait {
		status, err = waitForPatronictlMaintenanceState(ctx, client, false)
		if err != nil {
			return err
		}
	}

	return renderPatronictlMaintenanceOutput(a.stdout, options.format, status)
}

func (a *App) runPatronictlSwitchover(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlSwitchoverOptions(args)
	if err != nil {
		return err
	}

	status, err := client.clusterStatus(ctx)
	if err != nil {
		return err
	}
	if err := ensurePatronictlScope(options.scope, status.ClusterName); err != nil {
		return err
	}
	if err := ensurePatronictlLeader(options.leader, status.CurrentPrimary); err != nil {
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

	return renderPatronictlOperationOutput(a.stdout, options.format, response)
}

func (a *App) runPatronictlFailover(ctx context.Context, client *apiClient, args []string) error {
	options, err := parsePatronictlFailoverOptions(args)
	if err != nil {
		return err
	}

	status, err := client.clusterStatus(ctx)
	if err != nil {
		return err
	}
	if err := ensurePatronictlScope(options.scope, status.ClusterName); err != nil {
		return err
	}
	if err := ensurePatronictlLeader(options.leader, status.CurrentPrimary); err != nil {
		return err
	}

	var response operationAcceptedResponse
	if options.leader != "" {
		response, err = client.switchover(ctx, switchoverRequestJSON{
			Candidate:   options.candidate,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
			Force:       options.force,
		})
	} else {
		response, err = client.failover(ctx, failoverRequestJSON{
			Candidate:   options.candidate,
			Reason:      options.reason,
			RequestedBy: options.requestedBy,
		})
	}
	if err != nil {
		return err
	}

	return renderPatronictlOperationOutput(a.stdout, options.format, response)
}

func parsePatronictlListOptions(args []string) (patronictlListOptions, error) {
	options := patronictlListOptions{format: outputFormatPretty}
	for index := 0; index < len(args); index++ {
		token := args[index]
		switch {
		case token == "-e" || token == "--extended":
			options.extended = true
		case token == "-t" || token == "--timestamp":
			options.timestamp = true
		case token == "-f" || token == "--format":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlListOptions{}, err
			}
			index = next
			options.format = value
		case strings.HasPrefix(token, "-f=") || strings.HasPrefix(token, "--format="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlListOptions{}, err
			}
			options.format = value
		case strings.HasPrefix(token, "-"):
			return patronictlListOptions{}, fmt.Errorf("unsupported patronictl-compatible flag: %s", token)
		default:
			if options.scope != "" {
				return patronictlListOptions{}, fmt.Errorf("unexpected patronictl-compatible arguments: %s", strings.Join(args[index:], " "))
			}
			options.scope = token
		}
	}

	format, err := validatePatronictlFormat(options.format, outputFormatPretty, outputFormatTSV, outputFormatJSON, outputFormatYAML)
	if err != nil {
		return patronictlListOptions{}, err
	}
	options.format = format

	return options, nil
}

func parsePatronictlHistoryOptions(args []string) (patronictlHistoryOptions, error) {
	options := patronictlHistoryOptions{format: outputFormatPretty}
	for index := 0; index < len(args); index++ {
		token := args[index]
		switch {
		case token == "-f" || token == "--format":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlHistoryOptions{}, err
			}
			index = next
			options.format = value
		case strings.HasPrefix(token, "-f=") || strings.HasPrefix(token, "--format="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlHistoryOptions{}, err
			}
			options.format = value
		case strings.HasPrefix(token, "-"):
			return patronictlHistoryOptions{}, fmt.Errorf("unsupported patronictl-compatible flag: %s", token)
		default:
			if options.scope != "" {
				return patronictlHistoryOptions{}, fmt.Errorf("unexpected patronictl-compatible arguments: %s", strings.Join(args[index:], " "))
			}
			options.scope = token
		}
	}

	format, err := validatePatronictlFormat(options.format, outputFormatPretty, outputFormatTSV, outputFormatJSON, outputFormatYAML)
	if err != nil {
		return patronictlHistoryOptions{}, err
	}
	options.format = format

	return options, nil
}

func parsePatronictlShowConfigOptions(args []string) (patronictlShowConfigOptions, error) {
	options := patronictlShowConfigOptions{format: outputFormatYAML}
	for index := 0; index < len(args); index++ {
		token := args[index]
		switch {
		case token == "-f" || token == "--format":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlShowConfigOptions{}, err
			}
			index = next
			options.format = value
		case strings.HasPrefix(token, "-f=") || strings.HasPrefix(token, "--format="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlShowConfigOptions{}, err
			}
			options.format = value
		case strings.HasPrefix(token, "-"):
			return patronictlShowConfigOptions{}, fmt.Errorf("unsupported patronictl-compatible flag: %s", token)
		default:
			if options.scope != "" {
				return patronictlShowConfigOptions{}, fmt.Errorf("unexpected patronictl-compatible arguments: %s", strings.Join(args[index:], " "))
			}
			options.scope = token
		}
	}

	format, err := validatePatronictlFormat(options.format, outputFormatPretty, outputFormatJSON, outputFormatYAML)
	if err != nil {
		return patronictlShowConfigOptions{}, err
	}
	options.format = format

	return options, nil
}

func parsePatronictlMaintenanceOptions(args []string) (patronictlMaintenanceOptions, error) {
	options := patronictlMaintenanceOptions{format: outputFormatPretty}
	for index := 0; index < len(args); index++ {
		token := args[index]
		switch {
		case token == "--wait":
			options.wait = true
		case strings.HasPrefix(token, "--wait="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			options.wait = strings.EqualFold(value, "true")
		case token == "--reason":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			index = next
			options.reason = value
		case strings.HasPrefix(token, "--reason="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			options.reason = value
		case token == "--requested-by":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			index = next
			options.requestedBy = value
		case strings.HasPrefix(token, "--requested-by="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			options.requestedBy = value
		case token == "-f" || token == "--format":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			index = next
			options.format = value
		case strings.HasPrefix(token, "-f=") || strings.HasPrefix(token, "--format="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlMaintenanceOptions{}, err
			}
			options.format = value
		case strings.HasPrefix(token, "-"):
			return patronictlMaintenanceOptions{}, fmt.Errorf("unsupported patronictl-compatible flag: %s", token)
		default:
			if options.scope != "" {
				return patronictlMaintenanceOptions{}, fmt.Errorf("unexpected patronictl-compatible arguments: %s", strings.Join(args[index:], " "))
			}
			options.scope = token
		}
	}

	format, err := validatePatronictlFormat(options.format, outputFormatPretty, outputFormatJSON, outputFormatYAML)
	if err != nil {
		return patronictlMaintenanceOptions{}, err
	}
	options.format = format
	options.reason = strings.TrimSpace(options.reason)
	options.requestedBy = strings.TrimSpace(options.requestedBy)

	return options, nil
}

func parsePatronictlSwitchoverOptions(args []string) (patronictlSwitchoverOptions, error) {
	options := patronictlSwitchoverOptions{format: outputFormatPretty}
	for index := 0; index < len(args); index++ {
		token := args[index]
		switch {
		case token == "--leader" || token == "--primary":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			index = next
			options.leader = value
		case strings.HasPrefix(token, "--leader=") || strings.HasPrefix(token, "--primary="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.leader = value
		case token == "--candidate":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			index = next
			options.candidate = value
		case strings.HasPrefix(token, "--candidate="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.candidate = value
		case token == "--scheduled":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			index = next
			parsed, err := parsePatronictlTime(value, "--scheduled")
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.scheduledAt = parsed
		case strings.HasPrefix(token, "--scheduled="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			parsed, err := parsePatronictlTime(value, "--scheduled")
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.scheduledAt = parsed
		case token == "--reason":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			index = next
			options.reason = value
		case strings.HasPrefix(token, "--reason="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.reason = value
		case token == "--requested-by":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			index = next
			options.requestedBy = value
		case strings.HasPrefix(token, "--requested-by="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.requestedBy = value
		case token == "--force":
			options.force = true
		case token == "-f" || token == "--format":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			index = next
			options.format = value
		case strings.HasPrefix(token, "-f=") || strings.HasPrefix(token, "--format="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlSwitchoverOptions{}, err
			}
			options.format = value
		case strings.HasPrefix(token, "-"):
			return patronictlSwitchoverOptions{}, fmt.Errorf("unsupported patronictl-compatible flag: %s", token)
		default:
			if options.scope != "" {
				return patronictlSwitchoverOptions{}, fmt.Errorf("unexpected patronictl-compatible arguments: %s", strings.Join(args[index:], " "))
			}
			options.scope = token
		}
	}

	if strings.TrimSpace(options.candidate) == "" {
		return patronictlSwitchoverOptions{}, errorsForPatroniCandidate("--candidate")
	}

	format, err := validatePatronictlFormat(options.format, outputFormatPretty, outputFormatJSON, outputFormatYAML)
	if err != nil {
		return patronictlSwitchoverOptions{}, err
	}
	options.format = format
	options.scope = strings.TrimSpace(options.scope)
	options.leader = strings.TrimSpace(options.leader)
	options.candidate = strings.TrimSpace(options.candidate)
	options.reason = strings.TrimSpace(options.reason)
	options.requestedBy = strings.TrimSpace(options.requestedBy)

	return options, nil
}

func parsePatronictlFailoverOptions(args []string) (patronictlFailoverOptions, error) {
	options := patronictlFailoverOptions{format: outputFormatPretty}
	for index := 0; index < len(args); index++ {
		token := args[index]
		switch {
		case token == "--leader" || token == "--primary":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			index = next
			options.leader = value
		case strings.HasPrefix(token, "--leader=") || strings.HasPrefix(token, "--primary="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			options.leader = value
		case token == "--candidate":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			index = next
			options.candidate = value
		case strings.HasPrefix(token, "--candidate="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			options.candidate = value
		case token == "--reason":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			index = next
			options.reason = value
		case strings.HasPrefix(token, "--reason="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			options.reason = value
		case token == "--requested-by":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			index = next
			options.requestedBy = value
		case strings.HasPrefix(token, "--requested-by="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			options.requestedBy = value
		case token == "--force":
			options.force = true
		case token == "-f" || token == "--format":
			value, next, err := parsePatronictlValueFlag(args, index, token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			index = next
			options.format = value
		case strings.HasPrefix(token, "-f=") || strings.HasPrefix(token, "--format="):
			value, err := parsePatronictlInlineValue(token)
			if err != nil {
				return patronictlFailoverOptions{}, err
			}
			options.format = value
		case strings.HasPrefix(token, "-"):
			return patronictlFailoverOptions{}, fmt.Errorf("unsupported patronictl-compatible flag: %s", token)
		default:
			if options.scope != "" {
				return patronictlFailoverOptions{}, fmt.Errorf("unexpected patronictl-compatible arguments: %s", strings.Join(args[index:], " "))
			}
			options.scope = token
		}
	}

	if strings.TrimSpace(options.candidate) == "" {
		return patronictlFailoverOptions{}, errorsForPatroniCandidate("--candidate")
	}

	format, err := validatePatronictlFormat(options.format, outputFormatPretty, outputFormatJSON, outputFormatYAML)
	if err != nil {
		return patronictlFailoverOptions{}, err
	}
	options.format = format
	options.scope = strings.TrimSpace(options.scope)
	options.leader = strings.TrimSpace(options.leader)
	options.candidate = strings.TrimSpace(options.candidate)
	options.reason = strings.TrimSpace(options.reason)
	options.requestedBy = strings.TrimSpace(options.requestedBy)

	return options, nil
}

func parsePatronictlValueFlag(args []string, index int, flagName string) (string, int, error) {
	if index+1 >= len(args) {
		return "", index, fmt.Errorf("%s requires a value", flagName)
	}

	return args[index+1], index + 1, nil
}

func parsePatronictlInlineValue(token string) (string, error) {
	parts := strings.SplitN(token, "=", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid flag syntax: %s", token)
	}

	return parts[1], nil
}

func parsePatronictlTime(value, flagName string) (*time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("invalid %s value %q: %w", flagName, value, err)
	}

	parsed = parsed.UTC()
	return &parsed, nil
}

func validatePatronictlFormat(format string, allowed ...string) (string, error) {
	normalized := normalizeOutputFormat(format)
	for _, candidate := range allowed {
		if normalized == candidate {
			return normalized, nil
		}
	}

	return "", fmt.Errorf("%w: %s", errUnsupportedOutputFormat, format)
}

func ensurePatronictlScope(requested, actual string) error {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return nil
	}
	if strings.TrimSpace(actual) == "" {
		return fmt.Errorf("cluster name %q was provided but API did not report a cluster name", requested)
	}
	if requested != actual {
		return fmt.Errorf("cluster name mismatch: requested %q, API reports %q", requested, actual)
	}

	return nil
}

func ensurePatronictlLeader(requested, actual string) error {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return nil
	}
	if requested != strings.TrimSpace(actual) {
		return fmt.Errorf("leader mismatch: requested %q, current primary is %q", requested, actual)
	}

	return nil
}

func validatePatronictlScopeWithClusterStatus(ctx context.Context, client *apiClient, scope string) error {
	if strings.TrimSpace(scope) == "" {
		return nil
	}

	status, err := client.clusterStatus(ctx)
	if err != nil {
		return err
	}

	return ensurePatronictlScope(scope, status.ClusterName)
}

func waitForPatronictlMaintenanceState(ctx context.Context, client *apiClient, enabled bool) (maintenanceModeStatusJSON, error) {
	waitCtx, cancel := context.WithTimeout(ctx, patronictlWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(patronictlWaitPollInterval)
	defer ticker.Stop()

	for {
		status, err := client.maintenanceStatus(waitCtx)
		if err != nil {
			return maintenanceModeStatusJSON{}, err
		}
		if status.Enabled == enabled {
			return status, nil
		}

		select {
		case <-waitCtx.Done():
			return maintenanceModeStatusJSON{}, fmt.Errorf("maintenance state did not converge to enabled=%t before timeout", enabled)
		case <-ticker.C:
		}
	}
}

func renderPatronictlMaintenanceOutput(writer io.Writer, format string, status maintenanceModeStatusJSON) error {
	return renderOutput(writer, format, status, renderMaintenanceStatusText)
}

func renderPatronictlOperationOutput(writer io.Writer, format string, response operationAcceptedResponse) error {
	return renderOutput(writer, format, response, renderOperationAcceptedText)
}

func renderPatronictlListPretty(writer io.Writer, status clusterStatusResponse, options patronictlListOptions) error {
	tab := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)

	headers := []string{"Cluster", "Member", "Host", "Role", "State", "TL", "Lag in MB"}
	if options.timestamp {
		headers = append(headers, "Last Seen")
	}
	if options.extended {
		headers = append(headers, "API URL", "Needs Rejoin", "Tags")
	}
	if _, err := fmt.Fprintln(tab, strings.Join(headers, "\t")); err != nil {
		return err
	}

	for _, member := range status.Members {
		fields := patronictlListFields(status.ClusterName, member, options)
		if _, err := fmt.Fprintln(tab, strings.Join(fields, "\t")); err != nil {
			return err
		}
	}

	return tab.Flush()
}

func renderPatronictlListTSV(writer io.Writer, status clusterStatusResponse, options patronictlListOptions) error {
	headers := []string{"Cluster", "Member", "Host", "Role", "State", "TL", "Lag in MB"}
	if options.timestamp {
		headers = append(headers, "Last Seen")
	}
	if options.extended {
		headers = append(headers, "API URL", "Needs Rejoin", "Tags")
	}
	if _, err := fmt.Fprintln(writer, strings.Join(headers, "\t")); err != nil {
		return err
	}

	for _, member := range status.Members {
		if _, err := fmt.Fprintln(writer, strings.Join(patronictlListFields(status.ClusterName, member, options), "\t")); err != nil {
			return err
		}
	}

	return nil
}

func renderPatronictlHistoryTSV(writer io.Writer, response historyResponse) error {
	if _, err := fmt.Fprintln(writer, "Operation ID\tKind\tResult\tTimeline\tWAL LSN\tFrom\tTo\tFinished At\tReason"); err != nil {
		return err
	}

	for _, item := range response.Items {
		if _, err := fmt.Fprintf(
			writer,
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

	return nil
}

func patronictlListFields(clusterName string, member memberStatusJSON, options patronictlListOptions) []string {
	fields := []string{
		orDash(clusterName),
		orDash(member.Name),
		formatPatronictlHost(member),
		orDash(member.Role),
		orDash(member.State),
		formatOptionalInt64(member.Timeline),
		formatPatronictlLagMB(member.LagBytes),
	}
	if options.timestamp {
		fields = append(fields, formatTime(member.LastSeenAt))
	}
	if options.extended {
		fields = append(fields, orDash(member.APIURL), fmt.Sprintf("%t", member.NeedsRejoin), formatMap(member.Tags))
	}

	return fields
}

func buildPatronictlListDocument(status clusterStatusResponse) patronictlListDocument {
	document := patronictlListDocument{
		Cluster: status.ClusterName,
		Members: make([]patronictlListItem, len(status.Members)),
	}

	for index, member := range status.Members {
		item := patronictlListItem{
			Member:      member.Name,
			Host:        formatPatronictlHost(member),
			Role:        member.Role,
			State:       member.State,
			Timeline:    member.Timeline,
			LagMB:       patronictlLagMBValue(member.LagBytes),
			Tags:        member.Tags,
			APIURL:      member.APIURL,
			NeedsRejoin: member.NeedsRejoin,
		}
		if !member.LastSeenAt.IsZero() {
			lastSeen := member.LastSeenAt
			item.LastSeenAt = &lastSeen
		}
		document.Members[index] = item
	}

	return document
}

func buildPatronictlDynamicConfig(spec clusterSpecResponse) map[string]any {
	config := map[string]any{
		"pause": spec.Maintenance.Enabled,
		"postgresql": map[string]any{
			"use_pg_rewind": spec.Postgres.UsePgRewind,
			"parameters":    spec.Postgres.Parameters,
		},
		"maximum_lag_on_failover": spec.Failover.MaximumLagBytes,
		"pacman": map[string]any{
			"cluster_name": spec.ClusterName,
			"generation":   spec.Generation,
			"maintenance": map[string]any{
				"default_reason": spec.Maintenance.DefaultReason,
			},
			"failover": map[string]any{
				"mode":             spec.Failover.Mode,
				"check_timeline":   spec.Failover.CheckTimeline,
				"require_quorum":   spec.Failover.RequireQuorum,
				"fencing_required": spec.Failover.FencingRequired,
			},
			"switchover": map[string]any{
				"allow_scheduled": spec.Switchover.AllowScheduled,
				"require_specific_candidate_during_maintenance": spec.Switchover.RequireSpecificCandidateDuringMaintenance,
			},
			"members": spec.Members,
		},
	}

	switch spec.Postgres.SynchronousMode {
	case "disabled", "":
		config["synchronous_mode"] = false
		config["synchronous_mode_strict"] = false
	case "quorum":
		config["synchronous_mode"] = true
		config["synchronous_mode_strict"] = false
	case "strict":
		config["synchronous_mode"] = true
		config["synchronous_mode_strict"] = true
	default:
		config["synchronous_mode"] = spec.Postgres.SynchronousMode
	}

	return config
}

func formatPatronictlHost(member memberStatusJSON) string {
	if member.Host != "" && member.Port > 0 {
		return fmt.Sprintf("%s:%d", member.Host, member.Port)
	}
	if member.Host != "" {
		return member.Host
	}
	if member.APIURL != "" {
		return member.APIURL
	}

	return "-"
}

func formatPatronictlLagMB(lagBytes int64) string {
	value := patronictlLagMBValue(lagBytes)
	if value == 0 {
		return "0"
	}
	if value == float64(int64(value)) {
		return fmt.Sprintf("%.0f", value)
	}

	return fmt.Sprintf("%.1f", value)
}

func patronictlLagMBValue(lagBytes int64) float64 {
	if lagBytes <= 0 {
		return 0
	}

	return float64(lagBytes) / (1024.0 * 1024.0)
}

func errorsForPatroniCandidate(flagName string) error {
	return fmt.Errorf("%s is required for patronictl-compatible command", flagName)
}
