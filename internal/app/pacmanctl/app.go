package pacmanctl

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"go.uber.org/fx"
	"gopkg.in/yaml.v3"

	"github.com/polkiloo/pacman/internal/version"
)

const (
	defaultAPIURL       = "http://127.0.0.1:8080"
	defaultOutputFormat = "text"
	httpRequestTimeout  = 5 * time.Second
	outputFormatText    = "text"
	outputFormatPretty  = "pretty"
	outputFormatTSV     = "tsv"
	outputFormatJSON    = "json"
	outputFormatYAML    = "yaml"
)

var (
	errAPIURLRequired          = errors.New("pacmanctl api-url is required")
	errCandidateRequired       = errors.New("switchover candidate is required: use -candidate")
	errNodeNameRequired        = errors.New("node name is required: use `node status NODE_NAME` or -node")
	errUnsupportedOutputFormat = errors.New("unsupported output format")
)

// App is the pacmanctl process entrypoint.
type App struct {
	stdout io.Writer
	stderr io.Writer
	logger *slog.Logger
}

// Params defines pacmanctl constructor dependencies.
type Params struct {
	fx.In

	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
	Logger *slog.Logger
}

// New constructs a pacmanctl application.
func New(params Params) *App {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &App{
		stdout: params.Stdout,
		stderr: params.Stderr,
		logger: logger,
	}
}

// Run parses process flags and dispatches CLI commands.
func (a *App) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("pacmanctl", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	showVersion := fs.Bool("version", false, "print version and exit")
	apiURL := fs.String("api-url", defaultCLIAPIURL(), "PACMAN API base URL")
	apiToken := fs.String("api-token", defaultCLIAPIToken(), "PACMAN API bearer token")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	remaining := fs.Args()
	command := inferCommandPath(*showVersion, remaining)
	logger := a.commandLogger(strings.TrimSpace(*apiURL), strings.TrimSpace(*apiToken), command)
	logger.LogAttrs(ctx, slog.LevelInfo, "starting pacmanctl command")
	defer func() {
		if err == nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "completed pacmanctl command")
			return
		}

		logger.LogAttrs(ctx, slog.LevelError, "pacmanctl command failed", slog.String("error", err.Error()))
	}()

	if *showVersion {
		_, err = fmt.Fprintln(a.stdout, version.String())
		return
	}

	if strings.TrimSpace(*apiURL) == "" {
		return errAPIURLRequired
	}

	if len(remaining) == 0 {
		return a.printCommandHelp()
	}

	client, err := newAPIClient(strings.TrimSpace(*apiURL), strings.TrimSpace(*apiToken), &http.Client{Timeout: httpRequestTimeout})
	if err != nil {
		return err
	}
	client.logger = logger

	switch remaining[0] {
	case "cluster":
		return a.runCluster(ctx, client, remaining[1:])
	case "members":
		return a.runMembers(ctx, client, remaining[1:])
	case "history":
		if len(remaining) > 1 && remaining[1] == "list" {
			return a.runHistory(ctx, client, remaining[1:])
		}
		return a.runPatronictlHistory(ctx, client, remaining[1:])
	case "node":
		return a.runNode(ctx, client, remaining[1:])
	case "diagnostics":
		return a.runDiagnostics(ctx, client, remaining[1:])
	case "list", "topology":
		return a.runPatronictlList(ctx, client, remaining[1:])
	case "show-config":
		return a.runPatronictlShowConfig(ctx, client, remaining[1:])
	case "pause":
		return a.runPatronictlPause(ctx, client, remaining[1:])
	case "resume":
		return a.runPatronictlResume(ctx, client, remaining[1:])
	case "switchover":
		return a.runPatronictlSwitchover(ctx, client, remaining[1:])
	case "failover":
		return a.runPatronictlFailover(ctx, client, remaining[1:])
	default:
		return fmt.Errorf("unsupported pacmanctl command: %s", strings.Join(remaining, " "))
	}
}

func (a *App) commandLogger(apiURL, apiToken, command string) *slog.Logger {
	attributes := []any{
		slog.String("component", "pacmanctl"),
		slog.String("command", command),
		slog.Bool("api_token_configured", strings.TrimSpace(apiToken) != ""),
	}
	if sanitized := sanitizeLogAPIURL(apiURL); sanitized != "" && commandUsesAPI(command) {
		attributes = append(attributes, slog.String("api_url", sanitized))
	}

	return a.logger.With(attributes...)
}

func inferCommandPath(showVersion bool, remaining []string) string {
	if showVersion {
		return "version"
	}
	if len(remaining) == 0 {
		return "help"
	}

	switch remaining[0] {
	case "cluster":
		if len(remaining) < 2 {
			return "cluster"
		}
		if remaining[1] == "spec" {
			if len(remaining) >= 3 {
				return "cluster spec " + remaining[2]
			}
			return "cluster spec"
		}
		if remaining[1] == "maintenance" {
			if len(remaining) >= 3 {
				return "cluster maintenance " + remaining[2]
			}
			return "cluster maintenance"
		}
		return "cluster " + remaining[1]
	case "members", "node", "diagnostics":
		if len(remaining) >= 2 {
			return remaining[0] + " " + remaining[1]
		}
	case "history":
		if len(remaining) >= 2 {
			return "history " + remaining[1]
		}
	case "list", "topology", "show-config", "pause", "resume", "switchover", "failover":
		return remaining[0]
	}

	return remaining[0]
}

func commandUsesAPI(command string) bool {
	switch command {
	case "", "help", "version":
		return false
	default:
		return true
	}
}

func sanitizeLogAPIURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return ""
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}

	sanitized := *parsed
	sanitized.User = nil
	sanitized.RawQuery = ""
	sanitized.Fragment = ""
	return sanitized.String()
}

func (a *App) printCommandHelp() error {
	_, err := fmt.Fprintln(a.stdout, "pacmanctl commands: cluster status, cluster spec show, cluster switchover, cluster failover, cluster maintenance enable, cluster maintenance disable, members list, history list, node status, diagnostics show, patronictl-compatible: list, topology, history, show-config, pause, resume, switchover, failover")
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

func parseOutputFormat(command string, args []string, stderr io.Writer) (string, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)

	format := defaultOutputFormat
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return "", err
	}

	if len(fs.Args()) > 0 {
		return "", fmt.Errorf("unexpected arguments for %s: %s", command, strings.Join(fs.Args(), " "))
	}

	return validateOutputFormat(format)
}

func normalizeOutputFormat(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return defaultOutputFormat
	}

	return trimmed
}

type switchoverCommandOptions struct {
	format      string
	candidate   string
	scheduledAt *time.Time
	reason      string
	requestedBy string
	force       bool
}

type failoverCommandOptions struct {
	format      string
	candidate   string
	reason      string
	requestedBy string
}

type maintenanceCommandOptions struct {
	format      string
	reason      string
	requestedBy string
}

type nodeStatusOptions struct {
	format   string
	nodeName string
}

type diagnosticsOptions struct {
	format         string
	includeMembers bool
}

func parseSwitchoverCommandOptions(args []string, stderr io.Writer) (switchoverCommandOptions, error) {
	fs := flag.NewFlagSet("cluster switchover", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var candidate string
	var scheduledAt string
	var reason string
	var requestedBy string
	var force bool
	format := defaultOutputFormat

	fs.StringVar(&candidate, "candidate", "", "switchover target member")
	fs.StringVar(&scheduledAt, "scheduled-at", "", "RFC3339 schedule time")
	fs.StringVar(&reason, "reason", "", "operator reason for the switchover")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	fs.BoolVar(&force, "force", false, "cancel any pending operation and proceed")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return switchoverCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return switchoverCommandOptions{}, fmt.Errorf("unexpected arguments for cluster switchover: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return switchoverCommandOptions{}, err
	}

	trimmedCandidate := strings.TrimSpace(candidate)
	if trimmedCandidate == "" {
		return switchoverCommandOptions{}, errCandidateRequired
	}

	options := switchoverCommandOptions{
		format:      normalizedFormat,
		candidate:   trimmedCandidate,
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
		force:       force,
	}

	if strings.TrimSpace(scheduledAt) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(scheduledAt))
		if err != nil {
			return switchoverCommandOptions{}, fmt.Errorf("invalid -scheduled-at value %q: %w", scheduledAt, err)
		}
		parsed = parsed.UTC()
		options.scheduledAt = &parsed
	}

	return options, nil
}

func parseFailoverCommandOptions(args []string, stderr io.Writer) (failoverCommandOptions, error) {
	fs := flag.NewFlagSet("cluster failover", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var candidate string
	var reason string
	var requestedBy string
	format := defaultOutputFormat

	fs.StringVar(&candidate, "candidate", "", "preferred failover target member")
	fs.StringVar(&reason, "reason", "", "operator reason for the failover")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return failoverCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return failoverCommandOptions{}, fmt.Errorf("unexpected arguments for cluster failover: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return failoverCommandOptions{}, err
	}

	return failoverCommandOptions{
		format:      normalizedFormat,
		candidate:   strings.TrimSpace(candidate),
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
	}, nil
}

func parseMaintenanceCommandOptions(command string, args []string, stderr io.Writer) (maintenanceCommandOptions, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)

	var reason string
	var requestedBy string
	format := defaultOutputFormat

	fs.StringVar(&reason, "reason", "", "maintenance reason")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return maintenanceCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return maintenanceCommandOptions{}, fmt.Errorf("unexpected arguments for %s: %s", command, strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return maintenanceCommandOptions{}, err
	}

	return maintenanceCommandOptions{
		format:      normalizedFormat,
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
	}, nil
}

func parseNodeStatusOptions(args []string, stderr io.Writer) (nodeStatusOptions, error) {
	fs := flag.NewFlagSet("node status", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var nodeName string
	format := defaultOutputFormat

	fs.StringVar(&nodeName, "node", "", "PACMAN node name")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return nodeStatusOptions{}, err
	}

	trimmedName := strings.TrimSpace(nodeName)
	remaining := fs.Args()
	switch {
	case trimmedName != "" && len(remaining) > 0:
		return nodeStatusOptions{}, fmt.Errorf("unexpected arguments for node status: %s", strings.Join(remaining, " "))
	case trimmedName == "" && len(remaining) == 1:
		trimmedName = strings.TrimSpace(remaining[0])
	case trimmedName == "" && len(remaining) == 0:
		return nodeStatusOptions{}, errNodeNameRequired
	case trimmedName == "" && len(remaining) > 1:
		return nodeStatusOptions{}, fmt.Errorf("unexpected arguments for node status: %s", strings.Join(remaining, " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return nodeStatusOptions{}, err
	}

	return nodeStatusOptions{
		format:   normalizedFormat,
		nodeName: trimmedName,
	}, nil
}

func parseDiagnosticsOptions(args []string, stderr io.Writer) (diagnosticsOptions, error) {
	fs := flag.NewFlagSet("diagnostics show", flag.ContinueOnError)
	fs.SetOutput(stderr)

	includeMembers := true
	format := defaultOutputFormat

	fs.BoolVar(&includeMembers, "include-members", true, "include per-member diagnostics")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return diagnosticsOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return diagnosticsOptions{}, fmt.Errorf("unexpected arguments for diagnostics show: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return diagnosticsOptions{}, err
	}

	return diagnosticsOptions{
		format:         normalizedFormat,
		includeMembers: includeMembers,
	}, nil
}

func addOutputFormatFlags(fs *flag.FlagSet, format *string) {
	fs.StringVar(format, "format", defaultOutputFormat, "output format: text|pretty|json|yaml")
	fs.StringVar(format, "o", defaultOutputFormat, "output format: text|pretty|json|yaml")
}

func validateOutputFormat(format string) (string, error) {
	normalized := normalizeOutputFormat(format)
	switch normalized {
	case outputFormatText, outputFormatPretty, outputFormatJSON, outputFormatYAML:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: %s", errUnsupportedOutputFormat, format)
	}
}

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
	if _, err := fmt.Fprintln(tab, "NAME\tROLE\tSTATE\tHEALTHY\tLEADER\tTIMELINE\tLAG BYTES\tLAST SEEN"); err != nil {
		return err
	}

	for _, member := range members {
		if _, err := fmt.Fprintf(
			tab,
			"%s\t%s\t%s\t%t\t%t\t%s\t%s\t%s\n",
			orDash(member.Name),
			orDash(member.Role),
			orDash(member.State),
			member.Healthy,
			member.Leader,
			formatOptionalInt64(member.Timeline),
			formatOptionalInt64(member.LagBytes),
			formatTime(member.LastSeenAt),
		); err != nil {
			return err
		}
	}

	return tab.Flush()
}

func defaultCLIAPIURL() string {
	for _, key := range []string{"PACMANCTL_API_URL", "PACMAN_API_URL"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	return defaultAPIURL
}

func defaultCLIAPIToken() string {
	for _, key := range []string{"PACMANCTL_API_TOKEN", "PACMAN_API_TOKEN"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	return ""
}

func formatMaintenance(status maintenanceModeStatusJSON) string {
	if !status.Enabled {
		return "disabled"
	}

	if status.Reason != "" {
		return "enabled (" + status.Reason + ")"
	}

	return "enabled"
}

func formatOperation(operation *operationJSON) string {
	if operation == nil {
		return "-"
	}

	parts := []string{operation.Kind, operation.State}
	if operation.ToMember != "" {
		parts = append(parts, "to="+operation.ToMember)
	}
	if operation.FromMember != "" {
		parts = append(parts, "from="+operation.FromMember)
	}
	return strings.Join(parts, " ")
}

func formatScheduledSwitchover(sw *scheduledSwitchoverJSON) string {
	if sw == nil {
		return "-"
	}

	parts := []string{sw.At.UTC().Format(time.RFC3339)}
	if sw.From != "" {
		parts = append(parts, "from="+sw.From)
	}
	if sw.To != "" {
		parts = append(parts, "to="+sw.To)
	}
	return strings.Join(parts, " ")
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}

	return value.UTC().Format(time.RFC3339)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return "-"
	}

	return formatTime(*value)
}

func formatOptionalInt64(value int64) string {
	if value == 0 {
		return "-"
	}

	return fmt.Sprintf("%d", value)
}

func formatOptionalInt(value int) string {
	if value == 0 {
		return "-"
	}

	return fmt.Sprintf("%d", value)
}

func formatOptionalBool(value *bool) string {
	if value == nil {
		return "-"
	}

	return fmt.Sprintf("%t", *value)
}

func formatMap(values map[string]any) string {
	if len(values) == 0 {
		return "-"
	}

	return formatAny(values)
}

func formatAny(value any) string {
	if value == nil {
		return "-"
	}

	switch typed := value.(type) {
	case string:
		return orDash(typed)
	case bool:
		return fmt.Sprintf("%t", typed)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", typed)
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}

	return string(encoded)
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}

	return value
}
