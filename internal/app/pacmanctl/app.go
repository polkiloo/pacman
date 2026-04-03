package pacmanctl

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"go.uber.org/dig"

	"github.com/polkiloo/pacman/internal/version"
)

const (
	defaultAPIURL       = "http://127.0.0.1:8080"
	defaultOutputFormat = "text"
	httpRequestTimeout  = 5 * time.Second
	outputFormatText    = "text"
	outputFormatJSON    = "json"
)

var (
	errAPIURLRequired          = errors.New("pacmanctl api-url is required")
	errUnsupportedOutputFormat = errors.New("unsupported output format")
)

// App is the pacmanctl process entrypoint.
type App struct {
	stdout io.Writer
	stderr io.Writer
}

// Params defines pacmanctl constructor dependencies.
type Params struct {
	dig.In

	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
}

// New constructs a pacmanctl application.
func New(params Params) *App {
	return &App{
		stdout: params.Stdout,
		stderr: params.Stderr,
	}
}

// Run parses process flags and dispatches CLI commands.
func (a *App) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pacmanctl", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	showVersion := fs.Bool("version", false, "print version and exit")
	apiURL := fs.String("api-url", defaultCLIAPIURL(), "PACMAN API base URL")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if *showVersion {
		_, err := fmt.Fprintln(a.stdout, version.String())
		return err
	}

	if strings.TrimSpace(*apiURL) == "" {
		return errAPIURLRequired
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		return a.printCommandHelp()
	}

	client, err := newAPIClient(strings.TrimSpace(*apiURL), &http.Client{Timeout: httpRequestTimeout})
	if err != nil {
		return err
	}

	switch remaining[0] {
	case "cluster":
		return a.runCluster(ctx, client, remaining[1:])
	case "members":
		return a.runMembers(ctx, client, remaining[1:])
	default:
		return fmt.Errorf("unsupported pacmanctl command: %s", strings.Join(remaining, " "))
	}
}

func (a *App) printCommandHelp() error {
	_, err := fmt.Fprintln(a.stdout, "pacmanctl commands: cluster status, members list")
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
	default:
		return fmt.Errorf("unsupported pacmanctl command: cluster %s", strings.Join(args, " "))
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

func parseOutputFormat(command string, args []string, stderr io.Writer) (string, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)

	format := defaultOutputFormat
	fs.StringVar(&format, "format", defaultOutputFormat, "output format: text|json")
	fs.StringVar(&format, "o", defaultOutputFormat, "output format: text|json")

	if err := fs.Parse(args); err != nil {
		return "", err
	}

	if len(fs.Args()) > 0 {
		return "", fmt.Errorf("unexpected arguments for %s: %s", command, strings.Join(fs.Args(), " "))
	}

	normalized := normalizeOutputFormat(format)
	switch normalized {
	case outputFormatText, outputFormatJSON:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: %s", errUnsupportedOutputFormat, format)
	}
}

func normalizeOutputFormat(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return defaultOutputFormat
	}

	return trimmed
}

func renderOutput[T any](writer io.Writer, format string, payload T, renderText func(io.Writer, T) error) error {
	switch format {
	case outputFormatText:
		return renderText(writer, payload)
	case outputFormatJSON:
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
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

func formatOptionalInt64(value int64) string {
	if value == 0 {
		return "-"
	}

	return fmt.Sprintf("%d", value)
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}

	return value
}
