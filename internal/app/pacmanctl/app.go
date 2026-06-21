package pacmanctl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/fx"

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
	errReinitMemberRequired    = errors.New("reinit member is required: use -member")
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
	IO     *commandIO `optional:"true"`
}

// New constructs a pacmanctl application.
func New(params Params) *App {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}
	stdout := params.Stdout
	stderr := params.Stderr
	if params.IO != nil {
		stdout = params.IO.stdout
		stderr = params.IO.stderr
	}

	return &App{
		stdout: stdout,
		stderr: stderr,
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
