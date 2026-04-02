package pacmand

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"go.uber.org/dig"

	"github.com/polkiloo/pacman/internal/app/localagent"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/pgext"
	"github.com/polkiloo/pacman/internal/version"
)

var (
	errConfigPathRequired   = errors.New("pacmand config path is required")
	errConfigSourceConflict = errors.New("pacmand config path and postgres extension environment are mutually exclusive")
)

// App is the pacmand process entrypoint.
type App struct {
	stdout io.Writer
	stderr io.Writer
	logger *slog.Logger
}

// Params defines pacmand constructor dependencies.
type Params struct {
	dig.In

	Stdout io.Writer `name:"stdout"`
	Stderr io.Writer `name:"stderr"`
	Logger *slog.Logger
}

// New constructs a pacmand application.
func New(params Params) *App {
	return &App{
		stdout: params.Stdout,
		stderr: params.Stderr,
		logger: params.Logger,
	}
}

// Run parses process flags and starts the local agent daemon.
func (a *App) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pacmand", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	showVersion := fs.Bool("version", false, "print version and exit")
	configPath := fs.String("config", "", "path to pacmand node configuration file")
	loadPGExtEnv := fs.Bool("pgext-env", false, "load PACMAN node configuration from PostgreSQL extension environment")

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

	if *loadPGExtEnv && *configPath != "" {
		return errConfigSourceConflict
	}

	var (
		loadedConfig config.Config
		configSource string
		err          error
	)

	switch {
	case *loadPGExtEnv:
		configSource = "pgext-env"
		loadedConfig, err = loadFromPGExtEnv()
	case *configPath == "":
		return errConfigPathRequired
	default:
		configSource = "file"
		loadedConfig, err = config.Load(*configPath)
	}
	if err != nil {
		return err
	}

	a.logLoadedConfig(ctx, loadedConfig, configSource, *configPath)

	return localagent.Run(ctx, a.logger, loadedConfig)
}

func loadFromPGExtEnv() (config.Config, error) {
	snapshot, err := pgext.LoadSnapshotFromEnv(os.LookupEnv)
	if err != nil {
		return config.Config{}, err
	}

	return snapshot.RuntimeConfig()
}

func (a *App) logLoadedConfig(ctx context.Context, loadedConfig config.Config, source, path string) {
	attributes := []slog.Attr{
		slog.String("component", "config"),
		slog.String("source", source),
		slog.String("node", loadedConfig.Node.Name),
		slog.String("role", loadedConfig.Node.Role.String()),
	}
	if path != "" {
		attributes = append(attributes, slog.String("path", path))
	}

	a.logger.LogAttrs(ctx, slog.LevelInfo, "loaded node configuration", attributes...)
}
