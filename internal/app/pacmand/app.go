package pacmand

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"go.uber.org/dig"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/version"
)

var errConfigPathRequired = errors.New("pacmand config path is required")

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

	if *configPath == "" {
		return errConfigPathRequired
	}

	loadedConfig, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	a.logger.InfoContext(
		ctx,
		"loaded node configuration",
		slog.String("component", "config"),
		slog.String("path", *configPath),
		slog.String("node", loadedConfig.Node.Name),
		slog.String("role", loadedConfig.Node.Role.String()),
	)

	daemon, err := agent.NewDaemon(loadedConfig, a.logger)
	if err != nil {
		return fmt.Errorf("construct local agent daemon: %w", err)
	}

	if err := daemon.Start(ctx); err != nil {
		return fmt.Errorf("start local agent daemon: %w", err)
	}

	daemon.Wait()

	return nil
}
