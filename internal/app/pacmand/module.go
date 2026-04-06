package pacmand

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/di"
	"github.com/polkiloo/pacman/internal/fxrun"
	"github.com/polkiloo/pacman/internal/pgext"
	"github.com/polkiloo/pacman/internal/security"
)

type commandOptions struct {
	ShowVersion  bool
	ConfigPath   string
	LoadPGExtEnv bool
	ParseErr     error
}

type commandOptionsParams struct {
	fx.In

	Args   []string  `name:"args"`
	Stderr io.Writer `name:"stderr"`
}

type runtimeConfig struct {
	Config config.Config
	Source string
	Path   string
	Err    error
}

// Module wires the pacmand command graph, including argument parsing and
// runtime config loading, into a single Fx module.
func Module(processName string, args []string, stdout, stderr io.Writer) fx.Option {
	return fx.Module(
		"pacmand",
		di.ProvideBase(processName, args, stdout, stderr),
		fx.Provide(newCommandOptions),
		fx.Provide(newRuntimeConfig),
		fx.Provide(newLoadedConfig),
		security.TLSModule(),
		fx.Provide(New),
		fx.Invoke(registerRunner),
	)
}

type runnerParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Shutdowner fx.Shutdowner
	Logger     *slog.Logger
	Context    context.Context
	App        *App
}

func registerRunner(params runnerParams) {
	fxrun.RegisterCommand(params.Lifecycle, params.Shutdowner, params.Logger, params.Context, params.App.Run)
}

func newCommandOptions(params commandOptionsParams) commandOptions {
	fs := flag.NewFlagSet("pacmand", flag.ContinueOnError)
	fs.SetOutput(params.Stderr)

	showVersion := fs.Bool("version", false, "print version and exit")
	configPath := fs.String("config", "", "path to pacmand node configuration file")
	loadPGExtEnv := fs.Bool("pgext-env", false, "load PACMAN node configuration from PostgreSQL extension environment")

	if err := fs.Parse(params.Args); err != nil {
		return commandOptions{ParseErr: err}
	}

	return commandOptions{
		ShowVersion:  *showVersion,
		ConfigPath:   strings.TrimSpace(*configPath),
		LoadPGExtEnv: *loadPGExtEnv,
	}
}

func newRuntimeConfig(options commandOptions) *runtimeConfig {
	if options.ParseErr != nil || options.ShowVersion {
		return nil
	}

	if options.LoadPGExtEnv && options.ConfigPath != "" {
		return &runtimeConfig{Err: errConfigSourceConflict}
	}

	switch {
	case options.LoadPGExtEnv:
		loadedConfig, err := loadFromPGExtEnv()
		return &runtimeConfig{
			Config: loadedConfig,
			Source: "pgext-env",
			Err:    err,
		}
	case options.ConfigPath == "":
		return &runtimeConfig{Err: errConfigPathRequired}
	default:
		loadedConfig, err := config.Load(options.ConfigPath)
		return &runtimeConfig{
			Config: loadedConfig,
			Source: "file",
			Path:   options.ConfigPath,
			Err:    err,
		}
	}
}

func loadFromPGExtEnv() (config.Config, error) {
	snapshot, err := pgext.LoadSnapshotFromEnv(os.LookupEnv)
	if err != nil {
		return config.Config{}, err
	}

	return snapshot.RuntimeConfig()
}

func newLoadedConfig(runtime *runtimeConfig) *config.Config {
	if runtime == nil || runtime.Err != nil {
		return nil
	}

	loadedConfig := runtime.Config
	return &loadedConfig
}
