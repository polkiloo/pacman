package pacmand

import (
	"flag"
	"io"
	"os"
	"strings"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/pgext"
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
	Config   config.Config
	Source   string
	Path     string
	Format   config.DocumentFormat
	Warnings []string
	Err      error
}

// ConfigModule wires command argument parsing and runtime config loading into
// the Fx graph used by pacmand.
func ConfigModule() fx.Option {
	return fx.Module(
		"pacmand.config",
		fx.Provide(newCommandOptions),
		fx.Provide(newRuntimeConfig),
		fx.Provide(newLoadedConfig),
	)
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
		report, err := config.LoadWithReport(options.ConfigPath)
		return &runtimeConfig{
			Config:   report.Config,
			Source:   "file",
			Path:     options.ConfigPath,
			Format:   report.Format,
			Warnings: append([]string(nil), report.Warnings...),
			Err:      err,
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
