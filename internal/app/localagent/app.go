package localagent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/postgres"
)

// Run constructs the shared PACMAN local-agent runtime, starts it, and blocks
// until the caller cancels the context.
func Run(ctx context.Context, logger *slog.Logger, cfg config.Config, options ...agent.Option) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	defaulted := cfg.WithDefaults()
	options = append(options, localPostgresOptions(defaulted)...)

	daemon, err := agent.NewDaemon(cfg, logger, options...)
	if err != nil {
		return fmt.Errorf("construct local agent daemon: %w", err)
	}

	if err := daemon.Start(ctx); err != nil {
		return fmt.Errorf("start local agent daemon: %w", err)
	}

	daemon.Wait()

	if logger != nil {
		logger.InfoContext(
			ctx,
			"stopped local agent daemon",
			slog.String("component", "agent"),
			slog.String("node", defaulted.Node.Name),
			slog.String("node_role", defaulted.Node.Role.String()),
		)
	}

	return nil
}

func localPostgresOptions(cfg config.Config) []agent.Option {
	if cfg.Postgres == nil {
		return nil
	}

	ctl := &postgres.PGCtl{
		BinDir:  cfg.Postgres.BinDir,
		DataDir: cfg.Postgres.DataDir,
	}

	opts := []agent.Option{agent.WithLocalPostgresCtl(ctl)}

	if cfg.Security != nil {
		if token, err := cfg.Security.ResolveAdminBearerToken(nil); err == nil && token != "" {
			opts = append(opts, agent.WithAdminToken(token))
		}
	}

	return opts
}
