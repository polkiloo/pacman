package localagent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/config"
)

// Run constructs the shared PACMAN local-agent runtime, starts it, and blocks
// until the caller cancels the context.
func Run(ctx context.Context, logger *slog.Logger, cfg config.Config, options ...agent.Option) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	daemon, err := agent.NewDaemon(cfg, logger, options...)
	if err != nil {
		return fmt.Errorf("construct local agent daemon: %w", err)
	}

	if err := daemon.Start(ctx); err != nil {
		return fmt.Errorf("start local agent daemon: %w", err)
	}

	daemon.Wait()

	return nil
}
