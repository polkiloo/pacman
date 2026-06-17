package agent

import (
	"context"
	"errors"
	"log/slog"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/postgres"
)

// pgCtlReinitStopper implements controlplane.ReinitPostgresStopExecutor using
// the local pg_ctl wrapper.
type pgCtlReinitStopper struct {
	pgCtl *postgres.PGCtl
}

func (s *pgCtlReinitStopper) StopPostgres(ctx context.Context, _ controlplane.ReinitPostgresStopRequest) error {
	running, err := s.pgCtl.Status(ctx)
	if err != nil {
		return err
	}
	if !running {
		return nil
	}

	return s.pgCtl.Stop(ctx, postgres.ShutdownModeFast)
}

func (daemon *Daemon) reconcileReinit(ctx context.Context, currentPostgres agentmodel.PostgresStatus) {
	if daemon.pgCtl == nil || !currentPostgres.Managed || !currentPostgres.Up {
		return
	}

	engine, ok := daemon.statePublisher.(controlplane.ReinitEngine)
	if !ok {
		return
	}

	stopper := &pgCtlReinitStopper{pgCtl: daemon.pgCtl}
	if _, err := engine.ExecuteReinitStopPostgres(ctx, daemon.config.Node.Name, stopper); err != nil {
		if errors.Is(err, controlplane.ErrReinitExecutionRequired) ||
			errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			return
		}

		daemon.logger.WarnContext(ctx, "reinit PostgreSQL stop failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	}

	daemon.logger.InfoContext(ctx, "reinit PostgreSQL stopped",
		daemon.logArgs("agent")...)
}
