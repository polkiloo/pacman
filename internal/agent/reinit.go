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

// localReinitDataDirArchiver implements controlplane.ReinitDataDirArchiveExecutor
// using local filesystem operations.
type localReinitDataDirArchiver struct {
	dataDir string
}

func (a *localReinitDataDirArchiver) ArchiveDataDir(ctx context.Context, request controlplane.ReinitDataDirArchiveRequest) (controlplane.ReinitDataDirArchiveResult, error) {
	if err := ctx.Err(); err != nil {
		return controlplane.ReinitDataDirArchiveResult{}, err
	}

	result, err := (postgres.DataDirArchive{
		DataDir:     a.dataDir,
		ArchiveName: request.Operation.ID,
	}).ArchiveForReinit()
	if err != nil {
		return controlplane.ReinitDataDirArchiveResult{}, err
	}

	if err := ctx.Err(); err != nil {
		return controlplane.ReinitDataDirArchiveResult{}, err
	}

	return controlplane.ReinitDataDirArchiveResult{
		DataDir:     result.DataDir,
		ArchivePath: result.ArchivePath,
		Archived:    result.Archived,
	}, nil
}

func (daemon *Daemon) reconcileReinit(ctx context.Context, currentPostgres agentmodel.PostgresStatus) {
	if daemon.pgCtl == nil || !currentPostgres.Managed {
		return
	}

	engine, ok := daemon.statePublisher.(controlplane.ReinitEngine)
	if !ok {
		return
	}

	stopper := &pgCtlReinitStopper{pgCtl: daemon.pgCtl}
	if _, err := engine.ExecuteReinitStopPostgres(ctx, daemon.config.Node.Name, stopper); err != nil {
		if !errors.Is(err, controlplane.ErrReinitExecutionRequired) &&
			!errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			daemon.logger.WarnContext(ctx, "reinit PostgreSQL stop failed",
				daemon.logArgs("agent", slog.String("error", err.Error()))...)
			return
		}
	} else {
		daemon.logger.InfoContext(ctx, "reinit PostgreSQL stopped",
			daemon.logArgs("agent")...)
		return
	}

	if currentPostgres.Up {
		return
	}

	if daemon.config.Postgres == nil {
		return
	}

	archiver := &localReinitDataDirArchiver{dataDir: daemon.config.Postgres.DataDir}
	if execution, err := engine.ExecuteReinitArchiveDataDir(ctx, daemon.config.Node.Name, archiver); err != nil {
		if errors.Is(err, controlplane.ErrReinitExecutionRequired) ||
			errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			return
		}

		daemon.logger.WarnContext(ctx, "reinit data directory archive failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	} else {
		daemon.logger.InfoContext(ctx, "reinit data directory archived",
			daemon.logArgs("agent", slog.String("archive_path", execution.ArchivePath))...)
	}
}
