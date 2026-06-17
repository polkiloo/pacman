package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/config"
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

// localReinitWALGRestorer implements controlplane.ReinitWALGRestoreExecutor
// using the node-local WAL-G binary and configured repository environment.
type localReinitWALGRestorer struct {
	dataDir string
	walg    config.WALGConfig
}

func (r *localReinitWALGRestorer) RestoreFromWALG(ctx context.Context, _ controlplane.ReinitWALGRestoreRequest) (controlplane.ReinitWALGRestoreResult, error) {
	if err := ctx.Err(); err != nil {
		return controlplane.ReinitWALGRestoreResult{}, err
	}

	walg := r.walg.WithDefaults()
	binary, args, err := walg.BackupFetchCommand(r.dataDir)
	if err != nil {
		return controlplane.ReinitWALGRestoreResult{}, err
	}

	restoreEnv, err := walg.RestoreEnvironment(nil, nil)
	if err != nil {
		return controlplane.ReinitWALGRestoreResult{}, err
	}

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), environmentMapToList(restoreEnv)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		renderedOutput := strings.TrimSpace(string(output))
		if renderedOutput == "" {
			return controlplane.ReinitWALGRestoreResult{}, fmt.Errorf("run WAL-G backup-fetch for reinit: %w", err)
		}
		return controlplane.ReinitWALGRestoreResult{}, fmt.Errorf("run WAL-G backup-fetch for reinit: %w: %s", err, renderedOutput)
	}

	return controlplane.ReinitWALGRestoreResult{
		DataDir:    strings.TrimSpace(r.dataDir),
		BackupName: walg.RestoreBackupName(),
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
		if errors.Is(err, controlplane.ErrReinitExecutionRequired) {
			return
		}

		if !errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			daemon.logger.WarnContext(ctx, "reinit data directory archive failed",
				daemon.logArgs("agent", slog.String("error", err.Error()))...)
			return
		}
	} else {
		daemon.logger.InfoContext(ctx, "reinit data directory archived",
			daemon.logArgs("agent", slog.String("archive_path", execution.ArchivePath))...)
		return
	}

	if daemon.config.Reinit == nil || daemon.config.Reinit.WALG == nil {
		return
	}

	restorer := &localReinitWALGRestorer{
		dataDir: daemon.config.Postgres.DataDir,
		walg:    *daemon.config.Reinit.WALG,
	}
	if execution, err := engine.ExecuteReinitWALGRestore(ctx, daemon.config.Node.Name, restorer); err != nil {
		if errors.Is(err, controlplane.ErrReinitExecutionRequired) ||
			errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			return
		}

		daemon.logger.WarnContext(ctx, "reinit WAL-G restore failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	} else {
		daemon.logger.InfoContext(ctx, "reinit WAL-G restore completed",
			daemon.logArgs("agent", slog.String("backup_name", execution.WALGBackupName))...)
	}
}

func environmentMapToList(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	env := make([]string, 0, len(values))
	for _, name := range names {
		env = append(env, name+"="+values[name])
	}

	return env
}
