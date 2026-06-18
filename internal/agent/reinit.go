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

// localReinitRecoveryConfigurator renders WAL-G archive recovery settings into
// the restored PostgreSQL data directory before PostgreSQL starts.
type localReinitRecoveryConfigurator struct {
	dataDir             string
	walg                config.WALGConfig
	replicationUser     string
	replicationPassword string
}

func (c *localReinitRecoveryConfigurator) ConfigureReinitRecovery(_ context.Context, req controlplane.ReinitRecoveryConfigRequest) (controlplane.ReinitRecoveryConfigResult, error) {
	restoreCommand, err := c.walg.WALFetchRestoreCommand(nil, nil)
	if err != nil {
		return controlplane.ReinitRecoveryConfigResult{}, err
	}

	standby := req.Standby
	standby.RestoreCommand = restoreCommand
	standby.PrimaryConnInfo = mergePrimaryConnInfoCredentials(
		standby.PrimaryConnInfo,
		c.replicationUser,
		c.replicationPassword,
	)

	rendered, err := postgres.RenderStandbyFiles(c.dataDir, standby)
	if err != nil {
		return controlplane.ReinitRecoveryConfigResult{}, fmt.Errorf("render reinit recovery files: %w", err)
	}

	merged, err := mergePostgresAutoConf(rendered.PostgresAutoConfPath, rendered.PostgresAutoConf)
	if err != nil {
		return controlplane.ReinitRecoveryConfigResult{}, err
	}

	if err := os.WriteFile(rendered.PostgresAutoConfPath, []byte(merged), 0640); err != nil {
		return controlplane.ReinitRecoveryConfigResult{}, fmt.Errorf("write postgresql.auto.conf: %w", err)
	}

	if err := os.WriteFile(rendered.StandbySignalPath, nil, 0640); err != nil {
		return controlplane.ReinitRecoveryConfigResult{}, fmt.Errorf("write standby.signal: %w", err)
	}

	return controlplane.ReinitRecoveryConfigResult{
		DataDir:        strings.TrimSpace(c.dataDir),
		RestoreCommand: restoreCommand,
	}, nil
}

// pgCtlReinitStandbyRestarter starts the restored reinit target as a standby.
type pgCtlReinitStandbyRestarter struct {
	pgCtl *postgres.PGCtl
}

func (r *pgCtlReinitStandbyRestarter) RestartReinitStandby(ctx context.Context, _ controlplane.ReinitStandbyRestartRequest) error {
	running, err := r.pgCtl.Status(ctx)
	if err != nil {
		return err
	}
	if running {
		if err := r.pgCtl.Stop(ctx, postgres.ShutdownModeFast); err != nil {
			return err
		}
	}
	return r.pgCtl.StartNoWait(ctx)
}

// localReinitReplicationVerifier verifies the restored PostgreSQL instance is
// attached to the expected primary as a streaming standby.
type localReinitReplicationVerifier struct {
	address string
	walg    config.WALGConfig
}

func (v *localReinitReplicationVerifier) VerifyReinitReplication(ctx context.Context, _ controlplane.ReinitReplicationVerificationRequest) (controlplane.ReinitReplicationVerificationResult, error) {
	backupName := v.walg.WithDefaults().RestoreBackupName()
	verification, err := postgres.QueryReinitReplicationVerification(ctx, v.address, backupName)
	if err != nil {
		return controlplane.ReinitReplicationVerificationResult{}, err
	}

	return controlplane.ReinitReplicationVerificationResult{
		SystemIdentifier:  verification.SystemIdentifier,
		Timeline:          verification.Timeline,
		BackupName:        verification.BackupName,
		PrimarySlotName:   verification.PrimarySlotName,
		WALReceiverStatus: verification.WALReceiverStatus,
		InRecovery:        verification.InRecovery,
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
		if daemon.config.Reinit == nil || daemon.config.Reinit.WALG == nil {
			return
		}

		verifier := &localReinitReplicationVerifier{
			address: currentPostgres.Address,
			walg:    *daemon.config.Reinit.WALG,
		}
		execution, err := engine.ExecuteReinitVerifyReplication(ctx, daemon.config.Node.Name, verifier)
		if err != nil {
			if !errors.Is(err, controlplane.ErrReinitExecutionRequired) &&
				!errors.Is(err, controlplane.ErrReinitExecutionChanged) &&
				!errors.Is(err, controlplane.ErrReinitReplicationNotHealthy) {
				daemon.logger.WarnContext(ctx, "reinit replication verification failed",
					daemon.logArgs("agent", slog.String("error", err.Error()))...)
			}
			return
		}

		daemon.logger.InfoContext(ctx, "reinit replication verified",
			daemon.logArgs("agent",
				slog.String("backup_name", execution.WALGBackupName),
				slog.String("primary_slot_name", execution.PrimarySlotName),
				slog.String("wal_receiver_status", execution.WALReceiverStatus))...)
		return
	}

	if daemon.config.Postgres == nil {
		return
	}

	if daemon.stateReader != nil {
		storedStatus, _ := daemon.stateReader.NodeStatus(daemon.config.Node.Name)
		if storedStatus.PendingRestart || storedStatus.Postgres.Details.PendingRestart {
			restarter := &pgCtlReinitStandbyRestarter{pgCtl: daemon.pgCtl}
			if _, err := engine.ExecuteReinitRestartAsStandby(ctx, daemon.config.Node.Name, restarter); err != nil {
				if !errors.Is(err, controlplane.ErrReinitExecutionRequired) &&
					!errors.Is(err, controlplane.ErrReinitExecutionChanged) {
					daemon.logger.WarnContext(ctx, "reinit standby restart failed",
						daemon.logArgs("agent", slog.String("error", err.Error()))...)
				}
				return
			}

			daemon.logger.InfoContext(ctx, "reinit standby restart started",
				daemon.logArgs("agent")...)
			return
		}
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
		if errors.Is(err, controlplane.ErrReinitExecutionRequired) {
			return
		}

		if !errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			daemon.logger.WarnContext(ctx, "reinit WAL-G restore failed",
				daemon.logArgs("agent", slog.String("error", err.Error()))...)
			return
		}
	} else {
		daemon.logger.InfoContext(ctx, "reinit WAL-G restore completed",
			daemon.logArgs("agent", slog.String("backup_name", execution.WALGBackupName))...)
		return
	}

	configurator := &localReinitRecoveryConfigurator{
		dataDir:             daemon.config.Postgres.DataDir,
		walg:                *daemon.config.Reinit.WALG,
		replicationUser:     daemon.config.Postgres.ReplicationUser,
		replicationPassword: daemon.config.Postgres.ReplicationPassword,
	}
	if _, err := engine.ExecuteReinitRecoveryConfig(ctx, daemon.config.Node.Name, configurator); err != nil {
		if errors.Is(err, controlplane.ErrReinitExecutionRequired) ||
			errors.Is(err, controlplane.ErrReinitExecutionChanged) {
			return
		}

		daemon.logger.WarnContext(ctx, "reinit recovery config failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	} else {
		daemon.logger.InfoContext(ctx, "reinit recovery configured",
			daemon.logArgs("agent")...)
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
