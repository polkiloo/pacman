package agent

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestPGCtlReinitStopperStopsRunningPostgres(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  exit 0
fi
if [ "$1" = "stop" ]; then
  exit 0
fi
exit 1
`)

	stopper := &pgCtlReinitStopper{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := stopper.StopPostgres(context.Background(), controlplane.ReinitPostgresStopRequest{}); err != nil {
		t.Fatalf("stop postgres for reinit: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
		"stop -D /var/lib/postgresql/data -w -m fast",
	})
}

func TestPGCtlReinitStopperSkipsStoppedPostgres(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  exit 3
fi
exit 1
`)

	stopper := &pgCtlReinitStopper{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := stopper.StopPostgres(context.Background(), controlplane.ReinitPostgresStopRequest{}); err != nil {
		t.Fatalf("stop postgres for reinit: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
	})
}

func TestLocalReinitDataDirArchiverArchivesWithOperationID(t *testing.T) {
	t.Parallel()

	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	archiver := &localReinitDataDirArchiver{dataDir: dataDir}
	result, err := archiver.ArchiveDataDir(context.Background(), controlplane.ReinitDataDirArchiveRequest{
		Operation: cluster.Operation{ID: "reinit-20260617T120000Z"},
	})
	if err != nil {
		t.Fatalf("archive data dir: %v", err)
	}

	if !result.Archived || !strings.Contains(result.ArchivePath, "reinit-20260617T120000Z") {
		t.Fatalf("unexpected archive result: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(result.ArchivePath, "PG_VERSION")); err != nil {
		t.Fatalf("expected marker in archive: %v", err)
	}
}

func TestLocalReinitWALGRestorerRunsBackupFetchWithConfiguredEnvironment(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "wal-g", `#!/bin/sh
trace=%q
printf 'args=%%s\n' "$*" >> "$trace"
printf 'prefix=%%s\n' "$WALG_FILE_PREFIX" >> "$trace"
mkdir -p "$2"
printf '17\n' > "$2/PG_VERSION"
exit 0
`)

	dataDir := filepath.Join(t.TempDir(), "restore")
	restorer := &localReinitWALGRestorer{
		dataDir: dataDir,
		walg: config.WALGConfig{
			Binary: filepath.Join(binDir, "wal-g"),
			Repository: config.WALGRepositoryConfig{
				Provider: config.WALGRepositoryProviderFilesystem,
				Prefix:   "/backups/alpha",
			},
			Restore: config.WALGRestoreConfig{BackupName: "base_000000010000000000000005"},
		},
	}

	result, err := restorer.RestoreFromWALG(context.Background(), controlplane.ReinitWALGRestoreRequest{})
	if err != nil {
		t.Fatalf("restore from WAL-G: %v", err)
	}

	if result.DataDir != dataDir || result.BackupName != "base_000000010000000000000005" {
		t.Fatalf("unexpected WAL-G restore result: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "PG_VERSION")); err != nil {
		t.Fatalf("expected restored data dir marker: %v", err)
	}
	if info, err := os.Stat(dataDir); err != nil {
		t.Fatalf("stat restored data dir: %v", err)
	} else if info.Mode().Perm() != 0o700 {
		t.Fatalf("expected restored data dir mode 0700, got %v", info.Mode().Perm())
	}
	assertTraceLines(t, tracePath, []string{
		"args=backup-fetch " + dataDir + " base_000000010000000000000005",
		"prefix=/backups/alpha",
	})
}

func TestLocalReinitRecoveryConfiguratorWritesWALGRestoreCommand(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, postgres.PostgresAutoConfFileName), []byte(strings.Join([]string{
		"listen_addresses = '*'",
		"restore_command = 'old restore'",
		"primary_conninfo = 'old primary'",
		"",
	}, "\n")), 0o640); err != nil {
		t.Fatalf("write existing auto conf: %v", err)
	}

	configurator := &localReinitRecoveryConfigurator{
		dataDir:             dataDir,
		replicationUser:     "replicator",
		replicationPassword: "replicator secret",
		walg: config.WALGConfig{
			Binary: "/usr/local/bin/wal-g",
			Repository: config.WALGRepositoryConfig{
				Provider: config.WALGRepositoryProviderFilesystem,
				Prefix:   "/backups/alpha",
			},
		},
	}

	result, err := configurator.ConfigureReinitRecovery(context.Background(), controlplane.ReinitRecoveryConfigRequest{
		Standby: postgres.StandbyConfig{
			PrimaryConnInfo: "host=alpha-1 port=5432 application_name=alpha-2",
		},
	})
	if err != nil {
		t.Fatalf("configure reinit recovery: %v", err)
	}

	if result.DataDir != dataDir || result.RestoreCommand == "" {
		t.Fatalf("unexpected recovery config result: %+v", result)
	}

	rendered := readTestFile(t, filepath.Join(dataDir, postgres.PostgresAutoConfFileName))
	assertContains(t, rendered, "listen_addresses = '*'")
	assertContains(t, rendered, "primary_conninfo = 'host=alpha-1 port=5432 application_name=alpha-2 user=replicator password=''replicator secret'''")
	assertContains(t, rendered, "restore_command = 'env ''WALG_FILE_PREFIX=/backups/alpha'' ''/usr/local/bin/wal-g'' wal-fetch ''%f'' ''%p'''")
	assertContains(t, rendered, "recovery_target_timeline = 'latest'")
	if strings.Contains(rendered, "old restore") || strings.Contains(rendered, "old primary") {
		t.Fatalf("expected old recovery settings to be replaced, got %q", rendered)
	}
	if _, err := os.Stat(filepath.Join(dataDir, postgres.StandbySignalFileName)); err != nil {
		t.Fatalf("standby.signal not written: %v", err)
	}
}

func TestDaemonReconcileReinit(t *testing.T) {
	t.Parallel()

	t.Run("executes PostgreSQL stop for running managed target", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		})

		if engine.stopCalls != 1 || engine.stopMember != "alpha-2" {
			t.Fatalf("unexpected reinit stop calls: calls=%d member=%q", engine.stopCalls, engine.stopMember)
		}
		assertContains(t, logs.String(), `"msg":"reinit PostgreSQL stopped"`)
	})

	t.Run("archives data directory when PostgreSQL is already down", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{stopErr: controlplane.ErrReinitExecutionChanged}
		daemon, _ := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 1 || engine.archiveMember != "alpha-2" {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d member=%q", engine.stopCalls, engine.archiveCalls, engine.archiveMember)
		}
	})

	t.Run("restores from WAL-G when archive phase is already complete", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{
			stopErr:    controlplane.ErrReinitExecutionChanged,
			archiveErr: controlplane.ErrReinitExecutionChanged,
			restoreResult: controlplane.ReinitExecution{
				WALGBackupName: "LATEST",
			},
		}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 1 || engine.restoreCalls != 1 || engine.restoreMember != "alpha-2" {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d restore=%d member=%q", engine.stopCalls, engine.archiveCalls, engine.restoreCalls, engine.restoreMember)
		}
		assertContains(t, logs.String(), `"msg":"reinit WAL-G restore completed"`)
	})

	t.Run("renders recovery config when WAL-G restore phase is complete", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{
			stopErr:    controlplane.ErrReinitExecutionChanged,
			archiveErr: controlplane.ErrReinitExecutionChanged,
			restoreErr: controlplane.ErrReinitExecutionChanged,
		}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 1 || engine.restoreCalls != 1 || engine.recoveryConfigCalls != 1 || engine.recoveryConfigMember != "alpha-2" {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d restore=%d recovery=%d member=%q",
				engine.stopCalls, engine.archiveCalls, engine.restoreCalls, engine.recoveryConfigCalls, engine.recoveryConfigMember)
		}
		assertContains(t, logs.String(), `"msg":"reinit recovery configured"`)
	})

	t.Run("restarts standby when recovery config marked pending restart", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{
			stopErr: controlplane.ErrReinitExecutionChanged,
		}
		daemon, logs := newReinitTestDaemon(t, engine)
		daemon.stateReader = stubNodeStatusReader{
			status: agentmodel.NodeStatus{
				PendingRestart: true,
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{PendingRestart: true},
				},
			},
			ok: true,
		}

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.restartCalls != 1 || engine.restartMember != "alpha-2" {
			t.Fatalf("unexpected reinit restart calls: calls=%d member=%q", engine.restartCalls, engine.restartMember)
		}
		if engine.archiveCalls != 0 || engine.restoreCalls != 0 || engine.recoveryConfigCalls != 0 {
			t.Fatalf("expected restart to short-circuit destructive phases, archive=%d restore=%d recovery=%d", engine.archiveCalls, engine.restoreCalls, engine.recoveryConfigCalls)
		}
		assertContains(t, logs.String(), `"msg":"reinit standby restart started"`)
	})

	t.Run("verifies restored standby replication when PostgreSQL is up", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{
			stopErr: controlplane.ErrReinitExecutionChanged,
			verifyResult: controlplane.ReinitExecution{
				WALGBackupName:      "LATEST",
				PrimarySlotName:     "alpha_2",
				WALReceiverStatus:   "streaming",
				ReplicationVerified: true,
			},
		}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Address: "127.0.0.1:5432",
		})

		if engine.verifyCalls != 1 || engine.verifyMember != "alpha-2" {
			t.Fatalf("unexpected reinit verification calls: calls=%d member=%q", engine.verifyCalls, engine.verifyMember)
		}
		if engine.archiveCalls != 0 || engine.restoreCalls != 0 || engine.recoveryConfigCalls != 0 || engine.restartCalls != 0 {
			t.Fatalf("expected verification to short-circuit other phases, archive=%d restore=%d recovery=%d restart=%d",
				engine.archiveCalls, engine.restoreCalls, engine.recoveryConfigCalls, engine.restartCalls)
		}
		assertContains(t, logs.String(), `"msg":"reinit replication verified"`)
	})

	t.Run("publishes stopped phase when PostgreSQL is already down", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 0 {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d", engine.stopCalls, engine.archiveCalls)
		}
		assertContains(t, logs.String(), `"msg":"reinit PostgreSQL stopped"`)
	})

	t.Run("suppresses missing active reinit operation", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{stopErr: controlplane.ErrReinitExecutionRequired}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		})

		if engine.stopCalls != 1 {
			t.Fatalf("expected one reinit stop attempt, got %d", engine.stopCalls)
		}
		if strings.Contains(logs.String(), "reinit PostgreSQL stop failed") {
			t.Fatalf("expected missing active operation to be quiet, logs=%s", logs.String())
		}
	})
}

func newReinitTestDaemon(t *testing.T, publisher controlplane.NodeStatePublisher) (*Daemon, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer
	daemon := &Daemon{
		config: config.Config{
			Node: config.NodeConfig{
				Name: "alpha-2",
				Role: cluster.NodeRoleData,
			},
			Postgres: &config.PostgresLocalConfig{
				DataDir: t.TempDir(),
			},
			Reinit: &config.ReinitConfig{
				WALG: &config.WALGConfig{
					Binary: "/usr/local/bin/wal-g",
					Repository: config.WALGRepositoryConfig{
						Provider: config.WALGRepositoryProviderFilesystem,
						Prefix:   "/backups/alpha",
					},
				},
			},
		},
		logger:         logging.New("pacmand", &logs),
		pgCtl:          &postgres.PGCtl{DataDir: t.TempDir()},
		statePublisher: publisher,
	}

	return daemon, &logs
}

type recordingReinitPublisher struct {
	stopCalls            int
	stopMember           string
	stopErr              error
	archiveCalls         int
	archiveMember        string
	archiveErr           error
	restoreCalls         int
	restoreMember        string
	restoreResult        controlplane.ReinitExecution
	restoreErr           error
	recoveryConfigCalls  int
	recoveryConfigMember string
	recoveryConfigErr    error
	restartCalls         int
	restartMember        string
	restartErr           error
	verifyCalls          int
	verifyMember         string
	verifyResult         controlplane.ReinitExecution
	verifyErr            error
}

func (*recordingReinitPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (*recordingReinitPublisher) ValidateReinit(context.Context, controlplane.ReinitRequest) (controlplane.ReinitValidation, error) {
	return controlplane.ReinitValidation{}, nil
}

func (*recordingReinitPublisher) CreateReinitIntent(context.Context, controlplane.ReinitRequest) (controlplane.ReinitIntent, error) {
	return controlplane.ReinitIntent{}, nil
}

func (publisher *recordingReinitPublisher) ExecuteReinitStopPostgres(_ context.Context, member string, _ controlplane.ReinitPostgresStopExecutor) (controlplane.ReinitExecution, error) {
	publisher.stopCalls++
	publisher.stopMember = member
	return controlplane.ReinitExecution{}, publisher.stopErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitArchiveDataDir(_ context.Context, member string, _ controlplane.ReinitDataDirArchiveExecutor) (controlplane.ReinitExecution, error) {
	publisher.archiveCalls++
	publisher.archiveMember = member
	return controlplane.ReinitExecution{ArchivePath: "/archive/data"}, publisher.archiveErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitWALGRestore(_ context.Context, member string, _ controlplane.ReinitWALGRestoreExecutor) (controlplane.ReinitExecution, error) {
	publisher.restoreCalls++
	publisher.restoreMember = member
	return publisher.restoreResult.Clone(), publisher.restoreErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitRecoveryConfig(_ context.Context, member string, _ controlplane.ReinitRecoveryConfigExecutor) (controlplane.ReinitExecution, error) {
	publisher.recoveryConfigCalls++
	publisher.recoveryConfigMember = member
	return controlplane.ReinitExecution{RecoveryConfig: true}, publisher.recoveryConfigErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitRestartAsStandby(_ context.Context, member string, _ controlplane.ReinitStandbyRestartExecutor) (controlplane.ReinitExecution, error) {
	publisher.restartCalls++
	publisher.restartMember = member
	return controlplane.ReinitExecution{RestartedAsStandby: true}, publisher.restartErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitVerifyReplication(_ context.Context, member string, _ controlplane.ReinitReplicationVerifier) (controlplane.ReinitExecution, error) {
	publisher.verifyCalls++
	publisher.verifyMember = member
	return publisher.verifyResult.Clone(), publisher.verifyErr
}
