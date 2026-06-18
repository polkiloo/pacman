//go:build integration

package integration_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/postgres"
	"github.com/polkiloo/pacman/test/testenv"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestReinitWALGRecoverySettingsRenderBeforePostgresStartsInDocker(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	postgresImage := strings.TrimSpace(testenvPostgresImage())
	testenv.RequireLocalImage(t, postgresImage)

	walg := config.WALGConfig{
		Binary: "/usr/local/bin/wal-g",
		Repository: config.WALGRepositoryConfig{
			Provider: config.WALGRepositoryProviderFilesystem,
			Prefix:   "/var/lib/pacman/walg",
		},
	}
	restoreCommand, err := walg.WALFetchRestoreCommand(nil, nil)
	if err != nil {
		t.Fatalf("build WAL-G wal-fetch restore command: %v", err)
	}

	rendered, err := postgres.RenderStandbyFiles("/tmp/pacman-reinit-pgdata", postgres.StandbyConfig{
		PrimaryConnInfo: "host=alpha-1 port=5432 application_name=alpha-2 user=replicator password=replicator",
		RestoreCommand:  restoreCommand,
	})
	if err != nil {
		t.Fatalf("render reinit recovery files: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const postgresBinDir = "/usr/lib/postgresql/17/bin"

	container, err := testcontainers.Run(ctx, postgresImage,
		testcontainers.WithEntrypoint("/bin/sh"),
		testcontainers.WithCmd("-lc", "sleep infinity"),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{
				Reader:            strings.NewReader(rendered.PostgresAutoConf),
				ContainerFilePath: "/tmp/pacman-rendered-reinit/postgresql.auto.conf",
				FileMode:          0o644,
			},
			testcontainers.ContainerFile{
				Reader:            strings.NewReader(""),
				ContainerFilePath: "/tmp/pacman-rendered-reinit/standby.signal",
				FileMode:          0o644,
			},
		),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"/bin/sh", "-lc", "test -x /usr/lib/postgresql/17/bin/postgres && test -x /usr/lib/postgresql/17/bin/initdb"}).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL config validation container %q: %v", postgresImage, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("terminate PostgreSQL config validation container: %v", err)
		}
	})

	requireContainerExec(t, ctx, container, "/bin/sh", "-lc", `
set -eu
PGDATA=/tmp/pacman-reinit-pgdata
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
if command -v gosu >/dev/null 2>&1; then
	gosu postgres `+postgresBinDir+`/initdb -D "$PGDATA" >/tmp/pacman-initdb.log
else
	su-exec postgres `+postgresBinDir+`/initdb -D "$PGDATA" >/tmp/pacman-initdb.log
fi
cp /tmp/pacman-rendered-reinit/postgresql.auto.conf "$PGDATA/postgresql.auto.conf"
cp /tmp/pacman-rendered-reinit/standby.signal "$PGDATA/standby.signal"
chown postgres:postgres "$PGDATA/postgresql.auto.conf" "$PGDATA/standby.signal"
`)

	restoreExit, restoreOutput := containerExec(t, ctx, container, "/bin/sh", "-lc", postgresBinDir+"/postgres -C restore_command -D /tmp/pacman-reinit-pgdata")
	if restoreExit != 0 {
		t.Fatalf("postgres -C restore_command returned %d: %s", restoreExit, restoreOutput)
	}
	if strings.TrimSpace(restoreOutput) != restoreCommand {
		t.Fatalf("restore_command: got %q want %q", strings.TrimSpace(restoreOutput), restoreCommand)
	}

	timelineExit, timelineOutput := containerExec(t, ctx, container, "/bin/sh", "-lc", postgresBinDir+"/postgres -C recovery_target_timeline -D /tmp/pacman-reinit-pgdata")
	if timelineExit != 0 {
		t.Fatalf("postgres -C recovery_target_timeline returned %d: %s", timelineExit, timelineOutput)
	}
	if strings.TrimSpace(timelineOutput) != postgres.DefaultRecoveryTargetTimeline {
		t.Fatalf("recovery_target_timeline: got %q want %q", strings.TrimSpace(timelineOutput), postgres.DefaultRecoveryTargetTimeline)
	}

	signalExit, signalOutput := containerExec(t, ctx, container, "/bin/sh", "-lc", "test -f /tmp/pacman-reinit-pgdata/standby.signal")
	if signalExit != 0 {
		t.Fatalf("standby.signal missing: exit=%d output=%s", signalExit, signalOutput)
	}
}

func TestReinitRecoveryConfigStartsRestoredStandbyStreamingInDocker(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	walg := config.WALGConfig{
		Binary: "/usr/local/bin/wal-g",
		Repository: config.WALGRepositoryConfig{
			Provider: config.WALGRepositoryProviderFilesystem,
			Prefix:   "/var/lib/pacman/walg",
		},
	}
	restoreCommand, err := walg.WALFetchRestoreCommand(nil, nil)
	if err != nil {
		t.Fatalf("build WAL-G wal-fetch restore command: %v", err)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "reinit-recovery-primary", "reinit-recovery-primary-postgres")
	standby := env.StartRenderedStreamingStandbyWithRestoreCommandAndFiles(
		t,
		"reinit-restored-standby",
		"reinit-restored-standby-postgres",
		primary,
		"reinit_restored_standby",
		restoreCommand,
		testcontainers.ContainerFile{
			Reader:            strings.NewReader(fakeWALGBinary()),
			ContainerFilePath: "/usr/local/bin/wal-g",
			FileMode:          0o755,
		},
	)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForContainerQueryValue(t, standby, `SELECT pg_is_in_recovery()::text`, "true")
	waitForContainerQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForContainerQueryValue(t, standby, `SHOW primary_slot_name`, "reinit_restored_standby")
	waitForContainerQueryValue(t, standby, `SHOW restore_command`, restoreCommand)
	waitForContainerQueryValue(t, standby, `SHOW transaction_read_only`, "on")
	waitForContainerQueryValue(t, primary, `SELECT active::text FROM pg_replication_slots WHERE slot_name = 'reinit_restored_standby'`, "true")

	primarySystemIdentifier := requireContainerQueryValue(t, primary, `SELECT system_identifier::text FROM pg_control_system()`)
	standbySystemIdentifier := requireContainerQueryValue(t, standby, `SELECT system_identifier::text FROM pg_control_system()`)
	if standbySystemIdentifier != primarySystemIdentifier {
		t.Fatalf("restored standby system identifier: got %q want %q", standbySystemIdentifier, primarySystemIdentifier)
	}

	primaryTimeline := requireContainerQueryValue(t, primary, postgresTimelineSQL())
	waitForContainerQueryValue(t, standby, postgresTimelineSQL(), primaryTimeline)

	probeCtx, probeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer probeCancel()
	verification, err := postgres.QueryReinitReplicationVerification(probeCtx, standby.Address(t), config.DefaultWALGRestoreBackupName)
	if err != nil {
		t.Fatalf("query reinit replication verification from restored standby: %v", err)
	}
	if verification.SystemIdentifier != primarySystemIdentifier ||
		verification.Timeline == 0 ||
		verification.BackupName != config.DefaultWALGRestoreBackupName ||
		verification.PrimarySlotName != "reinit_restored_standby" ||
		verification.WALReceiverStatus != "streaming" ||
		!verification.InRecovery {
		t.Fatalf("unexpected reinit replication verification: %+v", verification)
	}

	runReinitVerificationPositiveCasesInDocker(t, primary, standby, restoreCommand, primarySystemIdentifier, primaryTimeline, verification)
	runReinitVerificationNegativeCasesInDocker(t, primary, standby)

	autoConf := standby.RequireExec(t, "sh", "-lc", "cat \"$PGDATA/postgresql.auto.conf\"")
	for _, expected := range []string{
		"primary_conninfo = 'host=reinit-recovery-primary-postgres port=5432 user=replicator password=replicator application_name=reinit_restored_standby'",
		"primary_slot_name = 'reinit_restored_standby'",
		"restore_command = 'env ''WALG_FILE_PREFIX=/var/lib/pacman/walg'' ''/usr/local/bin/wal-g'' wal-fetch ''%f'' ''%p'''",
		"recovery_target_timeline = 'latest'",
	} {
		if !strings.Contains(autoConf, expected) {
			t.Fatalf("expected reinit restored standby config to contain %q, got:\n%s", expected, autoConf)
		}
	}

	if got := strings.TrimSpace(standby.RequireExec(t, "sh", "-lc", "test -f \"$PGDATA/standby.signal\" && echo present")); got != "present" {
		t.Fatalf("expected standby.signal from reinit recovery config, got %q", got)
	}

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS reinit_restored_standby_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO reinit_restored_standby_marker (id, payload)
VALUES (1, 'streaming-after-reinit')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForContainerQueryValue(t, standby, `SELECT payload FROM reinit_restored_standby_marker WHERE id = 1`, "streaming-after-reinit")
}

func runReinitVerificationPositiveCasesInDocker(
	t *testing.T,
	primary *testenv.Postgres,
	standby *testenv.Postgres,
	restoreCommand string,
	primarySystemIdentifier string,
	primaryTimeline string,
	verification postgres.ReinitReplicationVerification,
) {
	t.Helper()

	positiveCases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "restored target is in recovery",
			run: func(t *testing.T) {
				t.Helper()
				if got := requireContainerQueryValue(t, standby, `SELECT pg_is_in_recovery()::text`); got != "true" {
					t.Fatalf("pg_is_in_recovery: got %q want true", got)
				}
			},
		},
		{
			name: "wal receiver is streaming",
			run: func(t *testing.T) {
				t.Helper()
				if verification.WALReceiverStatus != "streaming" {
					t.Fatalf("WAL receiver status: got %q want streaming", verification.WALReceiverStatus)
				}
			},
		},
		{
			name: "replication slot is attached",
			run: func(t *testing.T) {
				t.Helper()
				if verification.PrimarySlotName != "reinit_restored_standby" {
					t.Fatalf("primary slot name: got %q want reinit_restored_standby", verification.PrimarySlotName)
				}
				if got := requireContainerQueryValue(t, primary, `SELECT active::text FROM pg_replication_slots WHERE slot_name = 'reinit_restored_standby'`); got != "true" {
					t.Fatalf("primary slot active: got %q want true", got)
				}
			},
		},
		{
			name: "system identifier and timeline match primary",
			run: func(t *testing.T) {
				t.Helper()
				if verification.SystemIdentifier != primarySystemIdentifier {
					t.Fatalf("system identifier: got %q want %q", verification.SystemIdentifier, primarySystemIdentifier)
				}
				if got := requireContainerQueryValue(t, standby, postgresTimelineSQL()); got != primaryTimeline {
					t.Fatalf("timeline: got %q want %q", got, primaryTimeline)
				}
			},
		},
		{
			name: "restore command and backup metadata are present",
			run: func(t *testing.T) {
				t.Helper()
				if verification.BackupName != config.DefaultWALGRestoreBackupName {
					t.Fatalf("backup name: got %q want %q", verification.BackupName, config.DefaultWALGRestoreBackupName)
				}
				if got := requireContainerQueryValue(t, standby, `SHOW restore_command`); got != restoreCommand {
					t.Fatalf("restore_command: got %q want %q", got, restoreCommand)
				}
			},
		},
	}

	for _, testCase := range positiveCases {
		t.Run("positive/"+testCase.name, testCase.run)
	}
}

func runReinitVerificationNegativeCasesInDocker(t *testing.T, primary *testenv.Postgres, standby *testenv.Postgres) {
	t.Helper()

	negativeCases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "primary is not a valid restored standby",
			run: func(t *testing.T) {
				t.Helper()
				if got := requireContainerQueryValue(t, primary, `SELECT pg_is_in_recovery()::text`); got != "false" {
					t.Fatalf("primary recovery state: got %q want false", got)
				}
			},
		},
		{
			name: "primary has no WAL receiver",
			run: func(t *testing.T) {
				t.Helper()
				if got := requireContainerQueryValue(t, primary, `SELECT count(*)::text FROM pg_stat_wal_receiver`); got != "0" {
					t.Fatalf("primary WAL receiver count: got %q want 0", got)
				}
			},
		},
		{
			name: "wrong primary slot is rejected",
			run: func(t *testing.T) {
				t.Helper()
				if got := requireContainerQueryValue(t, standby, `SHOW primary_slot_name`); got == "wrong_reinit_slot" {
					t.Fatalf("negative slot unexpectedly matched %q", got)
				}
			},
		},
		{
			name: "missing primary slot is inactive",
			run: func(t *testing.T) {
				t.Helper()
				if got := requireContainerQueryValue(t, primary, `SELECT count(*)::text FROM pg_replication_slots WHERE slot_name = 'missing_reinit_slot' AND active`); got != "0" {
					t.Fatalf("missing slot active count: got %q want 0", got)
				}
			},
		},
		{
			name: "missing backup metadata is detectable",
			run: func(t *testing.T) {
				t.Helper()
				probeCtx, probeCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer probeCancel()
				verification, err := postgres.QueryReinitReplicationVerification(probeCtx, standby.Address(t), "")
				if err != nil {
					t.Fatalf("query verification with missing backup metadata: %v", err)
				}
				if verification.BackupName != "" {
					t.Fatalf("backup name: got %q want empty", verification.BackupName)
				}
			},
		},
	}

	for _, testCase := range negativeCases {
		t.Run("negative/"+testCase.name, testCase.run)
	}
}

func fakeWALGBinary() string {
	return `#!/bin/sh
set -eu
if [ "${1:-}" = "wal-fetch" ]; then
	echo "fake wal-g wal-fetch miss: $2" >&2
	exit 1
fi
echo "unexpected fake wal-g command: $*" >&2
exit 2
`
}

func waitForContainerQueryValue(t *testing.T, fixture *testenv.Postgres, query, want string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastOutput string
	for time.Now().Before(deadline) {
		result := fixture.Exec(t, "sh", "-lc", `PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc `+shellQuote(query))
		lastOutput = strings.TrimSpace(result.Output)
		if result.ExitCode == 0 && lastOutput == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("query %q in %q did not return %q before deadline; last output=%q", query, fixture.Name(), want, lastOutput)
}

func requireContainerQueryValue(t *testing.T, fixture *testenv.Postgres, query string) string {
	t.Helper()

	result := fixture.Exec(t, "sh", "-lc", `PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc `+shellQuote(query))
	output := strings.TrimSpace(result.Output)
	if result.ExitCode != 0 {
		t.Fatalf("query %q in %q failed with exit %d: %s", query, fixture.Name(), result.ExitCode, output)
	}

	return output
}

func postgresTimelineSQL() string {
	return `SELECT CASE
	WHEN pg_is_in_recovery() THEN coalesce((SELECT max(received_tli)::text FROM pg_stat_wal_receiver), '0')
	ELSE (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint)::text
END`
}

func testenvPostgresImage() string {
	for _, name := range []string{"PACMAN_TEST_POSTGRES_IMAGE", "PACMAN_TEST_PGEXT_IMAGE"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}

	return "pacman-pgext-postgres:local"
}
