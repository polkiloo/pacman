//go:build integration

package integration_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

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

func testenvPostgresImage() string {
	for _, name := range []string{"PACMAN_TEST_POSTGRES_IMAGE", "PACMAN_TEST_PGEXT_IMAGE"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}

	return "pacman-pgext-postgres:local"
}
