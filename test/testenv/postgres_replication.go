package testenv

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pgconfig "github.com/polkiloo/pacman/internal/postgres"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	replicationUsername = "replicator"
	replicationPassword = "replicator"
	defaultPostgresData = "/var/lib/postgresql/data"
)

// StartReplicationPrimary starts a PostgreSQL fixture configured to accept
// physical replication connections from a standby.
func (e *Environment) StartReplicationPrimary(t *testing.T, name, alias string) *Postgres {
	t.Helper()

	return e.startCustomPostgres(t, postgresContainerConfig{
		Name:     name,
		Alias:    alias,
		Database: "pacman",
		Username: "pacman",
		Password: "pacman",
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(replicationPrimaryInitScript()),
				ContainerFilePath: "/docker-entrypoint-initdb.d/010-enable-replication.sh",
				FileMode:          0o755,
			},
		},
		WaitStrategy: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	})
}

// StartStreamingStandby starts a PostgreSQL fixture that clones itself from
// the provided primary and starts as a streaming replica.
func (e *Environment) StartStreamingStandby(t *testing.T, name, alias string, primary *Postgres, slotName string) *Postgres {
	t.Helper()

	if primary == nil {
		t.Fatal("primary postgres fixture must be provided")
	}

	if strings.TrimSpace(slotName) == "" {
		t.Fatal("replication slot name must be provided")
	}

	return e.startCustomPostgres(t, postgresContainerConfig{
		Name:     name,
		Alias:    alias,
		Database: primary.Database(),
		Username: primary.Username(),
		Password: primary.Password(),
		Env: map[string]string{
			"PRIMARY_HOST":          primary.Alias(),
			"PRIMARY_PORT":          "5432",
			"REPLICATION_USER":      replicationUsername,
			"REPLICATION_PASSWORD":  replicationPassword,
			"REPLICATION_SLOT_NAME": slotName,
			"PGDATA":                defaultPostgresData,
		},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(streamingStandbyEntrypoint()),
				ContainerFilePath: "/usr/local/bin/pacman-standby-entrypoint.sh",
				FileMode:          0o755,
			},
		},
		Entrypoint:   []string{"/usr/local/bin/pacman-standby-entrypoint.sh"},
		WaitStrategy: wait.ForListeningPort("5432/tcp").WithStartupTimeout(90 * time.Second),
	})
}

// StartRenderedStreamingStandby starts a PostgreSQL fixture that clones from
// the provided primary and uses PACMAN-rendered standby artifacts to stream.
func (e *Environment) StartRenderedStreamingStandby(t *testing.T, name, alias string, primary *Postgres, slotName string) *Postgres {
	t.Helper()

	return e.StartRenderedStreamingStandbyWithRestoreCommand(t, name, alias, primary, slotName, "")
}

// StartRenderedStreamingStandbyWithRestoreCommand starts a PostgreSQL fixture
// that uses PACMAN-rendered standby artifacts, including an optional
// restore_command, to stream from the provided primary.
func (e *Environment) StartRenderedStreamingStandbyWithRestoreCommand(
	t *testing.T,
	name string,
	alias string,
	primary *Postgres,
	slotName string,
	restoreCommand string,
) *Postgres {
	t.Helper()

	if primary == nil {
		t.Fatal("primary postgres fixture must be provided")
	}

	if strings.TrimSpace(slotName) == "" {
		t.Fatal("replication slot name must be provided")
	}

	rendered, err := pgconfig.RenderStandbyFiles(defaultPostgresData, pgconfig.StandbyConfig{
		PrimaryConnInfo: fmt.Sprintf(
			"host=%s port=5432 user=%s password=%s application_name=%s",
			primary.Alias(),
			replicationUsername,
			replicationPassword,
			slotName,
		),
		PrimarySlotName: slotName,
		RestoreCommand:  restoreCommand,
	})
	if err != nil {
		t.Fatalf("render standby files for %q: %v", name, err)
	}

	return e.startCustomPostgres(t, postgresContainerConfig{
		Name:     name,
		Alias:    alias,
		Database: primary.Database(),
		Username: primary.Username(),
		Password: primary.Password(),
		Env: map[string]string{
			"PRIMARY_HOST":          primary.Alias(),
			"PRIMARY_PORT":          "5432",
			"REPLICATION_USER":      replicationUsername,
			"REPLICATION_PASSWORD":  replicationPassword,
			"REPLICATION_SLOT_NAME": slotName,
			"PGDATA":                defaultPostgresData,
		},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(renderedStandbyEntrypoint()),
				ContainerFilePath: "/usr/local/bin/pacman-rendered-standby-entrypoint.sh",
				FileMode:          0o755,
			},
			{
				Reader:            strings.NewReader(rendered.PostgresAutoConf),
				ContainerFilePath: "/tmp/pacman-rendered-standby/postgresql.auto.conf",
				FileMode:          0o644,
			},
			{
				Reader:            strings.NewReader(""),
				ContainerFilePath: "/tmp/pacman-rendered-standby/standby.signal",
				FileMode:          0o644,
			},
		},
		Entrypoint:   []string{"/usr/local/bin/pacman-rendered-standby-entrypoint.sh"},
		WaitStrategy: wait.ForListeningPort("5432/tcp").WithStartupTimeout(90 * time.Second),
	})
}

type postgresContainerConfig struct {
	Name         string
	Alias        string
	Database     string
	Username     string
	Password     string
	Env          map[string]string
	Files        []testcontainers.ContainerFile
	Entrypoint   []string
	Cmd          []string
	WaitStrategy wait.Strategy
}

func (e *Environment) startCustomPostgres(t *testing.T, cfg postgresContainerConfig) *Postgres {
	t.Helper()

	if e.localPostgres {
		requireLocalImage(e.ctx, t, e.postgresImage)
	}

	if strings.TrimSpace(cfg.Name) == "" {
		t.Fatal("postgres fixture name must be provided")
	}

	if strings.TrimSpace(cfg.Alias) == "" {
		t.Fatal("postgres fixture alias must be provided")
	}

	if strings.TrimSpace(cfg.Database) == "" {
		cfg.Database = "pacman"
	}

	if strings.TrimSpace(cfg.Username) == "" {
		cfg.Username = "pacman"
	}

	if cfg.Password == "" {
		cfg.Password = "pacman"
	}

	env := map[string]string{
		"POSTGRES_DB":       cfg.Database,
		"POSTGRES_USER":     cfg.Username,
		"POSTGRES_PASSWORD": cfg.Password,
	}
	for key, value := range cfg.Env {
		env[key] = value
	}

	options := []testcontainers.ContainerCustomizer{
		testcontainers.WithName(fmt.Sprintf("%s-%s-%d", e.namePrefix, sanitizeName(cfg.Alias), time.Now().UnixNano())),
		testcontainers.WithEnv(env),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithFiles(cfg.Files...),
		network.WithNetwork([]string{cfg.Alias}, e.network),
	}

	if cfg.WaitStrategy != nil {
		options = append(options, testcontainers.WithWaitStrategy(cfg.WaitStrategy))
	}

	if len(cfg.Entrypoint) > 0 {
		options = append(options, testcontainers.WithEntrypoint(cfg.Entrypoint...))
	}

	if len(cfg.Cmd) > 0 {
		options = append(options, testcontainers.WithCmd(cfg.Cmd...))
	}

	runCtx, cancel := context.WithTimeout(e.ctx, 2*dockerOperationTimeout)
	defer cancel()

	container, err := testcontainers.Run(runCtx, e.postgresImage, options...)
	if err != nil {
		t.Fatalf("start postgres fixture %q: %v", cfg.Name, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), dockerOperationTimeout)
		defer cleanupCancel()

		if err := testcontainers.TerminateContainer(container, testcontainers.StopContext(cleanupCtx)); err != nil {
			t.Logf("terminate postgres fixture %q: %v", cfg.Name, err)
		}
	})

	return &Postgres{
		ctx:       e.ctx,
		name:      cfg.Name,
		alias:     cfg.Alias,
		database:  cfg.Database,
		username:  cfg.Username,
		password:  cfg.Password,
		container: container,
	}
}

func replicationPrimaryInitScript() string {
	return `#!/bin/sh
set -eu

cat <<'EOF' >> "$PGDATA/pg_hba.conf"
host all all all scram-sha-256
host replication replicator all scram-sha-256
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
ALTER SYSTEM SET wal_level = 'replica';
ALTER SYSTEM SET max_wal_senders = '10';
ALTER SYSTEM SET max_replication_slots = '10';
ALTER SYSTEM SET hot_standby = 'on';
ALTER SYSTEM SET listen_addresses = '*';
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
		CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
	END IF;
END
$$;
SQL
`
}

func streamingStandbyEntrypoint() string {
	return `#!/bin/sh
set -eu

run_as_postgres() {
	if command -v gosu >/dev/null 2>&1; then
		gosu postgres "$@"
		return
	fi

	su-exec postgres "$@"
}

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	rm -rf "$PGDATA"/*
	export PGPASSWORD="$REPLICATION_PASSWORD"
	until pg_isready -h "$PRIMARY_HOST" -p "${PRIMARY_PORT:-5432}" -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
		sleep 1
	done
	run_as_postgres pg_basebackup \
		-h "$PRIMARY_HOST" \
		-p "${PRIMARY_PORT:-5432}" \
		-U "$REPLICATION_USER" \
		-D "$PGDATA" \
		-R \
		-X stream \
		-C \
		-S "$REPLICATION_SLOT_NAME"
	printf "primary_slot_name = '%s'\n" "$REPLICATION_SLOT_NAME" >> "$PGDATA/postgresql.auto.conf"
	printf "hot_standby = 'on'\n" >> "$PGDATA/postgresql.auto.conf"
fi

if command -v gosu >/dev/null 2>&1; then
	exec gosu postgres postgres -D "$PGDATA" -c "listen_addresses=*"
fi

exec su-exec postgres postgres -D "$PGDATA" -c "listen_addresses=*"
`
}

func renderedStandbyEntrypoint() string {
	return `#!/bin/sh
set -eu

run_as_postgres() {
	if command -v gosu >/dev/null 2>&1; then
		gosu postgres "$@"
		return
	fi

	su-exec postgres "$@"
}

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	rm -rf "$PGDATA"/*
	export PGPASSWORD="$REPLICATION_PASSWORD"
	until pg_isready -h "$PRIMARY_HOST" -p "${PRIMARY_PORT:-5432}" -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
		sleep 1
	done
	run_as_postgres pg_basebackup \
		-h "$PRIMARY_HOST" \
		-p "${PRIMARY_PORT:-5432}" \
		-U "$REPLICATION_USER" \
		-D "$PGDATA" \
		-X stream \
		-C \
		-S "$REPLICATION_SLOT_NAME"
	cat /tmp/pacman-rendered-standby/postgresql.auto.conf >> "$PGDATA/postgresql.auto.conf"
	cp /tmp/pacman-rendered-standby/standby.signal "$PGDATA/standby.signal"
	chown postgres:postgres "$PGDATA/postgresql.auto.conf" "$PGDATA/standby.signal"
	chmod 0600 "$PGDATA/postgresql.auto.conf"
	chmod 0640 "$PGDATA/standby.signal"
fi

if command -v gosu >/dev/null 2>&1; then
	exec gosu postgres postgres -D "$PGDATA" -c "listen_addresses=*"
fi

exec su-exec postgres postgres -D "$PGDATA" -c "listen_addresses=*"
`
}
