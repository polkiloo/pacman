package config

import (
	"strings"
	"testing"
)

func TestPatroniInspiredBackupRestoreCloudConfigWarnsAndKeepsSafeParameters(t *testing.T) {
	t.Parallel()

	payload := `
scope: alpha
name: alpha-1
restapi:
  listen: 127.0.0.1:8008
etcd:
  host: 127.0.0.1:2379
callbacks:
  on_role_change: /usr/local/bin/aws-tag-role-change
postgresql:
  listen: 127.0.0.1:5432
  data_dir: /var/lib/postgresql/data
  create_replica_methods:
    - barman
    - wal_e
    - basebackup
  basebackup:
    checkpoint: fast
    max-rate: 100M
  barman:
    command: patroni_barman recover
    barman_server: alpha
  wal_e:
    env_dir: /etc/wal-e.d/env
  recovery_conf:
    restore_command: envdir /etc/wal-e.d/env wal-e wal-fetch %f %p
  parameters:
    max_connections: 200
    restore_command: envdir /etc/wal-e.d/env wal-e wal-fetch %f %p
    archive_cleanup_command: pg_archivecleanup /archive %r
`

	report, err := DecodeWithReport(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode Patroni backup/restore/cloud config: %v", err)
	}

	warnings := strings.Join(report.Warnings, "\n")
	for _, want := range []string{
		`Patroni key "callbacks"`,
		`postgresql.create_replica_methods`,
		`postgresql.basebackup`,
		`postgresql.barman`,
		`postgresql.wal_e`,
		`postgresql.recovery_conf`,
		`postgresql.parameters.restore_command`,
		`postgresql.parameters.archive_cleanup_command`,
	} {
		assertContains(t, warnings, want)
	}

	if report.Config.Postgres == nil {
		t.Fatal("expected translated postgres config")
	}
	if got := report.Config.Postgres.Parameters["max_connections"]; got != "200" {
		t.Fatalf("expected safe postgres parameter to survive migration, got %q", got)
	}
	if _, ok := report.Config.Postgres.Parameters["restore_command"]; ok {
		t.Fatalf("expected restore_command to remain cluster-managed and not enter local config: %+v", report.Config.Postgres.Parameters)
	}
	if _, ok := report.Config.Postgres.Parameters["archive_cleanup_command"]; ok {
		t.Fatalf("expected archive_cleanup_command to remain cluster-managed and not enter local config: %+v", report.Config.Postgres.Parameters)
	}
}
