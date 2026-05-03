//go:build integration

package integration_test

import (
	"strings"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/test/testenv"
)

func TestPatroniInspiredCloudRestoreCommandWithRenderedStandbyInTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	restoreCommand := "env WALE_S3_PREFIX=s3://pacman-test sh -c 'test ! -f /tmp/wal-archive/$1 || cp /tmp/wal-archive/$1 $2' sh %f %p"

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "cloud-restore-primary", "cloud-restore-primary-postgres")
	standby := env.StartRenderedStreamingStandbyWithRestoreCommand(
		t,
		"cloud-restore-standby",
		"cloud-restore-standby-postgres",
		primary,
		"cloud_restore_standby",
		restoreCommand,
	)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)

	waitForQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standby, `SHOW primary_slot_name`, "cloud_restore_standby")
	waitForQueryValue(t, standby, `SHOW restore_command`, restoreCommand)
	waitForQueryValue(t, standby, `SHOW recovery_target_timeline`, "latest")

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS backup_restore_cloud_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO backup_restore_cloud_marker (id, payload)
VALUES (1, 'restore-command-survived')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	waitForQueryValue(t, standby, `SELECT payload FROM backup_restore_cloud_marker WHERE id = 1`, "restore-command-survived")
}

func TestPatroniInspiredBackupRestoreCloudNegativeCasesInTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "backup-negative-primary", "backup-negative-primary-postgres")
	setPostgresObservationEnv(t, primary)
	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)

	testCases := []struct {
		name           string
		restoreCommand string
		wantOutput     string
	}{
		{
			name:           "negative wal-e envdir helper is absent",
			restoreCommand: "envdir /etc/wal-e.d/env wal-e wal-fetch %f %p",
			wantOutput:     "envdir",
		},
		{
			name:           "negative wal-g helper is absent",
			restoreCommand: "wal-g wal-fetch %f %p",
			wantOutput:     "wal-g",
		},
		{
			name:           "negative patroni barman helper is absent",
			restoreCommand: "patroni_barman recover --datadir /var/lib/postgresql/data --backup-id latest",
			wantOutput:     "patroni_barman",
		},
		{
			name:           "negative barman wal restore helper is absent",
			restoreCommand: "barman-wal-restore backup-api alpha %f %p",
			wantOutput:     "barman-wal-restore",
		},
		{
			name:           "negative restore command exits with failure",
			restoreCommand: "sh -c 'exit 2' sh %f %p",
		},
		{
			name:           "negative strict cloud object lookup misses archive",
			restoreCommand: "env WALE_S3_PREFIX=s3://pacman-test sh -c 'cp /tmp/wal-archive/$1 $2' sh %f %p",
			wantOutput:     "/tmp/wal-archive",
		},
		{
			name:           "negative restore command cannot write missing target directory",
			restoreCommand: "sh -c 'printf x > /tmp/pacman-missing-dir/$1' sh %f %p",
			wantOutput:     "/tmp/pacman-missing-dir",
		},
	}

	replacer := strings.NewReplacer(
		"%f", "000000020000000000000001",
		"%p", "/tmp/pacman-restored-wal",
	)
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			command := replacer.Replace(testCase.restoreCommand)
			result := primary.Exec(t, "sh", "-lc", command)
			if result.ExitCode == 0 {
				t.Fatalf("expected restore command %q to fail inside container, output=%q", command, result.Output)
			}
			if testCase.wantOutput != "" && !strings.Contains(result.Output, testCase.wantOutput) {
				t.Fatalf("expected restore command output %q to contain %q", result.Output, testCase.wantOutput)
			}
		})
	}
}
