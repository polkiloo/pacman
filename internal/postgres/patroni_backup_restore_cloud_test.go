package postgres

import (
	"strings"
	"testing"
)

func TestPatroniInspiredCloudRestoreCommandRendering(t *testing.T) {
	t.Parallel()

	restoreCommand := `envdir /etc/wal-e.d/env wal-e wal-fetch '%f' '%p'`
	rendered, err := RenderStandbyFiles("/srv/postgres", StandbyConfig{
		PrimaryConnInfo:        "host=primary port=5432 user=replicator password=secret application_name=alpha-1",
		PrimarySlotName:        "alpha_1",
		RestoreCommand:         restoreCommand,
		RecoveryTargetTimeline: "latest",
	})
	if err != nil {
		t.Fatalf("render standby files with cloud restore command: %v", err)
	}

	for _, want := range []string{
		"primary_conninfo = 'host=primary port=5432 user=replicator password=secret application_name=alpha-1'",
		"primary_slot_name = 'alpha_1'",
		"restore_command = 'envdir /etc/wal-e.d/env wal-e wal-fetch ''%f'' ''%p'''",
		"recovery_target_timeline = 'latest'",
	} {
		if !strings.Contains(rendered.PostgresAutoConf, want) {
			t.Fatalf("rendered cloud restore config %q does not contain %q", rendered.PostgresAutoConf, want)
		}
	}
}

func TestPatroniInspiredBarmanRestoreCommandRendering(t *testing.T) {
	t.Parallel()

	restoreCommand := `barman-wal-restore --user postgres backup-api alpha "%f" "%p"`
	rendered, err := RenderStandbyFiles("/srv/postgres", StandbyConfig{
		PrimaryConnInfo: "host=primary port=5432 user=replicator",
		RestoreCommand:  restoreCommand,
	})
	if err != nil {
		t.Fatalf("render standby files with barman restore command: %v", err)
	}

	if !strings.Contains(rendered.PostgresAutoConf, `restore_command = 'barman-wal-restore --user postgres backup-api alpha "%f" "%p"'`) {
		t.Fatalf("expected barman restore command in rendered config, got %q", rendered.PostgresAutoConf)
	}
	if !strings.Contains(rendered.PostgresAutoConf, "recovery_target_timeline = 'latest'") {
		t.Fatalf("expected default recovery target timeline in rendered config, got %q", rendered.PostgresAutoConf)
	}
}
