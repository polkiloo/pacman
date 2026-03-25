package postgres

import (
	"errors"
	"strings"
	"testing"
)

func TestStandbyConfigWithDefaults(t *testing.T) {
	t.Parallel()

	defaulted := (StandbyConfig{
		PrimaryConnInfo: "host=primary port=5432 user=replicator",
	}).WithDefaults()

	if defaulted.RecoveryTargetTimeline != DefaultRecoveryTargetTimeline {
		t.Fatalf("unexpected default recovery target timeline: got %q", defaulted.RecoveryTargetTimeline)
	}
}

func TestStandbyConfigValidateRequiresPrimaryConnInfo(t *testing.T) {
	t.Parallel()

	err := (StandbyConfig{}).Validate()
	if !errors.Is(err, ErrPrimaryConnInfoRequired) {
		t.Fatalf("expected primary conninfo error, got %v", err)
	}
}

func TestRenderStandbyFilesReturnsPathsAndConfig(t *testing.T) {
	t.Parallel()

	rendered, err := RenderStandbyFiles("/srv/postgres", StandbyConfig{
		PrimaryConnInfo:        "host=primary port=5432 user=replicator password=pa'ss\\word",
		PrimarySlotName:        "node_a",
		RestoreCommand:         "cp /archive/%f %p",
		RecoveryTargetTimeline: "latest",
	})
	if err != nil {
		t.Fatalf("render standby files: %v", err)
	}

	if rendered.PostgresAutoConfPath != "/srv/postgres/postgresql.auto.conf" {
		t.Fatalf("unexpected auto conf path: got %q", rendered.PostgresAutoConfPath)
	}

	if rendered.StandbySignalPath != "/srv/postgres/standby.signal" {
		t.Fatalf("unexpected standby signal path: got %q", rendered.StandbySignalPath)
	}

	wantLines := []string{
		"primary_conninfo = 'host=primary port=5432 user=replicator password=pa''ss\\\\word'",
		"primary_slot_name = 'node_a'",
		"restore_command = 'cp /archive/%f %p'",
		"recovery_target_timeline = 'latest'",
	}
	for _, wantLine := range wantLines {
		if !strings.Contains(rendered.PostgresAutoConf, wantLine) {
			t.Fatalf("rendered auto conf %q does not contain %q", rendered.PostgresAutoConf, wantLine)
		}
	}
}

func TestRenderStandbyFilesSkipsEmptyOptionalSettings(t *testing.T) {
	t.Parallel()

	rendered, err := RenderStandbyFiles("/srv/postgres", StandbyConfig{
		PrimaryConnInfo: "host=primary port=5432 user=replicator",
	})
	if err != nil {
		t.Fatalf("render standby files: %v", err)
	}

	if strings.Contains(rendered.PostgresAutoConf, "primary_slot_name") {
		t.Fatalf("expected primary_slot_name to be omitted: %q", rendered.PostgresAutoConf)
	}

	if strings.Contains(rendered.PostgresAutoConf, "restore_command") {
		t.Fatalf("expected restore_command to be omitted: %q", rendered.PostgresAutoConf)
	}

	if !strings.HasSuffix(rendered.PostgresAutoConf, "\n") {
		t.Fatalf("expected newline-terminated auto conf, got %q", rendered.PostgresAutoConf)
	}
}

func TestRenderStandbyFilesRequiresDataDir(t *testing.T) {
	t.Parallel()

	_, err := RenderStandbyFiles("", StandbyConfig{
		PrimaryConnInfo: "host=primary",
	})
	if !errors.Is(err, ErrDataDirRequired) {
		t.Fatalf("expected missing data dir error, got %v", err)
	}
}
