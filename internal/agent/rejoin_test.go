package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestMergePrimaryConnInfoCredentials(t *testing.T) {
	t.Parallel()

	got := mergePrimaryConnInfoCredentials(
		"host=primary port=5432 application_name=alpha-1",
		"replicator",
		"pa ss'word",
	)

	want := "host=primary port=5432 application_name=alpha-1 user=replicator password='pa ss\\'word'"
	if got != want {
		t.Fatalf("unexpected conninfo: got %q, want %q", got, want)
	}
}

func TestLocalStandbyConfiguratorConfigureStandbyInjectsReplicationCredentials(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	configurator := &localStandbyConfigurator{
		dataDir:             dataDir,
		replicationUser:     "replicator",
		replicationPassword: "replicator-secret",
	}

	request := controlplane.StandbyConfigRequest{
		Standby: postgres.StandbyConfig{
			PrimaryConnInfo: "host=pacman-replica port=5432 application_name=alpha-1",
		},
	}

	if err := configurator.ConfigureStandby(context.Background(), request); err != nil {
		t.Fatalf("configure standby: %v", err)
	}

	rendered, err := os.ReadFile(filepath.Join(dataDir, postgres.PostgresAutoConfFileName))
	if err != nil {
		t.Fatalf("read postgresql.auto.conf: %v", err)
	}

	text := string(rendered)
	if !strings.Contains(text, "user=replicator") {
		t.Fatalf("postgresql.auto.conf does not contain replication user: %s", text)
	}
	if !strings.Contains(text, "password=replicator-secret") {
		t.Fatalf("postgresql.auto.conf does not contain replication password: %s", text)
	}

	if _, err := os.Stat(filepath.Join(dataDir, postgres.StandbySignalFileName)); err != nil {
		t.Fatalf("standby.signal not written: %v", err)
	}
}

func TestLocalStandbyConfiguratorConfigureStandbyPreservesManagedAutoConf(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	autoConfPath := filepath.Join(dataDir, postgres.PostgresAutoConfFileName)
	if err := os.WriteFile(autoConfPath, []byte(strings.Join([]string{
		"max_connections = '100'",
		"primary_conninfo = 'old-primary'",
		"primary_slot_name = 'old-slot'",
		"restore_command = 'old-restore'",
		"recovery_target_timeline = 'latest'",
		"",
	}, "\n")), 0o600); err != nil {
		t.Fatalf("write existing postgresql.auto.conf: %v", err)
	}

	configurator := &localStandbyConfigurator{
		dataDir:             dataDir,
		replicationUser:     "replicator",
		replicationPassword: "replicator-secret",
	}

	request := controlplane.StandbyConfigRequest{
		Standby: postgres.StandbyConfig{
			PrimaryConnInfo:        "host=pacman-primary port=5432 application_name=alpha-1",
			PrimarySlotName:        "alpha_1",
			RestoreCommand:         "cp /archive/%f %p",
			RecoveryTargetTimeline: "current",
		},
	}

	if err := configurator.ConfigureStandby(context.Background(), request); err != nil {
		t.Fatalf("configure standby with existing auto.conf: %v", err)
	}

	rendered, err := os.ReadFile(autoConfPath)
	if err != nil {
		t.Fatalf("read merged postgresql.auto.conf: %v", err)
	}

	text := string(rendered)
	if !strings.Contains(text, "max_connections = '100'") {
		t.Fatalf("expected existing non-standby settings to be preserved, got:\n%s", text)
	}

	for _, unexpected := range []string{
		"primary_conninfo = 'old-primary'",
		"primary_slot_name = 'old-slot'",
		"restore_command = 'old-restore'",
	} {
		if strings.Contains(text, unexpected) {
			t.Fatalf("expected stale standby setting %q to be removed, got:\n%s", unexpected, text)
		}
	}

	for _, expected := range []string{
		"primary_conninfo = 'host=pacman-primary port=5432 application_name=alpha-1 user=replicator password=replicator-secret'",
		"restore_command = 'cp /archive/%f %p'",
		"recovery_target_timeline = 'current'",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("expected merged postgresql.auto.conf to contain %q, got:\n%s", expected, text)
		}
	}

	if strings.Contains(text, "primary_slot_name = 'alpha_1'") {
		t.Fatalf("expected rejoin standby configuration to drop primary_slot_name, got:\n%s", text)
	}
}

func TestMergePostgresAutoConfReturnsReadError(t *testing.T) {
	t.Parallel()

	_, err := mergePostgresAutoConf(t.TempDir(), "primary_conninfo = 'host=primary'\n")
	if err == nil || !strings.Contains(err.Error(), "read postgresql.auto.conf") {
		t.Fatalf("unexpected merge postgresql.auto.conf error: %v", err)
	}
}

func TestLocalStandbyConfiguratorConfigureStandbyReturnsRenderError(t *testing.T) {
	t.Parallel()

	configurator := &localStandbyConfigurator{dataDir: ""}
	err := configurator.ConfigureStandby(context.Background(), controlplane.StandbyConfigRequest{
		Standby: postgres.StandbyConfig{
			PrimaryConnInfo: "host=primary port=5432",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "render standby files") {
		t.Fatalf("unexpected standby render error: %v", err)
	}
}
