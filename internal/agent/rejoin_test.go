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
