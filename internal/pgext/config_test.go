package pgext

import (
	"errors"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
)

func TestSettingsRuntimeConfigAppliesDefaultsAndBootstrap(t *testing.T) {
	t.Parallel()

	cfg, err := (Settings{
		NodeName:        " alpha-1 ",
		PostgresDataDir: " /var/lib/postgresql/data ",
		ClusterName:     " alpha ",
		SeedAddresses:   []string{" 127.0.0.1:9090 ", ""},
		ExpectedMembers: []string{" alpha-1 ", "alpha-2"},
	}).RuntimeConfig()
	if err != nil {
		t.Fatalf("build runtime config: %v", err)
	}

	if cfg.Kind != config.KindNodeConfig || cfg.APIVersion != config.APIVersionV1Alpha1 {
		t.Fatalf("unexpected config identity: %+v", cfg)
	}

	if cfg.Node.Name != "alpha-1" {
		t.Fatalf("unexpected node name: %q", cfg.Node.Name)
	}

	if cfg.Node.Role != cluster.NodeRoleData {
		t.Fatalf("unexpected node role: %q", cfg.Node.Role)
	}

	if cfg.Node.APIAddress != config.DefaultAPIAddress {
		t.Fatalf("unexpected api address: %q", cfg.Node.APIAddress)
	}

	if cfg.Node.ControlAddress != config.DefaultControlAddress {
		t.Fatalf("unexpected control address: %q", cfg.Node.ControlAddress)
	}

	if cfg.Postgres == nil {
		t.Fatal("expected postgres config")
	}

	if cfg.Postgres.DataDir != "/var/lib/postgresql/data" {
		t.Fatalf("unexpected postgres data dir: %q", cfg.Postgres.DataDir)
	}

	if cfg.Postgres.ListenAddress != config.DefaultPostgresListenAddress {
		t.Fatalf("unexpected postgres listen address: %q", cfg.Postgres.ListenAddress)
	}

	if cfg.Postgres.Port != config.DefaultPostgresPort {
		t.Fatalf("unexpected postgres port: %d", cfg.Postgres.Port)
	}

	if cfg.Bootstrap == nil {
		t.Fatal("expected bootstrap config")
	}

	if cfg.Bootstrap.ClusterName != "alpha" {
		t.Fatalf("unexpected cluster name: %q", cfg.Bootstrap.ClusterName)
	}

	if cfg.Bootstrap.InitialPrimary != "alpha-1" {
		t.Fatalf("unexpected initial primary: %q", cfg.Bootstrap.InitialPrimary)
	}

	if len(cfg.Bootstrap.SeedAddresses) != 1 || cfg.Bootstrap.SeedAddresses[0] != "127.0.0.1:9090" {
		t.Fatalf("unexpected seed addresses: %+v", cfg.Bootstrap.SeedAddresses)
	}

	if len(cfg.Bootstrap.ExpectedMembers) != 2 || cfg.Bootstrap.ExpectedMembers[0] != "alpha-1" || cfg.Bootstrap.ExpectedMembers[1] != "alpha-2" {
		t.Fatalf("unexpected expected members: %+v", cfg.Bootstrap.ExpectedMembers)
	}
}

func TestSettingsRuntimeConfigOmitsBootstrapWhenUnset(t *testing.T) {
	t.Parallel()

	cfg, err := (Settings{
		NodeName:        "alpha-1",
		PostgresDataDir: "/var/lib/postgresql/data",
	}).RuntimeConfig()
	if err != nil {
		t.Fatalf("build runtime config: %v", err)
	}

	if cfg.Bootstrap != nil {
		t.Fatalf("expected bootstrap to be omitted, got %+v", cfg.Bootstrap)
	}
}

func TestSettingsRuntimeConfigRejectsWitnessRole(t *testing.T) {
	t.Parallel()

	_, err := (Settings{
		NodeName:        "alpha-witness",
		NodeRole:        cluster.NodeRoleWitness,
		PostgresDataDir: "/var/lib/postgresql/data",
	}).RuntimeConfig()
	if !errors.Is(err, ErrPostgresManagedNodeRequired) {
		t.Fatalf("expected postgres-managed-node error, got %v", err)
	}
}

func TestSupportsPostgresMajor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		major int
		want  bool
	}{
		{major: 16, want: false},
		{major: 17, want: true},
		{major: 18, want: false},
	}

	for _, testCase := range cases {
		if got := SupportsPostgresMajor(testCase.major); got != testCase.want {
			t.Fatalf("SupportsPostgresMajor(%d): got %v want %v", testCase.major, got, testCase.want)
		}
	}
}

func TestSnapshotRuntimeConfigParsesListSettings(t *testing.T) {
	t.Parallel()

	cfg, err := (Snapshot{
		NodeName:        "alpha-1",
		NodeRole:        "data",
		PostgresDataDir: "/var/lib/postgresql/data",
		ClusterName:     "alpha",
		SeedAddresses:   " alpha-1:9090, alpha-2:9090 ,, ",
		ExpectedMembers: " alpha-1 , alpha-2 ",
	}).RuntimeConfig()
	if err != nil {
		t.Fatalf("build runtime config from snapshot: %v", err)
	}

	if cfg.Bootstrap == nil {
		t.Fatal("expected bootstrap config")
	}

	if len(cfg.Bootstrap.SeedAddresses) != 2 || cfg.Bootstrap.SeedAddresses[0] != "alpha-1:9090" || cfg.Bootstrap.SeedAddresses[1] != "alpha-2:9090" {
		t.Fatalf("unexpected seed addresses: %+v", cfg.Bootstrap.SeedAddresses)
	}

	if len(cfg.Bootstrap.ExpectedMembers) != 2 || cfg.Bootstrap.ExpectedMembers[0] != "alpha-1" || cfg.Bootstrap.ExpectedMembers[1] != "alpha-2" {
		t.Fatalf("unexpected expected members: %+v", cfg.Bootstrap.ExpectedMembers)
	}
}
