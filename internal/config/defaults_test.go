package config

import (
	"reflect"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()

	original := Config{
		Node: NodeConfig{
			Name: "alpha-1",
		},
	}

	got := original.WithDefaults()

	if got.APIVersion != APIVersionV1Alpha1 {
		t.Fatalf("unexpected api version default: got %q, want %q", got.APIVersion, APIVersionV1Alpha1)
	}

	if got.Kind != KindNodeConfig {
		t.Fatalf("unexpected kind default: got %q, want %q", got.Kind, KindNodeConfig)
	}

	if got.Node.Role != cluster.NodeRoleData {
		t.Fatalf("unexpected node role default: got %q, want %q", got.Node.Role, cluster.NodeRoleData)
	}

	if got.Node.APIAddress != DefaultAPIAddress {
		t.Fatalf("unexpected api address default: got %q, want %q", got.Node.APIAddress, DefaultAPIAddress)
	}

	if got.Node.ControlAddress != DefaultControlAddress {
		t.Fatalf("unexpected control address default: got %q, want %q", got.Node.ControlAddress, DefaultControlAddress)
	}

	if original.APIVersion != "" || original.Kind != "" || original.Node.Role != "" || original.Node.APIAddress != "" || original.Node.ControlAddress != "" {
		t.Fatalf("expected defaults to avoid mutating original, got %+v", original)
	}
}

func TestConfigWithDefaultsPreservesExplicitValues(t *testing.T) {
	t.Parallel()

	original := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name:           "alpha-1",
			Role:           cluster.NodeRoleWitness,
			APIAddress:     "127.0.0.1:18080",
			ControlAddress: "127.0.0.1:19090",
		},
		TLS: &TLSConfig{
			Enabled:  true,
			CertFile: "tls/server.crt",
			KeyFile:  "tls/server.key",
		},
		Postgres: &PostgresLocalConfig{
			DataDir:       "/var/lib/postgresql/data",
			BinDir:        "/usr/lib/postgresql/17/bin",
			ListenAddress: "127.0.0.1",
			Port:          6432,
			Parameters: map[string]string{
				"max_connections": "100",
			},
		},
		Bootstrap: &ClusterBootstrapConfig{
			ClusterName:     "alpha",
			InitialPrimary:  "alpha-2",
			SeedAddresses:   []string{"127.0.0.1:19090"},
			ExpectedMembers: []string{"alpha-1", "alpha-2"},
		},
	}

	got := original.WithDefaults()

	if !reflect.DeepEqual(got, original) {
		t.Fatalf("unexpected defaults result: got %+v, want %+v", got, original)
	}
}

func TestConfigWithDefaultsAppliesSectionDefaults(t *testing.T) {
	t.Parallel()

	original := Config{
		Node: NodeConfig{
			Name:           "alpha-1",
			ControlAddress: "10.0.0.10:9090",
		},
		Postgres: &PostgresLocalConfig{
			DataDir: "/var/lib/postgresql/data",
		},
		Bootstrap: &ClusterBootstrapConfig{
			ClusterName: "alpha",
		},
	}

	got := original.WithDefaults()

	if got.Postgres == nil {
		t.Fatal("expected postgres defaults to be applied")
	}

	if got.Postgres.ListenAddress != DefaultPostgresListenAddress {
		t.Fatalf("unexpected postgres listenAddress default: got %q, want %q", got.Postgres.ListenAddress, DefaultPostgresListenAddress)
	}

	if got.Postgres.Port != DefaultPostgresPort {
		t.Fatalf("unexpected postgres port default: got %d, want %d", got.Postgres.Port, DefaultPostgresPort)
	}

	if got.Bootstrap == nil {
		t.Fatal("expected bootstrap defaults to be applied")
	}

	if got.Bootstrap.InitialPrimary != "alpha-1" {
		t.Fatalf("unexpected bootstrap initialPrimary default: got %q, want %q", got.Bootstrap.InitialPrimary, "alpha-1")
	}

	if !reflect.DeepEqual(got.Bootstrap.SeedAddresses, []string{"10.0.0.10:9090"}) {
		t.Fatalf("unexpected bootstrap seedAddresses default: got %v", got.Bootstrap.SeedAddresses)
	}

	if !reflect.DeepEqual(got.Bootstrap.ExpectedMembers, []string{"alpha-1"}) {
		t.Fatalf("unexpected bootstrap expectedMembers default: got %v", got.Bootstrap.ExpectedMembers)
	}

	if original.Postgres == nil || original.Postgres.ListenAddress != "" || original.Postgres.Port != 0 {
		t.Fatalf("expected postgres defaults to avoid mutating original, got %+v", original.Postgres)
	}

	if original.Bootstrap == nil || original.Bootstrap.InitialPrimary != "" || len(original.Bootstrap.SeedAddresses) != 0 || len(original.Bootstrap.ExpectedMembers) != 0 {
		t.Fatalf("expected bootstrap defaults to avoid mutating original, got %+v", original.Bootstrap)
	}
}
