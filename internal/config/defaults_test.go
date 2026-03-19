package config

import (
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
	}

	got := original.WithDefaults()

	if got != original {
		t.Fatalf("unexpected defaults result: got %+v, want %+v", got, original)
	}
}
