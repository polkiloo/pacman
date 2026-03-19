package config

import (
	"errors"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name: "valid config",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
			},
		},
		{
			name: "unsupported api version",
			config: Config{
				APIVersion: "pacman.io/v2",
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrUnsupportedAPIVersion,
		},
		{
			name: "unexpected kind",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       "ClusterConfig",
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrUnexpectedKind,
		},
		{
			name: "node name required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrNodeNameRequired,
		},
		{
			name: "node role required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrNodeRoleRequired,
		},
		{
			name: "node role must be valid",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRole("observer"),
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrNodeRoleInvalid,
		},
		{
			name: "api address required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrNodeAPIAddressRequired,
		},
		{
			name: "api address must be valid",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "broken",
					ControlAddress: "0.0.0.0:9090",
				},
			},
			wantErr: ErrNodeAPIAddressInvalid,
		},
		{
			name: "control address required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:       "alpha-1",
					Role:       cluster.NodeRoleData,
					APIAddress: "0.0.0.0:8080",
				},
			},
			wantErr: ErrNodeControlAddressRequired,
		},
		{
			name: "control address must be valid",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "127.0.0.1:99999",
				},
			},
			wantErr: ErrNodeControlAddressInvalid,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.config.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
