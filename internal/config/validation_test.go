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
				TLS: &TLSConfig{
					Enabled:  true,
					CertFile: "tls/server.crt",
					KeyFile:  "tls/server.key",
				},
				Security: &SecurityConfig{
					AdminBearerTokenFile: "/run/secrets/pacman-admin-token",
				},
				Postgres: &PostgresLocalConfig{
					DataDir:       "/var/lib/postgresql/data",
					BinDir:        "/usr/lib/postgresql/17/bin",
					ListenAddress: "127.0.0.1",
					Port:          5432,
					Parameters: map[string]string{
						"max_connections": "100",
					},
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:     "alpha",
					InitialPrimary:  "alpha-1",
					SeedAddresses:   []string{"10.0.0.10:9090"},
					ExpectedMembers: []string{"alpha-1", "alpha-2"},
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
			name: "node api address required",
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
			name: "node api address must be valid",
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
			name: "node control address required",
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
			name: "node control address must be valid",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "broken",
				},
			},
			wantErr: ErrNodeControlAddressInvalid,
		},
		{
			name: "tls enabled requires cert file",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled: true,
					KeyFile: "tls/server.key",
				},
			},
			wantErr: ErrTLSCertFileRequired,
		},
		{
			name: "tls enabled requires key file",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled:  true,
					CertFile: "tls/server.crt",
				},
			},
			wantErr: ErrTLSKeyFileRequired,
		},
		{
			name: "security requires token source",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Security: &SecurityConfig{},
			},
			wantErr: ErrSecurityAdminBearerTokenRequired,
		},
		{
			name: "security rejects inline token and token file together",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Security: &SecurityConfig{
					AdminBearerToken:     "secret-token",
					AdminBearerTokenFile: "/run/secrets/pacman-admin-token",
				},
			},
			wantErr: ErrSecurityAdminBearerTokenConflict,
		},
		{
			name: "security allows member mtls without admin token",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled:  true,
					CAFile:   "tls/ca.crt",
					CertFile: "tls/server.crt",
					KeyFile:  "tls/server.key",
				},
				Security: &SecurityConfig{
					MemberMTLSEnabled: true,
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:     "alpha",
					InitialPrimary:  "alpha-1",
					SeedAddresses:   []string{"0.0.0.0:9090"},
					ExpectedMembers: []string{"alpha-1"},
				},
			},
		},
		{
			name: "security member mtls requires tls",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Security: &SecurityConfig{
					MemberMTLSEnabled: true,
				},
			},
			wantErr: ErrSecurityMemberMTLSRequiresTLS,
		},
		{
			name: "security member mtls requires tls ca file",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled:  true,
					CertFile: "tls/server.crt",
					KeyFile:  "tls/server.key",
				},
				Security: &SecurityConfig{
					MemberMTLSEnabled: true,
				},
			},
			wantErr: ErrSecurityMemberMTLSCAFileRequired,
		},
		{
			name: "security member mtls requires bootstrap config",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled:  true,
					CAFile:   "tls/ca.crt",
					CertFile: "tls/server.crt",
					KeyFile:  "tls/server.key",
				},
				Security: &SecurityConfig{
					MemberMTLSEnabled: true,
				},
				// Bootstrap is nil
			},
			wantErr: ErrSecurityMemberMTLSBootstrapRequired,
		},
		{
			name: "security member mtls requires non-empty bootstrap expected members",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled:  true,
					CAFile:   "tls/ca.crt",
					CertFile: "tls/server.crt",
					KeyFile:  "tls/server.key",
				},
				Security: &SecurityConfig{
					MemberMTLSEnabled: true,
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:   "alpha",
					SeedAddresses: []string{"0.0.0.0:9090"},
					// ExpectedMembers intentionally empty
				},
			},
			wantErr: ErrSecurityMemberMTLSBootstrapRequired,
		},
		{
			name: "security member mtls requires local node in bootstrap expected members",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				TLS: &TLSConfig{
					Enabled:  true,
					CAFile:   "tls/ca.crt",
					CertFile: "tls/server.crt",
					KeyFile:  "tls/server.key",
				},
				Security: &SecurityConfig{
					MemberMTLSEnabled: true,
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:     "alpha",
					InitialPrimary:  "beta-1",
					SeedAddresses:   []string{"0.0.0.0:9090"},
					ExpectedMembers: []string{"beta-1"},
				},
			},
			wantErr: ErrSecurityMemberMTLSNodeUnknown,
		},
		{
			name: "postgres data dir required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Postgres: &PostgresLocalConfig{
					ListenAddress: "127.0.0.1",
					Port:          5432,
				},
			},
			wantErr: ErrPostgresDataDirRequired,
		},
		{
			name: "postgres listen address required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Postgres: &PostgresLocalConfig{
					DataDir: "/var/lib/postgresql/data",
					Port:    5432,
				},
			},
			wantErr: ErrPostgresListenAddressRequired,
		},
		{
			name: "postgres listen address must be valid host",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Postgres: &PostgresLocalConfig{
					DataDir:       "/var/lib/postgresql/data",
					ListenAddress: "127.0.0.1:5432",
					Port:          5432,
				},
			},
			wantErr: ErrPostgresListenAddressInvalid,
		},
		{
			name: "postgres port must be in range",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Postgres: &PostgresLocalConfig{
					DataDir:       "/var/lib/postgresql/data",
					ListenAddress: "127.0.0.1",
					Port:          70000,
				},
			},
			wantErr: ErrPostgresPortOutOfRange,
		},
		{
			name: "unsafe local postgres parameter rejected",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Postgres: &PostgresLocalConfig{
					DataDir:       "/var/lib/postgresql/data",
					ListenAddress: "127.0.0.1",
					Port:          5432,
					Parameters: map[string]string{
						"primary_conninfo": "host=alpha-1",
					},
				},
			},
			wantErr: ErrUnsafeClusterParameterOverride,
		},
		{
			name: "bootstrap cluster name required",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Bootstrap: &ClusterBootstrapConfig{
					InitialPrimary:  "alpha-1",
					SeedAddresses:   []string{"0.0.0.0:9090"},
					ExpectedMembers: []string{"alpha-1"},
				},
			},
			wantErr: ErrBootstrapClusterNameRequired,
		},
		{
			name: "bootstrap initial primary must be expected member",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:     "alpha",
					InitialPrimary:  "alpha-2",
					SeedAddresses:   []string{"0.0.0.0:9090"},
					ExpectedMembers: []string{"alpha-1"},
				},
			},
			wantErr: ErrBootstrapInitialPrimaryUnknown,
		},
		{
			name: "bootstrap seed address must be valid",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:     "alpha",
					InitialPrimary:  "alpha-1",
					SeedAddresses:   []string{"broken"},
					ExpectedMembers: []string{"alpha-1"},
				},
			},
			wantErr: ErrBootstrapSeedAddressInvalid,
		},
		{
			name: "bootstrap expected members cannot contain empty names",
			config: Config{
				APIVersion: APIVersionV1Alpha1,
				Kind:       KindNodeConfig,
				Node: NodeConfig{
					Name:           "alpha-1",
					Role:           cluster.NodeRoleData,
					APIAddress:     "0.0.0.0:8080",
					ControlAddress: "0.0.0.0:9090",
				},
				Bootstrap: &ClusterBootstrapConfig{
					ClusterName:     "alpha",
					InitialPrimary:  "alpha-1",
					SeedAddresses:   []string{"0.0.0.0:9090"},
					ExpectedMembers: []string{"alpha-1", " "},
				},
			},
			wantErr: ErrBootstrapExpectedMemberInvalid,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.config.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
