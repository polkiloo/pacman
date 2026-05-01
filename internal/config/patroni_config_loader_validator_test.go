package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
	"gopkg.in/yaml.v3"
)

func TestPatroniInspiredDecodeRejectsAmbiguousDCSBackends(t *testing.T) {
	t.Parallel()

	payload := `
scope: alpha
name: alpha-1
restapi:
  listen: 127.0.0.1:8008
etcd:
  host: 127.0.0.1:2379
raft:
  data_dir: /var/lib/patroni/raft
  self_addr: 127.0.0.1:2222
postgresql:
  listen: 127.0.0.1:5432
  data_dir: data/postgresql0
`

	_, err := DecodeWithReport(strings.NewReader(payload))
	if !errors.Is(err, ErrPatroniDCSBackendConflict) {
		t.Fatalf("unexpected decode error: got %v, want %v", err, ErrPatroniDCSBackendConflict)
	}
}

func TestPatroniInspiredDecodeRejectsInvalidHostListType(t *testing.T) {
	t.Parallel()

	payload := `
scope: alpha
name: alpha-1
restapi:
  listen: 127.0.0.1:8008
etcd:
  hosts:
    primary: 127.0.0.1:2379
postgresql:
  listen: 127.0.0.1:5432
  data_dir: data/postgresql0
`

	_, err := DecodeWithReport(strings.NewReader(payload))
	if err == nil {
		t.Fatal("expected invalid Patroni host list decode error")
	}

	assertContains(t, err.Error(), "decode Patroni hosts")
	assertContains(t, err.Error(), "expected string or list")
}

func TestPatroniInspiredGeneratedConfigRoundTrip(t *testing.T) {
	t.Parallel()

	generated := patroniInspiredCompleteConfig()
	payload, err := yaml.Marshal(generated)
	if err != nil {
		t.Fatalf("marshal generated config: %v", err)
	}

	report, err := DecodeWithReport(strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("decode generated config: %v", err)
	}

	if report.Format != DocumentFormatPACMAN {
		t.Fatalf("unexpected generated document format: got %q, want %q", report.Format, DocumentFormatPACMAN)
	}

	if len(report.Warnings) != 0 {
		t.Fatalf("expected no warnings for generated PACMAN config, got %+v", report.Warnings)
	}

	if !reflect.DeepEqual(report.Config, generated) {
		t.Fatalf("generated config did not round-trip:\ngot:  %+v\nwant: %+v", report.Config, generated)
	}
}

func TestPatroniInspiredValidatorEdgeMatrix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{
			name: "api port zero",
			mutate: func(config *Config) {
				config.Node.APIAddress = "127.0.0.1:0"
			},
			wantErr: ErrNodeAPIAddressInvalid,
		},
		{
			name: "api host omitted",
			mutate: func(config *Config) {
				config.Node.APIAddress = ":8080"
			},
			wantErr: ErrNodeAPIAddressInvalid,
		},
		{
			name: "control port not numeric",
			mutate: func(config *Config) {
				config.Node.ControlAddress = "127.0.0.1:not-a-port"
			},
			wantErr: ErrNodeControlAddressInvalid,
		},
		{
			name: "postgres listen address includes port",
			mutate: func(config *Config) {
				config.Postgres.ListenAddress = "127.0.0.1:5432"
			},
			wantErr: ErrPostgresListenAddressInvalid,
		},
		{
			name: "bootstrap seed port out of range",
			mutate: func(config *Config) {
				config.Bootstrap.SeedAddresses = []string{"127.0.0.1:70000"}
			},
			wantErr: ErrBootstrapSeedAddressInvalid,
		},
		{
			name: "bootstrap expected member blank",
			mutate: func(config *Config) {
				config.Bootstrap.ExpectedMembers = []string{"alpha-1", " "}
			},
			wantErr: ErrBootstrapExpectedMemberInvalid,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			cfg := patroniInspiredCompleteConfig()
			testCase.mutate(&cfg)

			err := cfg.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestPatroniInspiredSensitiveFileModeMatrix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		mode    os.FileMode
		wantErr bool
	}{
		{name: "owner read write", mode: 0o600},
		{name: "owner read only", mode: 0o400},
		{name: "owner all permissions", mode: 0o700},
		{name: "group readable", mode: 0o640, wantErr: true},
		{name: "other readable", mode: 0o604, wantErr: true},
		{name: "world readable and writable", mode: 0o666, wantErr: true},
		{name: "world executable", mode: 0o711, wantErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := validateSensitiveFileMode(testCase.mode)
			if testCase.wantErr && !errors.Is(err, ErrSensitiveConfigFilePermissions) {
				t.Fatalf("unexpected permission validation error: got %v, want %v", err, ErrSensitiveConfigFilePermissions)
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("expected mode %03o to pass, got %v", testCase.mode, err)
			}
		})
	}
}

func TestPatroniInspiredLoadRejectsPermissiveTranslatedEtcdPassword(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "patroni.yml")
	payload := `
scope: alpha
name: alpha-1
restapi:
  listen: 127.0.0.1:8008
etcd:
  host: 127.0.0.1:2379
  username: pacman
  password: secret-password
postgresql:
  listen: 127.0.0.1:5432
  data_dir: data/postgresql0
`

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write Patroni config: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod Patroni config: %v", err)
	}

	_, err := LoadWithReport(path)
	if !errors.Is(err, ErrSensitiveConfigFilePermissions) {
		t.Fatalf("unexpected load error: got %v, want %v", err, ErrSensitiveConfigFilePermissions)
	}
}

func patroniInspiredCompleteConfig() Config {
	return Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name:           "alpha-1",
			Role:           cluster.NodeRoleData,
			APIAddress:     "127.0.0.1:8080",
			ControlAddress: "127.0.0.1:9090",
		},
		DCS: &dcs.Config{
			Backend:      dcs.BackendEtcd,
			ClusterName:  "alpha",
			TTL:          30 * time.Second,
			RetryTimeout: 10 * time.Second,
			Etcd: &dcs.EtcdConfig{
				Endpoints: []string{"https://etcd-1.alpha.internal:2379"},
				Username:  "pacman",
			},
		},
		TLS: &TLSConfig{
			Enabled:    true,
			CAFile:     "/etc/pacman/tls/ca.crt",
			CertFile:   "/etc/pacman/tls/node.crt",
			KeyFile:    "/etc/pacman/tls/node.key",
			ServerName: "alpha.internal",
		},
		Security: &SecurityConfig{
			AdminBearerTokenFile: "/run/secrets/pacman-admin-token",
			MemberMTLSEnabled:    true,
		},
		Postgres: &PostgresLocalConfig{
			DataDir:       "/var/lib/postgresql/17/main",
			BinDir:        "/usr/lib/postgresql/17/bin",
			ListenAddress: "127.0.0.1",
			Port:          5432,
			Parameters: map[string]string{
				"max_connections": "300",
				"shared_buffers":  "1GB",
			},
		},
		Bootstrap: &ClusterBootstrapConfig{
			ClusterName:     "alpha",
			InitialPrimary:  "alpha-1",
			SeedAddresses:   []string{"127.0.0.1:9090", "127.0.0.2:9090"},
			ExpectedMembers: []string{"alpha-1", "alpha-2"},
		},
	}
}
