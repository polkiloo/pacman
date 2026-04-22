package config

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
)

func TestDecode(t *testing.T) {
	t.Parallel()

	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
  apiAddress: 0.0.0.0:8080
  controlAddress: 10.0.0.10:9090
tls:
  enabled: true
  certFile: tls/server.crt
  keyFile: tls/server.key
security:
  adminBearerTokenFile: /run/secrets/pacman-admin-token
postgres:
  dataDir: /var/lib/postgresql/data
  binDir: /usr/lib/postgresql/17/bin
  listenAddress: 127.0.0.1
  port: 5432
  replicationUser: replicator
  replicationPassword: secret
  parameters:
    max_connections: "100"
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-1
  seedAddresses:
    - 10.0.0.10:9090
  expectedMembers:
    - alpha-1
    - alpha-2
`

	got, err := Decode(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}

	want := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name:           "alpha-1",
			Role:           cluster.NodeRoleData,
			APIAddress:     "0.0.0.0:8080",
			ControlAddress: "10.0.0.10:9090",
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
			DataDir:             "/var/lib/postgresql/data",
			BinDir:              "/usr/lib/postgresql/17/bin",
			ListenAddress:       "127.0.0.1",
			Port:                5432,
			ReplicationUser:     "replicator",
			ReplicationPassword: "secret",
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
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected config: got %+v, want %+v", got, want)
	}
}

func TestDecodeAppliesDefaults(t *testing.T) {
	t.Parallel()

	payload := `
node:
  name: alpha-1
postgres:
  dataDir: /var/lib/postgresql/data
bootstrap:
  clusterName: alpha
`

	got, err := Decode(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode config with defaults: %v", err)
	}

	want := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name:           "alpha-1",
			Role:           cluster.NodeRoleData,
			APIAddress:     DefaultAPIAddress,
			ControlAddress: DefaultControlAddress,
		},
		Postgres: &PostgresLocalConfig{
			DataDir:       "/var/lib/postgresql/data",
			ListenAddress: DefaultPostgresListenAddress,
			Port:          DefaultPostgresPort,
		},
		Bootstrap: &ClusterBootstrapConfig{
			ClusterName:     "alpha",
			InitialPrimary:  "alpha-1",
			SeedAddresses:   []string{DefaultControlAddress},
			ExpectedMembers: []string{"alpha-1"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected defaulted config: got %+v, want %+v", got, want)
	}
}

func TestDecodeAppliesDCSDefaults(t *testing.T) {
	t.Parallel()

	payload := `
node:
  name: alpha-1
dcs:
  backend: raft
  raft:
    dataDir: /var/lib/pacman/raft
    bindAddress: 10.0.0.10:7100
    peers:
      - 10.0.0.10:7100
bootstrap:
  clusterName: alpha
`

	got, err := Decode(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode config with dcs defaults: %v", err)
	}

	if got.DCS == nil {
		t.Fatal("expected dcs config to be decoded")
	}

	if got.DCS.ClusterName != "alpha" {
		t.Fatalf("unexpected dcs cluster name default: got %q, want %q", got.DCS.ClusterName, "alpha")
	}

	if got.DCS.TTL != dcs.DefaultTTL {
		t.Fatalf("unexpected dcs ttl default: got %s, want %s", got.DCS.TTL, dcs.DefaultTTL)
	}

	if got.DCS.RetryTimeout != dcs.DefaultRetryTimeout {
		t.Fatalf("unexpected dcs retryTimeout default: got %s, want %s", got.DCS.RetryTimeout, dcs.DefaultRetryTimeout)
	}

	if got.DCS.Raft == nil || got.DCS.Raft.SnapshotInterval != dcs.DefaultRaftSnapshotInterval {
		t.Fatalf("expected raft defaults to be applied, got %+v", got.DCS.Raft)
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  unknownField: value
`

	_, err := Decode(strings.NewReader(payload))
	if err == nil {
		t.Fatal("expected decode error")
	}

	assertContains(t, err.Error(), "field unknownField not found")
}

func TestDecodeRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: observer
`

	_, err := Decode(strings.NewReader(payload))
	if err == nil {
		t.Fatal("expected decode error")
	}

	assertContains(t, err.Error(), "validate config document")
	assertContains(t, err.Error(), ErrNodeRoleInvalid.Error())
}

func TestDecodeRejectsUnsafeLocalClusterOverride(t *testing.T) {
	t.Parallel()

	payload := `
node:
  name: alpha-1
postgres:
  dataDir: /var/lib/postgresql/data
  parameters:
    synchronous_standby_names: "FIRST 1 (alpha-2)"
`

	_, err := Decode(strings.NewReader(payload))
	if err == nil {
		t.Fatal("expected decode error")
	}

	assertContains(t, err.Error(), "validate config document")
	assertContains(t, err.Error(), ErrUnsafeClusterParameterOverride.Error())
}

func TestLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: witness
postgres:
  dataDir: /var/lib/postgresql/data
bootstrap:
  clusterName: alpha
`

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got.Node.Name != "alpha-1" {
		t.Fatalf("unexpected node name: got %q, want %q", got.Node.Name, "alpha-1")
	}

	if got.Node.Role != cluster.NodeRoleWitness {
		t.Fatalf("unexpected node role: got %q, want %q", got.Node.Role, cluster.NodeRoleWitness)
	}

	if got.Postgres == nil || got.Postgres.Port != DefaultPostgresPort {
		t.Fatalf("expected postgres defaults on load, got %+v", got.Postgres)
	}

	if got.Bootstrap == nil || got.Bootstrap.InitialPrimary != "alpha-1" {
		t.Fatalf("expected bootstrap defaults on load, got %+v", got.Bootstrap)
	}
}

func TestLoadRejectsPermissiveConfigFileWithInlineSecret(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
postgres:
  dataDir: /var/lib/postgresql/data
security:
  adminBearerToken: secret-token
bootstrap:
  clusterName: alpha
`

	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected sensitive config file permission error")
	}

	if !errors.Is(err, ErrSensitiveConfigFilePermissions) {
		t.Fatalf("unexpected error: got %v, want %v", err, ErrSensitiveConfigFilePermissions)
	}
}

func TestLoadAllowsRestrictedConfigFileWithInlineSecret(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
postgres:
  dataDir: /var/lib/postgresql/data
security:
  adminBearerToken: secret-token
bootstrap:
  clusterName: alpha
`

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load restricted sensitive config: %v", err)
	}

	if loaded.Security == nil || loaded.Security.AdminBearerToken != "secret-token" {
		t.Fatalf("expected inline token to load from restricted config, got %+v", loaded.Security)
	}
}

func TestLoadAllowsPermissiveConfigFileWhenSecretIsFileBacked(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
postgres:
  dataDir: /var/lib/postgresql/data
security:
  adminBearerTokenFile: /run/secrets/pacman-admin-token
bootstrap:
  clusterName: alpha
`

	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load file-backed secret config: %v", err)
	}

	if loaded.Security == nil || loaded.Security.AdminBearerTokenFile != "/run/secrets/pacman-admin-token" {
		t.Fatalf("expected token file config to load, got %+v", loaded.Security)
	}
}

func TestLoadRejectsPermissiveConfigFileWithInlineDCSSecret(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
dcs:
  backend: etcd
  clusterName: alpha
  ttl: 30s
  retryTimeout: 10s
  etcd:
    endpoints:
      - https://127.0.0.1:2379
    username: pacman
    password: secret-password
`

	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected sensitive config file permission error")
	}

	if !errors.Is(err, ErrSensitiveConfigFilePermissions) {
		t.Fatalf("unexpected error: got %v, want %v", err, ErrSensitiveConfigFilePermissions)
	}
}

func TestLoadReturnsOpenError(t *testing.T) {
	t.Parallel()

	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("expected load error")
	}

	assertContains(t, err.Error(), "open config file")
}

func TestLoadReturnsStatError(t *testing.T) {
	previousOpenConfigFile := openConfigFile
	t.Cleanup(func() {
		openConfigFile = previousOpenConfigFile
	})

	openConfigFile = func(string) (configFile, error) {
		return stubConfigFile{
			Reader:  strings.NewReader(""),
			statErr: errors.New("stat failed"),
		}, nil
	}

	_, err := Load("broken.yaml")
	if err == nil {
		t.Fatal("expected stat error")
	}

	assertContains(t, err.Error(), "stat config file")
	assertContains(t, err.Error(), "stat failed")
}

type stubConfigFile struct {
	io.Reader
	statErr error
}

func (file stubConfigFile) Stat() (os.FileInfo, error) {
	return nil, file.statErr
}

func (stubConfigFile) Close() error {
	return nil
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
