package config

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestDecodeWithReportTranslatesPatroniEtcdConfig(t *testing.T) {
	t.Parallel()

	payload := `
scope: batman
name: postgresql1
restapi:
  listen: 127.0.0.1:8009
  connect_address: postgresql1.internal:8009
etcd:
  host: 127.0.0.1:2379
  username: etcd-user
  password: etcd-password
bootstrap:
  dcs:
    ttl: 30
    retry_timeout: 10
    maximum_lag_on_failover: 1048576
    postgresql:
      use_pg_rewind: true
postgresql:
  listen: 127.0.0.1:5433
  connect_address: postgresql1.internal:5433
  data_dir: data/postgresql1
  bin_dir: /usr/lib/postgresql/17/bin
`

	report, err := DecodeWithReport(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode Patroni config: %v", err)
	}

	if report.Format != DocumentFormatPatroni {
		t.Fatalf("unexpected document format: got %q, want %q", report.Format, DocumentFormatPatroni)
	}

	got := report.Config

	if got.Node.Name != "postgresql1" {
		t.Fatalf("unexpected translated node name: got %q", got.Node.Name)
	}

	if got.Node.APIAddress != "127.0.0.1:8009" {
		t.Fatalf("unexpected translated apiAddress: got %q", got.Node.APIAddress)
	}

	if got.Node.ControlAddress != DefaultControlAddress {
		t.Fatalf("unexpected translated controlAddress default: got %q", got.Node.ControlAddress)
	}

	if got.DCS == nil {
		t.Fatal("expected translated dcs config")
	}

	if got.DCS.Backend != dcs.BackendEtcd {
		t.Fatalf("unexpected translated dcs backend: got %q", got.DCS.Backend)
	}

	if got.DCS.ClusterName != "batman" {
		t.Fatalf("unexpected translated dcs cluster name: got %q", got.DCS.ClusterName)
	}

	if got.DCS.TTL != 30*time.Second {
		t.Fatalf("unexpected translated dcs ttl: got %s", got.DCS.TTL)
	}

	if got.DCS.RetryTimeout != 10*time.Second {
		t.Fatalf("unexpected translated dcs retry timeout: got %s", got.DCS.RetryTimeout)
	}

	if got.DCS.Etcd == nil {
		t.Fatal("expected translated etcd config")
	}

	if !reflect.DeepEqual(got.DCS.Etcd.Endpoints, []string{"http://127.0.0.1:2379"}) {
		t.Fatalf("unexpected translated etcd endpoints: got %+v", got.DCS.Etcd.Endpoints)
	}

	if got.DCS.Etcd.Username != "etcd-user" || got.DCS.Etcd.Password != "etcd-password" {
		t.Fatalf("unexpected translated etcd credentials: %+v", got.DCS.Etcd)
	}

	if got.Postgres == nil {
		t.Fatal("expected translated postgres config")
	}

	if got.Postgres.DataDir != "data/postgresql1" || got.Postgres.BinDir != "/usr/lib/postgresql/17/bin" {
		t.Fatalf("unexpected translated postgres paths: %+v", got.Postgres)
	}

	if got.Postgres.ListenAddress != "127.0.0.1" || got.Postgres.Port != 5433 {
		t.Fatalf("unexpected translated postgres listen config: %+v", got.Postgres)
	}

	if got.Bootstrap == nil {
		t.Fatal("expected translated bootstrap config")
	}

	if got.Bootstrap.ClusterName != "batman" {
		t.Fatalf("unexpected translated bootstrap clusterName: got %q", got.Bootstrap.ClusterName)
	}

	if got.Bootstrap.InitialPrimary != "postgresql1" {
		t.Fatalf("unexpected translated bootstrap initialPrimary default: got %q", got.Bootstrap.InitialPrimary)
	}

	if !reflect.DeepEqual(got.Bootstrap.SeedAddresses, []string{DefaultControlAddress}) {
		t.Fatalf("unexpected translated bootstrap seedAddresses: got %+v", got.Bootstrap.SeedAddresses)
	}

	if !reflect.DeepEqual(got.Bootstrap.ExpectedMembers, []string{"postgresql1"}) {
		t.Fatalf("unexpected translated bootstrap expectedMembers: got %+v", got.Bootstrap.ExpectedMembers)
	}

	warnings := strings.Join(report.Warnings, "\n")
	assertContains(t, warnings, "bootstrap.dcs.maximum_lag_on_failover")
	assertContains(t, warnings, "bootstrap.dcs.postgresql.use_pg_rewind")
	assertContains(t, warnings, "node.controlAddress")
	assertContains(t, warnings, "bootstrap.initialPrimary")
	assertContains(t, warnings, "restapi.connect_address")
	assertContains(t, warnings, "postgresql.connect_address")
}

func TestDecodeWithReportTranslatesPatroniEtcdHosts(t *testing.T) {
	t.Parallel()

	payload := `
scope: batman
name: postgresql2
restapi:
  listen: 127.0.0.1:8010
etcd:
  hosts:
    - 127.0.0.1:2379
    - https://etcd-2.internal:2379
    - 127.0.0.1:2379
postgresql:
  listen: 127.0.0.1:5434
  data_dir: data/postgresql2
`

	report, err := DecodeWithReport(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode Patroni etcd.hosts config: %v", err)
	}

	if report.Config.DCS == nil || report.Config.DCS.Etcd == nil {
		t.Fatalf("expected translated etcd config, got %+v", report.Config.DCS)
	}

	want := []string{"http://127.0.0.1:2379", "https://etcd-2.internal:2379"}
	if !reflect.DeepEqual(report.Config.DCS.Etcd.Endpoints, want) {
		t.Fatalf("unexpected translated etcd.hosts endpoints: got %+v, want %+v", report.Config.DCS.Etcd.Endpoints, want)
	}
}

func TestDecodeWithReportTranslatesPatroniRaftConfig(t *testing.T) {
	t.Parallel()

	payload := `
scope: batman
name: raft-1
restapi:
  connect_address: 10.0.0.11:8008
raft:
  data_dir: /var/lib/patroni/raft
  self_addr: 10.0.0.11:2222
  partner_addrs:
    - 10.0.0.12:2222
    - 10.0.0.13:2222
    - 10.0.0.12:2222
postgresql:
  connect_address: 10.0.0.11:5432
  data_dir: /var/lib/postgresql/data
`

	report, err := DecodeWithReport(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode Patroni raft config: %v", err)
	}

	if report.Config.Node.APIAddress != "10.0.0.11:8008" {
		t.Fatalf("unexpected translated apiAddress fallback: got %q", report.Config.Node.APIAddress)
	}

	if report.Config.Postgres == nil || report.Config.Postgres.ListenAddress != "10.0.0.11" || report.Config.Postgres.Port != 5432 {
		t.Fatalf("unexpected translated postgres connect_address fallback: %+v", report.Config.Postgres)
	}

	if report.Config.DCS == nil || report.Config.DCS.Raft == nil {
		t.Fatalf("expected translated raft config, got %+v", report.Config.DCS)
	}

	if report.Config.DCS.Backend != dcs.BackendRaft {
		t.Fatalf("unexpected translated raft backend: got %q", report.Config.DCS.Backend)
	}

	if report.Config.DCS.Raft.DataDir != "/var/lib/patroni/raft" {
		t.Fatalf("unexpected translated raft dataDir: got %q", report.Config.DCS.Raft.DataDir)
	}

	if report.Config.DCS.Raft.BindAddress != "10.0.0.11:2222" {
		t.Fatalf("unexpected translated raft bindAddress: got %q", report.Config.DCS.Raft.BindAddress)
	}

	wantPeers := []string{"10.0.0.11:2222", "10.0.0.12:2222", "10.0.0.13:2222"}
	if !reflect.DeepEqual(report.Config.DCS.Raft.Peers, wantPeers) {
		t.Fatalf("unexpected translated raft peers: got %+v, want %+v", report.Config.DCS.Raft.Peers, wantPeers)
	}

	warnings := strings.Join(report.Warnings, "\n")
	assertContains(t, warnings, `Patroni "raft" settings`)
	assertContains(t, warnings, `Patroni key "restapi.listen" is unset`)
	assertContains(t, warnings, `Patroni key "postgresql.listen" is unset`)
}

func TestDecodeWithReportRejectsUnsupportedPatroniDCSBackend(t *testing.T) {
	t.Parallel()

	payload := `
scope: batman
name: postgresql0
restapi:
  listen: 127.0.0.1:8008
consul:
  host: 127.0.0.1:8500
postgresql:
  listen: 127.0.0.1:5432
  data_dir: data/postgresql0
`

	_, err := DecodeWithReport(strings.NewReader(payload))
	if err == nil {
		t.Fatal("expected unsupported Patroni backend error")
	}

	if !errors.Is(err, ErrPatroniDCSBackendUnsupported) {
		t.Fatalf("unexpected error: got %v, want %v", err, ErrPatroniDCSBackendUnsupported)
	}

	assertContains(t, err.Error(), "consul")
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
