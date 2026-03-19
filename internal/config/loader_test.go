package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
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
postgres:
  dataDir: /var/lib/postgresql/data
  binDir: /usr/lib/postgresql/17/bin
  listenAddress: 127.0.0.1
  port: 5432
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

func TestLoadReturnsOpenError(t *testing.T) {
	t.Parallel()

	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("expected load error")
	}

	assertContains(t, err.Error(), "open config file")
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
