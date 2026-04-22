//go:build integration

package integration_test

import (
	"strings"
	"testing"

	"github.com/polkiloo/pacman/test/testenv"
)

// TestPatroniMigrationRejectsEtcdHostWithoutScheme verifies that a common
// migration mistake — copying Patroni's bare "etcd.host: host:port" value
// directly into dcs.etcd.endpoints without the required "http://" scheme —
// causes an immediate startup failure with a descriptive error.
func TestPatroniMigrationRejectsEtcdHostWithoutScheme(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	// Patroni: etcd.host: 127.0.0.1:2379  →  WRONG: dcs.etcd.endpoints[0]: "127.0.0.1:2379"
	// Correct migration: prefix with "http://" or "https://"
	cfg := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: postgresql0
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
dcs:
  backend: etcd
  clusterName: batman
  ttl: 30s
  retryTimeout: 10s
  etcd:
    endpoints:
      - 127.0.0.1:2379
bootstrap:
  clusterName: batman
  initialPrimary: postgresql0
  expectedMembers:
    - postgresql0
`
	runner := startDaemonRunner(t, env, "compat-no-scheme-runner", cfg, nil, nil)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit for etcd endpoint without URL scheme, got 0")
	}
	if !strings.Contains(result.Output, "etcd") &&
		!strings.Contains(result.Output, topologyValidateConfig) {
		t.Fatalf("expected etcd scheme error in output, got:\n%s", result.Output)
	}
}

// TestPatroniMigrationRejectsUnsupportedDCSBackend verifies that carrying over
// a Patroni DCS backend that PACMAN does not support (consul, zookeeper,
// kubernetes, exhibitor) is caught at startup. The test uses "consul" as the
// representative unsupported backend.
func TestPatroniMigrationRejectsUnsupportedDCSBackend(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	// Patroni supports consul / zookeeper / exhibitor / kubernetes backends.
	// PACMAN only supports etcd and raft; migrating any other backend must fail.
	cfg := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: postgresql0
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
dcs:
  backend: consul
  clusterName: batman
  etcd:
    endpoints:
      - http://127.0.0.1:2379
bootstrap:
  clusterName: batman
  initialPrimary: postgresql0
  expectedMembers:
    - postgresql0
`
	runner := startDaemonRunner(t, env, "compat-consul-runner", cfg, nil, nil)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit for unsupported DCS backend consul, got 0")
	}
	if !strings.Contains(result.Output, "dcs: backend is invalid") &&
		!strings.Contains(result.Output, topologyValidateConfig) {
		t.Fatalf("expected DCS backend error in output, got:\n%s", result.Output)
	}
}

// TestPatroniMigrationRejectsMissingClusterName verifies that a config where
// the Patroni "scope" field was not migrated to dcs.clusterName and
// bootstrap.clusterName is rejected at startup — guarding against the silent
// misconfiguration of an unnamed cluster.
func TestPatroniMigrationRejectsMissingClusterName(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	// Patroni scope: batman was not carried over to dcs.clusterName /
	// bootstrap.clusterName — both are omitted here.
	cfg := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: postgresql0
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
dcs:
  backend: etcd
  etcd:
    endpoints:
      - http://127.0.0.1:2379
bootstrap:
  initialPrimary: postgresql0
  expectedMembers:
    - postgresql0
`
	runner := startDaemonRunner(t, env, "compat-no-cluster-runner", cfg, nil, nil)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatal("expected non-zero exit for missing clusterName (Patroni scope not migrated), got 0")
	}
	if !strings.Contains(result.Output, topologyValidateConfig) {
		t.Fatalf("expected validate config error in output, got:\n%s", result.Output)
	}
}
