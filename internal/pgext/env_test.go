package pgext

import "testing"

func TestSnapshotEnvironmentRoundTrip(t *testing.T) {
	t.Parallel()

	original := Snapshot{
		NodeName:              "alpha-1",
		NodeRole:              "data",
		APIAddress:            "0.0.0.0:8080",
		ControlAddress:        "0.0.0.0:9090",
		PostgresDataDir:       "/var/lib/postgresql/data",
		PostgresBinDir:        "/usr/lib/postgresql/17/bin",
		PostgresListenAddress: "127.0.0.1",
		PostgresPort:          5432,
		ClusterName:           "alpha",
		InitialPrimary:        "alpha-1",
		SeedAddresses:         "alpha-1:9090,alpha-2:9090",
		ExpectedMembers:       "alpha-1,alpha-2",
	}

	roundTripped, err := LoadSnapshotFromEnv(mapLookup(original.Environment()))
	if err != nil {
		t.Fatalf("load snapshot from env: %v", err)
	}

	if roundTripped != original {
		t.Fatalf("unexpected round-trip snapshot: got %+v want %+v", roundTripped, original)
	}
}

func TestLoadSnapshotFromEnvIgnoresUnsetValues(t *testing.T) {
	t.Parallel()

	snapshot, err := LoadSnapshotFromEnv(mapLookup(map[string]string{
		EnvNodeName:        " alpha-1 ",
		EnvPostgresDataDir: " /var/lib/postgresql/data ",
	}))
	if err != nil {
		t.Fatalf("load snapshot from sparse env: %v", err)
	}

	if snapshot.NodeName != "alpha-1" {
		t.Fatalf("unexpected node name: %q", snapshot.NodeName)
	}

	if snapshot.PostgresDataDir != "/var/lib/postgresql/data" {
		t.Fatalf("unexpected postgres data dir: %q", snapshot.PostgresDataDir)
	}

	if snapshot.PostgresPort != 0 {
		t.Fatalf("unexpected postgres port: %d", snapshot.PostgresPort)
	}
}

func TestLoadSnapshotFromEnvRejectsInvalidPort(t *testing.T) {
	t.Parallel()

	_, err := LoadSnapshotFromEnv(mapLookup(map[string]string{
		EnvPostgresPort: "not-a-number",
	}))
	if err == nil {
		t.Fatal("expected invalid port error")
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
