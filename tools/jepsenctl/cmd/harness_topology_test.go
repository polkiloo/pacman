package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestHarnessTopologyHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"service alpha-1", serviceForMember("alpha-1"), "pacman-primary"},
		{"service alpha-2", serviceForMember("alpha-2"), "pacman-replica"},
		{"service alpha-3", serviceForMember("alpha-3"), "pacman-replica-2"},
		{"member primary", memberForService("pacman-primary"), "alpha-1"},
		{"dcs service", dcsMemberForService("pacman-dcs-2"), "alpha-dcs-2"},
		{"service ip", serviceIP("pacman-replica-2"), "172.28.0.13"},
		{"sql literal", sqlLiteral("alpha's"), "'alpha''s'"},
	}
	for _, test := range tests {
		if test.got != test.want {
			t.Fatalf("%s: got %q want %q", test.name, test.got, test.want)
		}
	}

	if got := peerServicesForMember("alpha-1"); !reflect.DeepEqual(got, []string{"pacman-replica", "pacman-replica-2"}) {
		t.Fatalf("alpha-1 peers: got %#v", got)
	}
	if got := serviceForMember("unknown"); got != "" {
		t.Fatalf("unknown service: got %q want empty", got)
	}
}

func TestVerifyThreeDataNodeClusterWaitsForHealthyShape(t *testing.T) {
	dir := t.TempDir()
	runner := &scriptedRunner{outputs: []string{
		`{"phase":"initializing","currentPrimary":"","members":[]}`,
		validClusterStatusJSON(),
	}}
	lab := newHarnessLab(harnessOptions{
		repoRoot: dir,
		runner:   runner,
	})
	lab.cfg.clusterVerifyTimeout = 200 * time.Millisecond
	lab.cfg.clusterVerifyInterval = time.Millisecond

	outputFile := filepath.Join(dir, "pacman-cluster-before.json")
	if err := lab.verifyThreeDataNodeCluster(context.Background(), outputFile); err != nil {
		t.Fatalf("verify cluster: %v", err)
	}
	if runner.calls < 2 {
		t.Fatalf("runner calls: got %d want at least 2", runner.calls)
	}
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	var status clusterStatus
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatalf("decode output file: %v", err)
	}
	if status.CurrentPrimary != "alpha-1" {
		t.Fatalf("current primary: got %q want alpha-1", status.CurrentPrimary)
	}
}

func TestPatroniClusterStatusUsesPostgresRoleProbes(t *testing.T) {
	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runOptions: runOptions{
			target: target,
		},
	})

	status := patroniClusterStatusFromProbes([]patroniRoleProbe{
		{node: target.DataNodes[0]},
		{node: target.DataNodes[1], inRecovery: true, streaming: true},
		{node: target.DataNodes[2], inRecovery: true, streaming: true},
	})
	if err := validateClusterStatusForMembers(status, []string{"patroni-1", "patroni-2", "patroni-3"}); err != nil {
		t.Fatalf("validate Patroni status: %v", err)
	}
	if status.CurrentPrimary != "patroni-1" {
		t.Fatalf("primary: got %q want patroni-1", status.CurrentPrimary)
	}
	if lab.cfg.composeFile != filepath.Join(lab.options.repoRoot, "deploy", "patroni-lab", "compose.yml") {
		t.Fatalf("compose file: got %q", lab.cfg.composeFile)
	}
	if lab.cfg.pgClientService != "patroni-primary" || lab.cfg.pgHost != "127.0.0.1" || lab.cfg.psqlBinary != "/usr/bin/psql" {
		t.Fatalf("Patroni PostgreSQL config: %#v", lab.cfg)
	}
	if got := lab.peerServicesForMember("patroni-1"); !reflect.DeepEqual(got, []string{"patroni-replica", "patroni-replica-2"}) {
		t.Fatalf("Patroni peers: got %#v", got)
	}
}

func TestPatroniNodeRuntimeStopsAndStartsComposeService(t *testing.T) {
	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	runner := &scriptedRunner{}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
		runOptions: runOptions{
			target: target,
		},
	})

	if err := lab.stopNodeRuntime(context.Background(), "patroni-primary"); err != nil {
		t.Fatalf("stop Patroni runtime: %v", err)
	}
	if err := lab.startNodeRuntime(context.Background(), "patroni-primary"); err != nil {
		t.Fatalf("start Patroni runtime: %v", err)
	}
	if len(runner.specs) != 2 {
		t.Fatalf("runner calls: got %d want 2", runner.calls)
	}
	if got, want := runner.specs[0].args, []string{"compose", "-f", lab.cfg.composeFile, "stop", "patroni-primary"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stop args: got %#v want %#v", got, want)
	}
	if got, want := runner.specs[1].args, []string{"compose", "-f", lab.cfg.composeFile, "start", "patroni-primary"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("start args: got %#v want %#v", got, want)
	}
}
