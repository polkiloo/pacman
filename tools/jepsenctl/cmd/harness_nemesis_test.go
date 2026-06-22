package cmd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestIPTablesPartitionReportsCommandFailure(t *testing.T) {
	t.Parallel()

	runner := &scriptedRunner{
		outputs:  []string{"iptables command not found\n"},
		statuses: []int{127},
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
	})

	err := lab.iptablesPartition(context.Background(), "pacman-primary", []string{"pacman-dcs-2"})
	if err == nil {
		t.Fatalf("partition succeeded without iptables")
	}
	assertContainsAll(t, "partition error", err.Error(), []string{
		"iptables partition pacman-primary from pacman-dcs-2 failed with status 127",
		"iptables command not found",
	})
	if len(runner.specs) != 1 {
		t.Fatalf("runner calls: got %d want 1", len(runner.specs))
	}
	if got := strings.Join(runner.specs[0].args, " "); !strings.Contains(got, "command -v iptables") {
		t.Fatalf("partition command missing iptables discovery: %s", got)
	}
}

func TestIPTablesPartitionRejectsUnknownPeer(t *testing.T) {
	t.Parallel()

	runner := &scriptedRunner{}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
	})

	err := lab.iptablesPartition(context.Background(), "pacman-primary", []string{"unknown-peer"})
	if err == nil || !strings.Contains(err.Error(), "unknown peer service") {
		t.Fatalf("partition error: got %v want unknown peer service", err)
	}
	if len(runner.specs) != 0 {
		t.Fatalf("runner calls: got %d want 0", len(runner.specs))
	}
}

func TestPacketKillNemesisRestartsAfterNetworkHeal(t *testing.T) {
	t.Parallel()

	runner := &clusterStatusRunner{
		initialPrimary:   "alpha-1",
		promotedPrimary:  "alpha-2",
		statusAfterCalls: 1,
	}
	target, err := resolveJepsenTarget(defaultJepsenTarget)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
		runOptions: runOptions{
			target: target,
		},
	})
	lab.cfg.nemesisHold = 0
	caseDir := t.TempDir()
	scheduleFile := filepath.Join(caseDir, "nemesis-schedule.edn")

	if err := lab.applyNemesis(context.Background(), "packet,kill", caseDir, scheduleFile); err != nil {
		t.Fatalf("apply packet,kill nemesis: %v", err)
	}

	healIndex := runner.firstCommandIndex("iptables -D INPUT")
	startIndex := runner.firstCommandIndex("exec /usr/bin/pacmand")
	if healIndex < 0 {
		t.Fatalf("missing iptables heal command in %v", runner.commands())
	}
	if startIndex < 0 {
		t.Fatalf("missing pacmand restart command in %v", runner.commands())
	}
	if startIndex < healIndex {
		t.Fatalf("pacmand restarted before partition heal:\n%s", strings.Join(runner.commands(), "\n"))
	}

	schedule := readTestFile(t, scheduleFile)
	assertContainsAll(t, "packet kill schedule", schedule, []string{
		`:nemesis :packet-kill :action :start :target "alpha-1"`,
		`:nemesis :packet-kill :action :heal :target "alpha-1" :promoted "alpha-2" :result :ok`,
		`:nemesis :packet-kill :action :stop :target "alpha-1" :promoted "alpha-2" :result :ok`,
	})
}

func TestHarnessSmallProfileHelpers(t *testing.T) {
	t.Parallel()

	if boolStatus(true) != 0 {
		t.Fatalf("boolStatus(true) should be 0")
	}
	if boolStatus(false) != 1 {
		t.Fatalf("boolStatus(false) should be 1")
	}
	if got := workloadTable("append-failover"); got != "jepsen.append_values" {
		t.Fatalf("append table: got %q", got)
	}
	if got := workloadTable("append-sync"); got != "jepsen.append_values" {
		t.Fatalf("sync append table: got %q", got)
	}
	if got := workloadTable("append-sync-two"); got != "jepsen.append_values" {
		t.Fatalf("two-standby sync append table: got %q", got)
	}
	if got := workloadTable("append-max-lag"); got != "jepsen.append_values" {
		t.Fatalf("maximum lag append table: got %q", got)
	}
	if got := workloadTable("append-check-timeline"); got != "jepsen.append_values" {
		t.Fatalf("check timeline append table: got %q", got)
	}
	if got := workloadTable("append-reinit"); got != "jepsen.append_values" {
		t.Fatalf("reinit append table: got %q", got)
	}
	if got := workloadTable("serializable-txn"); got != "jepsen.txn_ops" {
		t.Fatalf("txn table: got %q", got)
	}
	if got := workloadTable("unknown"); got != "" {
		t.Fatalf("unknown table: got %q want empty", got)
	}
	if got := maxDuration(2, 1); got != 2 {
		t.Fatalf("max duration: got %s", got)
	}
}
