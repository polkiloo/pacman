package cmd

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestDCSQuorumTargetStateDerivesKilledTargetsFromEndpointHealth(t *testing.T) {
	t.Parallel()

	health := dcsHealthResult{
		TotalEndpoints:   3,
		HealthyEndpoints: 2,
		FailedEndpoints:  1,
		Endpoints: []dcsEndpointInfo{
			{Endpoint: "http://pacman-dcs:2379", OK: true},
			{Endpoint: "http://pacman-dcs-2:2379", OK: false},
			{Endpoint: "http://pacman-dcs-3:2379", OK: true},
		},
	}

	running, allRunning := dcsQuorumTargetState("dcs-kill-one", []string{"pacman-dcs-2"}, health)
	if running != 0 || allRunning {
		t.Fatalf("killed target state: running=%d allRunning=%v, want 0 false", running, allRunning)
	}

	running, allRunning = dcsQuorumTargetState("primary-dcs-majority-partition", []string{"pacman-dcs-2", "pacman-dcs-3"}, health)
	if running != 2 || !allRunning {
		t.Fatalf("partition target state: running=%d allRunning=%v, want 2 true", running, allRunning)
	}
}

func TestRecordDCSQuorumRecoveryProbeRetriesUntilHealthy(t *testing.T) {
	t.Parallel()

	runner := &scriptedRunner{
		statuses: []int{1, 0, 0, 0, 0, 0},
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
	})
	lab.cfg.dcsRecoveryTimeout = 50 * time.Millisecond
	lab.cfg.dcsRecoveryInterval = time.Millisecond

	caseDir := t.TempDir()
	lab.recordDCSQuorumRecoveryProbe(context.Background(), caseDir, "dcs-lose-majority", "after-restart", []string{"pacman-dcs-2", "pacman-dcs-3"}, "pacman-primary")

	if runner.calls != 6 {
		t.Fatalf("runner calls: got %d want 6", runner.calls)
	}
	rows := readJSONL(filepath.Join(caseDir, dcsQuorumSampleFile))
	if len(rows) != 2 {
		t.Fatalf("samples: got %d want 2", len(rows))
	}
	if rows[0]["healthyEndpoints"] != float64(2) || rows[1]["healthyEndpoints"] != float64(3) {
		t.Fatalf("healthy endpoint samples: %#v", rows)
	}
}
