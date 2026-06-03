package cmd

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestResolvePatroniCheckTimelineProfile(t *testing.T) {
	t.Parallel()

	profile, ok := resolvePatroniCheckTimelineProfile("append-check-timeline")
	if !ok || !profile.CheckTimeline {
		t.Fatalf("profile: got %+v ok=%t", profile, ok)
	}
	if _, ok := resolvePatroniCheckTimelineProfile("append-failover"); ok {
		t.Fatal("append-failover should not use the Patroni check_timeline profile")
	}
}

func TestPatroniCheckTimelineReplicas(t *testing.T) {
	t.Parallel()

	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	stale, eligible, err := (&harnessLab{options: harnessOptions{runOptions: runOptions{target: target}}}).patroniCheckTimelineReplicas("patroni-1")
	if err != nil {
		t.Fatalf("select replicas: %v", err)
	}
	if stale.Name != "patroni-2" || eligible.Name != "patroni-3" {
		t.Fatalf("replicas: got stale=%+v eligible=%+v", stale, eligible)
	}
}

func TestCheckPatroniCheckTimelineProbes(t *testing.T) {
	t.Parallel()

	state := func(reachable, writable, inRecovery bool, timeline int) patroniCheckTimelineNodeState {
		return patroniCheckTimelineNodeState{
			Member:     "patroni-2",
			Service:    "patroni-replica",
			Reachable:  reachable,
			Writable:   writable,
			InRecovery: inRecovery,
			Timeline:   timeline,
		}
	}
	valid := []patroniCheckTimelineProbe{
		{Phase: "initial", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-1", PrimaryTimeline: 1, StaleReplicaState: state(true, false, true, 1)},
		{Phase: "after-first-promotion", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 2, StaleReplicaState: state(false, false, false, 0)},
		{Phase: "stale-candidate", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 2, StaleReplicaState: state(true, false, true, 1)},
		{Phase: "blocked", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "unknown", PrimaryTimeline: 2, StaleReplicaState: state(true, false, true, 1)},
		{Phase: "after-recovery", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 3, StaleReplicaState: state(true, false, true, 3)},
	}

	tests := []struct {
		name    string
		probes  []patroniCheckTimelineProbe
		wantErr string
	}{
		{name: "accepts blocked stale promotion and recovery", probes: valid},
		{name: "rejects missing probe", probes: valid[:4], wantErr: "missing required phases"},
		{
			name: "rejects candidate on current timeline",
			probes: []patroniCheckTimelineProbe{
				valid[0],
				valid[1],
				{Phase: "stale-candidate", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 2, StaleReplicaState: state(true, false, true, 2)},
				valid[3],
				valid[4],
			},
			wantErr: "not a stale replica",
		},
		{
			name: "rejects stale promotion",
			probes: []patroniCheckTimelineProbe{
				valid[0],
				valid[1],
				valid[2],
				{Phase: "blocked", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-2", PrimaryTimeline: 2, StaleReplicaState: state(true, true, false, 2)},
				valid[4],
			},
			wantErr: "did not block",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := checkPatroniCheckTimelineProbes(test.probes)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("check probes: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err: got %v want fragment %q", err, test.wantErr)
			}
		})
	}
}

func TestCheckPatroniCheckTimelineWritesCheckerArtifact(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	state := func(reachable, writable, inRecovery bool, timeline int) patroniCheckTimelineNodeState {
		return patroniCheckTimelineNodeState{Member: "patroni-2", Service: "patroni-replica", Reachable: reachable, Writable: writable, InRecovery: inRecovery, Timeline: timeline}
	}
	for _, probe := range []patroniCheckTimelineProbe{
		{Phase: "initial", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-1", PrimaryTimeline: 1, StaleReplicaState: state(true, false, true, 1)},
		{Phase: "after-first-promotion", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 2, StaleReplicaState: state(false, false, false, 0)},
		{Phase: "stale-candidate", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 2, StaleReplicaState: state(true, false, true, 1)},
		{Phase: "blocked", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "unknown", PrimaryTimeline: 2, StaleReplicaState: state(true, false, true, 1)},
		{Phase: "after-recovery", OldPrimary: "patroni-1", StaleReplica: "patroni-2", EligibleReplica: "patroni-3", CurrentPrimary: "patroni-3", PrimaryTimeline: 3, StaleReplicaState: state(true, false, true, 3)},
	} {
		appendJSONL(filepath.Join(caseDir, patroniCheckTimelineProbesFile), probe)
	}

	if err := (&harnessLab{}).checkPatroniCheckTimeline(patroniCheckTimelineNemesis, caseDir); err != nil {
		t.Fatalf("check Patroni check_timeline: %v", err)
	}
	output := readTestFile(t, filepath.Join(caseDir, patroniCheckTimelineCheckerFile))
	assertContainsAll(t, "checker artifact", output, []string{`"checker": "patroni-check-timeline"`, `"valid": true`})
}
