package cmd

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestResolvePatroniMaximumLagOnFailoverProfile(t *testing.T) {
	t.Parallel()

	profile, ok := resolvePatroniMaximumLagOnFailoverProfile("append-max-lag")
	if !ok || profile.MaximumLagBytes != maximumLagOnFailoverBytes {
		t.Fatalf("profile: got %+v ok=%t", profile, ok)
	}
	if _, ok := resolvePatroniMaximumLagOnFailoverProfile("append-failover"); ok {
		t.Fatal("append-failover should not use the Patroni maximum lag profile")
	}
}

func TestMaximumLagOnFailoverReplicas(t *testing.T) {
	t.Parallel()

	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	lagging, eligible, err := (&harnessLab{options: harnessOptions{runOptions: runOptions{target: target}}}).maximumLagOnFailoverReplicas("patroni-1")
	if err != nil {
		t.Fatalf("select replicas: %v", err)
	}
	if lagging.Name != "patroni-2" || eligible.Name != "patroni-3" {
		t.Fatalf("replicas: got lagging=%+v eligible=%+v", lagging, eligible)
	}
}

func TestCheckMaximumLagOnFailoverProbes(t *testing.T) {
	t.Parallel()

	state := func(paused bool, lag int64) patroniMaximumLagOnFailoverReplicaState {
		return patroniMaximumLagOnFailoverReplicaState{
			Member:       "patroni-2",
			Service:      "patroni-replica",
			ReplayPaused: paused,
			LagBytes:     lag,
		}
	}
	valid := []patroniMaximumLagOnFailoverProbe{
		{Phase: "before-pause", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", State: state(false, 0)},
		{Phase: "lagged", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", State: state(true, 2048)},
		{Phase: "after-promotion", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", Promoted: "patroni-3", State: state(false, 0)},
		{Phase: "after-resume", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", Promoted: "patroni-3", State: state(false, 0)},
	}

	tests := []struct {
		name    string
		probes  []patroniMaximumLagOnFailoverProbe
		wantErr string
	}{
		{name: "accepts eligible promotion and replay recovery", probes: valid},
		{name: "rejects missing probe", probes: valid[:3], wantErr: "missing required phases"},
		{
			name: "rejects insufficient lag",
			probes: []patroniMaximumLagOnFailoverProbe{
				valid[0],
				{Phase: "lagged", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", State: state(true, 1024)},
				valid[2],
				valid[3],
			},
			wantErr: "above threshold",
		},
		{
			name: "rejects lagging replica promotion",
			probes: []patroniMaximumLagOnFailoverProbe{
				valid[0],
				valid[1],
				{Phase: "after-promotion", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", Promoted: "patroni-2", State: state(true, 2048)},
				valid[3],
			},
			wantErr: "want eligible replica",
		},
		{
			name: "rejects missing replay resume",
			probes: []patroniMaximumLagOnFailoverProbe{
				valid[0],
				valid[1],
				valid[2],
				{Phase: "after-resume", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", Promoted: "patroni-3", State: state(true, 2048)},
			},
			wantErr: "resume replica replay",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := checkMaximumLagOnFailoverProbes(test.probes)
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

func TestCheckMaximumLagOnFailoverWritesCheckerArtifact(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	state := func(paused bool, lag int64) patroniMaximumLagOnFailoverReplicaState {
		return patroniMaximumLagOnFailoverReplicaState{Member: "patroni-2", Service: "patroni-replica", ReplayPaused: paused, LagBytes: lag}
	}
	for _, probe := range []patroniMaximumLagOnFailoverProbe{
		{Phase: "before-pause", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", State: state(false, 0)},
		{Phase: "lagged", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", State: state(true, 2048)},
		{Phase: "after-promotion", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", Promoted: "patroni-3", State: state(false, 0)},
		{Phase: "after-resume", MaximumLagBytes: 1024, OldPrimary: "patroni-1", LaggingReplica: "patroni-2", EligibleReplica: "patroni-3", Promoted: "patroni-3", State: state(false, 0)},
	} {
		appendJSONL(filepath.Join(caseDir, maximumLagOnFailoverProbesFile), probe)
	}

	if err := (&harnessLab{}).checkMaximumLagOnFailover(maximumLagOnFailoverNemesis, caseDir); err != nil {
		t.Fatalf("check maximum lag on failover: %v", err)
	}
	output := readTestFile(t, filepath.Join(caseDir, maximumLagOnFailoverCheckerFile))
	assertContainsAll(t, "checker artifact", output, []string{`"checker": "maximum-lag-on-failover"`, `"valid": true`})
}
