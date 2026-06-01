package cmd

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestPatroniSynchronousProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		workload   string
		wantStrict bool
		wantOK     bool
	}{
		{workload: "append-sync", wantOK: true},
		{workload: "append-strict-sync", wantStrict: true, wantOK: true},
		{workload: "append-failover"},
	}

	for _, test := range tests {
		t.Run(test.workload, func(t *testing.T) {
			t.Parallel()

			strict, ok := patroniSynchronousProfile(test.workload)
			if strict != test.wantStrict || ok != test.wantOK {
				t.Fatalf("profile %s: got strict=%t ok=%t want strict=%t ok=%t", test.workload, strict, ok, test.wantStrict, test.wantOK)
			}
		})
	}
}

func TestCheckStrictSyncNoStandbyProbes(t *testing.T) {
	t.Parallel()

	available := patroniSynchronousState{SynchronousMode: true, SynchronousModeStrict: true, SynchronousStandbyAvailable: true}
	unavailable := patroniSynchronousState{SynchronousMode: true, SynchronousModeStrict: true}
	valid := []strictSyncWriteProbe{
		{Phase: "before-no-standby", Available: true, State: available},
		{Phase: "during-no-standby", Available: false, ExitStatus: 124, State: unavailable},
		{Phase: "after-no-standby", Available: true, State: available},
	}

	tests := []struct {
		name    string
		probes  []strictSyncWriteProbe
		wantErr string
	}{
		{name: "accepts unavailable window and recovery", probes: valid},
		{name: "rejects missing probe", probes: valid[:2], wantErr: "missing required phases"},
		{
			name: "rejects write availability without standby",
			probes: []strictSyncWriteProbe{
				valid[0],
				{Phase: "during-no-standby", Available: true, State: unavailable},
				valid[2],
			},
			wantErr: "want true,false,true",
		},
		{
			name: "rejects missing synchronous standby recovery",
			probes: []strictSyncWriteProbe{
				valid[0],
				valid[1],
				{Phase: "after-no-standby", Available: true, State: unavailable},
			},
			wantErr: "did not remove and restore",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := checkStrictSyncNoStandbyProbes(test.probes)
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

func TestCheckStrictSyncNoStandbyWritesCheckerArtifact(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	available := patroniSynchronousState{SynchronousMode: true, SynchronousModeStrict: true, SynchronousStandbyAvailable: true}
	for _, probe := range []strictSyncWriteProbe{
		{Phase: "before-no-standby", Available: true, State: available},
		{Phase: "during-no-standby", ExitStatus: 124, State: patroniSynchronousState{SynchronousMode: true, SynchronousModeStrict: true}},
		{Phase: "after-no-standby", Available: true, State: available},
	} {
		appendJSONL(filepath.Join(caseDir, strictSyncWriteProbesFile), probe)
	}

	if err := (&harnessLab{}).checkStrictSyncNoStandby("no-standby", caseDir); err != nil {
		t.Fatalf("check strict sync: %v", err)
	}
	output := readTestFile(t, filepath.Join(caseDir, strictSyncNoStandbyCheckerFile))
	assertContainsAll(t, "checker artifact", output, []string{`"checker": "strict-sync-no-standby"`, `"valid": true`})
}
