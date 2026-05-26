package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestManualSwitchoverCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		operationFixture   string
		observationFixture string
		wantValid          bool
		wantAccepted       bool
		wantCandidate      string
		wantFinalPrimary   string
	}{
		{
			name:               "accepts requested candidate as final primary",
			operationFixture:   "accepted_alpha2.json",
			observationFixture: "alpha2_final_primary.jsonl",
			wantValid:          true,
			wantAccepted:       true,
			wantCandidate:      "alpha-2",
			wantFinalPrimary:   "alpha-2",
		},
		{
			name:               "rejects accepted request when another primary wins",
			operationFixture:   "accepted_alpha2.json",
			observationFixture: "alpha3_final_primary.jsonl",
			wantAccepted:       true,
			wantCandidate:      "alpha-2",
			wantFinalPrimary:   "alpha-3",
		},
		{
			name:               "rejects failed request",
			operationFixture:   "rejected_alpha2.json",
			observationFixture: "alpha1_final_primary.jsonl",
			wantCandidate:      "alpha-2",
			wantFinalPrimary:   "alpha-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			writeTestFile(t, filepath.Join(caseDir, manualSwitchoverFile), readTestFile(t, filepath.Join("testdata", "manual_switchover", test.operationFixture)))
			writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "manual_switchover", test.observationFixture)))

			gotValid, err := runManualSwitchoverChecker(manualSwitchoverCheckerOptions{
				caseDir: caseDir,
				nemesis: "switchover",
			})
			if err != nil {
				t.Fatalf("run manual switchover checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result manualSwitchoverCheckerResult
			readJSONTestFile(t, filepath.Join(caseDir, manualSwitchoverCheckerFile), &result)
			if result.Checker != manualSwitchoverCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, manualSwitchoverCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if !result.Applicable {
				t.Fatalf("applicable: got false want true")
			}
			if result.RequestAccepted != test.wantAccepted {
				t.Fatalf("request accepted: got %v want %v", result.RequestAccepted, test.wantAccepted)
			}
			if result.Candidate != test.wantCandidate {
				t.Fatalf("candidate: got %q want %q", result.Candidate, test.wantCandidate)
			}
			if result.FinalPrimary == nil || result.FinalPrimary.Member != test.wantFinalPrimary {
				t.Fatalf("final primary: got %+v want member %q", result.FinalPrimary, test.wantFinalPrimary)
			}
		})
	}
}

func TestManualSwitchoverCheckerSkipsOtherNemeses(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runManualSwitchoverChecker(manualSwitchoverCheckerOptions{caseDir: caseDir, nemesis: "packet"})
	if err != nil {
		t.Fatalf("run manual switchover checker: %v", err)
	}
	if !gotValid {
		t.Fatalf("valid: got false want true")
	}

	var result manualSwitchoverCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, manualSwitchoverCheckerFile), &result)
	if !result.Valid || result.Applicable {
		t.Fatalf("skip result: valid=%v applicable=%v want valid=true applicable=false", result.Valid, result.Applicable)
	}
}

func TestManualSwitchoverCheckerMissingInputs(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runManualSwitchoverChecker(manualSwitchoverCheckerOptions{caseDir: caseDir, nemesis: "switchover"})
	if err != nil {
		t.Fatalf("run manual switchover checker: %v", err)
	}
	if gotValid {
		t.Fatalf("valid: got true want false")
	}

	var result manualSwitchoverCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, manualSwitchoverCheckerFile), &result)
	if result.Valid || !result.Applicable {
		t.Fatalf("missing result: valid=%v applicable=%v want false true", result.Valid, result.Applicable)
	}
	if result.Error != "missing switchover operation metadata or primary observations" {
		t.Fatalf("error: got %q want missing input error", result.Error)
	}
}

func TestManualSwitchoverCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, manualSwitchoverFile), readTestFile(t, filepath.Join("testdata", "manual_switchover", "accepted_alpha2.json")))
	writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "manual_switchover", "alpha3_final_primary.jsonl")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "manual-switchover",
		"--case-dir", caseDir,
		"--nemesis", "switchover",
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "manual switchover checker failed") {
		t.Fatalf("stderr: got %q want manual switchover failure", stderr.String())
	}
}
