package cmd

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestDCSQuorumCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		nemesis            string
		fixture            string
		minLatencyMillis   int
		wantValid          bool
		wantBefore         int
		wantDuringExpected int
		wantAfterRecovered int
	}{
		{
			name:               "kill one DCS node remains quorate and recovers",
			nemesis:            "dcs-kill-one",
			fixture:            "kill_one_valid.jsonl",
			wantValid:          true,
			wantBefore:         1,
			wantDuringExpected: 1,
			wantAfterRecovered: 1,
		},
		{
			name:               "DCS majority loss records unavailable majority and recovery",
			nemesis:            "dcs-lose-majority",
			fixture:            "lose_majority_valid.jsonl",
			wantValid:          true,
			wantBefore:         1,
			wantDuringExpected: 1,
			wantAfterRecovered: 1,
		},
		{
			name:               "slow DCS requires measured endpoint latency",
			nemesis:            "dcs-slow-network",
			fixture:            "slow_network_valid.jsonl",
			minLatencyMillis:   100,
			wantValid:          true,
			wantBefore:         1,
			wantDuringExpected: 1,
			wantAfterRecovered: 1,
		},
		{
			name:               "primary partitioned from DCS majority keeps majority targets running",
			nemesis:            "primary-dcs-majority-partition",
			fixture:            "primary_majority_partition_valid.jsonl",
			wantValid:          true,
			wantBefore:         1,
			wantDuringExpected: 1,
			wantAfterRecovered: 1,
		},
		{
			name:               "full DCS restart observes complete outage and recovery",
			nemesis:            "dcs-full-restart",
			fixture:            "full_restart_valid.jsonl",
			wantValid:          true,
			wantBefore:         1,
			wantDuringExpected: 1,
			wantAfterRecovered: 1,
		},
		{
			name:               "missing recovery sample fails",
			nemesis:            "dcs-kill-one",
			fixture:            "missing_recovery_invalid.jsonl",
			wantValid:          false,
			wantBefore:         1,
			wantDuringExpected: 1,
			wantAfterRecovered: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			samplePath := filepath.Join(caseDir, dcsQuorumSampleFile)
			outputPath := filepath.Join(caseDir, dcsQuorumCheckerFile)
			fixturePath := filepath.Join("testdata", "dcs_quorum", test.fixture)
			writeTestFile(t, samplePath, readTestFile(t, fixturePath))

			minLatencyMillis := test.minLatencyMillis
			if minLatencyMillis == 0 {
				minLatencyMillis = 100
			}
			gotValid, err := runDCSQuorumChecker(dcsQuorumCheckerOptions{
				nemesis:              test.nemesis,
				caseDir:              caseDir,
				minSlowLatencyMillis: minLatencyMillis,
			})
			if err != nil {
				t.Fatalf("run DCS quorum checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result dcsQuorumCheckerResult
			readJSONTestFile(t, outputPath, &result)
			if result.Checker != dcsQuorumCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, dcsQuorumCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if !result.Applicable {
				t.Fatalf("applicable: got false want true")
			}
			if result.Nemesis != test.nemesis {
				t.Fatalf("nemesis: got %q want %q", result.Nemesis, test.nemesis)
			}
			if result.Samples != 3 {
				t.Fatalf("samples: got %d want 3", result.Samples)
			}
			if result.BeforeSamples != test.wantBefore {
				t.Fatalf("before samples: got %d want %d", result.BeforeSamples, test.wantBefore)
			}
			if result.DuringExpectedSamples != test.wantDuringExpected {
				t.Fatalf("during expected samples: got %d want %d", result.DuringExpectedSamples, test.wantDuringExpected)
			}
			if result.AfterRecoveredSamples != test.wantAfterRecovered {
				t.Fatalf("after recovered samples: got %d want %d", result.AfterRecoveredSamples, test.wantAfterRecovered)
			}
			if len(result.Observations) != result.Samples {
				t.Fatalf("observations: got %d want %d", len(result.Observations), result.Samples)
			}
		})
	}
}

func TestDCSQuorumCheckerNotApplicable(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runDCSQuorumChecker(dcsQuorumCheckerOptions{
		nemesis:              "packet",
		caseDir:              caseDir,
		minSlowLatencyMillis: 100,
	})
	if err != nil {
		t.Fatalf("run DCS quorum checker: %v", err)
	}
	if !gotValid {
		t.Fatalf("valid: got false want true")
	}

	var result dcsQuorumCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, dcsQuorumCheckerFile), &result)
	if !result.Valid || result.Applicable {
		t.Fatalf("result: got valid=%v applicable=%v want valid=true applicable=false", result.Valid, result.Applicable)
	}
	if result.Samples != 0 {
		t.Fatalf("samples: got %d want 0", result.Samples)
	}
}

func TestDCSQuorumCheckerMissingSamples(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runDCSQuorumChecker(dcsQuorumCheckerOptions{
		nemesis:              "dcs-kill-one",
		caseDir:              caseDir,
		minSlowLatencyMillis: 100,
	})
	if err != nil {
		t.Fatalf("run DCS quorum checker: %v", err)
	}
	if gotValid {
		t.Fatalf("valid: got true want false")
	}

	var result dcsQuorumCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, dcsQuorumCheckerFile), &result)
	if result.Valid || !result.Applicable {
		t.Fatalf("result: got valid=%v applicable=%v want valid=false applicable=true", result.Valid, result.Applicable)
	}
	if result.Error != "missing DCS quorum probe samples" {
		t.Fatalf("error: got %q", result.Error)
	}
}

func TestDCSQuorumCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, dcsQuorumSampleFile), readTestFile(t, filepath.Join("testdata", "dcs_quorum", "missing_recovery_invalid.jsonl")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "dcs-quorum",
		"--nemesis", "dcs-kill-one",
		"--case-dir", caseDir,
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "DCS quorum checker failed") {
		t.Fatalf("stderr: got %q want DCS quorum failure", stderr.String())
	}
}

func readJSONTestFile(t *testing.T, path string, target any) {
	t.Helper()

	if err := json.Unmarshal([]byte(readTestFile(t, path)), target); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}
