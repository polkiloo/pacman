package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestVIPRoutingCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                      string
		fixture                   string
		nemesis                   string
		wantValid                 bool
		wantSuccessfulWrites      int
		wantFailedWrites          int
		wantMatchedPrimaryMembers []string
		wantReplicaViolations     int
		wantMismatchViolations    int
	}{
		{
			name:                      "accepts switchover writes routed through both primaries",
			fixture:                   "switchover_valid.jsonl",
			nemesis:                   "switchover",
			wantValid:                 true,
			wantSuccessfulWrites:      2,
			wantMatchedPrimaryMembers: []string{"alpha-1", "alpha-2"},
		},
		{
			name:                      "accepts non switchover with one matched primary",
			fixture:                   "single_primary_valid.jsonl",
			nemesis:                   "packet",
			wantValid:                 true,
			wantSuccessfulWrites:      1,
			wantFailedWrites:          1,
			wantMatchedPrimaryMembers: []string{"alpha-1"},
		},
		{
			name:                      "rejects writes routed to replica",
			fixture:                   "replica_violation_invalid.jsonl",
			nemesis:                   "packet",
			wantSuccessfulWrites:      2,
			wantMatchedPrimaryMembers: []string{"alpha-1"},
			wantReplicaViolations:     1,
		},
		{
			name:                   "rejects stable VIP holder mismatch",
			fixture:                "stable_mismatch_invalid.jsonl",
			nemesis:                "packet",
			wantSuccessfulWrites:   1,
			wantMismatchViolations: 1,
		},
		{
			name:                      "rejects switchover without two primary matches",
			fixture:                   "single_primary_valid.jsonl",
			nemesis:                   "switchover",
			wantSuccessfulWrites:      1,
			wantFailedWrites:          1,
			wantMatchedPrimaryMembers: []string{"alpha-1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			writeTestFile(t, filepath.Join(caseDir, vipRoutingSamplesFile), readTestFile(t, filepath.Join("testdata", "vip_routing", test.fixture)))

			gotValid, err := runVIPRoutingChecker(vipRoutingCheckerOptions{
				workload: "vip-routing",
				nemesis:  test.nemesis,
				caseDir:  caseDir,
			})
			if err != nil {
				t.Fatalf("run VIP routing checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result vipRoutingCheckerResult
			readJSONTestFile(t, filepath.Join(caseDir, vipRoutingCheckerFile), &result)
			if result.Checker != vipRoutingCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, vipRoutingCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if !result.Applicable {
				t.Fatalf("applicable: got false want true")
			}
			if result.SuccessfulWrites != test.wantSuccessfulWrites {
				t.Fatalf("successful writes: got %d want %d", result.SuccessfulWrites, test.wantSuccessfulWrites)
			}
			if result.FailedWrites != test.wantFailedWrites {
				t.Fatalf("failed writes: got %d want %d", result.FailedWrites, test.wantFailedWrites)
			}
			assertStringSlicesEqual(t, result.MatchedPrimaryMembers, test.wantMatchedPrimaryMembers)
			if len(result.RoutedToReplicaViolations) != test.wantReplicaViolations {
				t.Fatalf("replica violations: got %d want %d", len(result.RoutedToReplicaViolations), test.wantReplicaViolations)
			}
			if len(result.StablePrimaryMismatchViolations) != test.wantMismatchViolations {
				t.Fatalf("mismatch violations: got %d want %d", len(result.StablePrimaryMismatchViolations), test.wantMismatchViolations)
			}
		})
	}
}

func TestVIPRoutingCheckerSkipsOtherWorkloads(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runVIPRoutingChecker(vipRoutingCheckerOptions{workload: "append-failover", caseDir: caseDir})
	if err != nil {
		t.Fatalf("run VIP routing checker: %v", err)
	}
	if !gotValid {
		t.Fatalf("valid: got false want true")
	}

	var result vipRoutingCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, vipRoutingCheckerFile), &result)
	if !result.Valid || result.Applicable {
		t.Fatalf("skip result: valid=%v applicable=%v want valid=true applicable=false", result.Valid, result.Applicable)
	}
}

func TestVIPRoutingCheckerMissingSamples(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runVIPRoutingChecker(vipRoutingCheckerOptions{workload: "vip-routing", caseDir: caseDir})
	if err != nil {
		t.Fatalf("run VIP routing checker: %v", err)
	}
	if gotValid {
		t.Fatalf("valid: got true want false")
	}

	var result vipRoutingCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, vipRoutingCheckerFile), &result)
	if result.Valid || !result.Applicable {
		t.Fatalf("missing result: valid=%v applicable=%v want false true", result.Valid, result.Applicable)
	}
	if result.Error != "missing VIP routing samples" {
		t.Fatalf("error: got %q want missing VIP routing samples", result.Error)
	}
}

func TestVIPRoutingCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, vipRoutingSamplesFile), readTestFile(t, filepath.Join("testdata", "vip_routing", "replica_violation_invalid.jsonl")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "vip-routing",
		"--workload", "vip-routing",
		"--case-dir", caseDir,
		"--nemesis", "packet",
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "VIP routing checker failed") {
		t.Fatalf("stderr: got %q want VIP routing failure", stderr.String())
	}
}
