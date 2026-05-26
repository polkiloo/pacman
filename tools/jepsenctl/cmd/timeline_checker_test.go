package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestTimelineCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		fixture                 string
		wantValid               bool
		wantPromotionObserved   bool
		wantTimelineAdvanced    bool
		wantReplicasConverged   bool
		wantOldPrimarySafe      bool
		wantReplicaViolations   int
		wantOldPrimaryFinalName string
	}{
		{
			name:                  "accepts stable primary without promotion",
			fixture:               "no_promotion_valid.jsonl",
			wantValid:             true,
			wantTimelineAdvanced:  true,
			wantReplicasConverged: true,
			wantOldPrimarySafe:    true,
		},
		{
			name:                    "accepts promotion with converged replicas",
			fixture:                 "promotion_valid.jsonl",
			wantValid:               true,
			wantPromotionObserved:   true,
			wantTimelineAdvanced:    true,
			wantReplicasConverged:   true,
			wantOldPrimarySafe:      true,
			wantOldPrimaryFinalName: "alpha-1",
		},
		{
			name:                    "rejects replica timeline mismatch",
			fixture:                 "replica_timeline_invalid.jsonl",
			wantPromotionObserved:   true,
			wantTimelineAdvanced:    true,
			wantOldPrimarySafe:      true,
			wantReplicaViolations:   1,
			wantOldPrimaryFinalName: "alpha-1",
		},
		{
			name:                    "rejects old primary on divergent timeline",
			fixture:                 "old_primary_unsafe_invalid.jsonl",
			wantPromotionObserved:   true,
			wantTimelineAdvanced:    true,
			wantReplicasConverged:   false,
			wantReplicaViolations:   1,
			wantOldPrimaryFinalName: "alpha-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "timeline", test.fixture)))

			gotValid, err := runTimelineChecker(timelineCheckerOptions{caseDir: caseDir})
			if err != nil {
				t.Fatalf("run timeline checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result timelineCheckerResult
			readJSONTestFile(t, filepath.Join(caseDir, timelineCheckerFile), &result)
			if result.Checker != timelineCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, timelineCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if result.Observations != 6 || result.Samples != 2 {
				t.Fatalf("counts: got observations=%d samples=%d want observations=6 samples=2", result.Observations, result.Samples)
			}
			if result.PromotionObserved != test.wantPromotionObserved {
				t.Fatalf("promotion observed: got %v want %v", result.PromotionObserved, test.wantPromotionObserved)
			}
			if result.TimelineAdvanced != test.wantTimelineAdvanced {
				t.Fatalf("timeline advanced: got %v want %v", result.TimelineAdvanced, test.wantTimelineAdvanced)
			}
			if result.ReplicasConverged != test.wantReplicasConverged {
				t.Fatalf("replicas converged: got %v want %v", result.ReplicasConverged, test.wantReplicasConverged)
			}
			if result.OldPrimarySafe != test.wantOldPrimarySafe {
				t.Fatalf("old primary safe: got %v want %v", result.OldPrimarySafe, test.wantOldPrimarySafe)
			}
			if len(result.ReplicaTimelineViolations) != test.wantReplicaViolations {
				t.Fatalf("replica timeline violations: got %d want %d", len(result.ReplicaTimelineViolations), test.wantReplicaViolations)
			}
			if test.wantOldPrimaryFinalName == "" {
				if result.OldPrimaryFinalState != nil {
					t.Fatalf("old primary final state: got %+v want nil", *result.OldPrimaryFinalState)
				}
			} else if result.OldPrimaryFinalState == nil || result.OldPrimaryFinalState.Member != test.wantOldPrimaryFinalName {
				t.Fatalf("old primary final state: got %+v want member %q", result.OldPrimaryFinalState, test.wantOldPrimaryFinalName)
			}
		})
	}
}

func TestTimelineCheckerMissingSamples(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runTimelineChecker(timelineCheckerOptions{caseDir: caseDir})
	if err != nil {
		t.Fatalf("run timeline checker: %v", err)
	}
	if gotValid {
		t.Fatalf("valid: got true want false")
	}

	var result timelineCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, timelineCheckerFile), &result)
	if result.Valid {
		t.Fatalf("result valid: got true want false")
	}
	if result.Error != "missing primary observations" {
		t.Fatalf("error: got %q want missing primary observations", result.Error)
	}
}

func TestTimelineCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "timeline", "replica_timeline_invalid.jsonl")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "timeline",
		"--case-dir", caseDir,
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "timeline convergence checker failed") {
		t.Fatalf("stderr: got %q want timeline failure", stderr.String())
	}
}
