package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestOldPrimaryRejoinCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		fixture              string
		nemesis              string
		wantValid            bool
		wantApplicable       bool
		wantPromotion        bool
		wantRejoined         bool
		wantSafeOrRejoined   bool
		wantOldPrimaryMember string
	}{
		{
			name:               "accepts no promotion",
			fixture:            "no_promotion_valid.jsonl",
			nemesis:            "packet",
			wantValid:          true,
			wantRejoined:       true,
			wantSafeOrRejoined: true,
		},
		{
			name:                 "accepts old primary rejoined as replica",
			fixture:              "promotion_rejoined_valid.jsonl",
			nemesis:              "packet",
			wantValid:            true,
			wantApplicable:       true,
			wantPromotion:        true,
			wantRejoined:         true,
			wantSafeOrRejoined:   true,
			wantOldPrimaryMember: "alpha-1",
		},
		{
			name:                 "accepts killed old primary unavailable",
			fixture:              "kill_unreachable_valid.jsonl",
			nemesis:              "kill",
			wantValid:            true,
			wantApplicable:       true,
			wantPromotion:        true,
			wantRejoined:         false,
			wantSafeOrRejoined:   true,
			wantOldPrimaryMember: "alpha-1",
		},
		{
			name:                 "rejects old primary not rejoined",
			fixture:              "old_primary_not_rejoined_invalid.jsonl",
			nemesis:              "packet",
			wantValid:            false,
			wantApplicable:       true,
			wantPromotion:        true,
			wantRejoined:         false,
			wantSafeOrRejoined:   false,
			wantOldPrimaryMember: "alpha-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "old_primary_rejoin", test.fixture)))

			gotValid, err := runOldPrimaryRejoinChecker(oldPrimaryRejoinCheckerOptions{
				caseDir: caseDir,
				nemesis: test.nemesis,
			})
			if err != nil {
				t.Fatalf("run old primary rejoin checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result oldPrimaryRejoinCheckerResult
			readJSONTestFile(t, filepath.Join(caseDir, oldPrimaryRejoinCheckerFile), &result)
			if result.Checker != oldPrimaryRejoinCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, oldPrimaryRejoinCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if result.Applicable != test.wantApplicable {
				t.Fatalf("applicable: got %v want %v", result.Applicable, test.wantApplicable)
			}
			if result.Observations != 6 || result.Samples != 2 {
				t.Fatalf("counts: got observations=%d samples=%d want observations=6 samples=2", result.Observations, result.Samples)
			}
			if result.PromotionObserved != test.wantPromotion {
				t.Fatalf("promotion observed: got %v want %v", result.PromotionObserved, test.wantPromotion)
			}
			if result.OldPrimaryRejoined != test.wantRejoined {
				t.Fatalf("old primary rejoined: got %v want %v", result.OldPrimaryRejoined, test.wantRejoined)
			}
			if result.OldPrimarySafeOrRejoined != test.wantSafeOrRejoined {
				t.Fatalf("old primary safe or rejoined: got %v want %v", result.OldPrimarySafeOrRejoined, test.wantSafeOrRejoined)
			}
			if test.wantOldPrimaryMember == "" {
				if result.OldPrimaryFinalState != nil {
					t.Fatalf("old primary final state: got %+v want nil", *result.OldPrimaryFinalState)
				}
			} else if result.OldPrimaryFinalState == nil || result.OldPrimaryFinalState.Member != test.wantOldPrimaryMember {
				t.Fatalf("old primary final state: got %+v want member %q", result.OldPrimaryFinalState, test.wantOldPrimaryMember)
			}
		})
	}
}

func TestOldPrimaryRejoinCheckerSkipsSwitchover(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runOldPrimaryRejoinChecker(oldPrimaryRejoinCheckerOptions{caseDir: caseDir, nemesis: "switchover"})
	if err != nil {
		t.Fatalf("run old primary rejoin checker: %v", err)
	}
	if !gotValid {
		t.Fatalf("valid: got false want true")
	}

	var result oldPrimaryRejoinCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, oldPrimaryRejoinCheckerFile), &result)
	if !result.Valid || result.Applicable {
		t.Fatalf("switchover skip result: valid=%v applicable=%v want valid=true applicable=false", result.Valid, result.Applicable)
	}
	if result.Reason != "manual switchover is covered by the manual switchover checker" {
		t.Fatalf("reason: got %q", result.Reason)
	}
}

func TestOldPrimaryRejoinCheckerMissingSamples(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runOldPrimaryRejoinChecker(oldPrimaryRejoinCheckerOptions{caseDir: caseDir, nemesis: "packet"})
	if err != nil {
		t.Fatalf("run old primary rejoin checker: %v", err)
	}
	if gotValid {
		t.Fatalf("valid: got true want false")
	}

	var result oldPrimaryRejoinCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, oldPrimaryRejoinCheckerFile), &result)
	if result.Valid || result.Applicable {
		t.Fatalf("missing samples result: valid=%v applicable=%v want false false", result.Valid, result.Applicable)
	}
	if result.Error != "missing primary observations" {
		t.Fatalf("error: got %q want missing primary observations", result.Error)
	}
}

func TestOldPrimaryRejoinCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "old_primary_rejoin", "old_primary_not_rejoined_invalid.jsonl")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "old-primary-rejoin",
		"--case-dir", caseDir,
		"--nemesis", "packet",
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "old primary rejoin checker failed") {
		t.Fatalf("stderr: got %q want old primary rejoin failure", stderr.String())
	}
}
