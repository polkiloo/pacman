package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestSinglePrimaryCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		fixture              string
		wantValid            bool
		wantObservations     int
		wantSamples          int
		wantWritable         int
		wantViolationSamples int
	}{
		{
			name:             "accepts one writable member per sample",
			fixture:          "valid.jsonl",
			wantValid:        true,
			wantObservations: 6,
			wantSamples:      2,
			wantWritable:     2,
		},
		{
			name:                 "rejects multiple writable members in one sample",
			fixture:              "split_brain_invalid.jsonl",
			wantValid:            false,
			wantObservations:     6,
			wantSamples:          2,
			wantWritable:         3,
			wantViolationSamples: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			samplePath := filepath.Join(caseDir, primaryObservationFile)
			outputPath := filepath.Join(caseDir, singlePrimaryCheckerFile)
			writeTestFile(t, samplePath, readTestFile(t, filepath.Join("testdata", "single_primary", test.fixture)))

			gotValid, err := runSinglePrimaryChecker(singlePrimaryCheckerOptions{caseDir: caseDir})
			if err != nil {
				t.Fatalf("run single primary checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result singlePrimaryCheckerResult
			readJSONTestFile(t, outputPath, &result)
			if result.Checker != singlePrimaryCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, singlePrimaryCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if result.Observations != test.wantObservations {
				t.Fatalf("observations: got %d want %d", result.Observations, test.wantObservations)
			}
			if result.Samples != test.wantSamples {
				t.Fatalf("samples: got %d want %d", result.Samples, test.wantSamples)
			}
			if result.WritableObservations != test.wantWritable {
				t.Fatalf("writable observations: got %d want %d", result.WritableObservations, test.wantWritable)
			}
			if len(result.ViolationSamples) != test.wantViolationSamples {
				t.Fatalf("violation samples: got %d want %d", len(result.ViolationSamples), test.wantViolationSamples)
			}
		})
	}
}

func TestSinglePrimaryCheckerMissingSamples(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	gotValid, err := runSinglePrimaryChecker(singlePrimaryCheckerOptions{caseDir: caseDir})
	if err != nil {
		t.Fatalf("run single primary checker: %v", err)
	}
	if gotValid {
		t.Fatalf("valid: got true want false")
	}

	var result singlePrimaryCheckerResult
	readJSONTestFile(t, filepath.Join(caseDir, singlePrimaryCheckerFile), &result)
	if result.Valid {
		t.Fatalf("result valid: got true want false")
	}
	if result.Observations != 0 || result.Samples != 0 || result.WritableObservations != 0 {
		t.Fatalf("empty result counts: got observations=%d samples=%d writable=%d", result.Observations, result.Samples, result.WritableObservations)
	}
	if len(result.ViolationSamples) != 0 {
		t.Fatalf("violation samples: got %d want 0", len(result.ViolationSamples))
	}
}

func TestSinglePrimaryCheckerConfirmsTimelineTransition(t *testing.T) {
	t.Parallel()

	observations := []primaryObservation{
		{SampleID: 14, Member: "alpha-1", Reachable: true, Writable: false, Timeline: 2},
		{SampleID: 14, Member: "alpha-2", Reachable: true, Writable: true, Timeline: 2},
		{SampleID: 14, Member: "alpha-3", Reachable: true, Writable: true, Timeline: 3},
		{SampleID: 14, ProbeRound: 1, Member: "alpha-1", Reachable: true, Writable: false, Timeline: 3},
		{SampleID: 14, ProbeRound: 1, Member: "alpha-2", Reachable: false, Writable: false},
		{SampleID: 14, ProbeRound: 1, Member: "alpha-3", Reachable: true, Writable: true, Timeline: 3},
	}

	result := checkSinglePrimaryObservations(observations)
	if !result.Valid {
		t.Fatalf("result valid: got false want true: %+v", result.ViolationSamples)
	}
	if result.ConfirmationSamples != 1 || len(result.TransitionSamples) != 1 {
		t.Fatalf("confirmation result: got samples=%d transitions=%d want 1 and 1", result.ConfirmationSamples, len(result.TransitionSamples))
	}
	if len(result.ViolationSamples) != 0 {
		t.Fatalf("violation samples: got %d want 0", len(result.ViolationSamples))
	}
}

func TestSinglePrimaryCheckerRejectsConfirmedSplitBrain(t *testing.T) {
	t.Parallel()

	observations := []primaryObservation{
		{SampleID: 7, Member: "alpha-1", Reachable: true, Writable: true, Timeline: 1},
		{SampleID: 7, Member: "alpha-2", Reachable: true, Writable: true, Timeline: 2},
		{SampleID: 7, ProbeRound: 1, Member: "alpha-1", Reachable: true, Writable: true, Timeline: 1},
		{SampleID: 7, ProbeRound: 1, Member: "alpha-2", Reachable: true, Writable: true, Timeline: 2},
	}

	result := checkSinglePrimaryObservations(observations)
	if result.Valid {
		t.Fatalf("result valid: got true want false")
	}
	if len(result.ViolationSamples) != 1 || result.ViolationSamples[0].ProbeRound != 1 {
		t.Fatalf("confirmed violations: got %+v want one round-1 violation", result.ViolationSamples)
	}
}

func TestSinglePrimaryCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, primaryObservationFile), readTestFile(t, filepath.Join("testdata", "single_primary", "split_brain_invalid.jsonl")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "single-primary",
		"--case-dir", caseDir,
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "single writable primary checker failed") {
		t.Fatalf("stderr: got %q want single primary failure", stderr.String())
	}
}
