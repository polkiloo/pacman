package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcknowledgedWriteCheckerFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		ackFixture       string
		countsFixture    string
		asyncLossAllowed bool
		wantValid        bool
		wantExpected     int
		wantObservedOnce int
		wantMissing      []string
		wantDuplicate    []string
		wantUnexpected   []string
	}{
		{
			name:             "accepts acknowledged writes observed exactly once",
			ackFixture:       "valid_acknowledged.txt",
			countsFixture:    "valid_counts.tsv",
			wantValid:        true,
			wantExpected:     3,
			wantObservedOnce: 3,
			wantUnexpected:   []string{"op-extra"},
		},
		{
			name:             "rejects missing acknowledged write",
			ackFixture:       "missing_acknowledged.txt",
			countsFixture:    "missing_counts.tsv",
			wantExpected:     3,
			wantObservedOnce: 2,
			wantMissing:      []string{"op-b"},
		},
		{
			name:             "allows missing acknowledged write in async loss profile",
			ackFixture:       "missing_acknowledged.txt",
			countsFixture:    "missing_counts.tsv",
			asyncLossAllowed: true,
			wantValid:        true,
			wantExpected:     3,
			wantObservedOnce: 2,
			wantMissing:      []string{"op-b"},
		},
		{
			name:             "rejects duplicated acknowledged write",
			ackFixture:       "duplicate_acknowledged.txt",
			countsFixture:    "duplicate_counts.tsv",
			wantExpected:     2,
			wantObservedOnce: 1,
			wantDuplicate:    []string{"op-a"},
		},
		{
			name:          "rejects empty acknowledged set",
			ackFixture:    "empty_acknowledged.txt",
			countsFixture: "valid_counts.tsv",
			wantUnexpected: []string{
				"op-a",
				"op-b",
				"op-c",
				"op-extra",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			caseDir := t.TempDir()
			writeTestFile(t, filepath.Join(caseDir, acknowledgedOpIDsFile), readTestFile(t, filepath.Join("testdata", "acknowledged_write", test.ackFixture)))
			writeTestFile(t, filepath.Join(caseDir, finalPrimaryOpCountsFile), readTestFile(t, filepath.Join("testdata", "acknowledged_write", test.countsFixture)))

			gotValid, err := runAcknowledgedWriteChecker(acknowledgedWriteCheckerOptions{
				workload:            "append-failover",
				runID:               "run-1",
				caseDir:             caseDir,
				table:               "jepsen.append_values",
				finalPrimary:        "alpha-2",
				finalPrimaryService: "pacman-replica",
				asyncLossAllowed:    test.asyncLossAllowed,
			})
			if err != nil {
				t.Fatalf("run acknowledged write checker: %v", err)
			}
			if gotValid != test.wantValid {
				t.Fatalf("valid: got %v want %v", gotValid, test.wantValid)
			}

			var result acknowledgedWriteCheckerResult
			readJSONTestFile(t, filepath.Join(caseDir, acknowledgedWriteCheckerFile), &result)
			if result.Checker != acknowledgedWriteCheckerName {
				t.Fatalf("checker: got %q want %q", result.Checker, acknowledgedWriteCheckerName)
			}
			if result.Valid != test.wantValid {
				t.Fatalf("result valid: got %v want %v", result.Valid, test.wantValid)
			}
			if result.ExpectedAcknowledged != test.wantExpected {
				t.Fatalf("expected acknowledged: got %d want %d", result.ExpectedAcknowledged, test.wantExpected)
			}
			if result.ObservedExactlyOnce != test.wantObservedOnce {
				t.Fatalf("observed exactly once: got %d want %d", result.ObservedExactlyOnce, test.wantObservedOnce)
			}
			assertStringSlicesEqual(t, result.MissingOpIDs, test.wantMissing)
			assertStringSlicesEqual(t, result.DuplicateOpIDs, test.wantDuplicate)
			assertStringSlicesEqual(t, result.UnacknowledgedObservedOpIDs, test.wantUnexpected)

			assertFileLines(t, filepath.Join(caseDir, "missing-acknowledged-op-ids.txt"), test.wantMissing)
			assertFileLines(t, filepath.Join(caseDir, "duplicate-acknowledged-op-ids.txt"), test.wantDuplicate)
			assertFileLines(t, filepath.Join(caseDir, "unacknowledged-observed-op-ids.txt"), test.wantUnexpected)
		})
	}
}

func TestAcknowledgedWriteCheckerCommandReportsFailure(t *testing.T) {
	t.Parallel()

	caseDir := t.TempDir()
	writeTestFile(t, filepath.Join(caseDir, acknowledgedOpIDsFile), readTestFile(t, filepath.Join("testdata", "acknowledged_write", "missing_acknowledged.txt")))
	writeTestFile(t, filepath.Join(caseDir, finalPrimaryOpCountsFile), readTestFile(t, filepath.Join("testdata", "acknowledged_write", "missing_counts.tsv")))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"checkers", "acknowledged-write",
		"--workload", "append-failover",
		"--run-id", "run-1",
		"--case-dir", caseDir,
		"--table", "jepsen.append_values",
		"--final-primary", "alpha-2",
		"--final-primary-service", "pacman-replica",
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "acknowledged write preservation checker failed") {
		t.Fatalf("stderr: got %q want acknowledged write failure", stderr.String())
	}
}

func assertFileLines(t *testing.T, path string, want []string) {
	t.Helper()

	content := strings.TrimSuffix(readTestFile(t, path), "\n")
	var got []string
	if content != "" {
		got = strings.Split(content, "\n")
	}
	assertStringSlicesEqual(t, got, want)
}
