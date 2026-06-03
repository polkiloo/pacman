package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollectArtifactIndex(t *testing.T) {
	t.Parallel()

	store := t.TempDir()
	writeTestFile(t, filepath.Join(store, "index.html"), "<html></html>\n")
	writeTestFile(t, filepath.Join(store, "failure-diagnostics.json"), "{}\n")
	writeTestFile(t, filepath.Join(store, "cases", "append__none", "checker.json"), "{}\n")
	writeTestFile(t, filepath.Join(store, "cases", "append__none", "primary-observations.jsonl"), "{}\n")
	writeTestFile(t, filepath.Join(store, "cases", "append__none", "debug.tmp"), "ignored\n")

	paths, err := collectArtifactIndex(store)
	if err != nil {
		t.Fatalf("collect artifact index: %v", err)
	}

	want := []string{
		filepath.Join(store, "cases", "append__none", "checker.json"),
		filepath.Join(store, "cases", "append__none", "primary-observations.jsonl"),
		filepath.Join(store, "failure-diagnostics.json"),
		filepath.Join(store, "index.html"),
	}
	assertStringSlicesEqual(t, paths, want)
}

func TestWriteArtifactSummary(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	store := filepath.Join(tempDir, "jepsen", "store", "pacman", "nightly", "run")
	summaryPath := filepath.Join(tempDir, "ci", "summary.md")
	indexPath := filepath.Join(tempDir, "ci", "artifact-index.txt")
	caseDir := filepath.Join(store, "cases", "append-failover__kill")

	writeTestFile(t, filepath.Join(store, "index.html"), "<html></html>\n")
	writeTestFile(t, filepath.Join(store, "nightly-failures.txt"), "append-failover:kill case failed\n")
	writeTestFile(t, filepath.Join(store, "case-results.jsonl"), `{"workload":"append-failover","nemesis":"kill","valid":false,"details":"timeline_checker_status=1"}`+"\n")
	writeTestFile(t, filepath.Join(caseDir, "timeline-checker.json"), `{"checker":"timeline-convergence","valid":false,"error":"timeline mismatch"}`+"\n")
	writeTestFile(t, filepath.Join(caseDir, "single-primary-checker.json"), `{"checker":"single-writable-primary","valid":true}`+"\n")

	err := writeArtifactSummary(artifactSummaryOptions{
		campaign:          "nightly",
		status:            1,
		harness:           filepath.Join(tempDir, "jepsen"),
		store:             store,
		runner:            filepath.Join(tempDir, "jepsen", "bin", "ci-nightly"),
		commit:            "abc123",
		githubRunID:       "42",
		repoRoot:          tempDir,
		summaryPath:       summaryPath,
		artifactIndexPath: indexPath,
	})
	if err != nil {
		t.Fatalf("write artifact summary: %v", err)
	}

	summary := readTestFile(t, summaryPath)
	assertContainsAll(t, "summary", summary, []string{
		"# Jepsen nightly failed",
		"- Campaign: `nightly`",
		"- Status: `failed`",
		"- Commit: `abc123`",
		"- GitHub run: `42`",
		"## Failure Summary",
		"- append-failover:kill case failed",
		"- append-failover:kill failed: timeline_checker_status=1",
		"- timeline-convergence checker failed: timeline mismatch",
		"- `jepsen/store/pacman/nightly/run/index.html`",
	})

	index := readTestFile(t, indexPath)
	assertContainsAll(t, "artifact index", index, []string{
		filepath.Join(store, "index.html"),
		filepath.Join(store, "case-results.jsonl"),
		filepath.Join(caseDir, "timeline-checker.json"),
	})
}

func TestWriteArtifactSummaryHandlesMissingStore(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	summaryPath := filepath.Join(tempDir, "ci", "summary.md")
	indexPath := filepath.Join(tempDir, "ci", "artifact-index.txt")

	err := writeArtifactSummary(artifactSummaryOptions{
		campaign:          "smoke",
		status:            0,
		statusLabel:       "skipped",
		harness:           filepath.Join(tempDir, "jepsen"),
		store:             filepath.Join(tempDir, "missing-store"),
		runner:            filepath.Join(tempDir, "jepsen", "bin", "ci-smoke"),
		commit:            "abc123",
		repoRoot:          tempDir,
		summaryPath:       summaryPath,
		artifactIndexPath: indexPath,
		summaryNote:       "Skipped because the Jepsen harness directory is not present yet.",
	})
	if err != nil {
		t.Fatalf("write artifact summary: %v", err)
	}

	summary := readTestFile(t, summaryPath)
	assertContainsAll(t, "summary", summary, []string{
		"# Jepsen smoke skipped",
		"Skipped because the Jepsen harness directory is not present yet.",
		"- Jepsen store path was not created.",
	})

	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read artifact index: %v", err)
	}
	if len(indexBytes) != 0 {
		t.Fatalf("artifact index: got %q want empty", string(indexBytes))
	}
}

func TestArtifactsSummarizeCommand(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	store := filepath.Join(tempDir, "store")
	summaryPath := filepath.Join(tempDir, "summary.md")
	indexPath := filepath.Join(tempDir, "artifact-index.txt")
	writeTestFile(t, filepath.Join(store, "results.edn"), "{:valid? true}\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"artifacts", "summarize",
		"--campaign", "case",
		"--case", "append-smoke-none",
		"--status", "0",
		"--harness", filepath.Join(tempDir, "jepsen"),
		"--store", store,
		"--runner", filepath.Join(tempDir, "jepsen", "bin", "ci-case"),
		"--commit", "abc123",
		"--repo-root", tempDir,
		"--summary-path", summaryPath,
		"--artifact-index-path", indexPath,
	}, &stdout, &stderr)

	if status != 0 {
		t.Fatalf("status: got %d want 0; stderr:\n%s", status, stderr.String())
	}

	summary := readTestFile(t, summaryPath)
	assertContainsAll(t, "summary", summary, []string{
		"# Jepsen case passed",
		"- Case: `append-smoke-none`",
	})
}

func TestCompareBaselineResultsMatchesExactProfiles(t *testing.T) {
	t.Parallel()

	comparisons := compareBaselineResults(
		[]caseResult{
			{Workload: "single-key-register", Nemesis: "packet", Valid: true},
			{Workload: "append-failover", Nemesis: "kill", Valid: true},
			{Workload: "append-failover", Nemesis: "kill", Valid: false},
			{Workload: "append-failover", Nemesis: "packet", Valid: false},
		},
		[]caseResult{
			{Workload: "append-failover", Nemesis: "kill", Valid: true},
			{Workload: "append-failover", Nemesis: "packet,kill", Valid: true},
			{Workload: "single-key-register", Nemesis: "packet", Valid: true},
		},
	)

	if len(comparisons) != 3 {
		t.Fatalf("comparisons: got %d want 3", len(comparisons))
	}
	if comparisons[0].Profile != "append-failover:kill" || comparisons[0].PACMAN != (profileResults{Runs: 2, Passed: 1}) || comparisons[0].Patroni == nil || *comparisons[0].Patroni != (profileResults{Runs: 1, Passed: 1}) {
		t.Fatalf("matching kill profile: %#v", comparisons[0])
	}
	if comparisons[1].Profile != "append-failover:packet" || comparisons[1].Patroni != nil {
		t.Fatalf("unmatched packet profile: %#v", comparisons[1])
	}
	if comparisons[2].Profile != "single-key-register:packet" || comparisons[2].Patroni == nil {
		t.Fatalf("matching register profile: %#v", comparisons[2])
	}
}

func TestArtifactsCompareBaselineCommand(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	pacmanPath := filepath.Join(tempDir, "pacman-case-results.jsonl")
	patroniPath := filepath.Join(tempDir, "patroni-case-results.jsonl")
	writeTestFile(t, pacmanPath, strings.Join([]string{
		`{"workload":"single-key-register","nemesis":"packet","valid":true,"details":"checkers passed"}`,
		`{"workload":"append-failover","nemesis":"packet","valid":false,"details":"timeline mismatch"}`,
	}, "\n")+"\n")
	writeTestFile(t, patroniPath, strings.Join([]string{
		`{"workload":"single-key-register","nemesis":"packet","valid":true,"details":"checkers passed"}`,
		`{"workload":"append-failover","nemesis":"packet,kill","valid":true,"details":"checkers passed"}`,
	}, "\n")+"\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"artifacts", "compare-baseline",
		"--pacman-results", pacmanPath,
		"--patroni-results", patroniPath,
	}, &stdout, &stderr)

	if status != 0 {
		t.Fatalf("status: got %d want 0; stderr:\n%s", status, stderr.String())
	}
	assertContainsAll(t, "stdout", stdout.String(), []string{
		"profile\tpacman\tpatroni\tcomparison",
		"append-failover:packet\tfailed (0/1 runs passed)\tmissing\tno-matching-profile",
		"single-key-register:packet\tpassed (1/1 runs passed)\tpassed (1/1 runs passed)\tmatching-profile",
		"compared 1 matching profile(s); 1 PACMAN profile(s) had no matching Patroni baseline",
	})
	if strings.Contains(stdout.String(), "append-failover:packet,kill") {
		t.Fatalf("stdout included unmatched Patroni-only profile:\n%s", stdout.String())
	}
}

func TestReadCaseResultsRejectsInvalidRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "empty",
			wantErr: "did not contain any results",
		},
		{
			name:    "invalid JSON",
			content: "{\n",
			wantErr: "parse case results",
		},
		{
			name:    "missing profile",
			content: `{"workload":"append-smoke","valid":true}` + "\n",
			wantErr: "workload and nemesis are required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "case-results.jsonl")
			writeTestFile(t, path, test.content)

			_, err := readCaseResults(path)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err: got %v want fragment %q", err, test.wantErr)
			}
		})
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.ReplaceAll(string(data), "\r\n", "\n")
}
