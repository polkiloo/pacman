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

func readTestFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.ReplaceAll(string(data), "\r\n", "\n")
}
