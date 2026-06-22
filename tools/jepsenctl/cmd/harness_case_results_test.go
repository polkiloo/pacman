package cmd

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestCaseHistoryArtifactValidation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.edn")
	runID := "20260603T120000Z-append-smoke__none"
	writeCaseEvent(historyPath, ":case", "invoke", "workload", fmt.Sprintf("{:workload %q :nemesis %q :run-id %q}", "append-smoke", "none", runID))
	writeCaseEvent(historyPath, "0", "invoke", "append", fmt.Sprintf("{:op-id %q}", runID+"-append-1"))
	writeCaseEvent(historyPath, "0", "ok", "append", fmt.Sprintf("{:op-id %q}", runID+"-append-1"))

	if err := validateCaseHistoryArtifact(historyPath, "append-smoke", "none", runID); err != nil {
		t.Fatalf("validate case history: %v", err)
	}

	resultsPath := filepath.Join(dir, "case-results.jsonl")
	writeTestFile(t, filepath.Join(dir, singlePrimaryCheckerFile), `{"checker":"single-writable-primary","valid":true,"samples":3,"writableObservations":3,"violationSamples":[]}`+"\n")
	writeTestFile(t, filepath.Join(dir, acknowledgedWriteCheckerFile), `{"checker":"acknowledged-write-preservation","valid":true,"expectedAcknowledged":2,"observedExactlyOnce":2,"missingAcknowledged":0,"duplicateAcknowledged":0}`+"\n")
	writeTestFile(t, filepath.Join(dir, timelineCheckerFile), `{"checker":"timeline-convergence","valid":true,"promotionObserved":true,"timelineAdvanced":true,"replicasConverged":true,"oldPrimarySafe":true}`+"\n")
	writeTestFile(t, filepath.Join(dir, oldPrimaryRejoinCheckerFile), `{"checker":"old-primary-rejoin-after-failover","valid":true,"applicable":true,"promotionObserved":true,"oldPrimaryRejoined":true,"oldPrimarySafeOrRejoined":true,"oldPrimaryUnsafeAfterPromotion":false,"initialPrimary":{"member":"alpha-1"},"finalPrimary":{"member":"alpha-2"}}`+"\n")
	recordCaseResult(resultsPath, "append-smoke", "none", runID, historyPath, true, "checkers passed", collectCaseCheckerReports(dir))
	results, err := readCaseResults(resultsPath)
	if err != nil {
		t.Fatalf("read case results: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results: got %d want 1", len(results))
	}
	result := results[0]
	if result.RunID != runID || result.History != historyPath || result.HistoryFormat != "edn" || result.HistoryEvents != 3 {
		t.Fatalf("history metadata: %+v", result)
	}
	for _, key := range []string{"splitBrain", "acknowledgedWritePreservation", "timelineConvergence", "failoverRejoin"} {
		report, ok := result.CheckerReports[key]
		if !ok {
			t.Fatalf("missing checker report %s in %+v", key, result.CheckerReports)
		}
		if report.Valid == nil || !*report.Valid {
			t.Fatalf("checker report %s: %+v", key, report)
		}
		if report.Summary == "" {
			t.Fatalf("checker report %s missing summary: %+v", key, report)
		}
	}
	failoverReport := result.CheckerReports["failoverRejoin"]
	if failoverReport.Facts["oldPrimaryRejoined"] != true || !strings.Contains(failoverReport.Summary, "initialPrimary=alpha-1") || !strings.Contains(failoverReport.Summary, "finalPrimary=alpha-2") {
		t.Fatalf("failover/rejoin report: %+v", failoverReport)
	}
}

func TestCaseHistoryArtifactValidationRejectsMissingWorkloadEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.edn")
	runID := "20260603T120000Z-append-smoke__none"
	writeCaseEvent(historyPath, ":case", "invoke", "workload", fmt.Sprintf("{:workload %q :nemesis %q :run-id %q}", "append-smoke", "none", runID))

	err := validateCaseHistoryArtifact(historyPath, "append-smoke", "none", runID)
	if err == nil || !strings.Contains(err.Error(), "missing workload events") {
		t.Fatalf("err: got %v want missing workload events", err)
	}
}

func TestWriteFailureDiagnosticsCapturesFailedRunEvidence(t *testing.T) {
	t.Parallel()

	target, err := resolveJepsenTarget(defaultJepsenTarget)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	runDir := t.TempDir()
	caseDir := filepath.Join(runDir, "cases", "append-failover__kill")
	lab := newHarnessLab(harnessOptions{
		runOptions: runOptions{campaign: "case", target: target},
	})

	writeTestFile(t, filepath.Join(runDir, "results.edn"), "{:valid? false}\n")
	writeTestFile(t, filepath.Join(runDir, "jepsen-history.edn"), "{}\n")
	writeTestFile(t, filepath.Join(runDir, "nemesis-schedule.edn"), "{}\n")
	writeTestFile(t, filepath.Join(runDir, "case-results.jsonl"), `{"workload":"append-failover","nemesis":"kill","valid":false,"details":"timeline mismatch"}`+"\n")
	writeTestFile(t, filepath.Join(runDir, "docker-compose-ps.txt"), "containers\n")
	writeTestFile(t, filepath.Join(runDir, "docker-compose.log"), "compose logs\n")
	writeTestFile(t, filepath.Join(runDir, "pacman-cluster-after.json"), "{}\n")
	writeTestFile(t, filepath.Join(runDir, "pacman-history.json"), "[]\n")
	writeTestFile(t, filepath.Join(runDir, "node-logs", "alpha-1-pacmand.log"), "pacmand\n")
	writeTestFile(t, filepath.Join(runDir, "postgres-logs", "alpha-1-postgres.log"), "postgres\n")
	writeTestFile(t, filepath.Join(runDir, "dcs-logs", "alpha-dcs-etcd.log"), "etcd\n")
	writeTestFile(t, filepath.Join(caseDir, "history.edn"), "{}\n")
	writeTestFile(t, filepath.Join(caseDir, "nemesis.log"), "kill alpha-1\n")
	writeTestFile(t, filepath.Join(caseDir, "nemesis-schedule.edn"), "{}\n")
	writeTestFile(t, filepath.Join(caseDir, "primary-observations.jsonl"), "{}\n")
	writeTestFile(t, filepath.Join(caseDir, "pacman-cluster-snapshots.jsonl"), "{}\n")
	writeTestFile(t, filepath.Join(caseDir, "checker.json"), `{"checker":"append","valid":true}`+"\n")
	writeTestFile(t, filepath.Join(caseDir, "timeline-checker.json"), `{"checker":"timeline-convergence","valid":false,"error":"timeline mismatch"}`+"\n")

	lab.writeFailureDiagnostics(runDir)

	var diagnostics map[string]any
	readJSONTestFile(t, filepath.Join(runDir, "failure-diagnostics.json"), &diagnostics)
	if diagnostics["target"] != defaultJepsenTarget {
		t.Fatalf("target: got %v", diagnostics["target"])
	}
	failures, ok := diagnostics["failures"].([]any)
	if !ok || len(failures) < 2 {
		t.Fatalf("failures: %#v", diagnostics["failures"])
	}
	assertContainsAll(t, "failure diagnostics", fmt.Sprint(failures), []string{
		"append-failover:kill failed: timeline mismatch",
		"timeline-convergence checker failed: timeline mismatch",
	})
	logDirs := diagnostics["logDirectories"].(map[string]any)
	for _, name := range []string{"node-logs", "postgres-logs", "dcs-logs"} {
		dir := logDirs[name].(map[string]any)
		if dir["present"] != true || dir["fileCount"] == float64(0) {
			t.Fatalf("%s diagnostics: %#v", name, dir)
		}
	}
	cases := diagnostics["cases"].([]any)
	if len(cases) != 1 {
		t.Fatalf("cases: %#v", cases)
	}
	firstCase := cases[0].(map[string]any)
	if firstCase["name"] != "append-failover__kill" {
		t.Fatalf("case name: %#v", firstCase)
	}
	checkers := fmt.Sprint(firstCase["checkerArtifacts"])
	assertContainsAll(t, "checker artifacts", checkers, []string{"checker.json", "timeline-checker.json"})
}
