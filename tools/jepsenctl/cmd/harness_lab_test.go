package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestHarnessCommandParsing(t *testing.T) {
	t.Parallel()

	command, err := parseHarnessCommand(`run_jepsen_case append-failover packet "/tmp/run dir" history.edn schedule.edn results.jsonl`)
	if err != nil {
		t.Fatalf("parse harness command: %v", err)
	}
	if command.name != "run_jepsen_case" {
		t.Fatalf("name: got %q", command.name)
	}
	wantArgs := []string{"append-failover", "packet", "/tmp/run dir", "history.edn", "schedule.edn", "results.jsonl"}
	if !reflect.DeepEqual(command.args, wantArgs) {
		t.Fatalf("args: got %#v want %#v", command.args, wantArgs)
	}

	if _, err := parseHarnessCommand(`run_jepsen_case "unterminated`); err == nil {
		t.Fatalf("unterminated quote parsed without error")
	}
	if _, err := parseHarnessCommand("   "); err == nil {
		t.Fatalf("empty command parsed without error")
	}
}

func TestHarnessDispatchValidationAndResultsFile(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	target, err := resolveJepsenTarget(defaultJepsenTarget)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	lab := newHarnessLab(harnessOptions{runOptions: runOptions{campaign: "case", target: target}})

	status, err := lab.dispatch(context.Background(), "missing_command")
	if status != 1 || err == nil || !strings.Contains(err.Error(), "unsupported Go harness command") {
		t.Fatalf("unsupported dispatch: status=%d err=%v", status, err)
	}

	status, err = lab.dispatch(context.Background(), "run_jepsen_case too few args")
	if status != 1 || err == nil || !strings.Contains(err.Error(), "expects 6 args") {
		t.Fatalf("arity dispatch: status=%d err=%v", status, err)
	}

	status, err = lab.dispatch(context.Background(), "write_results_file "+shellLiteral(runDir)+" true")
	if status != 0 || err != nil {
		t.Fatalf("write results dispatch: status=%d err=%v", status, err)
	}
	data, err := os.ReadFile(filepath.Join(runDir, "results.edn"))
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	assertContainsAll(t, "results", string(data), []string{":valid? true", `:campaign "case"`, `:target "pacman-3-data"`, `:target-store "pacman"`})
}

func TestHarnessTopologyHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"service alpha-1", serviceForMember("alpha-1"), "pacman-primary"},
		{"service alpha-2", serviceForMember("alpha-2"), "pacman-replica"},
		{"service alpha-3", serviceForMember("alpha-3"), "pacman-replica-2"},
		{"member primary", memberForService("pacman-primary"), "alpha-1"},
		{"dcs service", dcsMemberForService("pacman-dcs-2"), "alpha-dcs-2"},
		{"service ip", serviceIP("pacman-replica-2"), "172.28.0.13"},
		{"sql literal", sqlLiteral("alpha's"), "'alpha''s'"},
	}
	for _, test := range tests {
		if test.got != test.want {
			t.Fatalf("%s: got %q want %q", test.name, test.got, test.want)
		}
	}

	if got := peerServicesForMember("alpha-1"); !reflect.DeepEqual(got, []string{"pacman-replica", "pacman-replica-2"}) {
		t.Fatalf("alpha-1 peers: got %#v", got)
	}
	if got := serviceForMember("unknown"); got != "" {
		t.Fatalf("unknown service: got %q want empty", got)
	}
}

func TestDCSQuorumTargetStateDerivesKilledTargetsFromEndpointHealth(t *testing.T) {
	t.Parallel()

	health := dcsHealthResult{
		TotalEndpoints:   3,
		HealthyEndpoints: 2,
		FailedEndpoints:  1,
		Endpoints: []dcsEndpointInfo{
			{Endpoint: "http://pacman-dcs:2379", OK: true},
			{Endpoint: "http://pacman-dcs-2:2379", OK: false},
			{Endpoint: "http://pacman-dcs-3:2379", OK: true},
		},
	}

	running, allRunning := dcsQuorumTargetState("dcs-kill-one", []string{"pacman-dcs-2"}, health)
	if running != 0 || allRunning {
		t.Fatalf("killed target state: running=%d allRunning=%v, want 0 false", running, allRunning)
	}

	running, allRunning = dcsQuorumTargetState("primary-dcs-majority-partition", []string{"pacman-dcs-2", "pacman-dcs-3"}, health)
	if running != 2 || !allRunning {
		t.Fatalf("partition target state: running=%d allRunning=%v, want 2 true", running, allRunning)
	}
}

func TestRecordDCSQuorumRecoveryProbeRetriesUntilHealthy(t *testing.T) {
	t.Parallel()

	runner := &scriptedRunner{
		statuses: []int{1, 0, 0, 0, 0, 0},
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
	})
	lab.cfg.dcsRecoveryTimeout = 50 * time.Millisecond
	lab.cfg.dcsRecoveryInterval = time.Millisecond

	caseDir := t.TempDir()
	lab.recordDCSQuorumRecoveryProbe(context.Background(), caseDir, "dcs-lose-majority", "after-restart", []string{"pacman-dcs-2", "pacman-dcs-3"}, "pacman-primary")

	if runner.calls != 6 {
		t.Fatalf("runner calls: got %d want 6", runner.calls)
	}
	rows := readJSONL(filepath.Join(caseDir, dcsQuorumSampleFile))
	if len(rows) != 2 {
		t.Fatalf("samples: got %d want 2", len(rows))
	}
	if rows[0]["healthyEndpoints"] != float64(2) || rows[1]["healthyEndpoints"] != float64(3) {
		t.Fatalf("healthy endpoint samples: %#v", rows)
	}
}

func TestIPTablesPartitionReportsCommandFailure(t *testing.T) {
	t.Parallel()

	runner := &scriptedRunner{
		outputs:  []string{"iptables command not found\n"},
		statuses: []int{127},
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
	})

	err := lab.iptablesPartition(context.Background(), "pacman-primary", []string{"pacman-dcs-2"})
	if err == nil {
		t.Fatalf("partition succeeded without iptables")
	}
	assertContainsAll(t, "partition error", err.Error(), []string{
		"iptables partition pacman-primary from pacman-dcs-2 failed with status 127",
		"iptables command not found",
	})
	if len(runner.specs) != 1 {
		t.Fatalf("runner calls: got %d want 1", len(runner.specs))
	}
	if got := strings.Join(runner.specs[0].args, " "); !strings.Contains(got, "command -v iptables") {
		t.Fatalf("partition command missing iptables discovery: %s", got)
	}
}

func TestIPTablesPartitionRejectsUnknownPeer(t *testing.T) {
	t.Parallel()

	runner := &scriptedRunner{}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
	})

	err := lab.iptablesPartition(context.Background(), "pacman-primary", []string{"unknown-peer"})
	if err == nil || !strings.Contains(err.Error(), "unknown peer service") {
		t.Fatalf("partition error: got %v want unknown peer service", err)
	}
	if len(runner.specs) != 0 {
		t.Fatalf("runner calls: got %d want 0", len(runner.specs))
	}
}

func TestHarnessFileAndJSONHelpers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	linesPath := filepath.Join(dir, "lines.txt")
	if err := os.WriteFile(linesPath, []byte("one\n\n two \n"), 0o644); err != nil {
		t.Fatalf("write lines: %v", err)
	}
	if got := countLines(linesPath); got != 2 {
		t.Fatalf("count lines: got %d want 2", got)
	}
	if got := lastNonEmptyLine("a\n\n b \n"); got != "b" {
		t.Fatalf("last line: got %q want b", got)
	}
	if got := lastJSONObject("noise\n{\"first\":true}\ntext\n{\"last\":true}\n"); got != `{"last":true}` {
		t.Fatalf("last json object: got %q", got)
	}
	clusterOutput := `{
  "clusterName": "alpha",
  "phase": "healthy",
  "currentPrimary": "alpha-1",
  "members": [
    {"name": "alpha-1", "role": "primary", "state": "running", "healthy": true},
    {"name": "alpha-2", "role": "replica", "state": "streaming", "healthy": true},
    {"name": "alpha-3", "role": "replica", "state": "streaming", "healthy": true}
  ]
}
{"time":"2026-05-27T20:32:48Z","msg":"completed pacmanctl command"}`
	clusterJSON := clusterStatusJSONObject(clusterOutput)
	var status clusterStatus
	if err := json.Unmarshal([]byte(clusterJSON), &status); err != nil {
		t.Fatalf("decode extracted cluster json: %v\n%s", err, clusterJSON)
	}
	if status.CurrentPrimary != "alpha-1" || len(status.Members) != 3 {
		t.Fatalf("extracted wrong json object: %#v", status)
	}
	if got := clusterStatusJSONObject(`{"time":"2026-05-27T20:32:48Z","msg":"completed pacmanctl command"}`); got != "" {
		t.Fatalf("log-only json should not be treated as cluster status: %s", got)
	}

	jsonlPath := filepath.Join(dir, "rows.jsonl")
	appendJSONL(jsonlPath, map[string]any{"ok": true})
	appendJSONL(jsonlPath, map[string]any{"ok": false})
	rows := readJSONL(jsonlPath)
	if len(rows) != 2 || rows[0]["ok"] != true || rows[1]["ok"] != false {
		t.Fatalf("jsonl rows: %#v", rows)
	}
	if got := countSamples(rows, func(row map[string]any) bool { return row["ok"] == true }); got != 1 {
		t.Fatalf("sample count: got %d want 1", got)
	}

	schedulePath := filepath.Join(dir, "campaign-schedule.edn")
	caseSchedulePath := filepath.Join(dir, "case-schedule.edn")
	writeTestFile(t, schedulePath, "old\n")
	offset := fileSize(schedulePath)
	appendFile(schedulePath, "new\n")
	if err := copyScheduleTail(schedulePath, caseSchedulePath, offset); err != nil {
		t.Fatalf("copy schedule tail: %v", err)
	}
	if got := mustRead(caseSchedulePath); got != "new\n" {
		t.Fatalf("case schedule: got %q want only new entry", got)
	}

	jsonPath := filepath.Join(dir, "value.json")
	writeJSON(jsonPath, map[string]any{"name": "alpha"})
	var decoded map[string]string
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if decoded["name"] != "alpha" {
		t.Fatalf("decoded json: %#v", decoded)
	}
}

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

func TestVerifyThreeDataNodeClusterWaitsForHealthyShape(t *testing.T) {
	dir := t.TempDir()
	runner := &scriptedRunner{outputs: []string{
		`{"phase":"initializing","currentPrimary":"","members":[]}`,
		validClusterStatusJSON(),
	}}
	lab := newHarnessLab(harnessOptions{
		repoRoot: dir,
		runner:   runner,
	})
	lab.cfg.clusterVerifyTimeout = 200 * time.Millisecond
	lab.cfg.clusterVerifyInterval = time.Millisecond

	outputFile := filepath.Join(dir, "pacman-cluster-before.json")
	if err := lab.verifyThreeDataNodeCluster(context.Background(), outputFile); err != nil {
		t.Fatalf("verify cluster: %v", err)
	}
	if runner.calls < 2 {
		t.Fatalf("runner calls: got %d want at least 2", runner.calls)
	}
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	var status clusterStatus
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatalf("decode output file: %v", err)
	}
	if status.CurrentPrimary != "alpha-1" {
		t.Fatalf("current primary: got %q want alpha-1", status.CurrentPrimary)
	}
}

func TestPatroniClusterStatusUsesPostgresRoleProbes(t *testing.T) {
	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runOptions: runOptions{
			target: target,
		},
	})

	status := patroniClusterStatusFromProbes([]patroniRoleProbe{
		{node: target.DataNodes[0]},
		{node: target.DataNodes[1], inRecovery: true, streaming: true},
		{node: target.DataNodes[2], inRecovery: true, streaming: true},
	})
	if err := validateClusterStatusForMembers(status, []string{"patroni-1", "patroni-2", "patroni-3"}); err != nil {
		t.Fatalf("validate Patroni status: %v", err)
	}
	if status.CurrentPrimary != "patroni-1" {
		t.Fatalf("primary: got %q want patroni-1", status.CurrentPrimary)
	}
	if lab.cfg.composeFile != filepath.Join(lab.options.repoRoot, "deploy", "patroni-lab", "compose.yml") {
		t.Fatalf("compose file: got %q", lab.cfg.composeFile)
	}
	if lab.cfg.pgClientService != "patroni-primary" || lab.cfg.pgHost != "127.0.0.1" || lab.cfg.psqlBinary != "/usr/bin/psql" {
		t.Fatalf("Patroni PostgreSQL config: %#v", lab.cfg)
	}
	if got := lab.peerServicesForMember("patroni-1"); !reflect.DeepEqual(got, []string{"patroni-replica", "patroni-replica-2"}) {
		t.Fatalf("Patroni peers: got %#v", got)
	}
}

func TestPatroniNodeRuntimeStopsAndStartsComposeService(t *testing.T) {
	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	runner := &scriptedRunner{}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
		runOptions: runOptions{
			target: target,
		},
	})

	if err := lab.stopNodeRuntime(context.Background(), "patroni-primary"); err != nil {
		t.Fatalf("stop Patroni runtime: %v", err)
	}
	if err := lab.startNodeRuntime(context.Background(), "patroni-primary"); err != nil {
		t.Fatalf("start Patroni runtime: %v", err)
	}
	if len(runner.specs) != 2 {
		t.Fatalf("runner calls: got %d want 2", len(runner.specs))
	}
	if got, want := runner.specs[0].args, []string{"compose", "-f", lab.cfg.composeFile, "stop", "patroni-primary"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stop args: got %#v want %#v", got, want)
	}
	if got, want := runner.specs[1].args, []string{"compose", "-f", lab.cfg.composeFile, "start", "patroni-primary"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("start args: got %#v want %#v", got, want)
	}
}

func TestHarnessSmallProfileHelpers(t *testing.T) {
	t.Parallel()

	if boolStatus(true) != 0 {
		t.Fatalf("boolStatus(true) should be 0")
	}
	if boolStatus(false) != 1 {
		t.Fatalf("boolStatus(false) should be 1")
	}
	if got := workloadTable("append-failover"); got != "jepsen.append_values" {
		t.Fatalf("append table: got %q", got)
	}
	if got := workloadTable("append-sync"); got != "jepsen.append_values" {
		t.Fatalf("sync append table: got %q", got)
	}
	if got := workloadTable("append-sync-two"); got != "jepsen.append_values" {
		t.Fatalf("two-standby sync append table: got %q", got)
	}
	if got := workloadTable("append-max-lag"); got != "jepsen.append_values" {
		t.Fatalf("maximum lag append table: got %q", got)
	}
	if got := workloadTable("append-check-timeline"); got != "jepsen.append_values" {
		t.Fatalf("check timeline append table: got %q", got)
	}
	if got := workloadTable("serializable-txn"); got != "jepsen.txn_ops" {
		t.Fatalf("txn table: got %q", got)
	}
	if got := workloadTable("unknown"); got != "" {
		t.Fatalf("unknown table: got %q want empty", got)
	}
	if got := maxDuration(2, 1); got != 2 {
		t.Fatalf("max duration: got %s", got)
	}
}

type scriptedRunner struct {
	outputs  []string
	statuses []int
	calls    int
	specs    []commandSpec
}

func (runner *scriptedRunner) Run(_ context.Context, spec commandSpec) (int, error) {
	output := ""
	if runner.calls < len(runner.outputs) {
		output = runner.outputs[runner.calls]
	} else if len(runner.outputs) > 0 {
		output = runner.outputs[len(runner.outputs)-1]
	}
	runner.calls++
	runner.specs = append(runner.specs, spec)
	if spec.stdout != nil {
		fmt.Fprint(spec.stdout, output)
	}
	status := 0
	if runner.calls <= len(runner.statuses) {
		status = runner.statuses[runner.calls-1]
	}
	return status, nil
}
