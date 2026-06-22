package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (lab *harnessLab) ensureWorkloadSchema(ctx context.Context) error {
	_, err := lab.psqlVIP(ctx, `
CREATE SCHEMA IF NOT EXISTS jepsen;
CREATE TABLE IF NOT EXISTS jepsen.append_values (
  run_id text NOT NULL,
  op_id text PRIMARY KEY,
  key_id integer NOT NULL,
  value text NOT NULL,
  client_id integer NOT NULL,
  observed_primary text NOT NULL,
  isolation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jepsen.register_values (
  run_id text NOT NULL,
  op_id text PRIMARY KEY,
  value bigint NOT NULL,
  client_id integer NOT NULL,
  observed_primary text NOT NULL,
  isolation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jepsen.txn_accounts (
  run_id text NOT NULL,
  key_id integer NOT NULL,
  balance bigint NOT NULL,
  PRIMARY KEY (run_id, key_id)
);
CREATE TABLE IF NOT EXISTS jepsen.txn_ops (
  run_id text NOT NULL,
  op_id text PRIMARY KEY,
  key_id integer NOT NULL,
  amount bigint NOT NULL,
  observed_total bigint NOT NULL,
  client_id integer NOT NULL,
  observed_primary text NOT NULL,
  isolation text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jepsen.client_availability_probes (
  probe_id text PRIMARY KEY,
  nemesis text NOT NULL,
  observed_at timestamptz NOT NULL DEFAULT now()
);`)
	return err
}

func (lab *harnessLab) runCases(ctx context.Context, cases []string, runDir, historyFile, scheduleFile string) error {
	if err := os.MkdirAll(filepath.Join(runDir, "cases"), 0o755); err != nil {
		return err
	}
	caseResults := filepath.Join(runDir, "case-results.jsonl")
	if err := os.WriteFile(caseResults, nil, 0o644); err != nil {
		return err
	}
	if err := lab.ensureWorkloadSchema(ctx); err != nil {
		return err
	}
	failed := false
	for _, spec := range cases {
		workload, nemesis := splitCaseSpec(spec)
		if err := lab.runCase(ctx, workload, nemesis, runDir, historyFile, scheduleFile, caseResults); err != nil {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("one or more Jepsen cases failed")
	}
	return nil
}

func (lab *harnessLab) runCase(ctx context.Context, workload, nemesis, runDir, campaignHistory, scheduleFile, caseResults string) error {
	if !lab.options.target.supportsCase(workload, nemesis) {
		return fmt.Errorf("Jepsen target %s does not support case %s:%s", lab.options.target.Name, workload, nemesis)
	}

	slug := caseSlug(workload + "__" + nemesis)
	caseDir := filepath.Join(runDir, "cases", slug)
	runID := envOrDefault("PACMAN_JEPSEN_RUN_ID", time.Now().UTC().Format("20060102T150405Z")) + "-" + slug
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return err
	}
	for _, file := range []string{"history.edn", "nemesis.log", "primary-observations.jsonl", "pacman-cluster-snapshots.jsonl"} {
		if err := os.WriteFile(filepath.Join(caseDir, file), nil, 0o644); err != nil {
			return err
		}
	}
	if err := lab.prepareWorkloadProfile(ctx, workload, caseDir); err != nil {
		return err
	}

	caseHistory := filepath.Join(caseDir, "history.edn")
	_, _ = writeEDNEvent(campaignHistory, workload+"/"+nemesis, "invoke", fmt.Sprintf("%q", runID))
	writeCaseEvent(caseHistory, ":case", "invoke", "workload", fmt.Sprintf("{:workload %q :nemesis %q :run-id %q}", workload, nemesis, runID))
	scheduleOffset := fileSize(scheduleFile)
	_ = lab.captureClusterSnapshot(ctx, caseDir, "before-nemesis", nemesis, "", "")
	_ = lab.samplePrimaryState(ctx, 0, filepath.Join(caseDir, "primary-observations.jsonl"))

	sampler := lab.startPrimarySampler(ctx, caseDir)
	nemesisRun := lab.runNemesisProfile(ctx, nemesis, caseDir, scheduleFile, lab.cfg.defaultDuration)
	workloadStatus := lab.runWorkloadProfile(ctx, workload, runID, caseDir)
	nemesisStatus := nemesisRun.wait()
	_ = copyScheduleTail(scheduleFile, filepath.Join(caseDir, "nemesis-schedule.edn"), scheduleOffset)
	lab.settleAfterNemesis(caseDir, nemesis)
	observationFile := filepath.Join(caseDir, primaryObservationFile)
	_ = lab.waitForTimelineConvergence(ctx, observationFile)
	_ = lab.waitForOldPrimaryRejoin(ctx, observationFile, caseDir, nemesis)
	sampler.stop()
	_ = lab.captureClusterSnapshot(ctx, caseDir, "after-settle", nemesis, "", "")
	_ = lab.samplePrimaryState(ctx, 1000000000, observationFile)
	_ = lab.capturePGStatReplication(ctx, caseDir, "final")
	_ = lab.capturePGStatWalReceiver(ctx, caseDir, "final")

	checks := map[string]error{
		"workload":                         workloadStatus,
		"nemesis":                          nemesisStatus,
		"case_history":                     validateCaseHistoryArtifact(caseHistory, workload, nemesis, runID),
		"workload_checker":                 lab.checkWorkloadProfile(ctx, workload, runID, caseDir),
		"primary_checker":                  runChecker(func() error { return execSinglePrimaryChecker(caseDir) }),
		"acknowledged_checker":             lab.checkAcknowledgedWrite(ctx, workload, runID, caseDir),
		"timeline_checker":                 runChecker(func() error { return execTimelineChecker(caseDir) }),
		"old_primary_rejoin_checker":       runChecker(func() error { return execOldPrimaryRejoinChecker(caseDir, nemesis) }),
		"manual_switchover_checker":        runChecker(func() error { return execManualSwitchoverChecker(caseDir, nemesis) }),
		"client_traffic_checker":           lab.checkClientTraffic(nemesis, caseDir),
		"replication_traffic_checker":      lab.checkReplicationTraffic(nemesis, caseDir),
		"dcs_traffic_checker":              lab.checkDCSTraffic(nemesis, caseDir),
		"dcs_quorum_checker":               runChecker(func() error { return execDCSQuorumChecker(caseDir, nemesis, lab.cfg.dcsSlowMinLatencyMS) }),
		"failover_chain_checker":           lab.checkFailoverChain(nemesis, caseDir),
		"reinit_checker":                   lab.checkReinitProcedure(nemesis, caseDir),
		"open_transaction_checker":         lab.checkOpenTransaction(workload, caseDir),
		"vip_routing_checker":              runChecker(func() error { return execVIPRoutingChecker(workload, nemesis, caseDir) }),
		"synchronous_replication_checker":  lab.checkSynchronousReplication(ctx, workload, caseDir),
		"synchronous_standby_kill_checker": lab.checkSynchronousStandbyKill(nemesis, caseDir),
		"maximum_lag_on_failover_checker":  lab.checkMaximumLagOnFailover(nemesis, caseDir),
		"patroni_check_timeline_checker":   lab.checkPatroniCheckTimeline(nemesis, caseDir),
		"strict_sync_checker":              lab.checkStrictSyncNoStandby(nemesis, caseDir),
		"nemesis_schedule_checker_status": runChecker(func() error {
			return validateNemesisScheduleFile(workload, nemesis, filepath.Join(caseDir, "nemesis-schedule.edn"))
		}),
	}

	var failures []string
	for name, err := range checks {
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s=%v", name, err))
		}
	}
	sort.Strings(failures)
	if len(failures) == 0 {
		writeCaseEvent(caseHistory, ":case", "ok", "workload", fmt.Sprintf("{:workload %q :nemesis %q :run-id %q}", workload, nemesis, runID))
		appendFile(campaignHistory, mustRead(caseHistory))
		_, _ = writeEDNEvent(campaignHistory, workload+"/"+nemesis, "ok", fmt.Sprintf("%q", runID))
		recordCaseResult(caseResults, workload, nemesis, runID, caseHistory, true, "checkers passed", collectCaseCheckerReports(caseDir))
		return nil
	}

	details := strings.Join(failures, " ")
	writeCaseEvent(caseHistory, ":case", "fail", "workload", fmt.Sprintf("{:workload %q :nemesis %q :run-id %q :details %q}", workload, nemesis, runID, details))
	appendFile(campaignHistory, mustRead(caseHistory))
	_, _ = writeEDNEvent(campaignHistory, workload+"/"+nemesis, "fail", fmt.Sprintf("%q", runID))
	recordCaseResult(caseResults, workload, nemesis, runID, caseHistory, false, details, collectCaseCheckerReports(caseDir))
	return fmt.Errorf("%s", details)
}

func runChecker(fn func() error) error {
	return fn()
}

func writeCaseEvent(path, process, status, functionName, value string) {
	line := fmt.Sprintf("{:time %q :process %s :type :%s :f :%s :value %s}\n", time.Now().UTC().Format(time.RFC3339), process, status, functionName, value)
	appendFile(path, line)
}

func recordCaseResult(path, workload, nemesis, runID, historyPath string, valid bool, details string, checkerReports map[string]caseCheckerReport) {
	appendJSONL(path, map[string]any{
		"workload":       workload,
		"nemesis":        nemesis,
		"runId":          runID,
		"valid":          valid,
		"details":        details,
		"history":        historyPath,
		"historyFormat":  "edn",
		"historyEvents":  countLines(historyPath),
		"checkerReports": checkerReports,
	})
}

func collectCaseCheckerReports(caseDir string) map[string]caseCheckerReport {
	specs := []struct {
		key  string
		file string
	}{
		{key: "splitBrain", file: singlePrimaryCheckerFile},
		{key: "acknowledgedWritePreservation", file: acknowledgedWriteCheckerFile},
		{key: "timelineConvergence", file: timelineCheckerFile},
		{key: "failoverRejoin", file: oldPrimaryRejoinCheckerFile},
		{key: "fullReplicaReinit", file: reinitCheckerFile},
	}

	reports := make(map[string]caseCheckerReport, len(specs))
	for _, spec := range specs {
		reports[spec.key] = readCaseCheckerReport(caseDir, spec.file)
	}
	return reports
}

func readCaseCheckerReport(caseDir, file string) caseCheckerReport {
	report := caseCheckerReport{File: file}
	path := filepath.Join(caseDir, file)
	data, err := os.ReadFile(path)
	if err != nil {
		report.Error = "missing checker artifact"
		valid := false
		report.Valid = &valid
		return report
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		report.Error = fmt.Sprintf("invalid checker artifact: %v", err)
		valid := false
		report.Valid = &valid
		return report
	}

	report.Checker = stringValue(raw["checker"])
	report.Valid = boolPointer(raw["valid"])
	report.Applicable = boolPointer(raw["applicable"])
	report.Error = stringValue(raw["error"])
	report.Reason = stringValue(raw["reason"])
	report.Summary = summarizeCaseCheckerReport(file, raw)
	report.Facts = selectedCheckerFacts(file, raw)
	return report
}

func summarizeCaseCheckerReport(file string, raw map[string]any) string {
	if errText := stringValue(raw["error"]); errText != "" {
		return errText
	}
	if reason := stringValue(raw["reason"]); reason != "" {
		return reason
	}

	switch file {
	case singlePrimaryCheckerFile:
		return fmt.Sprintf("valid=%s samples=%s confirmationSamples=%s transitionSamples=%d violationSamples=%d",
			fieldString(raw, "valid"),
			fieldString(raw, "samples"),
			fieldString(raw, "confirmationSamples"),
			arrayLength(raw["transitionSamples"]),
			arrayLength(raw["violationSamples"]))
	case acknowledgedWriteCheckerFile:
		return fmt.Sprintf("valid=%s expectedAcknowledged=%s observedExactlyOnce=%s missingAcknowledged=%s duplicateAcknowledged=%s",
			fieldString(raw, "valid"),
			fieldString(raw, "expectedAcknowledged"),
			fieldString(raw, "observedExactlyOnce"),
			fieldString(raw, "missingAcknowledged"),
			fieldString(raw, "duplicateAcknowledged"))
	case timelineCheckerFile:
		return fmt.Sprintf("valid=%s promotionObserved=%s timelineAdvanced=%s replicasConverged=%s oldPrimarySafe=%s",
			fieldString(raw, "valid"),
			fieldString(raw, "promotionObserved"),
			fieldString(raw, "timelineAdvanced"),
			fieldString(raw, "replicasConverged"),
			fieldString(raw, "oldPrimarySafe"))
	case oldPrimaryRejoinCheckerFile:
		return fmt.Sprintf("valid=%s promotionObserved=%s oldPrimaryRejoined=%s oldPrimarySafeOrRejoined=%s unsafeAfterPromotion=%s initialPrimary=%s finalPrimary=%s",
			fieldString(raw, "valid"),
			fieldString(raw, "promotionObserved"),
			fieldString(raw, "oldPrimaryRejoined"),
			fieldString(raw, "oldPrimarySafeOrRejoined"),
			fieldString(raw, "oldPrimaryUnsafeAfterPromotion"),
			memberName(raw["initialPrimary"]),
			memberName(raw["finalPrimary"]))
	case reinitCheckerFile:
		return fmt.Sprintf("valid=%s completed=%s source=%s target=%s operationId=%s observations=%s",
			fieldString(raw, "valid"),
			fieldString(raw, "completed"),
			fieldString(raw, "source"),
			fieldString(raw, "target"),
			fieldString(raw, "operationId"),
			fieldString(raw, "observations"))
	default:
		return fmt.Sprintf("valid=%s", fieldString(raw, "valid"))
	}
}

func selectedCheckerFacts(file string, raw map[string]any) map[string]any {
	switch file {
	case singlePrimaryCheckerFile:
		return selectFields(raw, "observations", "samples", "writableObservations", "confirmationSamples", "transitionSamples", "violationSamples")
	case acknowledgedWriteCheckerFile:
		return selectFields(raw, "workload", "runId", "finalPrimary", "finalPrimaryService", "asyncLossAllowed", "expectedAcknowledged", "observedExactlyOnce", "missingAcknowledged", "duplicateAcknowledged", "unacknowledgedObserved")
	case timelineCheckerFile:
		return selectFields(raw, "promotionObserved", "timelineAdvanced", "replicasConverged", "oldPrimarySafe", "initialSample", "finalSample", "oldPrimaryFinalState")
	case oldPrimaryRejoinCheckerFile:
		return selectFields(raw, "nemesis", "promotionObserved", "initialPrimary", "finalPrimary", "oldPrimaryRejoined", "oldPrimarySafeOrRejoined", "oldPrimaryUnsafeAfterPromotion", "oldPrimaryFinalState")
	case reinitCheckerFile:
		return selectFields(raw, "source", "target", "operationId", "completed", "observations", "finalStatus")
	default:
		return nil
	}
}

func selectFields(raw map[string]any, fields ...string) map[string]any {
	selected := make(map[string]any, len(fields))
	for _, field := range fields {
		if value, ok := raw[field]; ok {
			selected[field] = value
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func boolPointer(value any) *bool {
	boolean, ok := value.(bool)
	if !ok {
		return nil
	}
	return &boolean
}

func fieldString(raw map[string]any, field string) string {
	value, ok := raw[field]
	if !ok {
		return "unknown"
	}
	return fmt.Sprint(value)
}

func arrayLength(value any) int {
	values, ok := value.([]any)
	if !ok {
		return 0
	}
	return len(values)
}

func memberName(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		return "unknown"
	}
	member := stringValue(object["member"])
	if member == "" {
		return "unknown"
	}
	return member
}

func validateCaseHistoryArtifact(path, workload, nemesis, runID string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read case history: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	events := 0
	hasCaseInvoke := false
	hasWorkloadEvent := false
	for index, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		events++
		if !strings.HasPrefix(line, "{:") || !strings.HasSuffix(line, "}") {
			return fmt.Errorf("history line %d is not an EDN map", index+1)
		}
		for _, token := range []string{":time", ":process", ":type", ":f", ":value"} {
			if !strings.Contains(line, token) {
				return fmt.Errorf("history line %d is missing %s", index+1, token)
			}
		}
		if strings.Contains(line, ":process :case") &&
			strings.Contains(line, ":type :invoke") &&
			strings.Contains(line, fmt.Sprintf(":workload %q", workload)) &&
			strings.Contains(line, fmt.Sprintf(":nemesis %q", nemesis)) &&
			strings.Contains(line, fmt.Sprintf(":run-id %q", runID)) {
			hasCaseInvoke = true
		}
		if !strings.Contains(line, ":process :case") {
			hasWorkloadEvent = true
		}
	}
	if events == 0 {
		return fmt.Errorf("case history is empty")
	}
	if !hasCaseInvoke {
		return fmt.Errorf("case history is missing case invoke event")
	}
	if !hasWorkloadEvent {
		return fmt.Errorf("case history is missing workload events")
	}
	return nil
}

func appendJSONL(path string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	appendFile(path, string(data)+"\n")
}

func appendFile(path, value string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.WriteString(value)
}

func mustRead(path string) string {
	data, _ := os.ReadFile(path)
	return string(data)
}

func copyScheduleTail(scheduleFile, caseScheduleFile string, offset int64) error {
	data, err := os.ReadFile(scheduleFile)
	if err != nil {
		return err
	}
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	return os.WriteFile(caseScheduleFile, data[offset:], 0o644)
}

func (lab *harnessLab) waitForTimelineConvergence(ctx context.Context, observationFile string) bool {
	if lab.cfg.timelineConvergenceTimeout <= 0 {
		return false
	}
	deadline := time.Now().Add(lab.cfg.timelineConvergenceTimeout)
	sampleID := 900000000
	for {
		_ = lab.samplePrimaryState(ctx, sampleID, observationFile)
		sampleID++

		observations, err := readPrimaryObservations(observationFile)
		if err == nil && checkTimelineConvergence(observations).Valid {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(lab.cfg.timelineConvergenceInterval):
		}
	}
}

func (lab *harnessLab) waitForOldPrimaryRejoin(ctx context.Context, observationFile, caseDir, nemesis string) bool {
	if _, skipped := oldPrimaryRejoinDedicatedCheckerReason(nemesis); skipped || failureNemesisAllowsUnavailableOldPrimary(nemesis) {
		return true
	}
	if lab.cfg.oldPrimaryRejoinTimeout <= 0 {
		return false
	}

	logPath := filepath.Join(caseDir, "nemesis.log")
	appendFile(logPath, fmt.Sprintf("waiting up to %s for old primary rejoin after %s\n", lab.cfg.oldPrimaryRejoinTimeout, nemesis))
	deadline := time.Now().Add(lab.cfg.oldPrimaryRejoinTimeout)
	interval := lab.cfg.oldPrimaryRejoinInterval
	if interval <= 0 {
		interval = time.Second
	}
	sampleID := 950000000
	for {
		_ = lab.samplePrimaryState(ctx, sampleID, observationFile)
		sampleID++

		state, err := oldPrimaryRejoinWaitState(observationFile, nemesis)
		if err == nil {
			switch {
			case !state.applicable:
				appendFile(logPath, "old primary rejoin wait skipped: no promotion observed\n")
				return true
			case state.valid:
				appendFile(logPath, fmt.Sprintf("old primary rejoin observed after %s\n", nemesis))
				return true
			case state.unsafeAfterPromotion:
				appendFile(logPath, "old primary rejoin wait stopped: unsafe old primary observation recorded\n")
				return false
			}
		}

		if time.Now().After(deadline) {
			if err != nil {
				appendFile(logPath, fmt.Sprintf("old primary rejoin wait timed out after %s: %v\n", lab.cfg.oldPrimaryRejoinTimeout, err))
			} else {
				appendFile(logPath, fmt.Sprintf("old primary rejoin wait timed out after %s\n", lab.cfg.oldPrimaryRejoinTimeout))
			}
			return false
		}
		select {
		case <-ctx.Done():
			appendFile(logPath, fmt.Sprintf("old primary rejoin wait canceled: %v\n", ctx.Err()))
			return false
		case <-time.After(interval):
		}
	}
}

type oldPrimaryRejoinWaitStatus struct {
	valid                bool
	applicable           bool
	unsafeAfterPromotion bool
}

func oldPrimaryRejoinWaitState(observationFile, nemesis string) (oldPrimaryRejoinWaitStatus, error) {
	observations, err := readPrimaryObservations(observationFile)
	if err != nil {
		return oldPrimaryRejoinWaitStatus{}, err
	}
	result := checkOldPrimaryRejoinAfterFailover(observations, nemesis)
	return oldPrimaryRejoinWaitStatus{
		valid:                result.Valid,
		applicable:           result.Applicable,
		unsafeAfterPromotion: result.OldPrimaryUnsafeAfterPromotion,
	}, nil
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
