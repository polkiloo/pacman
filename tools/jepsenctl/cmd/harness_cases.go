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
	sampler.stop()
	_ = lab.captureClusterSnapshot(ctx, caseDir, "after-settle", nemesis, "", "")
	_ = lab.samplePrimaryState(ctx, 1000000000, observationFile)
	_ = lab.capturePGStatReplication(ctx, caseDir, "final")
	_ = lab.capturePGStatWalReceiver(ctx, caseDir, "final")

	checks := map[string]error{
		"workload":                    workloadStatus,
		"nemesis":                     nemesisStatus,
		"workload_checker":            lab.checkWorkloadProfile(ctx, workload, runID, caseDir),
		"primary_checker":             runChecker(func() error { return execSinglePrimaryChecker(caseDir) }),
		"acknowledged_checker":        lab.checkAcknowledgedWrite(ctx, workload, runID, caseDir),
		"timeline_checker":            runChecker(func() error { return execTimelineChecker(caseDir) }),
		"old_primary_rejoin_checker":  runChecker(func() error { return execOldPrimaryRejoinChecker(caseDir, nemesis) }),
		"manual_switchover_checker":   runChecker(func() error { return execManualSwitchoverChecker(caseDir, nemesis) }),
		"client_traffic_checker":      lab.checkClientTraffic(nemesis, caseDir),
		"replication_traffic_checker": lab.checkReplicationTraffic(nemesis, caseDir),
		"dcs_traffic_checker":         lab.checkDCSTraffic(nemesis, caseDir),
		"dcs_quorum_checker":          runChecker(func() error { return execDCSQuorumChecker(caseDir, nemesis, lab.cfg.dcsSlowMinLatencyMS) }),
		"failover_chain_checker":      lab.checkFailoverChain(nemesis, caseDir),
		"open_transaction_checker":    lab.checkOpenTransaction(workload, caseDir),
		"vip_routing_checker":         runChecker(func() error { return execVIPRoutingChecker(workload, nemesis, caseDir) }),
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
		recordCaseResult(caseResults, workload, nemesis, true, "checkers passed")
		return nil
	}

	details := strings.Join(failures, " ")
	writeCaseEvent(caseHistory, ":case", "fail", "workload", fmt.Sprintf("{:workload %q :nemesis %q :run-id %q :details %q}", workload, nemesis, runID, details))
	appendFile(campaignHistory, mustRead(caseHistory))
	_, _ = writeEDNEvent(campaignHistory, workload+"/"+nemesis, "fail", fmt.Sprintf("%q", runID))
	recordCaseResult(caseResults, workload, nemesis, false, details)
	return fmt.Errorf("%s", details)
}

func runChecker(fn func() error) error {
	return fn()
}

func writeCaseEvent(path, process, status, functionName, value string) {
	line := fmt.Sprintf("{:time %q :process %s :type :%s :f :%s :value %s}\n", time.Now().UTC().Format(time.RFC3339), process, status, functionName, value)
	appendFile(path, line)
}

func recordCaseResult(path, workload, nemesis string, valid bool, details string) {
	appendJSONL(path, map[string]any{
		"workload": workload,
		"nemesis":  nemesis,
		"valid":    valid,
		"details":  details,
	})
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

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
