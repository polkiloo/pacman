package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func (lab *harnessLab) checkAcknowledgedWrite(ctx context.Context, workload, runID, caseDir string) error {
	table := workloadTable(workload)
	if table == "" {
		writeJSON(filepath.Join(caseDir, "acknowledged-write-checker.json"), map[string]any{"checker": "acknowledged-write-preservation", "valid": false, "error": "unsupported workload", "workload": workload})
		return fmt.Errorf("unsupported workload %s", workload)
	}
	finalPrimary := lab.currentPrimaryName(ctx)
	if finalPrimary == "unknown" {
		finalPrimary = "alpha-1"
	}
	finalService := serviceForMember(finalPrimary)
	if finalService == "" {
		finalService = "pacman-primary"
	}
	output, err := lab.psqlService(ctx, finalService, fmt.Sprintf("SELECT op_id, count(*)::int FROM %s WHERE run_id = %s GROUP BY op_id ORDER BY op_id;", table, sqlLiteral(runID)))
	countsFile := filepath.Join(caseDir, "final-primary-op-counts.tsv")
	_ = os.WriteFile(countsFile, []byte(output+"\n"), 0o644)
	if err != nil {
		writeJSON(filepath.Join(caseDir, "acknowledged-write-checker.json"), map[string]any{"checker": "acknowledged-write-preservation", "valid": false, "workload": workload, "runId": runID, "finalPrimary": finalPrimary, "finalPrimaryService": finalService, "table": table, "error": err.Error()})
		return err
	}
	args := []string{"checkers", "acknowledged-write", "--workload", workload, "--run-id", runID, "--case-dir", caseDir, "--table", table, "--final-primary", finalPrimary, "--final-primary-service", finalService, "--async-loss-allowed=" + strconv.FormatBool(lab.cfg.allowAsyncLoss)}
	return runJepsenctlCommand(args...)
}

func workloadTable(workload string) string {
	switch workload {
	case "append-smoke", "append-failover", "append-switchover", "append-dcs-quorum", "open-transaction-failover", "vip-routing":
		return "jepsen.append_values"
	case "single-key-register":
		return "jepsen.register_values"
	case "read-committed-txn", "serializable-txn":
		return "jepsen.txn_ops"
	default:
		return ""
	}
}

func runJepsenctlCommand(args ...string) error {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run(args, &stdout, &stderr)
	if status != 0 {
		return fmt.Errorf("%s%s", stdout.String(), stderr.String())
	}
	return nil
}

func execSinglePrimaryChecker(caseDir string) error {
	return runJepsenctlCommand("checkers", "single-primary", "--case-dir", caseDir)
}

func execTimelineChecker(caseDir string) error {
	return runJepsenctlCommand("checkers", "timeline", "--case-dir", caseDir)
}

func execOldPrimaryRejoinChecker(caseDir, nemesis string) error {
	return runJepsenctlCommand("checkers", "old-primary-rejoin", "--case-dir", caseDir, "--nemesis", nemesis)
}

func execManualSwitchoverChecker(caseDir, nemesis string) error {
	return runJepsenctlCommand("checkers", "manual-switchover", "--case-dir", caseDir, "--nemesis", nemesis)
}

func execDCSQuorumChecker(caseDir, nemesis string, minLatency int) error {
	return runJepsenctlCommand("checkers", "dcs-quorum", "--nemesis", nemesis, "--case-dir", caseDir, "--min-slow-latency-ms", strconv.Itoa(minLatency))
}

func execVIPRoutingChecker(workload, nemesis, caseDir string) error {
	return runJepsenctlCommand("checkers", "vip-routing", "--workload", workload, "--case-dir", caseDir, "--nemesis", nemesis)
}

func validateNemesisScheduleFile(workload, nemesis, path string) error {
	return runJepsenctlCommand("nemesis", "validate-schedule", "--workload", workload, "--nemesis", nemesis, "--schedule-file", path)
}

func (lab *harnessLab) checkClientTraffic(nemesis, caseDir string) error {
	checkerFile := filepath.Join(caseDir, "client-traffic-during-nemesis-checker.json")
	sampleFile := filepath.Join(caseDir, "client-traffic-during-nemesis.jsonl")
	if nemesis != "primary-dcs-partition" {
		writeJSON(checkerFile, map[string]any{"checker": "client-traffic-during-nemesis", "valid": true, "applicable": false})
		return nil
	}
	samples := readJSONL(sampleFile)
	success := countSamples(samples, func(sample map[string]any) bool { return sample["ok"] == true })
	result := map[string]any{"checker": "client-traffic-during-nemesis", "valid": success > 0, "applicable": true, "samples": len(samples), "successfulSamples": success, "failedSamples": len(samples) - success, "observations": samples}
	if len(samples) == 0 {
		result["error"] = "missing client traffic probe samples"
	}
	writeJSON(checkerFile, result)
	if success == 0 {
		return fmt.Errorf("missing successful client traffic samples")
	}
	return nil
}

func (lab *harnessLab) checkReplicationTraffic(nemesis, caseDir string) error {
	checkerFile := filepath.Join(caseDir, "replication-traffic-during-nemesis-checker.json")
	sampleFile := filepath.Join(caseDir, "replication-traffic-during-nemesis.jsonl")
	if nemesis != "primary-dcs-partition" {
		writeJSON(checkerFile, map[string]any{"checker": "replication-traffic-during-nemesis", "valid": true, "applicable": false})
		return nil
	}
	samples := readJSONL(sampleFile)
	healthy := countSamples(samples, func(sample map[string]any) bool {
		value, _ := sample["streamingReplicas"].(float64)
		return sample["ok"] == true && value >= 2
	})
	writeJSON(checkerFile, map[string]any{"checker": "replication-traffic-during-nemesis", "valid": healthy > 0, "applicable": true, "samples": len(samples), "healthySamples": healthy, "observations": samples})
	if healthy == 0 {
		return fmt.Errorf("missing healthy replication traffic samples")
	}
	return nil
}

func (lab *harnessLab) checkDCSTraffic(nemesis, caseDir string) error {
	checkerFile := filepath.Join(caseDir, "dcs-traffic-during-nemesis-checker.json")
	sampleFile := filepath.Join(caseDir, "dcs-traffic-during-nemesis.jsonl")
	if nemesis != "primary-replication-partition" {
		writeJSON(checkerFile, map[string]any{"checker": "dcs-traffic-during-nemesis", "valid": true, "applicable": false})
		return nil
	}
	samples := readJSONL(sampleFile)
	healthy := countSamples(samples, func(sample map[string]any) bool { return sample["ok"] == true })
	writeJSON(checkerFile, map[string]any{"checker": "dcs-traffic-during-nemesis", "valid": healthy > 0, "applicable": true, "samples": len(samples), "healthySamples": healthy, "observations": samples})
	if healthy == 0 {
		return fmt.Errorf("missing healthy DCS traffic samples")
	}
	return nil
}

func (lab *harnessLab) checkFailoverChain(nemesis, caseDir string) error {
	checkerFile := filepath.Join(caseDir, "failover-chain-checker.json")
	if nemesis != "failover-chain" {
		writeJSON(checkerFile, map[string]any{"checker": "failover-chain", "valid": true, "applicable": false})
		return nil
	}
	steps := readJSONL(filepath.Join(caseDir, "failover-chain.jsonl"))
	observations := readJSONL(filepath.Join(caseDir, "primary-observations.jsonl"))
	writable := map[string]struct{}{}
	successful := 0
	for _, step := range steps {
		if value, _ := step["exitStatus"].(float64); value == 0 {
			successful++
		}
	}
	for _, observation := range observations {
		if observation["reachable"] == true && observation["writable"] == true {
			if member, ok := observation["member"].(string); ok {
				writable[member] = struct{}{}
			}
		}
	}
	valid := len(steps) >= 2 && successful == len(steps) && len(writable) == 3
	members := make([]string, 0, len(writable))
	for member := range writable {
		members = append(members, member)
	}
	sort.Strings(members)
	writeJSON(checkerFile, map[string]any{"checker": "failover-chain", "valid": valid, "applicable": true, "steps": len(steps), "successfulSteps": successful, "writablePrimaryMembers": members, "chain": steps})
	if !valid {
		return fmt.Errorf("failover chain checker failed")
	}
	return nil
}

func (lab *harnessLab) checkOpenTransaction(workload, caseDir string) error {
	checkerFile := filepath.Join(caseDir, "open-transaction-checker.json")
	if workload != "open-transaction-failover" {
		writeJSON(checkerFile, map[string]any{"checker": "open-transaction-during-failover", "valid": true, "applicable": false})
		return nil
	}
	var meta map[string]any
	data, err := os.ReadFile(filepath.Join(caseDir, "open-transaction.json"))
	if err != nil || json.Unmarshal(data, &meta) != nil {
		writeJSON(checkerFile, map[string]any{"checker": "open-transaction-during-failover", "valid": false, "applicable": true, "error": "missing open transaction metadata"})
		return fmt.Errorf("missing open transaction metadata")
	}
	acks := readLines(filepath.Join(caseDir, "acknowledged-op-ids.txt"))
	hasAck := func(key string) bool {
		value, _ := meta[key].(string)
		for _, ack := range acks {
			if ack == value {
				return true
			}
		}
		return false
	}
	openStatus, _ := meta["openExitStatus"].(float64)
	valid := hasAck("preOpId") && hasAck("postOpId") && ((openStatus == 0 && hasAck("openOpId")) || (openStatus != 0 && !hasAck("openOpId")))
	writeJSON(checkerFile, map[string]any{"checker": "open-transaction-during-failover", "valid": valid, "applicable": true, "metadata": meta})
	if !valid {
		return fmt.Errorf("open transaction checker failed")
	}
	return nil
}

func readJSONL(path string) []map[string]any {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	var rows []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var row map[string]any
		if json.Unmarshal(scanner.Bytes(), &row) == nil {
			rows = append(rows, row)
		}
	}
	return rows
}

func countSamples(samples []map[string]any, predicate func(map[string]any) bool) int {
	count := 0
	for _, sample := range samples {
		if predicate(sample) {
			count++
		}
	}
	return count
}

func readLines(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var lines []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
