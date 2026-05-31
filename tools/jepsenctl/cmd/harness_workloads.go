package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (lab *harnessLab) runWorkloadProfile(ctx context.Context, workload, runID, caseDir string) error {
	switch workload {
	case "append-smoke", "append-failover", "append-dcs-quorum":
		return lab.runAppendWorkload(ctx, runID, caseDir, "read committed", lab.cfg.defaultOps, lab.cfg.defaultKeys, 0)
	case "append-switchover":
		return lab.runAppendWorkload(ctx, runID, caseDir, "read committed", lab.cfg.defaultOps, lab.cfg.defaultKeys, lab.cfg.appendSwitchoverOpDelay)
	case "open-transaction-failover":
		return lab.runOpenTransactionWorkload(ctx, runID, caseDir, "read committed")
	case "vip-routing":
		return lab.runVIPRoutingWorkload(ctx, runID, caseDir, "read committed")
	case "single-key-register":
		return lab.runRegisterWorkload(ctx, runID, caseDir, "read committed")
	case "read-committed-txn":
		return lab.runTxnWorkload(ctx, runID, caseDir, "read committed")
	case "serializable-txn":
		return lab.runTxnWorkload(ctx, runID, caseDir, "serializable")
	default:
		return fmt.Errorf("unsupported workload profile %s", workload)
	}
}

func (lab *harnessLab) checkWorkloadProfile(ctx context.Context, workload, runID, caseDir string) error {
	switch workload {
	case "append-smoke", "append-failover", "append-switchover", "append-dcs-quorum", "open-transaction-failover", "vip-routing":
		return lab.checkAppendWorkload(ctx, runID, caseDir)
	case "single-key-register":
		return lab.checkRegisterWorkload(ctx, runID, caseDir)
	case "read-committed-txn":
		return lab.checkTxnWorkload(ctx, runID, caseDir, "read-committed-txn")
	case "serializable-txn":
		return lab.checkTxnWorkload(ctx, runID, caseDir, "serializable-txn")
	default:
		return fmt.Errorf("unsupported workload checker %s", workload)
	}
}

func (lab *harnessLab) runAppendWorkload(ctx context.Context, runID, caseDir, isolation string, ops, keys int, delay time.Duration) error {
	history := filepath.Join(caseDir, "history.edn")
	ackFile := filepath.Join(caseDir, "acknowledged-op-ids.txt")
	failures := filepath.Join(caseDir, "failures.log")
	_ = os.WriteFile(ackFile, nil, 0o644)
	_ = os.WriteFile(failures, nil, 0o644)
	acked := 0
	for op := 1; op <= ops; op++ {
		client := (op - 1) % lab.cfg.defaultClients
		key := (op - 1) % keys
		opID := fmt.Sprintf("%s-append-%d", runID, op)
		value := fmt.Sprintf("v-%d", op)
		primary := lab.currentPrimaryName(ctx)
		writeCaseEvent(history, strconv.Itoa(client), "invoke", "append", fmt.Sprintf("{:op-id %q :key %d :value %q :primary %q}", opID, key, value, primary))
		sql := fmt.Sprintf(`
BEGIN ISOLATION LEVEL %s;
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES (%s, %s, %d, %s, %d, %s, %s);
COMMIT;`, isolation, sqlLiteral(runID), sqlLiteral(opID), key, sqlLiteral(value), client, sqlLiteral(primary), sqlLiteral(isolation))
		if _, err := lab.psqlVIP(ctx, sql); err != nil {
			appendFile(failures, err.Error()+"\n")
			writeCaseEvent(history, strconv.Itoa(client), "fail", "append", fmt.Sprintf("{:op-id %q :key %d :value %q :primary %q}", opID, key, value, primary))
		} else {
			appendFile(ackFile, opID+"\n")
			acked++
			writeCaseEvent(history, strconv.Itoa(client), "ok", "append", fmt.Sprintf("{:op-id %q :key %d :value %q :primary %q}", opID, key, value, primary))
		}
		if delay > 0 && op < ops {
			time.Sleep(delay)
		}
	}
	if acked == 0 {
		return fmt.Errorf("append workload acknowledged no writes")
	}
	return nil
}

func (lab *harnessLab) runRegisterWorkload(ctx context.Context, runID, caseDir, isolation string) error {
	history := filepath.Join(caseDir, "history.edn")
	ackFile := filepath.Join(caseDir, "acknowledged-op-ids.txt")
	failures := filepath.Join(caseDir, "failures.log")
	_ = os.WriteFile(ackFile, nil, 0o644)
	_ = os.WriteFile(failures, nil, 0o644)
	acked := 0
	for op := 1; op <= lab.cfg.defaultOps; op++ {
		client := (op - 1) % lab.cfg.defaultClients
		opID := fmt.Sprintf("%s-register-%d", runID, op)
		primary := lab.currentPrimaryName(ctx)
		writeCaseEvent(history, strconv.Itoa(client), "invoke", "write", fmt.Sprintf("{:op-id %q :value %d :primary %q}", opID, op, primary))
		sql := fmt.Sprintf(`
WITH inserted AS (
  INSERT INTO jepsen.register_values(run_id, op_id, value, client_id, observed_primary, isolation)
  VALUES (%s, %s, %d, %d, %s, %s)
  RETURNING value
)
SELECT max(value) FROM jepsen.register_values WHERE run_id = %s;`, sqlLiteral(runID), sqlLiteral(opID), op, client, sqlLiteral(primary), sqlLiteral(isolation), sqlLiteral(runID))
		output, err := lab.psqlVIP(ctx, sql)
		if err != nil {
			appendFile(failures, err.Error()+"\n")
			writeCaseEvent(history, strconv.Itoa(client), "fail", "write", fmt.Sprintf("{:op-id %q :value %d :primary %q}", opID, op, primary))
			continue
		}
		appendFile(ackFile, opID+"\n")
		acked++
		writeCaseEvent(history, strconv.Itoa(client), "ok", "write", fmt.Sprintf("{:op-id %q :value %d :read %q :primary %q}", opID, op, lastNonEmptyLine(output), primary))
	}
	if acked == 0 {
		return fmt.Errorf("register workload acknowledged no writes")
	}
	return nil
}

func (lab *harnessLab) runTxnWorkload(ctx context.Context, runID, caseDir, isolation string) error {
	history := filepath.Join(caseDir, "history.edn")
	ackFile := filepath.Join(caseDir, "acknowledged-op-ids.txt")
	failures := filepath.Join(caseDir, "failures.log")
	_ = os.WriteFile(ackFile, nil, 0o644)
	_ = os.WriteFile(failures, nil, 0o644)
	for key := 0; key < lab.cfg.defaultKeys; key++ {
		_, _ = lab.psqlVIP(ctx, fmt.Sprintf("INSERT INTO jepsen.txn_accounts(run_id, key_id, balance) VALUES (%s, %d, 0) ON CONFLICT (run_id, key_id) DO NOTHING;", sqlLiteral(runID), key))
	}
	acked := 0
	for op := 1; op <= lab.cfg.defaultOps; op++ {
		client := (op - 1) % lab.cfg.defaultClients
		key := (op - 1) % lab.cfg.defaultKeys
		opID := fmt.Sprintf("%s-txn-%d", runID, op)
		primary := lab.currentPrimaryName(ctx)
		writeCaseEvent(history, strconv.Itoa(client), "invoke", "txn", fmt.Sprintf("{:op-id %q :key %d :amount 1 :isolation %q :primary %q}", opID, key, isolation, primary))
		sql := fmt.Sprintf(`
BEGIN ISOLATION LEVEL %s;
UPDATE jepsen.txn_accounts SET balance = balance + 1 WHERE run_id = %s AND key_id = %d;
WITH total AS (
  SELECT sum(balance) AS value FROM jepsen.txn_accounts WHERE run_id = %s
), inserted AS (
  INSERT INTO jepsen.txn_ops(run_id, op_id, key_id, amount, observed_total, client_id, observed_primary, isolation)
  SELECT %s, %s, %d, 1, value, %d, %s, %s FROM total
)
SELECT value FROM total;
COMMIT;`, isolation, sqlLiteral(runID), key, sqlLiteral(runID), sqlLiteral(runID), sqlLiteral(opID), key, client, sqlLiteral(primary), sqlLiteral(isolation))
		output, err := lab.psqlVIP(ctx, sql)
		if err != nil {
			appendFile(failures, err.Error()+"\n")
			writeCaseEvent(history, strconv.Itoa(client), "fail", "txn", fmt.Sprintf("{:op-id %q :key %d :isolation %q :primary %q}", opID, key, isolation, primary))
			continue
		}
		appendFile(ackFile, opID+"\n")
		acked++
		writeCaseEvent(history, strconv.Itoa(client), "ok", "txn", fmt.Sprintf("{:op-id %q :key %d :total %q :isolation %q :primary %q}", opID, key, lastNonEmptyLine(output), isolation, primary))
	}
	if acked == 0 {
		return fmt.Errorf("txn workload acknowledged no writes")
	}
	return nil
}

func (lab *harnessLab) runOpenTransactionWorkload(ctx context.Context, runID, caseDir, isolation string) error {
	if err := lab.runAppendWorkload(ctx, runID, caseDir, isolation, 1, lab.cfg.defaultKeys, 0); err != nil {
		return err
	}
	openOpID := runID + "-open-txn-held"
	postOpID := runID + "-open-txn-post"
	primary := lab.currentPrimaryName(ctx)
	startedAt := time.Now().UTC().Format(time.RFC3339)
	sleepSeconds := int(lab.cfg.nemesisHold.Seconds()) + int(lab.cfg.defaultDuration.Seconds()/3) + 4
	sql := fmt.Sprintf(`
BEGIN ISOLATION LEVEL %s;
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES (%s, %s, 1, 'held', 1, %s, %s);
SELECT pg_sleep(%d);
COMMIT;`, isolation, sqlLiteral(runID), sqlLiteral(openOpID), sqlLiteral(primary), sqlLiteral(isolation), sleepSeconds)
	output, openErr := lab.psqlVIP(ctx, sql)
	finishedAt := time.Now().UTC().Format(time.RFC3339)
	openStatus := 0
	if openErr != nil {
		openStatus = 1
		output = openErr.Error()
	} else {
		appendFile(filepath.Join(caseDir, "acknowledged-op-ids.txt"), openOpID+"\n")
	}
	_ = lab.waitForVIPWritable(ctx, 90*time.Second)
	finalPrimary := lab.currentPrimaryName(ctx)
	postStatus := 0
	postSQL := fmt.Sprintf(`
BEGIN ISOLATION LEVEL %s;
INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
VALUES (%s, %s, 2, 'post', 2, %s, %s);
COMMIT;`, isolation, sqlLiteral(runID), sqlLiteral(postOpID), sqlLiteral(finalPrimary), sqlLiteral(isolation))
	if _, err := lab.psqlVIP(ctx, postSQL); err != nil {
		postStatus = 1
	} else {
		appendFile(filepath.Join(caseDir, "acknowledged-op-ids.txt"), postOpID+"\n")
	}
	writeJSON(filepath.Join(caseDir, "open-transaction.json"), map[string]any{
		"workload":         "open-transaction-failover",
		"runId":            runID,
		"isolation":        isolation,
		"initialPrimary":   primary,
		"finalPrimary":     finalPrimary,
		"preOpId":          runID + "-append-1",
		"openOpId":         openOpID,
		"postOpId":         postOpID,
		"openStartedAt":    startedAt,
		"openFinishedAt":   finishedAt,
		"openSleepSeconds": sleepSeconds,
		"openExitStatus":   openStatus,
		"postExitStatus":   postStatus,
		"output":           output,
	})
	if postStatus != 0 {
		return fmt.Errorf("post open-transaction write failed")
	}
	return nil
}

func (lab *harnessLab) runVIPRoutingWorkload(ctx context.Context, runID, caseDir, isolation string) error {
	routeFile := filepath.Join(caseDir, "vip-routing.jsonl")
	_ = os.WriteFile(routeFile, nil, 0o644)
	deadline := time.Now().Add(lab.cfg.defaultDuration + lab.cfg.nemesisHold + 4*time.Second)
	okCount := 0
	op := 0
	for time.Now().Before(deadline) {
		op++
		opID := fmt.Sprintf("%s-vip-routing-%d", runID, op)
		primaryBefore := lab.currentPrimaryName(ctx)
		vipBefore := lab.vipHolder(ctx)
		sql := fmt.Sprintf(`
BEGIN ISOLATION LEVEL %s;
WITH inserted AS (
  INSERT INTO jepsen.append_values(run_id, op_id, key_id, value, client_id, observed_primary, isolation)
  VALUES (%s, %s, %d, %s, %d, %s, %s)
  RETURNING op_id
)
SELECT pg_is_in_recovery(), coalesce(inet_server_addr()::text, ''), op_id FROM inserted;
COMMIT;`, isolation, sqlLiteral(runID), sqlLiteral(opID), op%lab.cfg.defaultKeys, sqlLiteral(fmt.Sprintf("route-%d", op)), op%lab.cfg.defaultClients, sqlLiteral(primaryBefore), sqlLiteral(isolation))
		output, err := lab.psqlVIP(ctx, sql)
		primaryAfter := lab.currentPrimaryName(ctx)
		vipAfter := lab.vipHolder(ctx)
		sample := map[string]any{
			"observedAt":          time.Now().UTC().Format(time.RFC3339),
			"opId":                opID,
			"ok":                  err == nil,
			"status":              boolStatus(err == nil),
			"pacmanPrimaryBefore": primaryBefore,
			"pacmanPrimaryAfter":  primaryAfter,
			"vipHolderBefore":     vipBefore,
			"vipHolderAfter":      vipAfter,
			"inRecovery":          false,
			"serverAddr":          "",
			"returnedOp":          "",
			"error":               "",
		}
		if err != nil {
			sample["error"] = err.Error()
		} else {
			parts := strings.Split(lastNonEmptyLine(output), "\t")
			if len(parts) >= 3 {
				sample["inRecovery"] = parts[0] == "t"
				sample["serverAddr"] = parts[1]
				sample["returnedOp"] = parts[2]
			}
			appendFile(filepath.Join(caseDir, "acknowledged-op-ids.txt"), opID+"\n")
			okCount++
		}
		appendJSONL(routeFile, sample)
		time.Sleep(time.Second)
	}
	if okCount == 0 {
		return fmt.Errorf("vip routing workload had no successful writes")
	}
	return nil
}

func boolStatus(ok bool) int {
	if ok {
		return 0
	}
	return 1
}

func (lab *harnessLab) waitForVIPWritable(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := lab.psqlVIP(ctx, "SELECT 1;"); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for VIP writable")
}

func (lab *harnessLab) vipHolder(ctx context.Context) string {
	for _, service := range []string{"pacman-primary", "pacman-replica", "pacman-replica-2"} {
		output, status, _ := lab.composeExec(ctx, service, "/bin/sh", "-lc", fmt.Sprintf("ip -o -4 addr show dev %s | grep -q ' %s/'", lab.cfg.vipInterface, lab.cfg.pgHost))
		_ = output
		if status == 0 {
			if member := memberForService(service); member != "" {
				return member
			}
		}
	}
	return "unknown"
}

func (lab *harnessLab) checkAppendWorkload(ctx context.Context, runID, caseDir string) error {
	expected := countLines(filepath.Join(caseDir, "acknowledged-op-ids.txt"))
	_ = lab.waitForWorkloadRows(ctx, "jepsen.append_values", runID, expected)
	finalPrimary, finalService := lab.finalPrimaryService(ctx)
	actual, actualErr := lab.queryIntResult(ctx, finalService, fmt.Sprintf("SELECT count(*) FROM jepsen.append_values WHERE run_id = %s;", sqlLiteral(runID)))
	duplicates, duplicateErr := lab.queryIntResult(ctx, finalService, fmt.Sprintf("SELECT count(*) FROM (SELECT op_id FROM jepsen.append_values WHERE run_id = %s GROUP BY op_id HAVING count(*) > 1) dup;", sqlLiteral(runID)))
	result := map[string]any{"checker": "append", "expectedAcknowledged": expected, "actualRows": actual, "duplicateOpIds": duplicates, "finalPrimary": finalPrimary, "finalPrimaryService": finalService}
	if actualErr != nil {
		result["actualError"] = actualErr.Error()
	}
	if duplicateErr != nil {
		result["duplicateError"] = duplicateErr.Error()
	}
	writeJSON(filepath.Join(caseDir, "checker.json"), result)
	if actualErr != nil {
		return fmt.Errorf("append checker failed querying final primary %s: %w", finalPrimary, actualErr)
	}
	if duplicateErr != nil {
		return fmt.Errorf("append checker failed querying duplicates on final primary %s: %w", finalPrimary, duplicateErr)
	}
	if expected <= 0 || actual != expected || duplicates != 0 {
		return fmt.Errorf("append checker failed expected=%d actual=%d duplicates=%d", expected, actual, duplicates)
	}
	return nil
}

func (lab *harnessLab) checkRegisterWorkload(ctx context.Context, runID, caseDir string) error {
	expected := countLines(filepath.Join(caseDir, "acknowledged-op-ids.txt"))
	_ = lab.waitForWorkloadRows(ctx, "jepsen.register_values", runID, expected)
	finalPrimary, finalService := lab.finalPrimaryService(ctx)
	actual, actualErr := lab.queryIntResult(ctx, finalService, fmt.Sprintf("SELECT count(*) FROM jepsen.register_values WHERE run_id = %s;", sqlLiteral(runID)))
	maxValue, maxErr := lab.queryIntResult(ctx, finalService, fmt.Sprintf("SELECT COALESCE(max(value), 0) FROM jepsen.register_values WHERE run_id = %s;", sqlLiteral(runID)))
	result := map[string]any{"checker": "single-key-register", "expectedAcknowledged": expected, "actualRows": actual, "maxValue": maxValue, "finalPrimary": finalPrimary, "finalPrimaryService": finalService}
	if actualErr != nil {
		result["actualError"] = actualErr.Error()
	}
	if maxErr != nil {
		result["maxError"] = maxErr.Error()
	}
	writeJSON(filepath.Join(caseDir, "checker.json"), result)
	if actualErr != nil {
		return fmt.Errorf("register checker failed querying final primary %s: %w", finalPrimary, actualErr)
	}
	if maxErr != nil {
		return fmt.Errorf("register checker failed querying max on final primary %s: %w", finalPrimary, maxErr)
	}
	if expected <= 0 || actual != expected || maxValue < expected {
		return fmt.Errorf("register checker failed expected=%d actual=%d max=%d", expected, actual, maxValue)
	}
	return nil
}

func (lab *harnessLab) checkTxnWorkload(ctx context.Context, runID, caseDir, checker string) error {
	expected := countLines(filepath.Join(caseDir, "acknowledged-op-ids.txt"))
	_ = lab.waitForWorkloadRows(ctx, "jepsen.txn_ops", runID, expected)
	finalPrimary, finalService := lab.finalPrimaryService(ctx)
	opCount, opErr := lab.queryIntResult(ctx, finalService, fmt.Sprintf("SELECT count(*) FROM jepsen.txn_ops WHERE run_id = %s;", sqlLiteral(runID)))
	total, totalErr := lab.queryIntResult(ctx, finalService, fmt.Sprintf("SELECT COALESCE(sum(balance), 0) FROM jepsen.txn_accounts WHERE run_id = %s;", sqlLiteral(runID)))
	result := map[string]any{"checker": checker, "expectedAcknowledged": expected, "actualOps": opCount, "accountTotal": total, "finalPrimary": finalPrimary, "finalPrimaryService": finalService}
	if opErr != nil {
		result["actualOpsError"] = opErr.Error()
	}
	if totalErr != nil {
		result["accountTotalError"] = totalErr.Error()
	}
	writeJSON(filepath.Join(caseDir, "checker.json"), result)
	if opErr != nil {
		return fmt.Errorf("txn checker failed querying ops on final primary %s: %w", finalPrimary, opErr)
	}
	if totalErr != nil {
		return fmt.Errorf("txn checker failed querying total on final primary %s: %w", finalPrimary, totalErr)
	}
	if expected <= 0 || opCount != expected || total != expected {
		return fmt.Errorf("txn checker failed expected=%d ops=%d total=%d", expected, opCount, total)
	}
	return nil
}

func (lab *harnessLab) queryIntResult(ctx context.Context, service, sql string) (int, error) {
	output, err := lab.psqlService(ctx, service, sql)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(strings.TrimSpace(lastNonEmptyLine(output)))
	if err != nil {
		return 0, fmt.Errorf("parse integer query result %q: %w", lastNonEmptyLine(output), err)
	}
	return value, nil
}

func (lab *harnessLab) finalPrimaryService(ctx context.Context) (string, string) {
	finalPrimary := lab.currentPrimaryName(ctx)
	if finalPrimary == "" || finalPrimary == "unknown" {
		finalPrimary = lab.options.target.firstDataMember()
	}
	finalService := lab.serviceForMember(finalPrimary)
	if finalService == "" {
		finalService = lab.options.target.firstDataService()
	}
	return finalPrimary, finalService
}

func (lab *harnessLab) waitForWorkloadRows(ctx context.Context, table, runID string, expected int) bool {
	if expected <= 0 || lab.cfg.workloadVisibilityTimeout <= 0 {
		return false
	}
	deadline := time.Now().Add(lab.cfg.workloadVisibilityTimeout)
	query := fmt.Sprintf("SELECT count(*) FROM %s WHERE run_id = %s;", table, sqlLiteral(runID))
	for {
		_, finalService := lab.finalPrimaryService(ctx)
		actual, err := lab.queryIntResult(ctx, finalService, query)
		if err == nil && actual >= expected {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(lab.cfg.workloadVisibilityInterval):
		}
	}
}

func countLines(path string) int {
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return 0
	}
	count := 0
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			count++
		}
	}
	return count
}

func lastNonEmptyLine(value string) string {
	lines := strings.Split(value, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}

func writeJSON(path string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(path, append(data, '\n'), 0o644)
}
