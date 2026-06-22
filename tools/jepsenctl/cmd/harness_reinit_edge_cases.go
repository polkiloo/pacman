package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type reinitPostRestoreState struct {
	SystemIdentifier    string
	Timeline            int64
	InRecovery          bool
	WALReceiverStatus   string
	WALReceiverConninfo string
}

func (lab *harnessLab) reinitReplicaWithLag(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis: "reinit-replica-with-lag",
		Reason:  "jepsen-reinit-replica-with-lag",
		BeforeRequest: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			return lab.prepareReinitLaggingTarget(ctx, run)
		},
		AfterWait: func(ctx context.Context, run reinitRunContext, wait reinitWaitResult) reinitVariantResult {
			if !wait.Valid {
				return reinitVariantResult{Valid: true, Details: map[string]any{"skipped": "wait did not reach successful reinit"}}
			}
			check := lab.verifyReinitLaggingTarget(ctx, run, wait)
			return reinitVariantResult{Valid: check.Valid, Error: check.Error, Details: map[string]any{"verification": check}}
		},
	})
}

func (lab *harnessLab) prepareReinitLaggingTarget(ctx context.Context, run reinitRunContext) reinitVariantResult {
	result := reinitVariantResult{Valid: true, Details: map[string]any{
		"targetService": run.TargetService,
		"sourceService": run.SourceService,
	}}
	if run.TargetService == "" {
		result.Valid = false
		result.Error = fmt.Sprintf("target service is unknown for %s", run.Target)
		return result
	}
	if run.SourceService == "" {
		result.Valid = false
		result.Error = fmt.Sprintf("source service is unknown for %s", run.Source)
		return result
	}

	if _, err := lab.psqlService(ctx, run.TargetService, "SELECT pg_wal_replay_pause();"); err != nil {
		result.Valid = false
		result.Error = err.Error()
		return result
	}
	paused := true
	defer func() {
		if paused && !result.Valid {
			_, _ = lab.psqlService(ctx, run.TargetService, "SELECT pg_wal_replay_resume();")
		}
	}()

	if _, err := lab.psqlService(ctx, run.SourceService, reinitReplicaLagWALSQL()); err != nil {
		result.Valid = false
		result.Error = err.Error()
		return result
	}

	state, err := lab.waitForReinitLagState(ctx, run.Target, run.TargetService, maxDuration(lab.cfg.synchronousStandbyTimeout, 30*time.Second), func(state reinitLagState) bool {
		return state.ReplayPaused && state.LagBytes > 0
	})
	result.Details["lagState"] = state
	if err != nil {
		result.Valid = false
		result.Error = err.Error()
		return result
	}
	paused = false
	return result
}

func reinitReplicaLagWALSQL() string {
	return fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS jepsen;
CREATE TABLE IF NOT EXISTS jepsen.reinit_lag_probes (
  probe_id text PRIMARY KEY,
  payload text NOT NULL
);
INSERT INTO jepsen.reinit_lag_probes(probe_id, payload)
SELECT %s, string_agg(md5(value::text), '')
FROM generate_series(1, 8192) AS value;
SELECT pg_switch_wal();`, sqlLiteral(fmt.Sprintf("reinit-lag-%d", time.Now().UnixNano())))
}

func (lab *harnessLab) reinitLagState(ctx context.Context, member, service string) (reinitLagState, error) {
	output, err := lab.psqlService(ctx, service, `
SELECT
  pg_is_wal_replay_paused(),
  coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0)::bigint,
  coalesce(pg_last_wal_receive_lsn()::text, ''),
  coalesce(pg_last_wal_replay_lsn()::text, '');`)
	if err != nil {
		return reinitLagState{}, err
	}
	return parseReinitLagState(member, service, output)
}

func parseReinitLagState(member, service, output string) (reinitLagState, error) {
	parts := strings.Split(lastNonEmptyLine(output), "\t")
	if len(parts) != 4 {
		return reinitLagState{}, fmt.Errorf("decode reinit lag state %q", output)
	}
	lagBytes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return reinitLagState{}, fmt.Errorf("decode reinit lag bytes %q: %w", parts[1], err)
	}
	return reinitLagState{
		Member:       member,
		Service:      service,
		ReplayPaused: parts[0] == "t",
		LagBytes:     lagBytes,
		ReceiveLSN:   parts[2],
		ReplayLSN:    parts[3],
	}, nil
}

func (lab *harnessLab) waitForReinitLagState(ctx context.Context, member, service string, timeout time.Duration, predicate func(reinitLagState) bool) (reinitLagState, error) {
	deadline := time.Now().Add(timeout)
	var lastState reinitLagState
	var lastErr error
	for {
		lastState, lastErr = lab.reinitLagState(ctx, member, service)
		if lastErr == nil && predicate(lastState) {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for reinit lag state on %s: %w", member, lastErr)
			}
			return lastState, fmt.Errorf("wait for reinit lag state on %s: got %+v", member, lastState)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(lab.cfg.clusterVerifyInterval):
		}
	}
}

func (lab *harnessLab) verifyReinitLaggingTarget(ctx context.Context, run reinitRunContext, wait reinitWaitResult) reinitLagVerification {
	check := reinitLagVerification{
		Target:        run.Target,
		Source:        run.Source,
		TargetService: run.TargetService,
		SourceService: run.SourceService,
		ExpectedSlot:  reinitExpectedSlotName(run.Target),
	}
	if wait.FinalStatus == nil {
		check.Error = "reinit wait result did not include final cluster status"
		return check
	}

	primarySystemID, primaryTimeline, err := lab.reinitPrimaryIdentity(ctx, run.SourceService)
	if err != nil {
		check.Error = err.Error()
		return check
	}
	targetState, err := lab.reinitTargetPostRestoreState(ctx, run.TargetService)
	if err != nil {
		check.Error = err.Error()
		return check
	}
	slot, ok, err := lab.reinitPrimarySlot(ctx, run.SourceService, check.ExpectedSlot)
	if err != nil {
		check.Error = err.Error()
		return check
	}

	check.PrimarySystemIdentifier = primarySystemID
	check.TargetSystemIdentifier = targetState.SystemIdentifier
	check.PrimaryTimeline = primaryTimeline
	check.TargetTimeline = targetState.Timeline
	check.TargetInRecovery = targetState.InRecovery
	check.WALReceiverStatus = targetState.WALReceiverStatus
	check.WALReceiverConninfo = targetState.WALReceiverConninfo
	check.StreamingSource = wait.FinalStatus.CurrentPrimary
	if ok {
		check.SlotActive = slot.Active
		check.SlotRestartLSN = slot.RestartLSN
	}
	return checkReinitLagVerification(*wait.FinalStatus, check, run.OperationID)
}

func (lab *harnessLab) reinitPrimaryIdentity(ctx context.Context, service string) (string, int64, error) {
	output, err := lab.psqlService(ctx, service, `
SELECT
  system_identifier::text,
  (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint)::text
FROM pg_control_system();`)
	if err != nil {
		return "", 0, err
	}
	parts := strings.Split(lastNonEmptyLine(output), "\t")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("decode primary identity %q", output)
	}
	timeline, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("decode primary timeline %q: %w", parts[1], err)
	}
	return parts[0], timeline, nil
}

func (lab *harnessLab) reinitTargetPostRestoreState(ctx context.Context, service string) (reinitPostRestoreState, error) {
	output, err := lab.psqlService(ctx, service, `
SELECT
  system_identifier::text,
  (CASE
     WHEN pg_is_in_recovery() THEN (pg_control_checkpoint()).timeline_id
     ELSE (('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint)
   END)::text,
  pg_is_in_recovery(),
  coalesce((SELECT status FROM pg_stat_wal_receiver LIMIT 1), ''),
  coalesce((SELECT conninfo FROM pg_stat_wal_receiver LIMIT 1), '')
FROM pg_control_system();`)
	if err != nil {
		return reinitPostRestoreState{}, err
	}
	return parseReinitPostRestoreState(output)
}

func parseReinitPostRestoreState(output string) (reinitPostRestoreState, error) {
	parts := strings.Split(lastNonEmptyLine(output), "\t")
	if len(parts) != 5 {
		return reinitPostRestoreState{}, fmt.Errorf("decode post-restore state %q", output)
	}
	timeline, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return reinitPostRestoreState{}, fmt.Errorf("decode post-restore timeline %q: %w", parts[1], err)
	}
	return reinitPostRestoreState{
		SystemIdentifier:    parts[0],
		Timeline:            timeline,
		InRecovery:          parts[2] == "t",
		WALReceiverStatus:   parts[3],
		WALReceiverConninfo: parts[4],
	}, nil
}

func (lab *harnessLab) reinitPrimarySlot(ctx context.Context, service, slotName string) (reinitSlotStatus, bool, error) {
	output, err := lab.psqlService(ctx, service, fmt.Sprintf("SELECT slot_name, active, coalesce(restart_lsn::text, '') FROM pg_replication_slots WHERE slot_name = %s;", sqlLiteral(slotName)))
	if err != nil {
		return reinitSlotStatus{}, false, err
	}
	slots := parseReinitSlotStatus(output)
	slot, ok := findReinitSlot(slots, slotName)
	return slot, ok, nil
}

func checkReinitLagVerification(status clusterStatus, check reinitLagVerification, operationID string) reinitLagVerification {
	check.Valid = false
	switch {
	case !reinitComplete(status, check.Target, check.Source, operationID):
		check.Error = "final cluster status did not show completed reinit with a healthy streaming target"
	case check.PrimarySystemIdentifier == "" || check.TargetSystemIdentifier == "":
		check.Error = "system identifier was empty after reinit"
	case check.PrimarySystemIdentifier != check.TargetSystemIdentifier:
		check.Error = fmt.Sprintf("target system identifier %s does not match source %s", check.TargetSystemIdentifier, check.PrimarySystemIdentifier)
	case check.PrimaryTimeline <= 0 || check.TargetTimeline <= 0:
		check.Error = "timeline was not positive after reinit"
	case check.PrimaryTimeline != check.TargetTimeline:
		check.Error = fmt.Sprintf("target timeline %d does not match source timeline %d", check.TargetTimeline, check.PrimaryTimeline)
	case !check.SlotActive:
		check.Error = fmt.Sprintf("expected replication slot %s is not active", check.ExpectedSlot)
	case check.SlotRestartLSN == "":
		check.Error = fmt.Sprintf("expected replication slot %s has no restart_lsn", check.ExpectedSlot)
	case !check.TargetInRecovery:
		check.Error = fmt.Sprintf("target %s is not in recovery after reinit", check.Target)
	case check.WALReceiverStatus != "streaming":
		check.Error = fmt.Sprintf("target WAL receiver status is %q, want streaming", check.WALReceiverStatus)
	case check.StreamingSource != check.Source:
		check.Error = fmt.Sprintf("target is streaming from cluster primary %q, want %q", check.StreamingSource, check.Source)
	default:
		check.Valid = true
		check.Error = ""
	}
	return check
}

func (lab *harnessLab) reinitReplicaWALGFetchFailure(ctx context.Context, caseDir, scheduleFile string) error {
	restoreService := ""
	err := lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis:       "reinit-replica-walg-fetch-failure",
		Reason:        "jepsen-reinit-walg-fetch-failure",
		WaitForResult: lab.waitForReinitWALGFetchFailure,
		BeforeRequest: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			restoreService = run.TargetService
			return lab.injectReinitWALGFetchFailure(ctx, run.TargetService)
		},
	})
	if restoreService == "" {
		return err
	}
	restore := lab.restoreReinitWALGShim(ctx, restoreService)
	if !restore.Valid {
		if err != nil {
			return fmt.Errorf("%w; restore WAL-G shim after reinit failure: %s", err, restore.Error)
		}
		return fmt.Errorf("restore WAL-G shim after reinit failure: %s", restore.Error)
	}
	return err
}

const injectReinitWALGFetchFailureCommand = `set -eu
if [ ! -x /usr/local/bin/pacman-lab-wal-g ]; then
  echo "missing /usr/local/bin/pacman-lab-wal-g" >&2
  exit 1
fi
if [ ! -f /usr/local/bin/pacman-lab-wal-g.jepsen.bak ]; then
  cp /usr/local/bin/pacman-lab-wal-g /usr/local/bin/pacman-lab-wal-g.jepsen.bak
fi
cat > /usr/local/bin/pacman-lab-wal-g <<'PACMAN_JEPSEN_WALG'
#!/usr/bin/env bash
set -eu
if [ "${1:-}" = "backup-fetch" ]; then
  echo "jepsen injected wal-g backup-fetch failure" >&2
  exit 42
fi
exec /usr/local/bin/pacman-lab-wal-g.jepsen.bak "$@"
PACMAN_JEPSEN_WALG
chmod 0755 /usr/local/bin/pacman-lab-wal-g`

const restoreReinitWALGShimCommand = `set -eu
if [ -f /usr/local/bin/pacman-lab-wal-g.jepsen.bak ]; then
  mv /usr/local/bin/pacman-lab-wal-g.jepsen.bak /usr/local/bin/pacman-lab-wal-g
  chmod 0755 /usr/local/bin/pacman-lab-wal-g
fi`

func (lab *harnessLab) injectReinitWALGFetchFailure(ctx context.Context, service string) reinitVariantResult {
	result := reinitVariantResult{Valid: true, Details: map[string]any{"targetService": service}}
	if service == "" {
		result.Valid = false
		result.Error = "target service is unknown for WAL-G failure injection"
		return result
	}
	output, status, err := lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", injectReinitWALGFetchFailureCommand)
	result.Details["exitStatus"] = status
	result.Details["output"] = output
	if err != nil || status != 0 {
		result.Valid = false
		result.Error = fmt.Sprintf("inject WAL-G backup-fetch failure on %s failed with status %d: %s", service, status, strings.TrimSpace(output))
	}
	return result
}

func (lab *harnessLab) restoreReinitWALGShim(ctx context.Context, service string) reinitVariantResult {
	result := reinitVariantResult{Valid: true, Details: map[string]any{"targetService": service}}
	output, status, err := lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", restoreReinitWALGShimCommand)
	result.Details["exitStatus"] = status
	result.Details["output"] = output
	if err != nil || status != 0 {
		result.Valid = false
		result.Error = fmt.Sprintf("restore WAL-G shim on %s failed with status %d: %s", service, status, strings.TrimSpace(output))
	}
	return result
}
