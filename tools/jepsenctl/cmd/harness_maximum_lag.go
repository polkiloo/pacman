package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	maximumLagOnFailoverConfigFile        = "maximum-lag-on-failover-config.json"
	maximumLagOnFailoverProbesFile        = "maximum-lag-on-failover-probes.jsonl"
	maximumLagOnFailoverCheckerFile       = "maximum-lag-on-failover-checker.json"
	maximumLagOnFailoverNemesis           = "lagging-replica-failover"
	maximumLagOnFailoverBytes       int64 = 1024
)

type patroniMaximumLagOnFailoverSettings struct {
	MaximumLagBytes int64 `json:"maximumLagBytes"`
}

type patroniMaximumLagOnFailoverReplicaState struct {
	Member       string `json:"member"`
	Service      string `json:"service"`
	ReplayPaused bool   `json:"replayPaused"`
	LagBytes     int64  `json:"lagBytes"`
}

type patroniMaximumLagOnFailoverProbe struct {
	ObservedAt      string                                  `json:"observedAt"`
	Phase           string                                  `json:"phase"`
	MaximumLagBytes int64                                   `json:"maximumLagBytes"`
	OldPrimary      string                                  `json:"oldPrimary"`
	LaggingReplica  string                                  `json:"laggingReplica"`
	EligibleReplica string                                  `json:"eligibleReplica"`
	Promoted        string                                  `json:"promoted,omitempty"`
	State           patroniMaximumLagOnFailoverReplicaState `json:"state"`
}

func resolvePatroniMaximumLagOnFailoverProfile(workload string) (patroniMaximumLagOnFailoverSettings, bool) {
	if workload != "append-max-lag" {
		return patroniMaximumLagOnFailoverSettings{}, false
	}
	return patroniMaximumLagOnFailoverSettings{MaximumLagBytes: maximumLagOnFailoverBytes}, true
}

func (lab *harnessLab) prepareMaximumLagOnFailoverProfile(ctx context.Context, workload, caseDir string, settings patroniMaximumLagOnFailoverSettings) error {
	if !lab.options.target.supportsPatroniLab() {
		return fmt.Errorf("workload profile %s requires the Patroni baseline target", workload)
	}

	state, err := lab.configurePatroniMaximumLagOnFailover(ctx, settings)
	result := map[string]any{
		"workload": workload,
		"profile":  settings,
		"state":    state,
	}
	if err != nil {
		result["error"] = err.Error()
	}
	writeJSON(filepath.Join(caseDir, maximumLagOnFailoverConfigFile), result)
	return err
}

func (lab *harnessLab) configurePatroniMaximumLagOnFailover(ctx context.Context, settings patroniMaximumLagOnFailoverSettings) (patroniMaximumLagOnFailoverSettings, error) {
	payload := fmt.Sprintf(`{"maximum_lag_on_failover":%d}`, settings.MaximumLagBytes)
	service := lab.options.target.firstDataService()
	output, status, err := lab.composeExec(ctx, service,
		"curl", "-fsS", "-X", "PATCH",
		"-H", "Content-Type: application/json",
		"-d", payload,
		"http://127.0.0.1:8008/config",
	)
	if err != nil {
		return patroniMaximumLagOnFailoverSettings{}, err
	}
	if status != 0 {
		return patroniMaximumLagOnFailoverSettings{}, fmt.Errorf("configure Patroni maximum lag on failover failed with status %d: %s", status, strings.TrimSpace(output))
	}

	return lab.waitForPatroniMaximumLagOnFailover(ctx, settings.MaximumLagBytes, lab.cfg.synchronousStandbyTimeout)
}

func (lab *harnessLab) waitForPatroniMaximumLagOnFailover(ctx context.Context, want int64, timeout time.Duration) (patroniMaximumLagOnFailoverSettings, error) {
	deadline := time.Now().Add(timeout)
	var lastState patroniMaximumLagOnFailoverSettings
	var lastErr error
	for {
		lastState, lastErr = lab.patroniMaximumLagOnFailoverSettings(ctx)
		if lastErr == nil && lastState.MaximumLagBytes == want {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for Patroni maximum_lag_on_failover=%d: %w", want, lastErr)
			}
			return lastState, fmt.Errorf("wait for Patroni maximum_lag_on_failover=%d: got %d", want, lastState.MaximumLagBytes)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(lab.cfg.synchronousStandbyInterval):
		}
	}
}

func (lab *harnessLab) patroniMaximumLagOnFailoverSettings(ctx context.Context) (patroniMaximumLagOnFailoverSettings, error) {
	service := lab.serviceForMember(lab.currentPrimaryName(ctx))
	if service == "" {
		return patroniMaximumLagOnFailoverSettings{}, fmt.Errorf("Patroni primary is unavailable")
	}
	output, status, err := lab.composeExec(ctx, service, "curl", "-fsS", "http://127.0.0.1:8008/config")
	if err != nil {
		return patroniMaximumLagOnFailoverSettings{}, err
	}
	if status != 0 {
		return patroniMaximumLagOnFailoverSettings{}, fmt.Errorf("read Patroni config failed with status %d: %s", status, strings.TrimSpace(output))
	}
	var config struct {
		MaximumLagBytes int64 `json:"maximum_lag_on_failover"`
	}
	if err := json.Unmarshal([]byte(output), &config); err != nil {
		return patroniMaximumLagOnFailoverSettings{}, fmt.Errorf("decode Patroni config: %w", err)
	}
	return patroniMaximumLagOnFailoverSettings{MaximumLagBytes: config.MaximumLagBytes}, nil
}

func (lab *harnessLab) maximumLagOnFailover(ctx context.Context, caseDir, scheduleFile string) error {
	settings, _ := resolvePatroniMaximumLagOnFailoverProfile("append-max-lag")
	oldPrimary := lab.currentPrimaryName(ctx)
	oldPrimaryService := lab.serviceForMember(oldPrimary)
	lagging, eligible, err := lab.maximumLagOnFailoverReplicas(oldPrimary)
	if err != nil {
		return err
	}

	event := func(action, value string) {
		writeNemesisScheduleEvent(scheduleFile, maximumLagOnFailoverNemesis, action, value)
	}
	event("start", fmt.Sprintf(":target %q :lagging-replica %q :eligible-replica %q", oldPrimary, lagging.Name, eligible.Name))

	replayPaused := false
	oldPrimaryStopped := false
	cleanup := func() error {
		var failures []string
		if oldPrimaryStopped {
			if err := lab.startNodeRuntime(ctx, oldPrimaryService); err != nil {
				failures = append(failures, err.Error())
			} else {
				oldPrimaryStopped = false
			}
		}
		if replayPaused {
			if _, err := lab.psqlService(ctx, lagging.Service, `SELECT pg_wal_replay_resume();`); err != nil {
				failures = append(failures, err.Error())
			} else {
				replayPaused = false
			}
		}
		if len(failures) > 0 {
			return fmt.Errorf("clean up maximum lag failover: %s", strings.Join(failures, "; "))
		}
		return nil
	}
	fail := func(cause error) error {
		if cleanupErr := cleanup(); cleanupErr != nil {
			cause = errors.Join(cause, cleanupErr)
		}
		event("stop", fmt.Sprintf(":target %q :lagging-replica %q :eligible-replica %q :result :fail :error %q", oldPrimary, lagging.Name, eligible.Name, cause))
		return cause
	}

	before, err := lab.patroniMaximumLagOnFailoverReplicaState(ctx, lagging)
	if err != nil {
		return fail(err)
	}
	lab.recordMaximumLagOnFailoverProbe(caseDir, "before-pause", settings.MaximumLagBytes, oldPrimary, lagging.Name, eligible.Name, "", before)

	if _, err := lab.psqlService(ctx, lagging.Service, `SELECT pg_wal_replay_pause();`); err != nil {
		return fail(err)
	}
	replayPaused = true
	if _, err := lab.psqlService(ctx, oldPrimaryService, maximumLagOnFailoverWALSQL()); err != nil {
		return fail(err)
	}

	lagged, err := lab.waitForPatroniMaximumLagOnFailoverReplicaState(ctx, lagging, lab.cfg.synchronousStandbyTimeout, func(state patroniMaximumLagOnFailoverReplicaState) bool {
		return state.ReplayPaused && state.LagBytes > settings.MaximumLagBytes
	})
	if err != nil {
		return fail(err)
	}
	lab.recordMaximumLagOnFailoverProbe(caseDir, "lagged", settings.MaximumLagBytes, oldPrimary, lagging.Name, eligible.Name, "", lagged)

	if err := lab.stopNodeRuntime(ctx, oldPrimaryService); err != nil {
		return fail(err)
	}
	oldPrimaryStopped = true
	promoted := lab.waitForCurrentPrimaryNot(ctx, oldPrimary, 90*time.Second)
	if promoted == "unknown" {
		return fail(fmt.Errorf("timed out waiting for promotion after stopping %s", oldPrimary))
	}
	afterPromotion, err := lab.patroniMaximumLagOnFailoverReplicaState(ctx, lagging)
	if err != nil {
		return fail(err)
	}
	lab.recordMaximumLagOnFailoverProbe(caseDir, "after-promotion", settings.MaximumLagBytes, oldPrimary, lagging.Name, eligible.Name, promoted, afterPromotion)
	_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", maximumLagOnFailoverNemesis, lagging.Name, lagging.Service)
	time.Sleep(lab.cfg.nemesisHold)

	if err := cleanup(); err != nil {
		return fail(err)
	}
	afterResume, err := lab.waitForPatroniMaximumLagOnFailoverReplicaState(ctx, lagging, lab.cfg.synchronousStandbyTimeout, func(state patroniMaximumLagOnFailoverReplicaState) bool {
		return !state.ReplayPaused
	})
	if err != nil {
		return fail(err)
	}
	lab.recordMaximumLagOnFailoverProbe(caseDir, "after-resume", settings.MaximumLagBytes, oldPrimary, lagging.Name, eligible.Name, promoted, afterResume)
	event("stop", fmt.Sprintf(":target %q :lagging-replica %q :eligible-replica %q :promoted %q :result :ok", oldPrimary, lagging.Name, eligible.Name, promoted))
	return nil
}

func (lab *harnessLab) maximumLagOnFailoverReplicas(primary string) (targetNode, targetNode, error) {
	var replicas []targetNode
	for _, node := range lab.options.target.DataNodes {
		if node.Name != primary {
			replicas = append(replicas, node)
		}
	}
	if len(replicas) != 2 {
		return targetNode{}, targetNode{}, fmt.Errorf("maximum lag failover requires exactly two replicas, got %d", len(replicas))
	}
	return replicas[0], replicas[1], nil
}

func maximumLagOnFailoverWALSQL() string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS jepsen.maximum_lag_probes (
  probe_id text PRIMARY KEY,
  payload text NOT NULL
);
INSERT INTO jepsen.maximum_lag_probes(probe_id, payload)
SELECT %s, string_agg(md5(value::text), '')
FROM generate_series(1, 8192) AS value;
SELECT pg_switch_wal();`, sqlLiteral(fmt.Sprintf("maximum-lag-%d", time.Now().UnixNano())))
}

func (lab *harnessLab) patroniMaximumLagOnFailoverReplicaState(ctx context.Context, node targetNode) (patroniMaximumLagOnFailoverReplicaState, error) {
	output, err := lab.psqlService(ctx, node.Service, `
SELECT
  pg_is_wal_replay_paused(),
  coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0)::bigint;`)
	if err != nil {
		return patroniMaximumLagOnFailoverReplicaState{}, err
	}
	parts := strings.Split(lastNonEmptyLine(output), "\t")
	if len(parts) != 2 {
		return patroniMaximumLagOnFailoverReplicaState{}, fmt.Errorf("decode replica lag state %q", output)
	}
	lagBytes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return patroniMaximumLagOnFailoverReplicaState{}, fmt.Errorf("decode replica lag bytes %q: %w", parts[1], err)
	}
	return patroniMaximumLagOnFailoverReplicaState{
		Member:       node.Name,
		Service:      node.Service,
		ReplayPaused: parts[0] == "t",
		LagBytes:     lagBytes,
	}, nil
}

func (lab *harnessLab) waitForPatroniMaximumLagOnFailoverReplicaState(ctx context.Context, node targetNode, timeout time.Duration, predicate func(patroniMaximumLagOnFailoverReplicaState) bool) (patroniMaximumLagOnFailoverReplicaState, error) {
	deadline := time.Now().Add(timeout)
	var lastState patroniMaximumLagOnFailoverReplicaState
	var lastErr error
	for {
		lastState, lastErr = lab.patroniMaximumLagOnFailoverReplicaState(ctx, node)
		if lastErr == nil && predicate(lastState) {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for Patroni replica %s lag state: %w", node.Name, lastErr)
			}
			return lastState, fmt.Errorf("wait for Patroni replica %s lag state: got %+v", node.Name, lastState)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(lab.cfg.synchronousStandbyInterval):
		}
	}
}

func (lab *harnessLab) recordMaximumLagOnFailoverProbe(caseDir, phase string, maximumLagBytes int64, oldPrimary, laggingReplica, eligibleReplica, promoted string, state patroniMaximumLagOnFailoverReplicaState) {
	appendJSONL(filepath.Join(caseDir, maximumLagOnFailoverProbesFile), patroniMaximumLagOnFailoverProbe{
		ObservedAt:      time.Now().UTC().Format(time.RFC3339),
		Phase:           phase,
		MaximumLagBytes: maximumLagBytes,
		OldPrimary:      oldPrimary,
		LaggingReplica:  laggingReplica,
		EligibleReplica: eligibleReplica,
		Promoted:        promoted,
		State:           state,
	})
}

func checkMaximumLagOnFailoverProbes(probes []patroniMaximumLagOnFailoverProbe) error {
	byPhase := make(map[string]patroniMaximumLagOnFailoverProbe)
	for _, probe := range probes {
		byPhase[probe.Phase] = probe
	}
	before, beforeOK := byPhase["before-pause"]
	lagged, laggedOK := byPhase["lagged"]
	afterPromotion, afterPromotionOK := byPhase["after-promotion"]
	afterResume, afterResumeOK := byPhase["after-resume"]
	if !beforeOK || !laggedOK || !afterPromotionOK || !afterResumeOK {
		return fmt.Errorf("maximum lag on failover probes missing required phases")
	}
	for _, probe := range []patroniMaximumLagOnFailoverProbe{lagged, afterPromotion, afterResume} {
		if probe.MaximumLagBytes != before.MaximumLagBytes ||
			probe.OldPrimary != before.OldPrimary ||
			probe.LaggingReplica != before.LaggingReplica ||
			probe.EligibleReplica != before.EligibleReplica {
			return fmt.Errorf("maximum lag on failover probes do not identify one failover scenario")
		}
	}
	if before.MaximumLagBytes <= 0 {
		return fmt.Errorf("maximum lag on failover threshold must be positive")
	}
	if before.State.ReplayPaused || !lagged.State.ReplayPaused || lagged.State.LagBytes <= before.MaximumLagBytes {
		return fmt.Errorf("maximum lag on failover probe did not pause replica %s above threshold %d", before.LaggingReplica, before.MaximumLagBytes)
	}
	if afterPromotion.Promoted != before.EligibleReplica || afterPromotion.Promoted == before.LaggingReplica {
		return fmt.Errorf("maximum lag on failover promoted %s; want eligible replica %s instead of lagging replica %s", afterPromotion.Promoted, before.EligibleReplica, before.LaggingReplica)
	}
	if afterResume.State.ReplayPaused {
		return fmt.Errorf("maximum lag on failover did not resume replica replay after promotion")
	}
	return nil
}

func readMaximumLagOnFailoverProbes(path string) []patroniMaximumLagOnFailoverProbe {
	rows := readJSONL(path)
	probes := make([]patroniMaximumLagOnFailoverProbe, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}
		var probe patroniMaximumLagOnFailoverProbe
		if json.Unmarshal(data, &probe) == nil {
			probes = append(probes, probe)
		}
	}
	return probes
}
