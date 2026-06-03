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
	patroniCheckTimelineConfigFile  = "patroni-check-timeline-config.json"
	patroniCheckTimelineProbesFile  = "patroni-check-timeline-probes.jsonl"
	patroniCheckTimelineCheckerFile = "patroni-check-timeline-checker.json"
	patroniCheckTimelineNemesis     = "stale-timeline-failover"
	patroniCheckTimelineHold        = 20 * time.Second
)

type patroniCheckTimelineSettings struct {
	CheckTimeline bool `json:"checkTimeline"`
}

type patroniCheckTimelineNodeState struct {
	Member     string `json:"member"`
	Service    string `json:"service"`
	Reachable  bool   `json:"reachable"`
	Writable   bool   `json:"writable"`
	InRecovery bool   `json:"inRecovery"`
	Timeline   int    `json:"timeline"`
	Error      string `json:"error,omitempty"`
}

type patroniCheckTimelineProbe struct {
	ObservedAt        string                        `json:"observedAt"`
	Phase             string                        `json:"phase"`
	OldPrimary        string                        `json:"oldPrimary"`
	StaleReplica      string                        `json:"staleReplica"`
	EligibleReplica   string                        `json:"eligibleReplica"`
	CurrentPrimary    string                        `json:"currentPrimary"`
	PrimaryTimeline   int                           `json:"primaryTimeline"`
	StaleReplicaState patroniCheckTimelineNodeState `json:"staleReplicaState"`
}

func resolvePatroniCheckTimelineProfile(workload string) (patroniCheckTimelineSettings, bool) {
	if workload != "append-check-timeline" {
		return patroniCheckTimelineSettings{}, false
	}
	return patroniCheckTimelineSettings{CheckTimeline: true}, true
}

func (lab *harnessLab) preparePatroniCheckTimelineProfile(ctx context.Context, workload, caseDir string, settings patroniCheckTimelineSettings) error {
	if !lab.options.target.supportsPatroniLab() {
		return fmt.Errorf("workload profile %s requires the Patroni baseline target", workload)
	}

	state, err := lab.configurePatroniCheckTimeline(ctx, settings)
	result := map[string]any{
		"workload": workload,
		"profile":  settings,
		"state":    state,
	}
	if err != nil {
		result["error"] = err.Error()
	}
	writeJSON(filepath.Join(caseDir, patroniCheckTimelineConfigFile), result)
	return err
}

func (lab *harnessLab) configurePatroniCheckTimeline(ctx context.Context, settings patroniCheckTimelineSettings) (patroniCheckTimelineSettings, error) {
	payload := fmt.Sprintf(`{"check_timeline":%t}`, settings.CheckTimeline)
	service := lab.options.target.firstDataService()
	output, status, err := lab.composeExec(ctx, service,
		"curl", "-fsS", "-X", "PATCH",
		"-H", "Content-Type: application/json",
		"-d", payload,
		"http://127.0.0.1:8008/config",
	)
	if err != nil {
		return patroniCheckTimelineSettings{}, err
	}
	if status != 0 {
		return patroniCheckTimelineSettings{}, fmt.Errorf("configure Patroni check_timeline failed with status %d: %s", status, strings.TrimSpace(output))
	}

	return lab.waitForPatroniCheckTimeline(ctx, settings.CheckTimeline, lab.cfg.synchronousStandbyTimeout)
}

func (lab *harnessLab) waitForPatroniCheckTimeline(ctx context.Context, want bool, timeout time.Duration) (patroniCheckTimelineSettings, error) {
	deadline := time.Now().Add(timeout)
	var lastState patroniCheckTimelineSettings
	var lastErr error
	for {
		lastState, lastErr = lab.patroniCheckTimelineSettings(ctx)
		if lastErr == nil && lastState.CheckTimeline == want {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for Patroni check_timeline=%t: %w", want, lastErr)
			}
			return lastState, fmt.Errorf("wait for Patroni check_timeline=%t: got %t", want, lastState.CheckTimeline)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(lab.cfg.synchronousStandbyInterval):
		}
	}
}

func (lab *harnessLab) patroniCheckTimelineSettings(ctx context.Context) (patroniCheckTimelineSettings, error) {
	service := lab.serviceForMember(lab.currentPrimaryName(ctx))
	if service == "" {
		return patroniCheckTimelineSettings{}, fmt.Errorf("Patroni primary is unavailable")
	}
	output, status, err := lab.composeExec(ctx, service, "curl", "-fsS", "http://127.0.0.1:8008/config")
	if err != nil {
		return patroniCheckTimelineSettings{}, err
	}
	if status != 0 {
		return patroniCheckTimelineSettings{}, fmt.Errorf("read Patroni config failed with status %d: %s", status, strings.TrimSpace(output))
	}
	var config struct {
		CheckTimeline bool `json:"check_timeline"`
	}
	if err := json.Unmarshal([]byte(output), &config); err != nil {
		return patroniCheckTimelineSettings{}, fmt.Errorf("decode Patroni config: %w", err)
	}
	return patroniCheckTimelineSettings{CheckTimeline: config.CheckTimeline}, nil
}

func (lab *harnessLab) patroniCheckTimelineFailover(ctx context.Context, caseDir, scheduleFile string) error {
	oldPrimary := lab.currentPrimaryName(ctx)
	oldPrimaryService := lab.serviceForMember(oldPrimary)
	stale, eligible, err := lab.patroniCheckTimelineReplicas(oldPrimary)
	if err != nil {
		return err
	}

	event := func(action, value string) {
		writeNemesisScheduleEvent(scheduleFile, patroniCheckTimelineNemesis, action, value)
	}
	event("start", fmt.Sprintf(":target %q :stale-replica %q :eligible-replica %q", oldPrimary, stale.Name, eligible.Name))

	oldPrimaryStopped := false
	staleReplicaStopped := false
	eligibleReplicaStopped := false
	replicationPartitioned := false
	cleanup := func() error {
		var failures []string
		if eligibleReplicaStopped {
			if err := lab.startNodeRuntime(ctx, eligible.Service); err != nil {
				failures = append(failures, err.Error())
			} else {
				eligibleReplicaStopped = false
			}
		}
		if replicationPartitioned {
			lab.healPatroniCheckTimelineReplication(ctx, eligible.Service)
			replicationPartitioned = false
		}
		if staleReplicaStopped {
			if err := lab.startNodeRuntime(ctx, stale.Service); err != nil {
				failures = append(failures, err.Error())
			} else {
				staleReplicaStopped = false
			}
		}
		if oldPrimaryStopped {
			if err := lab.startNodeRuntime(ctx, oldPrimaryService); err != nil {
				failures = append(failures, err.Error())
			} else {
				oldPrimaryStopped = false
			}
		}
		if len(failures) == 0 {
			if _, err := lab.waitForThreeDataNodeCluster(ctx); err != nil {
				failures = append(failures, err.Error())
			}
		}
		if len(failures) > 0 {
			return fmt.Errorf("clean up Patroni check_timeline failover: %s", strings.Join(failures, "; "))
		}
		return nil
	}
	fail := func(cause error) error {
		if cleanupErr := cleanup(); cleanupErr != nil {
			cause = errors.Join(cause, cleanupErr)
		}
		event("stop", fmt.Sprintf(":target %q :stale-replica %q :eligible-replica %q :result :fail :error %q", oldPrimary, stale.Name, eligible.Name, cause))
		return cause
	}

	initialPrimary, err := lab.patroniCheckTimelineNodeState(ctx, targetNode{Name: oldPrimary, Service: oldPrimaryService})
	if err != nil {
		return fail(err)
	}
	initialStale, err := lab.patroniCheckTimelineNodeState(ctx, stale)
	if err != nil {
		return fail(err)
	}
	lab.recordPatroniCheckTimelineProbe(caseDir, "initial", oldPrimary, stale.Name, eligible.Name, oldPrimary, initialPrimary.Timeline, initialStale)

	if err := lab.stopNodeRuntime(ctx, stale.Service); err != nil {
		return fail(err)
	}
	staleReplicaStopped = true
	if err := lab.stopNodeRuntime(ctx, oldPrimaryService); err != nil {
		return fail(err)
	}
	oldPrimaryStopped = true
	promoted := lab.waitForCurrentPrimaryNot(ctx, oldPrimary, 90*time.Second)
	if promoted != eligible.Name {
		return fail(fmt.Errorf("Patroni promoted %s; want eligible replica %s", promoted, eligible.Name))
	}
	promotedState, err := lab.patroniCheckTimelineNodeState(ctx, eligible)
	if err != nil {
		return fail(err)
	}
	if promotedState.Timeline <= initialPrimary.Timeline {
		return fail(fmt.Errorf("Patroni promoted timeline %d; want above initial timeline %d", promotedState.Timeline, initialPrimary.Timeline))
	}
	lab.recordPatroniCheckTimelineProbe(caseDir, "after-first-promotion", oldPrimary, stale.Name, eligible.Name, promoted, promotedState.Timeline, patroniCheckTimelineNodeState{Member: stale.Name, Service: stale.Service})

	if err := lab.partitionPatroniCheckTimelineReplication(ctx, eligible.Service); err != nil {
		return fail(err)
	}
	replicationPartitioned = true
	if err := lab.startNodeRuntime(ctx, stale.Service); err != nil {
		return fail(err)
	}
	staleReplicaStopped = false
	staleState, err := lab.waitForPatroniCheckTimelineNodeState(ctx, stale, lab.cfg.synchronousStandbyTimeout, func(state patroniCheckTimelineNodeState) bool {
		return state.Reachable && state.InRecovery && !state.Writable && state.Timeline > 0 && state.Timeline < promotedState.Timeline
	})
	if err != nil {
		return fail(err)
	}
	lab.recordPatroniCheckTimelineProbe(caseDir, "stale-candidate", oldPrimary, stale.Name, eligible.Name, promoted, promotedState.Timeline, staleState)

	if err := lab.stopNodeRuntime(ctx, eligible.Service); err != nil {
		return fail(err)
	}
	eligibleReplicaStopped = true
	blockedState, err := lab.verifyPatroniCheckTimelineBlocksPromotion(ctx, stale, maxDuration(lab.cfg.nemesisHold, patroniCheckTimelineHold))
	if err != nil {
		return fail(err)
	}
	lab.recordPatroniCheckTimelineProbe(caseDir, "blocked", oldPrimary, stale.Name, eligible.Name, lab.currentPrimaryName(ctx), promotedState.Timeline, blockedState)
	_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", patroniCheckTimelineNemesis, stale.Name, stale.Service)

	if err := cleanup(); err != nil {
		return fail(err)
	}
	recoveredPrimary := lab.currentPrimaryName(ctx)
	recoveredPrimaryState, err := lab.patroniCheckTimelineNodeState(ctx, targetNode{Name: recoveredPrimary, Service: lab.serviceForMember(recoveredPrimary)})
	if err != nil {
		return fail(err)
	}
	recoveredStale, err := lab.patroniCheckTimelineNodeState(ctx, stale)
	if err != nil {
		return fail(err)
	}
	lab.recordPatroniCheckTimelineProbe(caseDir, "after-recovery", oldPrimary, stale.Name, eligible.Name, recoveredPrimary, recoveredPrimaryState.Timeline, recoveredStale)
	event("stop", fmt.Sprintf(":target %q :stale-replica %q :eligible-replica %q :promoted %q :result :ok", oldPrimary, stale.Name, eligible.Name, recoveredPrimary))
	return nil
}

func (lab *harnessLab) patroniCheckTimelineReplicas(primary string) (targetNode, targetNode, error) {
	var replicas []targetNode
	for _, node := range lab.options.target.DataNodes {
		if node.Name != primary {
			replicas = append(replicas, node)
		}
	}
	if len(replicas) != 2 {
		return targetNode{}, targetNode{}, fmt.Errorf("Patroni check_timeline failover requires exactly two replicas, got %d", len(replicas))
	}
	return replicas[0], replicas[1], nil
}

func (lab *harnessLab) partitionPatroniCheckTimelineReplication(ctx context.Context, service string) error {
	output, status, err := lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", "iptables_bin=$(command -v iptables || command -v /usr/sbin/iptables || true); if [ -z \"$iptables_bin\" ]; then echo 'iptables command not found' >&2; exit 127; fi; \"$iptables_bin\" -I INPUT ! -s 127.0.0.1 -p tcp --dport 5432 -j DROP && \"$iptables_bin\" -I OUTPUT ! -d 127.0.0.1 -p tcp --sport 5432 -j DROP")
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("partition Patroni replication on %s failed with status %d: %s", service, status, strings.TrimSpace(output))
	}
	return nil
}

func (lab *harnessLab) healPatroniCheckTimelineReplication(ctx context.Context, service string) {
	_, _, _ = lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", "while iptables -D INPUT ! -s 127.0.0.1 -p tcp --dport 5432 -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT ! -d 127.0.0.1 -p tcp --sport 5432 -j DROP 2>/dev/null; do :; done")
}

func (lab *harnessLab) patroniCheckTimelineNodeState(ctx context.Context, node targetNode) (patroniCheckTimelineNodeState, error) {
	state := patroniCheckTimelineNodeState{Member: node.Name, Service: node.Service}
	output, err := lab.psqlService(ctx, node.Service, `
SELECT
  pg_is_in_recovery(),
  CASE
    WHEN pg_is_in_recovery() THEN (pg_control_checkpoint()).timeline_id
    ELSE ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int
  END;`)
	if err != nil {
		state.Error = err.Error()
		return state, err
	}
	parts := strings.Split(lastNonEmptyLine(output), "\t")
	if len(parts) != 2 {
		return state, fmt.Errorf("decode Patroni timeline node state %q", output)
	}
	timeline, err := strconv.Atoi(parts[1])
	if err != nil {
		return state, fmt.Errorf("decode Patroni timeline %q: %w", parts[1], err)
	}
	state.Reachable = true
	state.InRecovery = parts[0] == "t"
	state.Writable = !state.InRecovery
	state.Timeline = timeline
	return state, nil
}

func (lab *harnessLab) waitForPatroniCheckTimelineNodeState(ctx context.Context, node targetNode, timeout time.Duration, predicate func(patroniCheckTimelineNodeState) bool) (patroniCheckTimelineNodeState, error) {
	deadline := time.Now().Add(timeout)
	var lastState patroniCheckTimelineNodeState
	var lastErr error
	for {
		lastState, lastErr = lab.patroniCheckTimelineNodeState(ctx, node)
		if lastErr == nil && predicate(lastState) {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for Patroni node %s timeline state: %w", node.Name, lastErr)
			}
			return lastState, fmt.Errorf("wait for Patroni node %s timeline state: got %+v", node.Name, lastState)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(lab.cfg.synchronousStandbyInterval):
		}
	}
}

func (lab *harnessLab) verifyPatroniCheckTimelineBlocksPromotion(ctx context.Context, stale targetNode, hold time.Duration) (patroniCheckTimelineNodeState, error) {
	deadline := time.Now().Add(hold)
	var lastState patroniCheckTimelineNodeState
	for time.Now().Before(deadline) {
		state, err := lab.patroniCheckTimelineNodeState(ctx, stale)
		if err != nil {
			return state, err
		}
		lastState = state
		if state.Writable || lab.currentPrimaryName(ctx) == stale.Name {
			return state, fmt.Errorf("Patroni promoted stale timeline replica %s", stale.Name)
		}
		select {
		case <-ctx.Done():
			return state, ctx.Err()
		case <-time.After(lab.cfg.synchronousStandbyInterval):
		}
	}
	return lastState, nil
}

func (lab *harnessLab) recordPatroniCheckTimelineProbe(caseDir, phase, oldPrimary, staleReplica, eligibleReplica, currentPrimary string, primaryTimeline int, staleReplicaState patroniCheckTimelineNodeState) {
	appendJSONL(filepath.Join(caseDir, patroniCheckTimelineProbesFile), patroniCheckTimelineProbe{
		ObservedAt:        time.Now().UTC().Format(time.RFC3339),
		Phase:             phase,
		OldPrimary:        oldPrimary,
		StaleReplica:      staleReplica,
		EligibleReplica:   eligibleReplica,
		CurrentPrimary:    currentPrimary,
		PrimaryTimeline:   primaryTimeline,
		StaleReplicaState: staleReplicaState,
	})
}

func checkPatroniCheckTimelineProbes(probes []patroniCheckTimelineProbe) error {
	byPhase := make(map[string]patroniCheckTimelineProbe)
	for _, probe := range probes {
		byPhase[probe.Phase] = probe
	}
	initial, initialOK := byPhase["initial"]
	afterPromotion, afterPromotionOK := byPhase["after-first-promotion"]
	staleCandidate, staleCandidateOK := byPhase["stale-candidate"]
	blocked, blockedOK := byPhase["blocked"]
	afterRecovery, afterRecoveryOK := byPhase["after-recovery"]
	if !initialOK || !afterPromotionOK || !staleCandidateOK || !blockedOK || !afterRecoveryOK {
		return fmt.Errorf("Patroni check_timeline probes missing required phases")
	}
	for _, probe := range []patroniCheckTimelineProbe{afterPromotion, staleCandidate, blocked, afterRecovery} {
		if probe.OldPrimary != initial.OldPrimary ||
			probe.StaleReplica != initial.StaleReplica ||
			probe.EligibleReplica != initial.EligibleReplica {
			return fmt.Errorf("Patroni check_timeline probes do not identify one failover scenario")
		}
	}
	if initial.PrimaryTimeline <= 0 || afterPromotion.PrimaryTimeline <= initial.PrimaryTimeline {
		return fmt.Errorf("Patroni check_timeline first promotion did not advance the primary timeline")
	}
	if afterPromotion.CurrentPrimary != initial.EligibleReplica {
		return fmt.Errorf("Patroni check_timeline promoted %s; want eligible replica %s", afterPromotion.CurrentPrimary, initial.EligibleReplica)
	}
	if !staleCandidate.StaleReplicaState.Reachable ||
		staleCandidate.StaleReplicaState.Writable ||
		!staleCandidate.StaleReplicaState.InRecovery ||
		staleCandidate.StaleReplicaState.Timeline <= 0 ||
		staleCandidate.StaleReplicaState.Timeline >= afterPromotion.PrimaryTimeline {
		return fmt.Errorf("Patroni check_timeline candidate is not a stale replica: %+v", staleCandidate.StaleReplicaState)
	}
	if blocked.CurrentPrimary != "unknown" ||
		blocked.StaleReplicaState.Writable ||
		!blocked.StaleReplicaState.InRecovery {
		return fmt.Errorf("Patroni check_timeline did not block stale replica promotion: %+v", blocked)
	}
	if afterRecovery.CurrentPrimary == "" || afterRecovery.CurrentPrimary == "unknown" ||
		!afterRecovery.StaleReplicaState.Reachable ||
		afterRecovery.StaleReplicaState.Writable ||
		!afterRecovery.StaleReplicaState.InRecovery {
		return fmt.Errorf("Patroni check_timeline cluster did not recover: %+v", afterRecovery)
	}
	return nil
}

func readPatroniCheckTimelineProbes(path string) []patroniCheckTimelineProbe {
	rows := readJSONL(path)
	probes := make([]patroniCheckTimelineProbe, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}
		var probe patroniCheckTimelineProbe
		if json.Unmarshal(data, &probe) == nil {
			probes = append(probes, probe)
		}
	}
	return probes
}
