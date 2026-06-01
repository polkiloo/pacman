package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	synchronousReplicationConfigFile     = "synchronous-replication-config.json"
	synchronousReplicationCheckerFile    = "synchronous-replication-checker.json"
	synchronousStandbyKillProbesFile     = "synchronous-standby-kill-probes.jsonl"
	synchronousStandbyKillCheckerFile    = "synchronous-standby-kill-checker.json"
	strictSyncWriteProbesFile            = "strict-sync-write-probes.jsonl"
	strictSyncNoStandbyCheckerFile       = "strict-sync-no-standby-checker.json"
	synchronousStandbyKillNemesisProfile = "sync-standby-kill"
)

type patroniSynchronousState struct {
	SynchronousMode             bool     `json:"synchronousMode"`
	SynchronousModeStrict       bool     `json:"synchronousModeStrict"`
	SynchronousNodeCount        int      `json:"synchronousNodeCount"`
	SynchronousStandbyNames     string   `json:"synchronousStandbyNames"`
	SynchronousStandbyMembers   []string `json:"synchronousStandbyMembers"`
	SynchronousStandbys         int      `json:"synchronousStandbys"`
	SynchronousStandbyAvailable bool     `json:"synchronousStandbyAvailable"`
}

type patroniSynchronousSettings struct {
	Strict               bool `json:"strict"`
	SynchronousNodeCount int  `json:"synchronousNodeCount"`
}

type synchronousStandbyKillProbe struct {
	ObservedAt string                  `json:"observedAt"`
	Phase      string                  `json:"phase"`
	Target     string                  `json:"target"`
	Service    string                  `json:"service"`
	State      patroniSynchronousState `json:"state"`
}

type strictSyncWriteProbe struct {
	ObservedAt    string                  `json:"observedAt"`
	Phase         string                  `json:"phase"`
	Service       string                  `json:"service"`
	Available     bool                    `json:"available"`
	ExitStatus    int                     `json:"exitStatus"`
	ElapsedMillis int64                   `json:"elapsedMillis"`
	Output        string                  `json:"output"`
	State         patroniSynchronousState `json:"state"`
}

func (lab *harnessLab) prepareWorkloadProfile(ctx context.Context, workload, caseDir string) error {
	profile, ok := resolvePatroniSynchronousProfile(workload)
	if !ok {
		return nil
	}
	if !lab.options.target.supportsPatroniLab() {
		return fmt.Errorf("workload profile %s requires the Patroni baseline target", workload)
	}

	state, err := lab.configurePatroniSynchronousMode(ctx, profile)
	result := map[string]any{
		"workload": workload,
		"profile":  profile,
		"state":    state,
	}
	if err != nil {
		result["error"] = err.Error()
	}
	writeJSON(filepath.Join(caseDir, synchronousReplicationConfigFile), result)
	return err
}

func resolvePatroniSynchronousProfile(workload string) (patroniSynchronousSettings, bool) {
	switch workload {
	case "append-sync", "append-strict-sync":
		return patroniSynchronousSettings{Strict: workload == "append-strict-sync", SynchronousNodeCount: 1}, true
	case "append-sync-two":
		return patroniSynchronousSettings{SynchronousNodeCount: 2}, true
	default:
		return patroniSynchronousSettings{}, false
	}
}

func patroniSynchronousProfile(workload string) (strict bool, ok bool) {
	profile, ok := resolvePatroniSynchronousProfile(workload)
	return profile.Strict, ok
}

func (profile patroniSynchronousSettings) matches(state patroniSynchronousState, standbyAvailable bool) bool {
	if !state.SynchronousMode ||
		state.SynchronousModeStrict != profile.Strict ||
		state.SynchronousNodeCount != profile.SynchronousNodeCount ||
		state.SynchronousStandbyAvailable != standbyAvailable {
		return false
	}
	if standbyAvailable && state.SynchronousStandbys < profile.SynchronousNodeCount {
		return false
	}
	return true
}

func (profile patroniSynchronousSettings) validate(state patroniSynchronousState) error {
	if !profile.matches(state, true) {
		return fmt.Errorf("Patroni synchronous state is %+v; want strict=%t node-count=%d with enough synchronous standbys", state, profile.Strict, profile.SynchronousNodeCount)
	}
	return nil
}

func (lab *harnessLab) configurePatroniSynchronousMode(ctx context.Context, profile patroniSynchronousSettings) (patroniSynchronousState, error) {
	payload := fmt.Sprintf(`{"synchronous_mode":true,"synchronous_mode_strict":%t,"synchronous_node_count":%d}`, profile.Strict, profile.SynchronousNodeCount)
	service := lab.options.target.firstDataService()
	output, status, err := lab.composeExec(ctx, service,
		"curl", "-fsS", "-X", "PATCH",
		"-H", "Content-Type: application/json",
		"-d", payload,
		"http://127.0.0.1:8008/config",
	)
	if err != nil {
		return patroniSynchronousState{}, err
	}
	if status != 0 {
		return patroniSynchronousState{}, fmt.Errorf("configure Patroni synchronous mode failed with status %d: %s", status, strings.TrimSpace(output))
	}
	return lab.waitForPatroniSynchronousState(ctx, profile, true, lab.cfg.synchronousStandbyTimeout)
}

func (lab *harnessLab) waitForPatroniSynchronousState(ctx context.Context, profile patroniSynchronousSettings, standbyAvailable bool, timeout time.Duration) (patroniSynchronousState, error) {
	return lab.waitForPatroniSynchronousStateMatching(ctx, profile, standbyAvailable, timeout, nil)
}

func (lab *harnessLab) waitForPatroniSynchronousStateMatching(ctx context.Context, profile patroniSynchronousSettings, standbyAvailable bool, timeout time.Duration, predicate func(patroniSynchronousState) bool) (patroniSynchronousState, error) {
	deadline := time.Now().Add(timeout)
	var lastState patroniSynchronousState
	var lastErr error
	for {
		lastState, lastErr = lab.patroniSynchronousState(ctx)
		if lastErr == nil && profile.matches(lastState, standbyAvailable) && (predicate == nil || predicate(lastState)) {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for Patroni synchronous state strict=%t node-count=%d standby-available=%t: state=%+v: %w", profile.Strict, profile.SynchronousNodeCount, standbyAvailable, lastState, lastErr)
			}
			return lastState, fmt.Errorf("wait for Patroni synchronous state strict=%t node-count=%d standby-available=%t: state=%+v", profile.Strict, profile.SynchronousNodeCount, standbyAvailable, lastState)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(lab.cfg.synchronousStandbyInterval):
		}
	}
}

func (lab *harnessLab) patroniSynchronousState(ctx context.Context) (patroniSynchronousState, error) {
	service := lab.serviceForMember(lab.currentPrimaryName(ctx))
	if service == "" {
		return patroniSynchronousState{}, fmt.Errorf("Patroni primary is unavailable")
	}

	configOutput, status, err := lab.composeExec(ctx, service, "curl", "-fsS", "http://127.0.0.1:8008/config")
	if err != nil {
		return patroniSynchronousState{}, err
	}
	if status != 0 {
		return patroniSynchronousState{}, fmt.Errorf("read Patroni config failed with status %d: %s", status, strings.TrimSpace(configOutput))
	}
	var state patroniSynchronousState
	var config struct {
		SynchronousMode       bool `json:"synchronous_mode"`
		SynchronousModeStrict bool `json:"synchronous_mode_strict"`
		SynchronousNodeCount  int  `json:"synchronous_node_count"`
	}
	if err := json.Unmarshal([]byte(configOutput), &config); err != nil {
		return patroniSynchronousState{}, fmt.Errorf("decode Patroni config: %w", err)
	}
	state.SynchronousMode = config.SynchronousMode
	state.SynchronousModeStrict = config.SynchronousModeStrict
	state.SynchronousNodeCount = config.SynchronousNodeCount

	sql := `
SELECT json_build_object(
  'synchronousStandbyNames', current_setting('synchronous_standby_names'),
  'synchronousStandbys', count(*) FILTER (WHERE sync_state IN ('sync', 'quorum')),
  'synchronousStandbyMembers', coalesce(json_agg(application_name ORDER BY application_name) FILTER (WHERE sync_state IN ('sync', 'quorum')), '[]'::json)
)
FROM pg_stat_replication;`
	output, status, err := lab.composeExec(ctx, service,
		"env", "PGPASSWORD="+lab.cfg.pgPassword,
		lab.cfg.psqlBinary,
		"-v", "ON_ERROR_STOP=1",
		"-h", "127.0.0.1",
		"-p", lab.cfg.pgPort,
		"-U", lab.cfg.pgUser,
		"-d", lab.cfg.pgDatabase,
		"-Atq",
		"-c", sql,
	)
	if err != nil {
		return patroniSynchronousState{}, err
	}
	if status != 0 {
		return patroniSynchronousState{}, fmt.Errorf("read PostgreSQL synchronous state failed with status %d: %s", status, strings.TrimSpace(output))
	}
	if err := json.Unmarshal([]byte(lastNonEmptyLine(output)), &state); err != nil {
		return patroniSynchronousState{}, fmt.Errorf("decode PostgreSQL synchronous state %q: %w", strings.TrimSpace(output), err)
	}
	state.SynchronousStandbyAvailable = state.SynchronousStandbyNames != "" && state.SynchronousStandbys > 0
	return state, nil
}

func (lab *harnessLab) synchronousStandbyKill(ctx context.Context, caseDir, scheduleFile string) error {
	profile, _ := resolvePatroniSynchronousProfile("append-sync")
	before, err := lab.waitForPatroniSynchronousState(ctx, profile, true, lab.cfg.synchronousStandbyTimeout)
	if err != nil {
		return err
	}
	target, service := lab.firstSynchronousStandby(before)
	if service == "" {
		return fmt.Errorf("Patroni synchronous standby is unavailable: %+v", before)
	}

	event := func(action, value string) {
		writeNemesisScheduleEvent(scheduleFile, synchronousStandbyKillNemesisProfile, action, value)
	}
	event("start", fmt.Sprintf(":target %q :service %q", target, service))
	lab.recordSynchronousStandbyKillProbe(caseDir, "before-kill", target, service, before)

	if err := lab.stopNodeRuntime(ctx, service); err != nil {
		event("stop", fmt.Sprintf(":target %q :service %q :result :fail :error %q", target, service, err))
		return err
	}
	restart := func() error { return lab.startNodeRuntime(ctx, service) }

	during, err := lab.waitForPatroniSynchronousStateMatching(ctx, profile, true, lab.cfg.synchronousStandbyTimeout, func(state patroniSynchronousState) bool {
		return !containsString(state.SynchronousStandbyMembers, target)
	})
	if err != nil {
		_ = restart()
		event("stop", fmt.Sprintf(":target %q :service %q :result :fail :error %q", target, service, err))
		return err
	}
	lab.recordSynchronousStandbyKillProbe(caseDir, "during-kill", target, service, during)
	_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", synchronousStandbyKillNemesisProfile, target, service)
	time.Sleep(lab.cfg.nemesisHold)

	if err := restart(); err != nil {
		event("stop", fmt.Sprintf(":target %q :service %q :result :fail :error %q", target, service, err))
		return err
	}
	after, err := lab.waitForPatroniSynchronousState(ctx, profile, true, lab.cfg.synchronousStandbyTimeout)
	if err != nil {
		event("stop", fmt.Sprintf(":target %q :service %q :result :fail :error %q", target, service, err))
		return err
	}
	lab.recordSynchronousStandbyKillProbe(caseDir, "after-restart", target, service, after)
	event("stop", fmt.Sprintf(":target %q :service %q :result :ok", target, service))
	return nil
}

func (lab *harnessLab) firstSynchronousStandby(state patroniSynchronousState) (string, string) {
	for _, member := range state.SynchronousStandbyMembers {
		if service := lab.serviceForMember(member); service != "" {
			return member, service
		}
	}
	return "", ""
}

func (lab *harnessLab) recordSynchronousStandbyKillProbe(caseDir, phase, target, service string, state patroniSynchronousState) {
	appendJSONL(filepath.Join(caseDir, synchronousStandbyKillProbesFile), synchronousStandbyKillProbe{
		ObservedAt: time.Now().UTC().Format(time.RFC3339),
		Phase:      phase,
		Target:     target,
		Service:    service,
		State:      state,
	})
}

func checkSynchronousStandbyKillProbes(probes []synchronousStandbyKillProbe) error {
	byPhase := make(map[string]synchronousStandbyKillProbe)
	for _, probe := range probes {
		byPhase[probe.Phase] = probe
	}
	before, beforeOK := byPhase["before-kill"]
	during, duringOK := byPhase["during-kill"]
	after, afterOK := byPhase["after-restart"]
	if !beforeOK || !duringOK || !afterOK {
		return fmt.Errorf("synchronous standby kill probes missing required phases")
	}
	if before.Target == "" || before.Target != during.Target || before.Target != after.Target {
		return fmt.Errorf("synchronous standby kill probes do not identify one target")
	}
	if !containsString(before.State.SynchronousStandbyMembers, before.Target) ||
		containsString(during.State.SynchronousStandbyMembers, before.Target) {
		return fmt.Errorf("synchronous standby kill did not remove target %s from the synchronous standby set", before.Target)
	}
	profile, _ := resolvePatroniSynchronousProfile("append-sync")
	for _, probe := range []synchronousStandbyKillProbe{before, during, after} {
		if err := profile.validate(probe.State); err != nil {
			return fmt.Errorf("%s probe: %w", probe.Phase, err)
		}
	}
	return nil
}

func readSynchronousStandbyKillProbes(path string) []synchronousStandbyKillProbe {
	rows := readJSONL(path)
	probes := make([]synchronousStandbyKillProbe, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}
		var probe synchronousStandbyKillProbe
		if json.Unmarshal(data, &probe) == nil {
			probes = append(probes, probe)
		}
	}
	return probes
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (lab *harnessLab) strictSyncNoStandby(ctx context.Context, caseDir, scheduleFile, member, service string, peers []string) error {
	event := func(action, value string) { writeNemesisScheduleEvent(scheduleFile, "no-standby", action, value) }
	event("start", fmt.Sprintf(":target %q :standbys %q", member, strings.Join(peers, " ")))
	if _, err := lab.recordStrictSyncWriteProbe(ctx, caseDir, "before-no-standby", service); err != nil {
		event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
		return err
	}

	var stopped []string
	restart := func() error {
		var failures []string
		for _, peer := range stopped {
			if err := lab.startNodeRuntime(ctx, peer); err != nil {
				failures = append(failures, err.Error())
			}
		}
		stopped = nil
		if len(failures) > 0 {
			return fmt.Errorf("restart strict-sync standbys: %s", strings.Join(failures, "; "))
		}
		return nil
	}
	for _, peer := range peers {
		if err := lab.stopNodeRuntime(ctx, peer); err != nil {
			_ = restart()
			event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
			return err
		}
		stopped = append(stopped, peer)
	}
	profile, _ := resolvePatroniSynchronousProfile("append-strict-sync")
	if _, err := lab.waitForPatroniSynchronousState(ctx, profile, false, lab.cfg.synchronousStandbyTimeout); err != nil {
		_ = restart()
		event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
		return err
	}
	if _, err := lab.recordStrictSyncWriteProbe(ctx, caseDir, "during-no-standby", service); err != nil {
		_ = restart()
		event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
		return err
	}
	time.Sleep(lab.cfg.nemesisHold)
	if err := restart(); err != nil {
		event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
		return err
	}
	if _, err := lab.waitForPatroniSynchronousState(ctx, profile, true, lab.cfg.synchronousStandbyTimeout); err != nil {
		event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
		return err
	}
	if _, err := lab.recordStrictSyncWriteProbe(ctx, caseDir, "after-no-standby", service); err != nil {
		event("stop", fmt.Sprintf(":target %q :standbys %q :result :fail :error %q", member, strings.Join(peers, " "), err))
		return err
	}
	event("stop", fmt.Sprintf(":target %q :standbys %q :result :ok", member, strings.Join(peers, " ")))
	return nil
}

func (lab *harnessLab) recordStrictSyncWriteProbe(ctx context.Context, caseDir, phase, service string) (strictSyncWriteProbe, error) {
	state, stateErr := lab.patroniSynchronousState(ctx)
	probeID := fmt.Sprintf("strict-sync-%s-%d", phase, time.Now().UnixNano())
	sql := fmt.Sprintf(`INSERT INTO jepsen.client_availability_probes(probe_id, nemesis) VALUES (%s, 'no-standby');`, sqlLiteral(probeID))
	started := time.Now()
	output, status, err := lab.composeExec(ctx, service,
		"timeout", fmt.Sprintf("%ds", int(lab.cfg.strictSyncProbeTimeout.Seconds())),
		"env", "PGPASSWORD="+lab.cfg.pgPassword,
		lab.cfg.psqlBinary,
		"-v", "ON_ERROR_STOP=1",
		"-h", "127.0.0.1",
		"-p", lab.cfg.pgPort,
		"-U", lab.cfg.pgUser,
		"-d", lab.cfg.pgDatabase,
		"-Atq",
		"-c", sql,
	)
	probe := strictSyncWriteProbe{
		ObservedAt:    time.Now().UTC().Format(time.RFC3339),
		Phase:         phase,
		Service:       service,
		Available:     err == nil && status == 0,
		ExitStatus:    status,
		ElapsedMillis: time.Since(started).Milliseconds(),
		Output:        strings.TrimSpace(output),
		State:         state,
	}
	if err != nil {
		probe.Output = strings.TrimSpace(strings.Join([]string{probe.Output, err.Error()}, " "))
	}
	if stateErr != nil {
		probe.Output = strings.TrimSpace(strings.Join([]string{probe.Output, stateErr.Error()}, " "))
	}
	appendJSONL(filepath.Join(caseDir, strictSyncWriteProbesFile), probe)
	if err != nil {
		return probe, err
	}
	if stateErr != nil {
		return probe, stateErr
	}
	return probe, nil
}

func checkStrictSyncNoStandbyProbes(probes []strictSyncWriteProbe) error {
	byPhase := make(map[string]strictSyncWriteProbe)
	for _, probe := range probes {
		byPhase[probe.Phase] = probe
	}
	before, beforeOK := byPhase["before-no-standby"]
	during, duringOK := byPhase["during-no-standby"]
	after, afterOK := byPhase["after-no-standby"]
	if !beforeOK || !duringOK || !afterOK {
		return fmt.Errorf("strict-sync probes missing required phases")
	}
	if !before.Available || during.Available || during.ExitStatus == 0 || !after.Available {
		return fmt.Errorf("strict-sync availability transition is %t,%t,%t; want true,false,true", before.Available, during.Available, after.Available)
	}
	if !before.State.SynchronousMode || !before.State.SynchronousModeStrict ||
		!during.State.SynchronousMode || !during.State.SynchronousModeStrict || during.State.SynchronousStandbyAvailable ||
		!after.State.SynchronousStandbyAvailable {
		return fmt.Errorf("strict-sync state transition did not remove and restore the synchronous standby")
	}
	return nil
}

func readStrictSyncWriteProbes(path string) []strictSyncWriteProbe {
	rows := readJSONL(path)
	probes := make([]strictSyncWriteProbe, 0, len(rows))
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			continue
		}
		var probe strictSyncWriteProbe
		if json.Unmarshal(data, &probe) == nil {
			probes = append(probes, probe)
		}
	}
	return probes
}
