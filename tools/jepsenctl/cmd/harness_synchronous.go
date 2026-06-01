package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	synchronousReplicationConfigFile = "synchronous-replication-config.json"
	strictSyncWriteProbesFile        = "strict-sync-write-probes.jsonl"
	strictSyncNoStandbyCheckerFile   = "strict-sync-no-standby-checker.json"
)

type patroniSynchronousState struct {
	SynchronousMode             bool   `json:"synchronousMode"`
	SynchronousModeStrict       bool   `json:"synchronousModeStrict"`
	SynchronousStandbyNames     string `json:"synchronousStandbyNames"`
	SynchronousStandbys         int    `json:"synchronousStandbys"`
	SynchronousStandbyAvailable bool   `json:"synchronousStandbyAvailable"`
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
	strict, ok := patroniSynchronousProfile(workload)
	if !ok {
		return nil
	}
	if !lab.options.target.supportsPatroniLab() {
		return fmt.Errorf("workload profile %s requires the Patroni baseline target", workload)
	}

	state, err := lab.configurePatroniSynchronousMode(ctx, strict)
	result := map[string]any{
		"workload": workload,
		"state":    state,
	}
	if err != nil {
		result["error"] = err.Error()
	}
	writeJSON(filepath.Join(caseDir, synchronousReplicationConfigFile), result)
	return err
}

func patroniSynchronousProfile(workload string) (strict bool, ok bool) {
	switch workload {
	case "append-sync":
		return false, true
	case "append-strict-sync":
		return true, true
	default:
		return false, false
	}
}

func (lab *harnessLab) configurePatroniSynchronousMode(ctx context.Context, strict bool) (patroniSynchronousState, error) {
	payload := fmt.Sprintf(`{"synchronous_mode":true,"synchronous_mode_strict":%t}`, strict)
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
	return lab.waitForPatroniSynchronousState(ctx, strict, true, lab.cfg.synchronousStandbyTimeout)
}

func (lab *harnessLab) waitForPatroniSynchronousState(ctx context.Context, strict, standbyAvailable bool, timeout time.Duration) (patroniSynchronousState, error) {
	deadline := time.Now().Add(timeout)
	var lastState patroniSynchronousState
	var lastErr error
	for {
		lastState, lastErr = lab.patroniSynchronousState(ctx)
		if lastErr == nil &&
			lastState.SynchronousMode &&
			lastState.SynchronousModeStrict == strict &&
			lastState.SynchronousStandbyAvailable == standbyAvailable {
			return lastState, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastState, fmt.Errorf("wait for Patroni synchronous state strict=%t standby-available=%t: state=%+v: %w", strict, standbyAvailable, lastState, lastErr)
			}
			return lastState, fmt.Errorf("wait for Patroni synchronous state strict=%t standby-available=%t: state=%+v", strict, standbyAvailable, lastState)
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
	var config struct {
		SynchronousMode       bool `json:"synchronous_mode"`
		SynchronousModeStrict bool `json:"synchronous_mode_strict"`
	}
	if err := json.Unmarshal([]byte(configOutput), &config); err != nil {
		return patroniSynchronousState{}, fmt.Errorf("decode Patroni config: %w", err)
	}

	sql := `SELECT current_setting('synchronous_standby_names'), count(*) FILTER (WHERE sync_state IN ('sync', 'quorum')) FROM pg_stat_replication;`
	output, status, err := lab.composeExec(ctx, service,
		"env", "PGPASSWORD="+lab.cfg.pgPassword,
		lab.cfg.psqlBinary,
		"-v", "ON_ERROR_STOP=1",
		"-h", "127.0.0.1",
		"-p", lab.cfg.pgPort,
		"-U", lab.cfg.pgUser,
		"-d", lab.cfg.pgDatabase,
		"-F", "\t",
		"-Atq",
		"-c", sql,
	)
	if err != nil {
		return patroniSynchronousState{}, err
	}
	if status != 0 {
		return patroniSynchronousState{}, fmt.Errorf("read PostgreSQL synchronous state failed with status %d: %s", status, strings.TrimSpace(output))
	}
	fields := strings.Split(lastNonEmptyLine(output), "\t")
	if len(fields) != 2 {
		return patroniSynchronousState{}, fmt.Errorf("read PostgreSQL synchronous state returned %q", strings.TrimSpace(output))
	}
	synchronousStandbys, err := strconv.Atoi(fields[1])
	if err != nil {
		return patroniSynchronousState{}, fmt.Errorf("parse PostgreSQL synchronous standby count %q: %w", fields[1], err)
	}

	return patroniSynchronousState{
		SynchronousMode:             config.SynchronousMode,
		SynchronousModeStrict:       config.SynchronousModeStrict,
		SynchronousStandbyNames:     fields[0],
		SynchronousStandbys:         synchronousStandbys,
		SynchronousStandbyAvailable: fields[0] != "" && synchronousStandbys > 0,
	}, nil
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
	if _, err := lab.waitForPatroniSynchronousState(ctx, true, false, lab.cfg.synchronousStandbyTimeout); err != nil {
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
	if _, err := lab.waitForPatroniSynchronousState(ctx, true, true, lab.cfg.synchronousStandbyTimeout); err != nil {
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
