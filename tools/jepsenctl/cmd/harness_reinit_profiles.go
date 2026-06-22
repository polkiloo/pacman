package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type reinitPreFailoverResult struct {
	RequestedAt     string `json:"requestedAt"`
	PreviousPrimary string `json:"previousPrimary"`
	PreviousService string `json:"previousService"`
	Candidate       string `json:"candidate"`
	ControlService  string `json:"controlService"`
	ExitStatus      int    `json:"exitStatus"`
	Output          string `json:"output"`
	PromotedPrimary string `json:"promotedPrimary"`
	Restarted       bool   `json:"restarted"`
}

func (lab *harnessLab) reinitReplica(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis: "reinit-replica",
		Reason:  "jepsen-full-replica-reinit",
	})
}

func (lab *harnessLab) reinitReplicaKillTarget(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis:       "reinit-replica-kill-target",
		Reason:        "jepsen-reinit-kill-target",
		WaitForResult: lab.waitForReinitCompletionOrTerminalFailure,
		AfterAccepted: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			result := reinitVariantResult{Valid: true, Details: map[string]any{
				"targetService": run.TargetService,
			}}
			if err := lab.stopNodeRuntime(ctx, run.TargetService); err != nil {
				result.Valid = false
				result.Error = err.Error()
				return result
			}
			time.Sleep(lab.cfg.nemesisHold)
			if err := lab.startNodeRuntime(ctx, run.TargetService); err != nil {
				result.Valid = false
				result.Error = err.Error()
				return result
			}
			result.Details["restarted"] = true
			return result
		},
	})
}

func (lab *harnessLab) reinitReplicaKillSource(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis:       "reinit-replica-kill-source",
		Reason:        "jepsen-reinit-kill-source",
		WaitForResult: lab.waitForReinitSourceFailure,
		AfterAccepted: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			result := reinitVariantResult{Valid: true, Details: map[string]any{
				"sourceService": run.SourceService,
			}}
			if run.SourceService == "" {
				result.Valid = false
				result.Error = fmt.Sprintf("source service is unknown for %s", run.Source)
				return result
			}
			restartAttempted := false
			defer func() {
				if restartAttempted {
					return
				}
				if err := lab.startNodeRuntime(ctx, run.SourceService); err != nil && result.Error == "" {
					result.Error = err.Error()
				}
			}()
			if err := lab.stopNodeRuntime(ctx, run.SourceService); err != nil {
				result.Valid = false
				result.Error = err.Error()
				return result
			}
			result.Details["sourceStopped"] = true
			result.Details["promoted"] = lab.waitForCurrentPrimaryNot(ctx, run.Source, 90*time.Second)
			if result.Details["promoted"] == "unknown" {
				result.Valid = false
				result.Error = fmt.Sprintf("timed out waiting for promotion after stopping source %s", run.Source)
			}
			time.Sleep(lab.cfg.nemesisHold)
			restartAttempted = true
			if err := lab.startNodeRuntime(ctx, run.SourceService); err != nil {
				result.Valid = false
				result.Error = err.Error()
				return result
			}
			result.Details["sourceRestarted"] = true
			return result
		},
	})
}

func (lab *harnessLab) reinitReplicaDCSPartitionTarget(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis:       "reinit-replica-dcs-partition-target",
		Reason:        "jepsen-reinit-dcs-partition-target",
		WaitForResult: lab.waitForReinitCompletionOrTerminalFailure,
		AfterAccepted: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			dcsServices := append([]string(nil), lab.cfg.dcsRestartServices...)
			result := reinitVariantResult{Valid: true, Details: map[string]any{
				"targetService": run.TargetService,
				"dcsServices":   dcsServices,
			}}
			if run.TargetService == "" {
				result.Valid = false
				result.Error = fmt.Sprintf("target service is unknown for %s", run.Target)
				return result
			}
			if len(dcsServices) == 0 {
				result.Valid = false
				result.Error = "no DCS services configured for target partition"
				return result
			}

			partitioned := false
			defer func() {
				if partitioned {
					lab.iptablesHeal(ctx, run.TargetService, dcsServices)
				}
			}()
			if err := lab.iptablesPartition(ctx, run.TargetService, dcsServices); err != nil {
				result.Valid = false
				result.Error = err.Error()
				return result
			}
			partitioned = true

			probe := lab.observeReinitDCSPartitionTarget(ctx, run, dcsServices, lab.cfg.nemesisHold)
			result.Details["dcsPartitionProbe"] = probe
			if !probe.Valid {
				result.Valid = false
				result.Error = probe.Error
			}
			return result
		},
	})
}

func (lab *harnessLab) reinitReplicaDCSPartitionPrimary(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis:       "reinit-replica-dcs-partition-primary",
		Reason:        "jepsen-reinit-dcs-partition-primary",
		WaitForResult: lab.waitForReinitCompletionOrTerminalFailure,
		AfterAccepted: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			dcsServices := append([]string(nil), lab.cfg.dcsMajorityPartitionServices...)
			result := reinitVariantResult{Valid: true, Details: map[string]any{
				"sourceService": run.SourceService,
				"dcsServices":   dcsServices,
			}}
			if run.SourceService == "" {
				result.Valid = false
				result.Error = fmt.Sprintf("source service is unknown for %s", run.Source)
				return result
			}
			if len(dcsServices) == 0 {
				result.Valid = false
				result.Error = "no DCS services configured for primary partition"
				return result
			}

			partitioned := false
			defer func() {
				if partitioned {
					lab.iptablesHeal(ctx, run.SourceService, dcsServices)
				}
			}()
			if err := lab.iptablesPartition(ctx, run.SourceService, dcsServices); err != nil {
				result.Valid = false
				result.Error = err.Error()
				return result
			}
			partitioned = true

			probe := lab.observeReinitDCSPartitionPrimary(ctx, run, dcsServices, lab.cfg.nemesisHold)
			result.Details["dcsPartitionProbe"] = probe
			if !probe.Valid {
				result.Valid = false
				result.Error = probe.Error
			}
			return result
		},
	})
}

func (lab *harnessLab) reinitReplicaRepeated(ctx context.Context, caseDir, scheduleFile string) error {
	const nemesis = "reinit-replica-repeated"
	result := reinitRepeatedResult{
		Valid:   true,
		Nemesis: nemesis,
	}

	first, err := lab.runRepeatedReinitStep(ctx, 1, nil, "jepsen-reinit-repeated-first")
	result.Steps = append(result.Steps, first)
	if err != nil {
		result.Valid = false
		result.Error = err.Error()
		lab.writeRepeatedReinitArtifacts(caseDir, result, err)
		writeNemesisScheduleEvent(scheduleFile, nemesis, "start", fmt.Sprintf(":source %q :target %q", first.Source, first.Target))
		writeNemesisScheduleEvent(scheduleFile, nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", first.Source, first.Target, first.OperationID, first.ExitStatus))
		return err
	}

	writeNemesisScheduleEvent(scheduleFile, nemesis, "start", fmt.Sprintf(":source %q :target %q", first.Source, first.Target))
	writeNemesisScheduleEvent(scheduleFile, nemesis, "step", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :ok", first.Source, first.Target, first.OperationID, first.ExitStatus))

	excluded := map[string]struct{}{first.Target: {}}
	second, err := lab.runRepeatedReinitStep(ctx, 2, excluded, "jepsen-reinit-repeated-second")
	result.Steps = append(result.Steps, second)
	if err != nil {
		result.Valid = false
		result.Error = err.Error()
		lab.writeRepeatedReinitArtifacts(caseDir, result, err)
		writeNemesisScheduleEvent(scheduleFile, nemesis, "step", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", second.Source, second.Target, second.OperationID, second.ExitStatus))
		writeNemesisScheduleEvent(scheduleFile, nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", second.Source, second.Target, second.OperationID, second.ExitStatus))
		return err
	}
	writeNemesisScheduleEvent(scheduleFile, nemesis, "step", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :ok", second.Source, second.Target, second.OperationID, second.ExitStatus))

	result.History = lab.checkRepeatedReinitHistory(ctx, result.Steps)
	result.Slots = lab.checkRepeatedReinitSlots(ctx, result.Steps)
	finalStatus, _, finalStatusErr := lab.pacmanClusterStatusAny(ctx)
	if finalStatusErr != nil {
		result.FinalHealth = reinitRepeatedFinalHealth{Valid: false, Targets: repeatedReinitTargets(result.Steps), Error: finalStatusErr.Error()}
	} else {
		result.FinalStatus = &finalStatus
		result.FinalHealth = checkRepeatedReinitFinalHealth(finalStatus, result.Steps)
	}

	result.Valid = result.History.Valid && result.Slots.Valid && result.FinalHealth.Valid
	if !result.Valid {
		result.Error = firstNonEmpty(result.History.Error, result.Slots.Error, result.FinalHealth.Error)
	}
	lab.writeRepeatedReinitArtifacts(caseDir, result, nil)

	statusLabel := "ok"
	if !result.Valid {
		statusLabel = "fail"
	}
	writeNemesisScheduleEvent(scheduleFile, nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :%s", second.Source, second.Target, second.OperationID, second.ExitStatus, statusLabel))
	if !result.Valid {
		return fmt.Errorf("repeated reinit checker failed: %s", result.Error)
	}
	return nil
}

func (lab *harnessLab) runRepeatedReinitStep(ctx context.Context, index int, excluded map[string]struct{}, reason string) (reinitRepeatedStep, error) {
	target, source, controlService, err := lab.reinitTargetExcluding(ctx, excluded)
	step := reinitRepeatedStep{
		Index:          index,
		RequestedAt:    time.Now().UTC().Format(time.RFC3339),
		Target:         target,
		Source:         source,
		ControlService: controlService,
		TargetService:  lab.serviceForMember(target),
	}
	if err != nil {
		step.Error = err.Error()
		return step, err
	}
	output, status := lab.requestReinit(ctx, target, controlService, reason)
	step.ExitStatus = status
	step.Output = output
	step.OperationID = reinitOperationIDFromOutput(output)
	if status != 0 {
		step.Error = fmt.Sprintf("reinit request for %s failed with status %d: %s", target, status, strings.TrimSpace(output))
		return step, fmt.Errorf("%s", step.Error)
	}
	wait := lab.waitForReinitCompletion(ctx, target, source, step.OperationID)
	step.Wait = wait
	step.Valid = wait.Valid
	if !wait.Valid {
		step.Error = wait.Error
		if step.Error == "" {
			step.Error = "timed out waiting for reinit completion"
		}
		return step, fmt.Errorf("reinit step %d for %s failed: %s", index, target, step.Error)
	}
	return step, nil
}

func (lab *harnessLab) writeRepeatedReinitArtifacts(caseDir string, result reinitRepeatedResult, err error) {
	if err != nil && result.Error == "" {
		result.Error = err.Error()
	}
	writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
	lab.writeReinitChecker(caseDir, result.Valid, strings.Join(repeatedReinitTargets(result.Steps), ","), "", repeatedReinitOperationIDs(result.Steps), result.Error, nil)
}

func (lab *harnessLab) checkRepeatedReinitHistory(ctx context.Context, steps []reinitRepeatedStep) reinitRepeatedHistoryCheck {
	check := reinitRepeatedHistoryCheck{Valid: true}
	service := lab.repeatedReinitHistoryService(steps)
	output, status, err := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "history", "list", "-o", "json")
	if err != nil || status != 0 {
		check.Valid = false
		check.Error = fmt.Sprintf("history list from %s failed with status %d: %s", service, status, strings.TrimSpace(output))
		return check
	}
	history, err := reinitHistoryFromOutput(output)
	if err != nil {
		check.Valid = false
		check.Error = err.Error()
		return check
	}
	for _, step := range steps {
		entry, ok := findReinitHistoryEntry(history.Items, step)
		if !ok {
			check.Valid = false
			check.Error = fmt.Sprintf("missing successful reinit history entry for operation %s", step.OperationID)
			return check
		}
		check.Entries = append(check.Entries, entry)
	}
	return check
}

func (lab *harnessLab) repeatedReinitHistoryService(steps []reinitRepeatedStep) string {
	for index := len(steps) - 1; index >= 0; index-- {
		if steps[index].ControlService != "" {
			return steps[index].ControlService
		}
	}
	return lab.options.target.firstDataService()
}

func reinitHistoryFromOutput(output string) (reinitHistoryResponse, error) {
	for index, char := range output {
		if char != '{' {
			continue
		}
		decoder := json.NewDecoder(strings.NewReader(output[index:]))
		var history reinitHistoryResponse
		if err := decoder.Decode(&history); err != nil || history.Items == nil {
			continue
		}
		return history, nil
	}
	return reinitHistoryResponse{}, fmt.Errorf("history output did not contain JSON response: %s", strings.TrimSpace(output))
}

func findReinitHistoryEntry(entries []reinitHistoryEntry, step reinitRepeatedStep) (reinitHistoryEntry, bool) {
	for _, entry := range entries {
		if entry.OperationID != step.OperationID {
			continue
		}
		return entry, entry.Kind == "reinit" &&
			entry.Result == "succeeded" &&
			entry.FromMember == step.Source &&
			entry.ToMember == step.Target
	}
	return reinitHistoryEntry{}, false
}

func (lab *harnessLab) checkRepeatedReinitSlots(ctx context.Context, steps []reinitRepeatedStep) reinitRepeatedSlotCheck {
	primary := lab.currentPrimaryName(ctx)
	if primary == "unknown" {
		primary = firstRepeatedReinitSource(steps)
	}
	service := lab.serviceForMember(primary)
	if service == "" {
		service = lab.options.target.firstDataService()
	}
	check := reinitRepeatedSlotCheck{
		Valid:          true,
		Primary:        primary,
		PrimaryService: service,
		ExpectedSlots:  repeatedReinitExpectedSlots(steps),
	}
	output, err := lab.psqlService(ctx, service, "SELECT slot_name, active, coalesce(restart_lsn::text, '') FROM pg_replication_slots WHERE slot_type = 'physical' ORDER BY slot_name;")
	if err != nil {
		check.Valid = false
		check.Error = err.Error()
		return check
	}
	check.Slots = parseReinitSlotStatus(output)
	for _, expected := range check.ExpectedSlots {
		slot, ok := findReinitSlot(check.Slots, expected)
		if !ok {
			check.Valid = false
			check.Error = fmt.Sprintf("missing replication slot %s on primary %s", expected, primary)
			return check
		}
		if !slot.Active {
			check.Valid = false
			check.Error = fmt.Sprintf("replication slot %s on primary %s is not active", expected, primary)
			return check
		}
		if slot.RestartLSN == "" {
			check.Valid = false
			check.Error = fmt.Sprintf("replication slot %s on primary %s has no restart_lsn", expected, primary)
			return check
		}
	}
	return check
}

func parseReinitSlotStatus(output string) []reinitSlotStatus {
	var slots []reinitSlotStatus
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}
		slot := reinitSlotStatus{
			SlotName: strings.TrimSpace(fields[0]),
			Active:   strings.TrimSpace(fields[1]) == "t",
		}
		if len(fields) > 2 {
			slot.RestartLSN = strings.TrimSpace(fields[2])
		}
		slots = append(slots, slot)
	}
	return slots
}

func findReinitSlot(slots []reinitSlotStatus, name string) (reinitSlotStatus, bool) {
	for _, slot := range slots {
		if slot.SlotName == name {
			return slot, true
		}
	}
	return reinitSlotStatus{}, false
}

func checkRepeatedReinitFinalHealth(status clusterStatus, steps []reinitRepeatedStep) reinitRepeatedFinalHealth {
	targets := repeatedReinitTargets(steps)
	check := reinitRepeatedFinalHealth{Valid: true, Targets: targets}
	if status.ActiveOperation != nil {
		check.Valid = false
		check.Error = fmt.Sprintf("active operation remained after repeated reinit: %s", status.ActiveOperation.ID)
		return check
	}
	if status.Phase != "healthy" {
		check.Valid = false
		check.Error = fmt.Sprintf("cluster phase is %s, want healthy", status.Phase)
		return check
	}
	for _, step := range steps {
		if !repeatedReinitMemberHealthy(status, step) {
			check.Valid = false
			check.Error = fmt.Sprintf("target %s is not a healthy streaming replica after operation %s", step.Target, step.OperationID)
			return check
		}
	}
	return check
}

func repeatedReinitMemberHealthy(status clusterStatus, step reinitRepeatedStep) bool {
	for _, member := range status.Members {
		if member.Name != step.Target {
			continue
		}
		return member.Healthy &&
			member.Role == "replica" &&
			member.State == "streaming" &&
			reinitStatusCompleted(member.Reinit, step.Target, step.Source, step.OperationID)
	}
	return false
}

func repeatedReinitTargets(steps []reinitRepeatedStep) []string {
	var targets []string
	seen := make(map[string]struct{})
	for _, step := range steps {
		if step.Target == "" {
			continue
		}
		if _, ok := seen[step.Target]; ok {
			continue
		}
		seen[step.Target] = struct{}{}
		targets = append(targets, step.Target)
	}
	return targets
}

func repeatedReinitOperationIDs(steps []reinitRepeatedStep) string {
	var ids []string
	for _, step := range steps {
		if step.OperationID != "" {
			ids = append(ids, step.OperationID)
		}
	}
	return strings.Join(ids, ",")
}

func firstRepeatedReinitSource(steps []reinitRepeatedStep) string {
	for _, step := range steps {
		if step.Source != "" {
			return step.Source
		}
	}
	return ""
}

func repeatedReinitExpectedSlots(steps []reinitRepeatedStep) []string {
	targets := repeatedReinitTargets(steps)
	slots := make([]string, 0, len(targets))
	for _, target := range targets {
		slots = append(slots, reinitExpectedSlotName(target))
	}
	return slots
}

func reinitExpectedSlotName(memberName string) string {
	normalized := strings.ToLower(strings.TrimSpace(memberName))
	if normalized == "" {
		return "pacman_rejoin"
	}
	var builder strings.Builder
	lastUnderscore := false
	for _, character := range normalized {
		switch {
		case character >= 'a' && character <= 'z':
			builder.WriteRune(character)
			lastUnderscore = false
		case character >= '0' && character <= '9':
			builder.WriteRune(character)
			lastUnderscore = false
		case character == '_':
			if !lastUnderscore {
				builder.WriteRune(character)
				lastUnderscore = true
			}
		default:
			if !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	slot := strings.Trim(builder.String(), "_")
	if slot == "" {
		return "pacman_rejoin"
	}
	if len(slot) > 63 {
		slot = strings.TrimRight(slot[:63], "_")
	}
	if slot == "" {
		return "pacman_rejoin"
	}
	return slot
}

func (lab *harnessLab) reinitReplicaConcurrentRequest(ctx context.Context, caseDir, scheduleFile string) error {
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis: "reinit-replica-concurrent-request",
		Reason:  "jepsen-reinit-concurrent-request",
		AfterAccepted: func(ctx context.Context, run reinitRunContext) reinitVariantResult {
			output, status := lab.requestReinit(ctx, run.Target, run.ControlService, "jepsen-reinit-concurrent-request-second")
			result := reinitVariantResult{
				Valid: status != 0,
				Details: map[string]any{
					"secondExitStatus": status,
					"secondOutput":     output,
					"secondRejected":   status != 0,
				},
			}
			if status == 0 {
				result.Error = "concurrent reinit request was accepted"
			}
			return result
		},
	})
}

func (lab *harnessLab) observeReinitDCSPartitionTarget(ctx context.Context, run reinitRunContext, dcsServices []string, duration time.Duration) reinitDCSPartitionTargetProbe {
	probe := reinitDCSPartitionTargetProbe{
		Valid:       true,
		DcsServices: append([]string(nil), dcsServices...),
	}
	deadline := time.Now().Add(maxDuration(duration, lab.cfg.clusterVerifyInterval))
	successfulObservations := 0

	for {
		status, service, err := lab.pacmanClusterStatusAny(ctx)
		observation := reinitObservation{
			ObservedAt: time.Now().UTC().Format(time.RFC3339),
			Service:    service,
		}
		if err != nil {
			observation.Error = err.Error()
		} else {
			successfulObservations++
			observation.ClusterPhase = status.Phase
			observation.CurrentPrimary = status.CurrentPrimary
			observation.ClusterReinit = status.Reinit
			for _, member := range status.Members {
				if member.Name == run.Target {
					observation.TargetMember = member
					observation.TargetHealthy = member.Healthy
					observation.TargetStreaming = member.Role == "replica" && member.State == "streaming"
					break
				}
			}
			if reinitTargetMisleadingHealthy(status, run.Target, run.OperationID) {
				probe.MisleadingHealthyObservations = append(probe.MisleadingHealthyObservations, observation)
			}
		}
		probe.Observations = append(probe.Observations, observation)

		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			probe.Valid = false
			probe.Error = ctx.Err().Error()
			return probe
		case <-time.After(lab.cfg.clusterVerifyInterval):
		}
	}

	if successfulObservations == 0 {
		probe.Valid = false
		probe.Error = "no successful cluster status observations while target was DCS-isolated"
		return probe
	}
	if len(probe.MisleadingHealthyObservations) > 0 {
		probe.Valid = false
		probe.Error = "target published misleading healthy state while DCS-isolated during reinit"
	}
	return probe
}

func reinitTargetMisleadingHealthy(status clusterStatus, target, operationID string) bool {
	if reinitStatusMatchesOperation(status.Reinit, operationID) {
		return false
	}
	for _, member := range status.Members {
		if member.Name != target {
			continue
		}
		if reinitStatusMatchesOperation(member.Reinit, operationID) {
			return false
		}
		return member.Healthy && (member.State == "running" || member.State == "streaming")
	}
	return false
}

func reinitStatusMatchesOperation(status *reinitStatus, operationID string) bool {
	return status != nil && operationID != "" && status.OperationID == operationID
}

func (lab *harnessLab) observeReinitDCSPartitionPrimary(ctx context.Context, run reinitRunContext, dcsServices []string, duration time.Duration) reinitDCSPartitionPrimaryProbe {
	observers := lab.reinitQuorumSideObserverServices(run)
	probe := reinitDCSPartitionPrimaryProbe{
		Valid:            true,
		SourceService:    run.SourceService,
		DcsServices:      append([]string(nil), dcsServices...),
		ObserverServices: append([]string(nil), observers...),
	}
	deadline := time.Now().Add(maxDuration(duration, lab.cfg.clusterVerifyInterval))
	successfulObservations := 0

	for {
		status, service, err := lab.pacmanClusterStatusFromServices(ctx, observers)
		observation := reinitObservation{
			ObservedAt: time.Now().UTC().Format(time.RFC3339),
			Service:    service,
		}
		if err != nil {
			observation.Error = err.Error()
			probe.UnavailableQuorumSideObservations++
		} else {
			successfulObservations++
			observation.ClusterPhase = status.Phase
			observation.CurrentPrimary = status.CurrentPrimary
			observation.ClusterReinit = status.Reinit
			for _, member := range status.Members {
				if member.Name == run.Target {
					observation.TargetMember = member
					observation.TargetHealthy = member.Healthy
					observation.TargetStreaming = member.Role == "replica" && member.State == "streaming"
					break
				}
			}
			if reinitTargetPromoted(status, run.Target) {
				probe.UnsafeTargetPrimaryObservations = append(probe.UnsafeTargetPrimaryObservations, observation)
			}
			if reinitSafeFailoverUnderPrimaryDCSPressure(status, run.Source, run.Target) {
				probe.SafeFailoverObserved = true
			}
		}
		probe.Observations = append(probe.Observations, observation)

		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			probe.Valid = false
			probe.Error = ctx.Err().Error()
			return probe
		case <-time.After(lab.cfg.clusterVerifyInterval):
		}
	}

	if successfulObservations == 0 {
		probe.Valid = false
		probe.Error = "no successful quorum-side cluster status observations while primary was DCS-isolated"
		return probe
	}
	if len(probe.UnsafeTargetPrimaryObservations) > 0 {
		probe.Valid = false
		probe.Error = "reinit target became primary while source primary was DCS-isolated"
	}
	return probe
}

func (lab *harnessLab) reinitQuorumSideObserverServices(run reinitRunContext) []string {
	seen := make(map[string]struct{})
	var services []string
	add := func(service string) {
		if service == "" || service == run.SourceService {
			return
		}
		if _, ok := seen[service]; ok {
			return
		}
		seen[service] = struct{}{}
		services = append(services, service)
	}
	for _, node := range lab.options.target.DataNodes {
		add(node.Service)
	}
	return services
}

func (lab *harnessLab) pacmanClusterStatusFromServices(ctx context.Context, services []string) (clusterStatus, string, error) {
	var lastErr error
	for _, service := range services {
		text, err := lab.clusterStatusJSON(ctx, service)
		if err != nil {
			lastErr = err
			continue
		}
		var status clusterStatus
		if err := json.Unmarshal([]byte(text), &status); err != nil {
			lastErr = err
			continue
		}
		return status, service, nil
	}
	return clusterStatus{}, "", lastErr
}

func reinitTargetPromoted(status clusterStatus, target string) bool {
	if status.CurrentPrimary == target {
		return true
	}
	for _, member := range status.Members {
		if member.Name == target {
			return member.Role == "primary"
		}
	}
	return false
}

func reinitSafeFailoverUnderPrimaryDCSPressure(status clusterStatus, source, target string) bool {
	if status.CurrentPrimary == "" || status.CurrentPrimary == source || status.CurrentPrimary == target {
		return false
	}
	for _, member := range status.Members {
		if member.Name != status.CurrentPrimary {
			continue
		}
		return member.Healthy && member.Role == "primary" && member.State == "running"
	}
	return false
}

func (lab *harnessLab) reinitReplicaAfterFailover(ctx context.Context, caseDir, scheduleFile string) error {
	failover, err := lab.reinitPreFailover(ctx)
	if err != nil {
		lab.writeReinitFailure(caseDir, "", "", "", err)
		return err
	}
	return lab.runReinitReplica(ctx, caseDir, scheduleFile, reinitRunOptions{
		Nemesis: "reinit-replica-after-failover",
		Reason:  "jepsen-reinit-after-failover",
		InitialDetails: map[string]any{
			"preReinitFailover": failover,
		},
	})
}

func (lab *harnessLab) reinitPreFailover(ctx context.Context) (reinitPreFailoverResult, error) {
	status, controlService, err := lab.pacmanClusterStatusAny(ctx)
	if err != nil {
		return reinitPreFailoverResult{}, err
	}
	previousPrimary := status.CurrentPrimary
	previousService := lab.serviceForMember(previousPrimary)
	candidate := ""
	for _, member := range status.Members {
		if member.Name == previousPrimary || !member.Healthy || member.Role != "replica" {
			continue
		}
		candidate = member.Name
		break
	}
	if candidate == "" {
		return reinitPreFailoverResult{}, fmt.Errorf("no healthy failover candidate before reinit")
	}
	if service := lab.serviceForMember(candidate); service != "" {
		controlService = service
	}
	result := reinitPreFailoverResult{
		RequestedAt:     time.Now().UTC().Format(time.RFC3339),
		PreviousPrimary: previousPrimary,
		PreviousService: previousService,
		Candidate:       candidate,
		ControlService:  controlService,
	}
	if previousService == "" {
		return result, fmt.Errorf("previous primary service is unknown for %s", previousPrimary)
	}
	if err := lab.stopPostgres(ctx, previousService); err != nil {
		return result, fmt.Errorf("stop previous primary PostgreSQL before reinit failover: %w", err)
	}
	output, exitStatus := lab.requestManualFailoverUntilAccepted(ctx, candidate, controlService, lab.cfg.oldPrimaryRejoinTimeout)
	result.ExitStatus = exitStatus
	result.Output = output
	if exitStatus != 0 {
		_ = lab.startPostgres(ctx, previousService)
		return result, fmt.Errorf("pre-reinit failover request failed with status %d: %s", exitStatus, strings.TrimSpace(output))
	}
	promoted := lab.waitForCurrentPrimary(ctx, candidate, lab.cfg.oldPrimaryRejoinTimeout)
	if !promoted {
		result.PromotedPrimary = lab.currentPrimaryName(ctx)
		_ = lab.startPostgres(ctx, previousService)
		return result, fmt.Errorf("pre-reinit failover did not promote %s", candidate)
	}
	if err := lab.startPostgres(ctx, previousService); err != nil {
		result.PromotedPrimary = candidate
		return result, fmt.Errorf("restart previous primary PostgreSQL after reinit failover: %w", err)
	}
	result.Restarted = true
	if !lab.waitForClusterSwitchoverReady(ctx, lab.cfg.oldPrimaryRejoinTimeout) {
		result.PromotedPrimary = lab.currentPrimaryName(ctx)
		return result, fmt.Errorf("cluster did not become reinit-ready after pre-reinit failover to %s", candidate)
	}
	result.PromotedPrimary = candidate
	return result, nil
}

func (lab *harnessLab) requestManualFailoverUntilAccepted(ctx context.Context, candidate, service string, timeout time.Duration) (string, int) {
	deadline := time.Now().Add(timeout)
	var output string
	var status int
	for {
		output, status = lab.requestManualFailover(ctx, candidate, service)
		if status == 0 || time.Now().After(deadline) {
			return output, status
		}
		select {
		case <-ctx.Done():
			return ctx.Err().Error(), 1
		case <-time.After(lab.cfg.clusterVerifyInterval):
		}
	}
}

func (lab *harnessLab) requestManualFailover(ctx context.Context, candidate, service string) (string, int) {
	if candidate == "" {
		return "no healthy non-primary failover candidate found", 2
	}
	output, status, _ := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "failover",
		"-candidate", candidate,
		"-reason", "jepsen-reinit-after-failover",
		"-requested-by", "jepsen",
		"-o", "json")
	return output, status
}
