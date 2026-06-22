package cmd

import (
	"context"
	"fmt"
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
