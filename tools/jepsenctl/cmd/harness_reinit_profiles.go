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
