package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	reinitArtifactFile = "reinit.json"
	reinitCheckerFile  = "reinit-checker.json"
)

type reinitObservation struct {
	ObservedAt      string        `json:"observedAt"`
	Service         string        `json:"service"`
	ClusterPhase    string        `json:"clusterPhase"`
	CurrentPrimary  string        `json:"currentPrimary"`
	ClusterReinit   *reinitStatus `json:"clusterReinit,omitempty"`
	TargetMember    clusterMember `json:"targetMember"`
	TargetStreaming bool          `json:"targetStreaming"`
	TargetHealthy   bool          `json:"targetHealthy"`
	Error           string        `json:"error,omitempty"`
}

type reinitWaitResult struct {
	Completed    bool                `json:"completed"`
	Valid        bool                `json:"valid"`
	Outcome      string              `json:"outcome,omitempty"`
	OperationID  string              `json:"operationId"`
	Target       string              `json:"target"`
	Source       string              `json:"source"`
	Observations []reinitObservation `json:"observations"`
	FinalStatus  *clusterStatus      `json:"finalStatus,omitempty"`
	Error        string              `json:"error,omitempty"`
}

type reinitRunOptions struct {
	Nemesis        string
	Reason         string
	InitialDetails map[string]any
	AfterAccepted  func(context.Context, reinitRunContext) reinitVariantResult
	WaitForResult  func(context.Context, string, string, string) reinitWaitResult
}

type reinitRunContext struct {
	Target         string
	Source         string
	ControlService string
	TargetService  string
	OperationID    string
}

type reinitVariantResult struct {
	Valid   bool
	Error   string
	Details map[string]any
}

func (lab *harnessLab) runReinitReplica(ctx context.Context, caseDir, scheduleFile string, options reinitRunOptions) error {
	if options.WaitForResult == nil {
		options.WaitForResult = lab.waitForReinitCompletion
	}
	target, source, controlService, err := lab.reinitTarget(ctx)
	if err != nil {
		lab.writeReinitFailure(caseDir, "", "", "", err)
		return err
	}

	writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "start", fmt.Sprintf(":source %q :target %q", source, target))
	requestedAt := time.Now().UTC()
	output, status := lab.requestReinit(ctx, target, controlService, options.Reason)
	operationID := reinitOperationIDFromOutput(output)
	result := map[string]any{
		"requestedAt":    requestedAt.Format(time.RFC3339),
		"nemesis":        options.Nemesis,
		"source":         source,
		"target":         target,
		"targetService":  lab.serviceForMember(target),
		"controlService": controlService,
		"exitStatus":     status,
		"output":         output,
		"operationId":    operationID,
	}
	for key, value := range options.InitialDetails {
		result[key] = value
	}

	if status != 0 {
		result["valid"] = false
		writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
		lab.writeReinitChecker(caseDir, false, target, source, operationID, "reinit request failed", nil)
		writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", source, target, operationID, status))
		return fmt.Errorf("reinit request for %s failed with status %d: %s", target, status, strings.TrimSpace(output))
	}

	var variant reinitVariantResult
	if options.AfterAccepted != nil {
		variant = options.AfterAccepted(ctx, reinitRunContext{
			Target:         target,
			Source:         source,
			ControlService: controlService,
			TargetService:  lab.serviceForMember(target),
			OperationID:    operationID,
		})
		result["variant"] = variant.Details
		if variant.Error != "" {
			result["variantError"] = variant.Error
		}
	} else {
		variant.Valid = true
	}

	wait := options.WaitForResult(ctx, target, source, operationID)
	result["valid"] = wait.Valid && variant.Valid
	result["completed"] = wait.Completed
	result["wait"] = wait
	writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
	checkerError := wait.Error
	if checkerError == "" {
		checkerError = variant.Error
	}
	lab.writeReinitChecker(caseDir, wait.Valid && variant.Valid, target, source, operationID, checkerError, &wait)

	statusLabel := "ok"
	if !wait.Valid || !variant.Valid {
		statusLabel = "fail"
	}
	writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :%s", source, target, operationID, status, statusLabel))
	if !wait.Valid || !variant.Valid {
		return fmt.Errorf("reinit did not satisfy %s for %s: %s", options.Nemesis, target, checkerError)
	}
	return nil
}

func (lab *harnessLab) reinitTarget(ctx context.Context) (target, source, controlService string, err error) {
	status, service, err := lab.pacmanClusterStatusAny(ctx)
	if err != nil {
		return "", "", "", err
	}
	source = status.CurrentPrimary
	for _, member := range status.Members {
		if member.Name == source || !member.Healthy || member.Role != "replica" {
			continue
		}
		if member.State == "streaming" || member.State == "running" {
			return member.Name, source, service, nil
		}
	}
	return "", source, service, fmt.Errorf("no healthy reinit replica target found")
}

func (lab *harnessLab) requestReinit(ctx context.Context, target, service, reason string) (string, int) {
	if target == "" {
		return "no healthy non-primary reinit target found", 2
	}
	output, status, _ := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "reinit",
		"-member", target,
		"-reason", reason,
		"-requested-by", "jepsen",
		"-o", "json")
	return output, status
}
