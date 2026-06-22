package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	OperationID  string              `json:"operationId"`
	Target       string              `json:"target"`
	Source       string              `json:"source"`
	Observations []reinitObservation `json:"observations"`
	FinalStatus  *clusterStatus      `json:"finalStatus,omitempty"`
	Error        string              `json:"error,omitempty"`
}

func (lab *harnessLab) reinitReplica(ctx context.Context, caseDir, scheduleFile string) error {
	target, source, controlService, err := lab.reinitTarget(ctx)
	if err != nil {
		lab.writeReinitFailure(caseDir, "", "", "", err)
		return err
	}

	writeNemesisScheduleEvent(scheduleFile, "reinit-replica", "start", fmt.Sprintf(":source %q :target %q", source, target))
	requestedAt := time.Now().UTC()
	output, status := lab.requestReinit(ctx, target, controlService)
	operationID := reinitOperationIDFromOutput(output)
	result := map[string]any{
		"requestedAt":    requestedAt.Format(time.RFC3339),
		"source":         source,
		"target":         target,
		"targetService":  lab.serviceForMember(target),
		"controlService": controlService,
		"exitStatus":     status,
		"output":         output,
		"operationId":    operationID,
	}

	if status != 0 {
		result["valid"] = false
		writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
		lab.writeReinitChecker(caseDir, false, target, source, operationID, "reinit request failed", nil)
		writeNemesisScheduleEvent(scheduleFile, "reinit-replica", "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", source, target, operationID, status))
		return fmt.Errorf("reinit request for %s failed with status %d: %s", target, status, strings.TrimSpace(output))
	}

	wait := lab.waitForReinitCompletion(ctx, target, source, operationID)
	result["valid"] = wait.Valid
	result["completed"] = wait.Completed
	result["wait"] = wait
	writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
	lab.writeReinitChecker(caseDir, wait.Valid, target, source, operationID, wait.Error, &wait)

	statusLabel := "ok"
	if !wait.Valid {
		statusLabel = "fail"
	}
	writeNemesisScheduleEvent(scheduleFile, "reinit-replica", "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :%s", source, target, operationID, status, statusLabel))
	if !wait.Valid {
		return fmt.Errorf("reinit did not complete successfully for %s: %s", target, wait.Error)
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

func (lab *harnessLab) requestReinit(ctx context.Context, target, service string) (string, int) {
	if target == "" {
		return "no healthy non-primary reinit target found", 2
	}
	output, status, _ := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "reinit",
		"-member", target,
		"-reason", "jepsen-full-replica-reinit",
		"-requested-by", "jepsen",
		"-o", "json")
	return output, status
}

func reinitOperationIDFromOutput(output string) string {
	for index, char := range output {
		if char != '{' {
			continue
		}
		decoder := json.NewDecoder(strings.NewReader(output[index:]))
		var raw struct {
			Operation struct {
				ID string `json:"id"`
			} `json:"operation"`
		}
		if err := decoder.Decode(&raw); err != nil || raw.Operation.ID == "" {
			continue
		}
		return raw.Operation.ID
	}
	return ""
}

func (lab *harnessLab) waitForReinitCompletion(ctx context.Context, target, source, operationID string) reinitWaitResult {
	result := reinitWaitResult{
		OperationID: operationID,
		Target:      target,
		Source:      source,
	}
	deadline := time.Now().Add(lab.cfg.oldPrimaryRejoinTimeout + lab.cfg.timelineConvergenceTimeout)
	var lastErr error

	for {
		status, service, err := lab.pacmanClusterStatusAny(ctx)
		observation := reinitObservation{
			ObservedAt: time.Now().UTC().Format(time.RFC3339),
			Service:    service,
		}
		if err != nil {
			lastErr = err
			observation.Error = err.Error()
			result.Observations = append(result.Observations, observation)
		} else {
			result.FinalStatus = &status
			observation.ClusterPhase = status.Phase
			observation.CurrentPrimary = status.CurrentPrimary
			observation.ClusterReinit = status.Reinit
			for _, member := range status.Members {
				if member.Name != target {
					continue
				}
				observation.TargetMember = member
				observation.TargetHealthy = member.Healthy
				observation.TargetStreaming = member.Role == "replica" && member.State == "streaming"
				break
			}
			result.Observations = append(result.Observations, observation)
			if reinitComplete(status, target, source, operationID) {
				result.Completed = true
				result.Valid = true
				return result
			}
		}

		if time.Now().After(deadline) {
			if lastErr != nil {
				result.Error = lastErr.Error()
			} else {
				result.Error = "timed out waiting for reinit completion"
			}
			return result
		}

		select {
		case <-ctx.Done():
			result.Error = ctx.Err().Error()
			return result
		case <-time.After(lab.cfg.clusterVerifyInterval):
		}
	}
}

func reinitComplete(status clusterStatus, target, source, operationID string) bool {
	if status.Phase != "healthy" || status.CurrentPrimary != source || status.Reinit == nil {
		return false
	}
	if !reinitStatusCompleted(status.Reinit, target, source, operationID) {
		return false
	}
	for _, member := range status.Members {
		if member.Name != target {
			continue
		}
		return member.Healthy &&
			member.Role == "replica" &&
			member.State == "streaming" &&
			reinitStatusCompleted(member.Reinit, target, source, operationID)
	}
	return false
}

func reinitStatusCompleted(status *reinitStatus, target, source, operationID string) bool {
	if status == nil {
		return false
	}
	if operationID != "" && status.OperationID != operationID {
		return false
	}
	return status.State == "completed" &&
		status.LastResult == "succeeded" &&
		status.ToMember == target &&
		status.FromMember == source
}

func (lab *harnessLab) writeReinitFailure(caseDir, target, source, operationID string, err error) {
	message := ""
	if err != nil {
		message = err.Error()
	}
	writeJSON(filepath.Join(caseDir, reinitArtifactFile), map[string]any{
		"valid":       false,
		"target":      target,
		"source":      source,
		"operationId": operationID,
		"error":       message,
	})
	lab.writeReinitChecker(caseDir, false, target, source, operationID, message, nil)
}

func (lab *harnessLab) writeReinitChecker(caseDir string, valid bool, target, source, operationID, errText string, wait *reinitWaitResult) {
	result := map[string]any{
		"checker":     "full-replica-reinit",
		"valid":       valid,
		"applicable":  true,
		"target":      target,
		"source":      source,
		"operationId": operationID,
	}
	if errText != "" {
		result["error"] = errText
	}
	if wait != nil {
		result["completed"] = wait.Completed
		result["observations"] = len(wait.Observations)
		result["finalStatus"] = wait.FinalStatus
	}
	writeJSON(filepath.Join(caseDir, reinitCheckerFile), result)
}

func (lab *harnessLab) checkReinitProcedure(nemesis, caseDir string) error {
	if nemesis != "reinit-replica" {
		writeJSON(filepath.Join(caseDir, reinitCheckerFile), map[string]any{"checker": "full-replica-reinit", "valid": true, "applicable": false})
		return nil
	}

	var result struct {
		Valid bool `json:"valid"`
	}
	data, err := os.ReadFile(filepath.Join(caseDir, reinitCheckerFile))
	if err != nil {
		lab.writeReinitChecker(caseDir, false, "", "", "", "missing reinit checker artifact", nil)
		return fmt.Errorf("missing reinit checker artifact")
	}
	if err := json.Unmarshal(data, &result); err != nil {
		lab.writeReinitChecker(caseDir, false, "", "", "", "invalid reinit checker artifact", nil)
		return fmt.Errorf("invalid reinit checker artifact: %w", err)
	}
	if !result.Valid {
		return fmt.Errorf("reinit checker failed")
	}
	return nil
}
