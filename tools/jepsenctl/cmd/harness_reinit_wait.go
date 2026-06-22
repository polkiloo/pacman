package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

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
	return lab.waitForReinitOutcome(ctx, target, source, operationID, reinitComplete)
}

func (lab *harnessLab) waitForReinitCompletionOrTerminalFailure(ctx context.Context, target, source, operationID string) reinitWaitResult {
	return lab.waitForReinitOutcome(ctx, target, source, operationID, func(status clusterStatus, target, source, operationID string) bool {
		return reinitComplete(status, target, source, operationID) || reinitTerminalFailure(status, target, source, operationID)
	})
}

func (lab *harnessLab) waitForReinitSourceFailure(ctx context.Context, target, source, operationID string) reinitWaitResult {
	return lab.waitForReinitOutcome(ctx, target, source, operationID, reinitSourceFailure)
}

func (lab *harnessLab) waitForReinitWALGFetchFailure(ctx context.Context, target, source, operationID string) reinitWaitResult {
	return lab.waitForReinitOutcome(ctx, target, source, operationID, reinitWALGFetchFailure)
}

func (lab *harnessLab) waitForReinitOutcome(ctx context.Context, target, source, operationID string, accepted func(clusterStatus, string, string, string) bool) reinitWaitResult {
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
			if accepted(status, target, source, operationID) {
				result.Completed = true
				result.Valid = true
				if reinitComplete(status, target, source, operationID) {
					result.Outcome = "completed"
				} else {
					result.Outcome = "terminal_failure"
				}
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

func reinitTerminalFailure(status clusterStatus, target, source, operationID string) bool {
	if status.CurrentPrimary == "" || status.CurrentPrimary == target || status.Reinit == nil {
		return false
	}
	if !reinitStatusTerminalFailure(status.Reinit, target, source, operationID) {
		return false
	}
	for _, member := range status.Members {
		if member.Name != target {
			continue
		}
		return member.Role != "primary" &&
			reinitStatusTerminalFailure(member.Reinit, target, source, operationID)
	}
	return false
}

func reinitSourceFailure(status clusterStatus, target, source, operationID string) bool {
	if status.CurrentPrimary == "" || status.CurrentPrimary == source || status.CurrentPrimary == target || status.Reinit == nil {
		return false
	}
	if !reinitStatusTerminalFailure(status.Reinit, target, source, operationID) {
		return false
	}
	targetFailed := false
	currentPrimaryHealthy := false
	for _, member := range status.Members {
		if member.Name == target {
			targetFailed = member.Role != "primary" && reinitStatusTerminalFailure(member.Reinit, target, source, operationID)
			continue
		}
		if member.Name == status.CurrentPrimary {
			currentPrimaryHealthy = member.Healthy && member.Role == "primary" && member.State == "running"
		}
	}
	return targetFailed && currentPrimaryHealthy
}

func reinitWALGFetchFailure(status clusterStatus, target, source, operationID string) bool {
	if !reinitTerminalFailure(status, target, source, operationID) {
		return false
	}
	for _, member := range status.Members {
		if member.Name != target {
			continue
		}
		return !member.Healthy && member.Role != "primary"
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

func reinitStatusTerminalFailure(status *reinitStatus, target, source, operationID string) bool {
	if status == nil {
		return false
	}
	if operationID != "" && status.OperationID != operationID {
		return false
	}
	return status.State == "failed" &&
		status.LastResult == "failed" &&
		status.ToMember == target &&
		status.FromMember == source
}
