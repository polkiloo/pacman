package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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
	if !strings.HasPrefix(nemesis, "reinit-replica") {
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
