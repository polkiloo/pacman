package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	manualSwitchoverCheckerName = "manual-switchover"
	manualSwitchoverCheckerFile = "manual-switchover-checker.json"
	manualSwitchoverFile        = "manual-switchover.json"
)

type manualSwitchoverCheckerOptions struct {
	caseDir         string
	nemesis         string
	operationPath   string
	observationPath string
	outputPath      string
}

type manualSwitchoverOperation struct {
	RequestedAt    string `json:"requestedAt"`
	Candidate      string `json:"candidate"`
	ControlService string `json:"controlService"`
	ExitStatus     *int   `json:"exitStatus"`
	Output         string `json:"output"`
}

type manualSwitchoverCheckerResult struct {
	Checker         string                 `json:"checker"`
	Valid           bool                   `json:"valid"`
	Applicable      bool                   `json:"applicable"`
	Error           string                 `json:"error,omitempty"`
	RequestedAt     string                 `json:"requestedAt,omitempty"`
	Candidate       string                 `json:"candidate,omitempty"`
	ControlService  string                 `json:"controlService,omitempty"`
	ExitStatus      *int                   `json:"exitStatus"`
	RequestAccepted bool                   `json:"requestAccepted"`
	FinalPrimary    *timelineMemberSummary `json:"finalPrimary"`
	Output          string                 `json:"output,omitempty"`
}

func newManualSwitchoverCheckerCommand() *cobra.Command {
	var options manualSwitchoverCheckerOptions

	command := &cobra.Command{
		Use:   "manual-switchover",
		Short: "check manual switchover selected the requested candidate",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers manual-switchover does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runManualSwitchoverChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("manual switchover checker failed")
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.caseDir, "case-dir", "", "Jepsen case artifact directory")
	command.Flags().StringVar(&options.nemesis, "nemesis", "", "selected Jepsen nemesis")
	command.Flags().StringVar(&options.operationPath, "operation-file", "", "manual switchover operation JSON path")
	command.Flags().StringVar(&options.observationPath, "sample-file", "", "primary observation JSONL path")
	command.Flags().StringVar(&options.outputPath, "output", "", "checker JSON output path")

	if err := command.MarkFlagRequired("case-dir"); err != nil {
		panic(err)
	}

	return command
}

func runManualSwitchoverChecker(options manualSwitchoverCheckerOptions) (bool, error) {
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, manualSwitchoverCheckerFile)
	}

	result := manualSwitchoverCheckerResult{
		Checker:    manualSwitchoverCheckerName,
		Valid:      true,
		Applicable: false,
	}
	if options.nemesis != "switchover" {
		if err := writeJSONFile(outputPath, result); err != nil {
			return false, err
		}
		return true, nil
	}

	result.Valid = false
	result.Applicable = true

	operationPath := options.operationPath
	if operationPath == "" {
		operationPath = filepath.Join(options.caseDir, manualSwitchoverFile)
	}
	observationPath := options.observationPath
	if observationPath == "" {
		observationPath = filepath.Join(options.caseDir, primaryObservationFile)
	}
	if fileIsMissingOrEmpty(operationPath) || fileIsMissingOrEmpty(observationPath) {
		result.Error = "missing switchover operation metadata or primary observations"
		if err := writeJSONFile(outputPath, result); err != nil {
			return false, err
		}
		return false, nil
	}

	operation, err := readManualSwitchoverOperation(operationPath)
	if err != nil {
		return false, err
	}
	observations, err := readPrimaryObservations(observationPath)
	if err != nil {
		return false, err
	}
	result = checkManualSwitchover(operation, observations)
	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}
	return result.Valid, nil
}

func readManualSwitchoverOperation(path string) (manualSwitchoverOperation, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return manualSwitchoverOperation{}, fmt.Errorf("read manual switchover operation %s: %w", path, err)
	}

	var operation manualSwitchoverOperation
	if err := json.Unmarshal(data, &operation); err != nil {
		return manualSwitchoverOperation{}, fmt.Errorf("parse manual switchover operation %s: %w", path, err)
	}
	return operation, nil
}

func checkManualSwitchover(operation manualSwitchoverOperation, observations []primaryObservation) manualSwitchoverCheckerResult {
	samples := groupTimelineSamples(observations)
	var finalPrimary *timelineMemberSummary
	if len(samples) > 0 {
		primary := primaryOf(samples[len(samples)-1])
		if primary != nil {
			summary := summarizeTimelineMember(*primary)
			finalPrimary = &summary
		}
	}

	requestAccepted := operation.ExitStatus != nil && *operation.ExitStatus == 0
	valid := operation.Candidate != "" &&
		requestAccepted &&
		finalPrimary != nil &&
		finalPrimary.Member == operation.Candidate

	return manualSwitchoverCheckerResult{
		Checker:         manualSwitchoverCheckerName,
		Valid:           valid,
		Applicable:      true,
		RequestedAt:     operation.RequestedAt,
		Candidate:       operation.Candidate,
		ControlService:  operation.ControlService,
		ExitStatus:      operation.ExitStatus,
		RequestAccepted: requestAccepted,
		FinalPrimary:    finalPrimary,
		Output:          operation.Output,
	}
}
