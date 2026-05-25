package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

const (
	dcsQuorumCheckerName = "dcs-quorum-during-nemesis"
	dcsQuorumSampleFile  = "dcs-quorum-during-nemesis.jsonl"
	dcsQuorumCheckerFile = "dcs-quorum-checker.json"
)

type dcsQuorumCheckerOptions struct {
	nemesis              string
	caseDir              string
	samplePath           string
	outputPath           string
	minSlowLatencyMillis int
}

type dcsQuorumSample struct {
	ObservedAt               string          `json:"observedAt,omitempty"`
	Nemesis                  string          `json:"nemesis,omitempty"`
	Phase                    string          `json:"phase,omitempty"`
	ObserverService          string          `json:"observerService,omitempty"`
	TargetService            string          `json:"targetService,omitempty"`
	TargetMember             string          `json:"targetMember,omitempty"`
	TargetCount              int             `json:"targetCount,omitempty"`
	RunningTargets           int             `json:"runningTargets,omitempty"`
	TargetRunning            bool            `json:"targetRunning"`
	OK                       bool            `json:"ok"`
	TotalEndpoints           int             `json:"totalEndpoints,omitempty"`
	HealthyEndpoints         int             `json:"healthyEndpoints,omitempty"`
	FailedEndpoints          int             `json:"failedEndpoints,omitempty"`
	TotalElapsedMillis       int             `json:"totalElapsedMillis,omitempty"`
	MaxEndpointLatencyMillis int             `json:"maxEndpointLatencyMillis,omitempty"`
	Health                   json.RawMessage `json:"health,omitempty"`
	Error                    string          `json:"error"`
}

type dcsQuorumCheckerResult struct {
	Checker               string            `json:"checker"`
	Valid                 bool              `json:"valid"`
	Applicable            bool              `json:"applicable"`
	Error                 string            `json:"error,omitempty"`
	Nemesis               string            `json:"nemesis,omitempty"`
	MinSlowLatencyMillis  int               `json:"minSlowLatencyMillis,omitempty"`
	Samples               int               `json:"samples,omitempty"`
	BeforeSamples         int               `json:"beforeSamples,omitempty"`
	DuringExpectedSamples int               `json:"duringExpectedSamples,omitempty"`
	AfterRecoveredSamples int               `json:"afterRecoveredSamples,omitempty"`
	Observations          []dcsQuorumSample `json:"observations,omitempty"`
}

func newDCSQuorumCheckerCommand() *cobra.Command {
	options := dcsQuorumCheckerOptions{
		minSlowLatencyMillis: 100,
	}

	command := &cobra.Command{
		Use:   "dcs-quorum",
		Short: "check DCS quorum behavior during nemesis",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers dcs-quorum does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runDCSQuorumChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("DCS quorum checker failed")
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.nemesis, "nemesis", "", "selected nemesis")
	command.Flags().StringVar(&options.caseDir, "case-dir", "", "Jepsen case artifact directory")
	command.Flags().StringVar(&options.samplePath, "sample-file", "", "DCS quorum sample JSONL path")
	command.Flags().StringVar(&options.outputPath, "output", "", "checker JSON output path")
	command.Flags().IntVar(&options.minSlowLatencyMillis, "min-slow-latency-ms", options.minSlowLatencyMillis, "minimum observed endpoint latency for dcs-slow-network")

	if err := command.MarkFlagRequired("nemesis"); err != nil {
		panic(err)
	}
	if err := command.MarkFlagRequired("case-dir"); err != nil {
		panic(err)
	}

	return command
}

func runDCSQuorumChecker(options dcsQuorumCheckerOptions) (bool, error) {
	samplePath := options.samplePath
	if samplePath == "" {
		samplePath = filepath.Join(options.caseDir, dcsQuorumSampleFile)
	}
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, dcsQuorumCheckerFile)
	}

	var result dcsQuorumCheckerResult
	var err error
	if !dcsQuorumNemesisApplicable(options.nemesis) {
		result = dcsQuorumCheckerResult{
			Checker:    dcsQuorumCheckerName,
			Valid:      true,
			Applicable: false,
		}
	} else if fileIsMissingOrEmpty(samplePath) {
		result = dcsQuorumCheckerResult{
			Checker:    dcsQuorumCheckerName,
			Valid:      false,
			Applicable: true,
			Error:      "missing DCS quorum probe samples",
		}
	} else {
		var samples []dcsQuorumSample
		samples, err = readDCSQuorumSamples(samplePath)
		if err != nil {
			return false, err
		}
		result = checkDCSQuorumSamples(options.nemesis, options.minSlowLatencyMillis, samples)
	}

	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}
	return result.Valid, nil
}

func dcsQuorumNemesisApplicable(nemesis string) bool {
	switch nemesis {
	case "dcs-kill-one", "dcs-lose-majority", "primary-dcs-majority-partition", "dcs-full-restart", "dcs-slow-network":
		return true
	default:
		return false
	}
}

func fileIsMissingOrEmpty(path string) bool {
	info, err := os.Stat(path)
	return err != nil || info.Size() == 0
}

func readDCSQuorumSamples(path string) ([]dcsQuorumSample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read DCS quorum samples %s: %w", path, err)
	}
	defer file.Close()

	var samples []dcsQuorumSample
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var sample dcsQuorumSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			return nil, fmt.Errorf("parse DCS quorum sample %s:%d: %w", path, lineNumber, err)
		}
		samples = append(samples, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan DCS quorum samples %s: %w", path, err)
	}

	return samples, nil
}

func checkDCSQuorumSamples(nemesis string, minSlowLatencyMillis int, samples []dcsQuorumSample) dcsQuorumCheckerResult {
	beforePhase, duringPhase, afterPhase := dcsQuorumPhases(nemesis)

	beforeSamples := 0
	duringExpectedSamples := 0
	afterRecoveredSamples := 0

	for _, sample := range samples {
		if sample.Phase == beforePhase {
			beforeSamples++
		}
		if sample.Phase == duringPhase && dcsQuorumDuringExpected(nemesis, minSlowLatencyMillis, sample) {
			duringExpectedSamples++
		}
		if sample.Phase == afterPhase && dcsQuorumAfterRecovered(sample) {
			afterRecoveredSamples++
		}
	}

	return dcsQuorumCheckerResult{
		Checker:               dcsQuorumCheckerName,
		Valid:                 duringExpectedSamples > 0 && afterRecoveredSamples > 0,
		Applicable:            true,
		Nemesis:               nemesis,
		MinSlowLatencyMillis:  minSlowLatencyMillis,
		Samples:               len(samples),
		BeforeSamples:         beforeSamples,
		DuringExpectedSamples: duringExpectedSamples,
		AfterRecoveredSamples: afterRecoveredSamples,
		Observations:          samples,
	}
}

func dcsQuorumPhases(nemesis string) (before, during, after string) {
	switch nemesis {
	case "dcs-lose-majority":
		return "before-majority-loss", "during-majority-loss", "after-restart"
	case "primary-dcs-majority-partition":
		return "before-primary-majority-partition", "during-primary-majority-partition", "after-primary-majority-partition"
	case "dcs-full-restart":
		return "before-full-restart", "during-full-restart", "after-full-restart"
	case "dcs-slow-network":
		return "before-dcs-slow-network", "during-dcs-slow-network", "after-dcs-slow-network"
	default:
		return "before-kill", "during-kill", "after-restart"
	}
}

func dcsQuorumDuringExpected(nemesis string, minSlowLatencyMillis int, sample dcsQuorumSample) bool {
	if !sample.OK {
		return false
	}

	switch nemesis {
	case "dcs-lose-majority":
		return sample.HealthyEndpoints <= 1 &&
			sample.FailedEndpoints >= 2 &&
			sample.TargetCount >= 2 &&
			sample.RunningTargets == 0 &&
			!sample.TargetRunning
	case "primary-dcs-majority-partition":
		return sample.HealthyEndpoints <= 1 &&
			sample.FailedEndpoints >= 2 &&
			sample.TargetCount >= 2 &&
			sample.RunningTargets == sample.TargetCount &&
			sample.TargetRunning
	case "dcs-full-restart":
		return sample.HealthyEndpoints == 0 &&
			sample.FailedEndpoints >= 3 &&
			sample.TargetCount >= 3 &&
			sample.RunningTargets == 0 &&
			!sample.TargetRunning
	case "dcs-slow-network":
		return sample.HealthyEndpoints == sample.TotalEndpoints &&
			sample.TotalEndpoints >= 3 &&
			sample.TargetCount >= 3 &&
			sample.RunningTargets == sample.TargetCount &&
			sample.TargetRunning &&
			sample.MaxEndpointLatencyMillis >= minSlowLatencyMillis
	default:
		return sample.HealthyEndpoints >= 2 &&
			sample.FailedEndpoints >= 1 &&
			!sample.TargetRunning
	}
}

func dcsQuorumAfterRecovered(sample dcsQuorumSample) bool {
	return sample.OK &&
		sample.HealthyEndpoints == sample.TotalEndpoints &&
		sample.TotalEndpoints >= 3 &&
		sample.TargetRunning
}

func writeJSONFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory for %s: %w", path, err)
	}

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
