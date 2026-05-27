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
	vipRoutingCheckerName = "vip-write-routing"
	vipRoutingCheckerFile = "vip-routing-checker.json"
	vipRoutingSamplesFile = "vip-routing.jsonl"
)

type vipRoutingCheckerOptions struct {
	workload   string
	nemesis    string
	caseDir    string
	samplePath string
	outputPath string
}

type vipRoutingSample struct {
	ObservedAt          string `json:"observedAt,omitempty"`
	OpID                string `json:"opId,omitempty"`
	OK                  bool   `json:"ok"`
	Status              int    `json:"status"`
	PACMANPrimaryBefore string `json:"pacmanPrimaryBefore,omitempty"`
	PACMANPrimaryAfter  string `json:"pacmanPrimaryAfter,omitempty"`
	VIPHolderBefore     string `json:"vipHolderBefore,omitempty"`
	VIPHolderAfter      string `json:"vipHolderAfter,omitempty"`
	InRecovery          bool   `json:"inRecovery"`
	ServerAddr          string `json:"serverAddr,omitempty"`
	ReturnedOp          string `json:"returnedOp,omitempty"`
	Error               string `json:"error,omitempty"`
}

type vipRoutingCheckerResult struct {
	Checker                         string             `json:"checker"`
	Valid                           bool               `json:"valid"`
	Applicable                      bool               `json:"applicable"`
	Error                           string             `json:"error,omitempty"`
	Samples                         int                `json:"samples,omitempty"`
	SuccessfulWrites                int                `json:"successfulWrites,omitempty"`
	FailedWrites                    int                `json:"failedWrites,omitempty"`
	MatchedPrimaryMembers           []string           `json:"matchedPrimaryMembers,omitempty"`
	RoutedToReplicaViolations       []vipRoutingSample `json:"routedToReplicaViolations,omitempty"`
	StablePrimaryMismatchViolations []vipRoutingSample `json:"stablePrimaryMismatchViolations,omitempty"`
	Observations                    []vipRoutingSample `json:"observations,omitempty"`
}

func newVIPRoutingCheckerCommand() *cobra.Command {
	var options vipRoutingCheckerOptions

	command := &cobra.Command{
		Use:   "vip-routing",
		Short: "check VIP writes are routed to the current PACMAN primary",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers vip-routing does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runVIPRoutingChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("VIP routing checker failed")
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.workload, "workload", "", "selected Jepsen workload")
	command.Flags().StringVar(&options.nemesis, "nemesis", "", "selected Jepsen nemesis")
	command.Flags().StringVar(&options.caseDir, "case-dir", "", "Jepsen case artifact directory")
	command.Flags().StringVar(&options.samplePath, "sample-file", "", "VIP routing JSONL path")
	command.Flags().StringVar(&options.outputPath, "output", "", "checker JSON output path")

	for _, flag := range []string{"workload", "case-dir"} {
		if err := command.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}

	return command
}

func runVIPRoutingChecker(options vipRoutingCheckerOptions) (bool, error) {
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, vipRoutingCheckerFile)
	}

	result := vipRoutingCheckerResult{
		Checker:    vipRoutingCheckerName,
		Valid:      true,
		Applicable: false,
	}
	if options.workload != "vip-routing" {
		if err := writeJSONFile(outputPath, result); err != nil {
			return false, err
		}
		return true, nil
	}

	result.Valid = false
	result.Applicable = true

	samplePath := options.samplePath
	if samplePath == "" {
		samplePath = filepath.Join(options.caseDir, vipRoutingSamplesFile)
	}
	if fileIsMissingOrEmpty(samplePath) {
		result.Error = "missing VIP routing samples"
		if err := writeJSONFile(outputPath, result); err != nil {
			return false, err
		}
		return false, nil
	}

	samples, err := readVIPRoutingSamples(samplePath)
	if err != nil {
		return false, err
	}
	result = checkVIPRoutingSamples(options.nemesis, samples)
	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}
	return result.Valid, nil
}

func readVIPRoutingSamples(path string) ([]vipRoutingSample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read VIP routing samples %s: %w", path, err)
	}
	defer file.Close()

	var samples []vipRoutingSample
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var sample vipRoutingSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			return nil, fmt.Errorf("parse VIP routing sample %s:%d: %w", path, lineNumber, err)
		}
		samples = append(samples, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan VIP routing samples %s: %w", path, err)
	}

	return samples, nil
}

func checkVIPRoutingSamples(nemesis string, samples []vipRoutingSample) vipRoutingCheckerResult {
	matchedPrimaries := make(map[string]struct{})
	replicaViolations := make([]vipRoutingSample, 0)
	mismatchViolations := make([]vipRoutingSample, 0)
	successfulWrites := 0
	failedWrites := 0

	for _, sample := range samples {
		if sample.OK {
			successfulWrites++
		} else {
			failedWrites++
		}

		if sample.OK && sample.InRecovery {
			replicaViolations = append(replicaViolations, sample)
		}
		if sample.OK && vipRoutingSampleStable(sample) && sample.PACMANPrimaryBefore != sample.VIPHolderBefore {
			mismatchViolations = append(mismatchViolations, sample)
		}
		if sample.OK && !sample.InRecovery && vipRoutingSampleStable(sample) && sample.PACMANPrimaryBefore == sample.VIPHolderBefore {
			matchedPrimaries[sample.PACMANPrimaryBefore] = struct{}{}
		}
	}

	matchedPrimaryMembers := sortedKeys(matchedPrimaries)
	requiredMatchedPrimaries := 1
	if nemesis == "switchover" {
		requiredMatchedPrimaries = 2
	}

	return vipRoutingCheckerResult{
		Checker:                         vipRoutingCheckerName,
		Valid:                           successfulWrites > 0 && len(replicaViolations) == 0 && len(mismatchViolations) == 0 && len(matchedPrimaryMembers) >= requiredMatchedPrimaries,
		Applicable:                      true,
		Samples:                         len(samples),
		SuccessfulWrites:                successfulWrites,
		FailedWrites:                    failedWrites,
		MatchedPrimaryMembers:           matchedPrimaryMembers,
		RoutedToReplicaViolations:       replicaViolations,
		StablePrimaryMismatchViolations: mismatchViolations,
		Observations:                    samples,
	}
}

func vipRoutingSampleStable(sample vipRoutingSample) bool {
	return vipRoutingKnown(sample.PACMANPrimaryBefore) &&
		vipRoutingKnown(sample.PACMANPrimaryAfter) &&
		vipRoutingKnown(sample.VIPHolderBefore) &&
		vipRoutingKnown(sample.VIPHolderAfter) &&
		sample.PACMANPrimaryBefore == sample.PACMANPrimaryAfter &&
		sample.VIPHolderBefore == sample.VIPHolderAfter
}

func vipRoutingKnown(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && value != "unknown"
}
