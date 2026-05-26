package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

const (
	singlePrimaryCheckerName = "single-writable-primary"
	primaryObservationFile   = "primary-observations.jsonl"
	singlePrimaryCheckerFile = "single-primary-checker.json"
)

type singlePrimaryCheckerOptions struct {
	caseDir    string
	samplePath string
	outputPath string
}

type primaryObservation struct {
	SampleID   int             `json:"sampleId"`
	ObservedAt string          `json:"observedAt,omitempty"`
	Member     string          `json:"member,omitempty"`
	Service    string          `json:"service,omitempty"`
	Reachable  bool            `json:"reachable"`
	Writable   bool            `json:"writable"`
	InRecovery json.RawMessage `json:"inRecovery,omitempty"`
	Timeline   int             `json:"timeline,omitempty"`
	LSN        string          `json:"lsn,omitempty"`
	Error      string          `json:"error"`
}

type singlePrimaryViolationSample struct {
	SampleID        int      `json:"sampleId"`
	ObservedAt      string   `json:"observedAt,omitempty"`
	WritableMembers []string `json:"writableMembers"`
	Timelines       []int    `json:"timelines"`
}

type singlePrimaryCheckerResult struct {
	Checker              string                         `json:"checker"`
	Valid                bool                           `json:"valid"`
	Observations         int                            `json:"observations"`
	Samples              int                            `json:"samples"`
	WritableObservations int                            `json:"writableObservations"`
	ViolationSamples     []singlePrimaryViolationSample `json:"violationSamples"`
}

func newSinglePrimaryCheckerCommand() *cobra.Command {
	var options singlePrimaryCheckerOptions

	command := &cobra.Command{
		Use:   "single-primary",
		Short: "check that only one PostgreSQL node is writable per observation sample",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers single-primary does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runSinglePrimaryChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("single writable primary checker failed")
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.caseDir, "case-dir", "", "Jepsen case artifact directory")
	command.Flags().StringVar(&options.samplePath, "sample-file", "", "primary observation JSONL path")
	command.Flags().StringVar(&options.outputPath, "output", "", "checker JSON output path")

	if err := command.MarkFlagRequired("case-dir"); err != nil {
		panic(err)
	}

	return command
}

func runSinglePrimaryChecker(options singlePrimaryCheckerOptions) (bool, error) {
	samplePath := options.samplePath
	if samplePath == "" {
		samplePath = filepath.Join(options.caseDir, primaryObservationFile)
	}
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, singlePrimaryCheckerFile)
	}

	result := singlePrimaryCheckerResult{
		Checker:          singlePrimaryCheckerName,
		Valid:            false,
		ViolationSamples: []singlePrimaryViolationSample{},
	}
	if !fileIsMissingOrEmpty(samplePath) {
		observations, err := readPrimaryObservations(samplePath)
		if err != nil {
			return false, err
		}
		result = checkSinglePrimaryObservations(observations)
	}

	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}
	return result.Valid && result.Samples > 0, nil
}

func readPrimaryObservations(path string) ([]primaryObservation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read primary observations %s: %w", path, err)
	}
	defer file.Close()

	var observations []primaryObservation
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var observation primaryObservation
		if err := json.Unmarshal([]byte(line), &observation); err != nil {
			return nil, fmt.Errorf("parse primary observation %s:%d: %w", path, lineNumber, err)
		}
		observations = append(observations, observation)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan primary observations %s: %w", path, err)
	}

	return observations, nil
}

func checkSinglePrimaryObservations(observations []primaryObservation) singlePrimaryCheckerResult {
	sampleIDs := make(map[int]struct{})
	writableBySample := make(map[int][]primaryObservation)
	for _, observation := range observations {
		sampleIDs[observation.SampleID] = struct{}{}
		if observation.Reachable && observation.Writable {
			writableBySample[observation.SampleID] = append(writableBySample[observation.SampleID], observation)
		}
	}

	samples := make([]int, 0, len(sampleIDs))
	for sampleID := range sampleIDs {
		samples = append(samples, sampleID)
	}
	sort.Ints(samples)

	violations := make([]singlePrimaryViolationSample, 0)
	writableObservations := 0
	for _, sampleID := range samples {
		writable := writableBySample[sampleID]
		writableObservations += len(writable)
		if len(writable) <= 1 {
			continue
		}

		members := make([]string, 0, len(writable))
		timelines := make([]int, 0, len(writable))
		for _, observation := range writable {
			members = append(members, observation.Member)
			timelines = append(timelines, observation.Timeline)
		}
		violations = append(violations, singlePrimaryViolationSample{
			SampleID:        sampleID,
			ObservedAt:      writable[0].ObservedAt,
			WritableMembers: members,
			Timelines:       timelines,
		})
	}

	return singlePrimaryCheckerResult{
		Checker:              singlePrimaryCheckerName,
		Valid:                len(violations) == 0,
		Observations:         len(observations),
		Samples:              len(samples),
		WritableObservations: writableObservations,
		ViolationSamples:     violations,
	}
}
