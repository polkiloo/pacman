package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

const (
	timelineCheckerName = "timeline-convergence"
	timelineCheckerFile = "timeline-checker.json"
)

type timelineCheckerOptions struct {
	caseDir    string
	samplePath string
	outputPath string
}

type timelineMemberSummary struct {
	Member     string `json:"member,omitempty"`
	Service    string `json:"service,omitempty"`
	Reachable  bool   `json:"reachable"`
	Writable   bool   `json:"writable"`
	InRecovery any    `json:"inRecovery"`
	Timeline   int    `json:"timeline,omitempty"`
	LSN        string `json:"lsn,omitempty"`
	Error      string `json:"error"`
}

type timelineSample struct {
	SampleID     int                  `json:"sampleId"`
	ObservedAt   string               `json:"observedAt,omitempty"`
	Observations []primaryObservation `json:"observations"`
}

type timelineSampleSummary struct {
	SampleID        int                     `json:"sampleId"`
	ObservedAt      string                  `json:"observedAt,omitempty"`
	Primary         *timelineMemberSummary  `json:"primary"`
	WritableMembers []string                `json:"writableMembers"`
	Members         []timelineMemberSummary `json:"members"`
}

type timelineCheckerResult struct {
	Checker                   string                  `json:"checker"`
	Valid                     bool                    `json:"valid"`
	Observations              int                     `json:"observations"`
	Samples                   int                     `json:"samples"`
	Error                     string                  `json:"error,omitempty"`
	InitialSample             *timelineSampleSummary  `json:"initialSample,omitempty"`
	FinalSample               *timelineSampleSummary  `json:"finalSample,omitempty"`
	PromotionObserved         bool                    `json:"promotionObserved"`
	TimelineAdvanced          bool                    `json:"timelineAdvanced"`
	ReplicasConverged         bool                    `json:"replicasConverged"`
	OldPrimarySafe            bool                    `json:"oldPrimarySafe"`
	ReplicaTimelineViolations []timelineMemberSummary `json:"replicaTimelineViolations"`
	OldPrimaryFinalState      *timelineMemberSummary  `json:"oldPrimaryFinalState"`
}

func newTimelineCheckerCommand() *cobra.Command {
	var options timelineCheckerOptions

	command := &cobra.Command{
		Use:   "timeline",
		Short: "check PostgreSQL timeline convergence after failover",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers timeline does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runTimelineChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("timeline convergence checker failed")
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

func runTimelineChecker(options timelineCheckerOptions) (bool, error) {
	samplePath := options.samplePath
	if samplePath == "" {
		samplePath = filepath.Join(options.caseDir, primaryObservationFile)
	}
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, timelineCheckerFile)
	}

	result := timelineCheckerResult{
		Checker:      timelineCheckerName,
		Valid:        false,
		Observations: 0,
		Samples:      0,
		Error:        "missing primary observations",
	}
	if !fileIsMissingOrEmpty(samplePath) {
		observations, err := readPrimaryObservations(samplePath)
		if err != nil {
			return false, err
		}
		result = checkTimelineConvergence(observations)
	}

	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}
	return result.Valid, nil
}

func checkTimelineConvergence(observations []primaryObservation) timelineCheckerResult {
	samples := groupTimelineSamples(observations)
	result := timelineCheckerResult{
		Checker:                   timelineCheckerName,
		Observations:              len(observations),
		Samples:                   len(samples),
		ReplicaTimelineViolations: []timelineMemberSummary{},
	}
	if len(samples) == 0 {
		result.Error = "missing primary observations"
		return result
	}

	initialSample := samples[0]
	finalSample := samples[len(samples)-1]
	initialWritable := writableMembers(initialSample)
	finalWritable := writableMembers(finalSample)
	initialPrimary := primaryOf(initialSample)
	finalPrimary := primaryOf(finalSample)
	hasPrimaries := initialPrimary != nil && finalPrimary != nil
	primaryMemberChanged := hasPrimaries && initialPrimary.Member != finalPrimary.Member
	promotionObserved := hasPrimaries && (primaryMemberChanged || finalPrimary.Timeline > initialPrimary.Timeline)
	timelineAdvanced := false
	if hasPrimaries {
		timelineAdvanced = !promotionObserved || finalPrimary.Timeline > initialPrimary.Timeline
	}

	replicaTimelineViolations := make([]timelineMemberSummary, 0)
	if finalPrimary != nil {
		for _, observation := range finalSample.Observations {
			if observation.Reachable && observation.Member != finalPrimary.Member && observation.Timeline != finalPrimary.Timeline {
				replicaTimelineViolations = append(replicaTimelineViolations, summarizeTimelineMember(observation))
			}
		}
	}

	var oldPrimaryFinalState *timelineMemberSummary
	if primaryMemberChanged {
		for _, observation := range finalSample.Observations {
			if observation.Member == initialPrimary.Member {
				summary := summarizeTimelineMember(observation)
				oldPrimaryFinalState = &summary
				break
			}
		}
	}

	oldPrimarySafe := true
	if primaryMemberChanged {
		oldPrimarySafe = false
		if oldPrimaryFinalState != nil {
			oldPrimarySafe = !oldPrimaryFinalState.Reachable || (!oldPrimaryFinalState.Writable && oldPrimaryFinalState.Timeline == finalPrimary.Timeline)
		}
	}
	replicasConverged := len(replicaTimelineViolations) == 0

	var problems []string
	if len(initialWritable) != 1 {
		problems = append(problems, fmt.Sprintf("initial writable member count is %d, want 1", len(initialWritable)))
	}
	if len(finalWritable) != 1 {
		problems = append(problems, fmt.Sprintf("final writable member count is %d, want 1", len(finalWritable)))
	}
	if !timelineAdvanced {
		problems = append(problems, "timeline did not advance after promotion")
	}
	if !replicasConverged {
		problems = append(problems, fmt.Sprintf("replica timeline violations: %d", len(replicaTimelineViolations)))
	}
	if !oldPrimarySafe {
		problems = append(problems, "old primary is still unsafe")
	}

	result.Valid = len(samples) > 0 &&
		len(initialWritable) == 1 &&
		len(finalWritable) == 1 &&
		timelineAdvanced &&
		replicasConverged &&
		oldPrimarySafe
	result.InitialSample = summarizeTimelineSample(initialSample)
	result.FinalSample = summarizeTimelineSample(finalSample)
	result.PromotionObserved = promotionObserved
	result.TimelineAdvanced = timelineAdvanced
	result.ReplicasConverged = replicasConverged
	result.OldPrimarySafe = oldPrimarySafe
	result.ReplicaTimelineViolations = replicaTimelineViolations
	result.OldPrimaryFinalState = oldPrimaryFinalState
	if !result.Valid {
		result.Error = strings.Join(problems, "; ")
	}
	return result
}

func groupTimelineSamples(observations []primaryObservation) []timelineSample {
	latestRoundBySample := make(map[int]int)
	for _, observation := range observations {
		if observation.ProbeRound > latestRoundBySample[observation.SampleID] {
			latestRoundBySample[observation.SampleID] = observation.ProbeRound
		}
	}

	grouped := make(map[int][]primaryObservation)
	for _, observation := range observations {
		if observation.ProbeRound != latestRoundBySample[observation.SampleID] {
			continue
		}
		grouped[observation.SampleID] = append(grouped[observation.SampleID], observation)
	}

	sampleIDs := make([]int, 0, len(grouped))
	for sampleID := range grouped {
		sampleIDs = append(sampleIDs, sampleID)
	}
	sort.Ints(sampleIDs)

	samples := make([]timelineSample, 0, len(sampleIDs))
	for _, sampleID := range sampleIDs {
		observations := grouped[sampleID]
		observedAt := ""
		if len(observations) > 0 {
			observedAt = observations[0].ObservedAt
		}
		samples = append(samples, timelineSample{
			SampleID:     sampleID,
			ObservedAt:   observedAt,
			Observations: observations,
		})
	}
	return samples
}

func writableMembers(sample timelineSample) []primaryObservation {
	writable := make([]primaryObservation, 0)
	for _, observation := range sample.Observations {
		if observation.Reachable && observation.Writable {
			writable = append(writable, observation)
		}
	}
	return writable
}

func primaryOf(sample timelineSample) *primaryObservation {
	writable := writableMembers(sample)
	sort.SliceStable(writable, func(i, j int) bool {
		return writable[i].Member < writable[j].Member
	})
	if len(writable) == 0 {
		return nil
	}
	return &writable[0]
}

func summarizeTimelineSample(sample timelineSample) *timelineSampleSummary {
	primaryObservation := primaryOf(sample)
	var primary *timelineMemberSummary
	if primaryObservation != nil {
		summary := summarizeTimelineMember(*primaryObservation)
		primary = &summary
	}

	writable := writableMembers(sample)
	writableNames := make([]string, 0, len(writable))
	for _, observation := range writable {
		writableNames = append(writableNames, observation.Member)
	}

	members := make([]timelineMemberSummary, 0, len(sample.Observations))
	for _, observation := range sample.Observations {
		members = append(members, summarizeTimelineMember(observation))
	}

	return &timelineSampleSummary{
		SampleID:        sample.SampleID,
		ObservedAt:      sample.ObservedAt,
		Primary:         primary,
		WritableMembers: writableNames,
		Members:         members,
	}
}

func summarizeTimelineMember(observation primaryObservation) timelineMemberSummary {
	return timelineMemberSummary{
		Member:     observation.Member,
		Service:    observation.Service,
		Reachable:  observation.Reachable,
		Writable:   observation.Writable,
		InRecovery: decodeRawJSONValue(observation.InRecovery),
		Timeline:   observation.Timeline,
		LSN:        observation.LSN,
		Error:      observation.Error,
	}
}

func decodeRawJSONValue(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return value
}
