package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	oldPrimaryRejoinCheckerName = "old-primary-rejoin-after-failover"
	oldPrimaryRejoinCheckerFile = "old-primary-rejoin-checker.json"
)

type oldPrimaryRejoinCheckerOptions struct {
	caseDir    string
	nemesis    string
	samplePath string
	outputPath string
}

type oldPrimaryRejoinCheckerResult struct {
	Checker                  string                 `json:"checker"`
	Valid                    bool                   `json:"valid"`
	Applicable               bool                   `json:"applicable"`
	Reason                   string                 `json:"reason,omitempty"`
	Error                    string                 `json:"error,omitempty"`
	Nemesis                  string                 `json:"nemesis,omitempty"`
	Observations             int                    `json:"observations"`
	Samples                  int                    `json:"samples"`
	PromotionObserved        bool                   `json:"promotionObserved"`
	InitialPrimary           *timelineMemberSummary `json:"initialPrimary"`
	FinalPrimary             *timelineMemberSummary `json:"finalPrimary"`
	OldPrimaryRejoined       bool                   `json:"oldPrimaryRejoined"`
	OldPrimarySafeOrRejoined bool                   `json:"oldPrimarySafeOrRejoined"`
	OldPrimaryFinalState     *timelineMemberSummary `json:"oldPrimaryFinalState"`
}

func newOldPrimaryRejoinCheckerCommand() *cobra.Command {
	var options oldPrimaryRejoinCheckerOptions

	command := &cobra.Command{
		Use:   "old-primary-rejoin",
		Short: "check old primary safety or rejoin after failover",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers old-primary-rejoin does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runOldPrimaryRejoinChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("old primary rejoin checker failed")
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.caseDir, "case-dir", "", "Jepsen case artifact directory")
	command.Flags().StringVar(&options.nemesis, "nemesis", "", "selected Jepsen nemesis")
	command.Flags().StringVar(&options.samplePath, "sample-file", "", "primary observation JSONL path")
	command.Flags().StringVar(&options.outputPath, "output", "", "checker JSON output path")

	if err := command.MarkFlagRequired("case-dir"); err != nil {
		panic(err)
	}

	return command
}

func runOldPrimaryRejoinChecker(options oldPrimaryRejoinCheckerOptions) (bool, error) {
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, oldPrimaryRejoinCheckerFile)
	}

	result := oldPrimaryRejoinCheckerResult{
		Checker:      oldPrimaryRejoinCheckerName,
		Valid:        false,
		Applicable:   false,
		Observations: 0,
		Samples:      0,
	}
	if options.nemesis == "switchover" {
		result.Valid = true
		result.Reason = "manual switchover is covered by the manual switchover checker"
		if err := writeJSONFile(outputPath, result); err != nil {
			return false, err
		}
		return true, nil
	}

	samplePath := options.samplePath
	if samplePath == "" {
		samplePath = filepath.Join(options.caseDir, primaryObservationFile)
	}
	if fileIsMissingOrEmpty(samplePath) {
		result.Error = "missing primary observations"
		if err := writeJSONFile(outputPath, result); err != nil {
			return false, err
		}
		return false, nil
	}

	observations, err := readPrimaryObservations(samplePath)
	if err != nil {
		return false, err
	}
	result = checkOldPrimaryRejoinAfterFailover(observations, options.nemesis)
	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}
	return result.Valid, nil
}

func checkOldPrimaryRejoinAfterFailover(observations []primaryObservation, nemesis string) oldPrimaryRejoinCheckerResult {
	samples := groupTimelineSamples(observations)
	result := oldPrimaryRejoinCheckerResult{
		Checker:      oldPrimaryRejoinCheckerName,
		Nemesis:      nemesis,
		Observations: len(observations),
		Samples:      len(samples),
	}
	if len(samples) == 0 {
		result.Error = "missing primary observations"
		return result
	}

	initialSample := samples[0]
	finalSample := samples[len(samples)-1]
	initialPrimary := primaryOf(initialSample)
	finalPrimary := primaryOf(finalSample)
	promotionObserved := initialPrimary != nil && finalPrimary != nil && initialPrimary.Member != finalPrimary.Member

	var initialPrimarySummary *timelineMemberSummary
	if initialPrimary != nil {
		summary := summarizeTimelineMember(*initialPrimary)
		initialPrimarySummary = &summary
	}
	var finalPrimarySummary *timelineMemberSummary
	if finalPrimary != nil {
		summary := summarizeTimelineMember(*finalPrimary)
		finalPrimarySummary = &summary
	}

	var oldPrimaryFinalState *timelineMemberSummary
	if promotionObserved {
		for _, observation := range finalSample.Observations {
			if observation.Member == initialPrimary.Member {
				summary := summarizeTimelineMember(observation)
				oldPrimaryFinalState = &summary
				break
			}
		}
	}

	oldPrimaryRejoined := true
	if promotionObserved {
		oldPrimaryRejoined = false
		if oldPrimaryFinalState != nil && finalPrimary != nil {
			oldPrimaryRejoined = oldPrimaryFinalState.Reachable &&
				!oldPrimaryFinalState.Writable &&
				timelineMemberInRecovery(*oldPrimaryFinalState) &&
				oldPrimaryFinalState.Timeline == finalPrimary.Timeline
		}
	}

	oldPrimarySafeOrRejoined := true
	if promotionObserved {
		oldPrimarySafeOrRejoined = false
		if oldPrimaryFinalState != nil && finalPrimary != nil {
			if failureNemesisAllowsUnavailableOldPrimary(nemesis) {
				oldPrimarySafeOrRejoined = !oldPrimaryFinalState.Reachable ||
					(!oldPrimaryFinalState.Writable &&
						(timelineMemberInRecovery(*oldPrimaryFinalState) ||
							oldPrimaryFinalState.Timeline == finalPrimary.Timeline))
			} else {
				oldPrimarySafeOrRejoined = oldPrimaryRejoined
			}
		}
	}

	result.Valid = len(samples) > 0 && (!promotionObserved || oldPrimarySafeOrRejoined)
	result.Applicable = promotionObserved
	result.PromotionObserved = promotionObserved
	result.InitialPrimary = initialPrimarySummary
	result.FinalPrimary = finalPrimarySummary
	result.OldPrimaryRejoined = oldPrimaryRejoined
	result.OldPrimarySafeOrRejoined = oldPrimarySafeOrRejoined
	result.OldPrimaryFinalState = oldPrimaryFinalState
	return result
}

func failureNemesisAllowsUnavailableOldPrimary(nemesis string) bool {
	switch nemesis {
	case "kill", "packet,kill", "repeated-failure":
		return true
	default:
		return false
	}
}

func timelineMemberInRecovery(member timelineMemberSummary) bool {
	value, ok := member.InRecovery.(bool)
	return ok && value
}
