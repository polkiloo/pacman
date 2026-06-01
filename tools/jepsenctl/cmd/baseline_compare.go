package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

type baselineCompareOptions struct {
	pacmanResults  string
	patroniResults string
}

type profileResults struct {
	Runs   int
	Passed int
}

type baselineComparison struct {
	Profile string
	PACMAN  profileResults
	Patroni *profileResults
}

func newArtifactsCompareBaselineCommand() *cobra.Command {
	options := baselineCompareOptions{}

	compare := &cobra.Command{
		Use:   "compare-baseline",
		Short: "compare PACMAN results with matching Patroni baseline profiles",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("artifacts compare-baseline does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			comparisons, err := compareBaselineResultFiles(options.pacmanResults, options.patroniResults)
			if err != nil {
				return err
			}
			writeBaselineComparisons(cmd, comparisons)
			return nil
		},
	}

	compare.Flags().StringVar(&options.pacmanResults, "pacman-results", "", "PACMAN case-results.jsonl path")
	compare.Flags().StringVar(&options.patroniResults, "patroni-results", "", "Patroni case-results.jsonl path")
	for _, flag := range []string{"pacman-results", "patroni-results"} {
		if err := compare.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}

	return compare
}

func compareBaselineResultFiles(pacmanPath, patroniPath string) ([]baselineComparison, error) {
	pacmanResults, err := readCaseResults(pacmanPath)
	if err != nil {
		return nil, err
	}
	patroniResults, err := readCaseResults(patroniPath)
	if err != nil {
		return nil, err
	}
	return compareBaselineResults(pacmanResults, patroniResults), nil
}

func readCaseResults(path string) ([]caseResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open case results %s: %w", path, err)
	}
	defer file.Close()

	var results []caseResult
	scanner := bufio.NewScanner(file)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var result caseResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			return nil, fmt.Errorf("parse case results %s:%d: %w", path, lineNumber, err)
		}
		if result.Workload == "" || result.Nemesis == "" {
			return nil, fmt.Errorf("parse case results %s:%d: workload and nemesis are required", path, lineNumber)
		}
		results = append(results, result)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan case results %s: %w", path, err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("case results %s did not contain any results", path)
	}

	return results, nil
}

func compareBaselineResults(pacmanResults, patroniResults []caseResult) []baselineComparison {
	pacmanProfiles := groupProfileResults(pacmanResults)
	patroniProfiles := groupProfileResults(patroniResults)

	profiles := make([]string, 0, len(pacmanProfiles))
	for profile := range pacmanProfiles {
		profiles = append(profiles, profile)
	}
	sort.Strings(profiles)

	comparisons := make([]baselineComparison, 0, len(profiles))
	for _, profile := range profiles {
		comparison := baselineComparison{
			Profile: profile,
			PACMAN:  pacmanProfiles[profile],
		}
		if patroni, ok := patroniProfiles[profile]; ok {
			comparison.Patroni = &patroni
		}
		comparisons = append(comparisons, comparison)
	}
	return comparisons
}

func groupProfileResults(results []caseResult) map[string]profileResults {
	profiles := make(map[string]profileResults)
	for _, result := range results {
		profile := result.Workload + ":" + result.Nemesis
		summary := profiles[profile]
		summary.Runs++
		if result.Valid {
			summary.Passed++
		}
		profiles[profile] = summary
	}
	return profiles
}

func writeBaselineComparisons(cmd *cobra.Command, comparisons []baselineComparison) {
	matched := 0
	unmatched := 0

	fmt.Fprintln(cmd.OutOrStdout(), "profile\tpacman\tpatroni\tcomparison")
	for _, comparison := range comparisons {
		patroni := "missing"
		status := "no-matching-profile"
		if comparison.Patroni != nil {
			patroni = formatProfileResults(*comparison.Patroni)
			status = "matching-profile"
			matched++
		} else {
			unmatched++
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%s\n",
			comparison.Profile,
			formatProfileResults(comparison.PACMAN),
			patroni,
			status,
		)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "compared %d matching profile(s); %d PACMAN profile(s) had no matching Patroni baseline\n", matched, unmatched)
}

func formatProfileResults(results profileResults) string {
	status := "failed"
	if results.Passed == results.Runs {
		status = "passed"
	}
	return fmt.Sprintf("%s (%d/%d runs passed)", status, results.Passed, results.Runs)
}
