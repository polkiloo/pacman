package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	acknowledgedWriteCheckerName = "acknowledged-write-preservation"
	acknowledgedWriteCheckerFile = "acknowledged-write-checker.json"
	acknowledgedOpIDsFile        = "acknowledged-op-ids.txt"
	finalPrimaryOpCountsFile     = "final-primary-op-counts.tsv"
)

type acknowledgedWriteCheckerOptions struct {
	workload            string
	runID               string
	caseDir             string
	table               string
	finalPrimary        string
	finalPrimaryService string
	asyncLossAllowed    bool
	ackPath             string
	countsPath          string
	outputPath          string
}

type opCount struct {
	OpID  string
	Count int
}

type acknowledgedWriteCheckerResult struct {
	Checker                     string   `json:"checker"`
	Valid                       bool     `json:"valid"`
	Workload                    string   `json:"workload"`
	RunID                       string   `json:"runId"`
	FinalPrimary                string   `json:"finalPrimary"`
	FinalPrimaryService         string   `json:"finalPrimaryService"`
	Table                       string   `json:"table"`
	AsyncLossAllowed            bool     `json:"asyncLossAllowed"`
	ExpectedAcknowledged        int      `json:"expectedAcknowledged"`
	ObservedExactlyOnce         int      `json:"observedExactlyOnce"`
	MissingAcknowledged         int      `json:"missingAcknowledged"`
	DuplicateAcknowledged       int      `json:"duplicateAcknowledged"`
	UnacknowledgedObserved      int      `json:"unacknowledgedObserved"`
	MissingOpIDs                []string `json:"missingOpIds"`
	DuplicateOpIDs              []string `json:"duplicateOpIds"`
	UnacknowledgedObservedOpIDs []string `json:"unacknowledgedObservedOpIds"`
}

func newAcknowledgedWriteCheckerCommand() *cobra.Command {
	var options acknowledgedWriteCheckerOptions

	command := &cobra.Command{
		Use:   "acknowledged-write",
		Short: "check acknowledged writes are preserved on the final primary",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("checkers acknowledged-write does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			valid, err := runAcknowledgedWriteChecker(options)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("acknowledged write preservation checker failed")
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.workload, "workload", "", "selected Jepsen workload")
	command.Flags().StringVar(&options.runID, "run-id", "", "Jepsen run identifier")
	command.Flags().StringVar(&options.caseDir, "case-dir", "", "Jepsen case artifact directory")
	command.Flags().StringVar(&options.table, "table", "", "final primary SQL table queried for op counts")
	command.Flags().StringVar(&options.finalPrimary, "final-primary", "", "final PACMAN primary member")
	command.Flags().StringVar(&options.finalPrimaryService, "final-primary-service", "", "final PACMAN primary service")
	command.Flags().BoolVar(&options.asyncLossAllowed, "async-loss-allowed", false, "allow missing acknowledged writes for async-loss measurement profiles")
	command.Flags().StringVar(&options.ackPath, "ack-file", "", "acknowledged operation IDs file")
	command.Flags().StringVar(&options.countsPath, "counts-file", "", "final primary operation counts TSV path")
	command.Flags().StringVar(&options.outputPath, "output", "", "checker JSON output path")

	for _, flag := range []string{"workload", "run-id", "case-dir", "table", "final-primary", "final-primary-service"} {
		if err := command.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}

	return command
}

func runAcknowledgedWriteChecker(options acknowledgedWriteCheckerOptions) (bool, error) {
	ackPath := options.ackPath
	if ackPath == "" {
		ackPath = filepath.Join(options.caseDir, acknowledgedOpIDsFile)
	}
	countsPath := options.countsPath
	if countsPath == "" {
		countsPath = filepath.Join(options.caseDir, finalPrimaryOpCountsFile)
	}
	outputPath := options.outputPath
	if outputPath == "" {
		outputPath = filepath.Join(options.caseDir, acknowledgedWriteCheckerFile)
	}

	acknowledged, err := readSortedUniqueLines(ackPath)
	if err != nil {
		return false, err
	}
	counts, err := readOpCounts(countsPath)
	if err != nil {
		return false, err
	}

	result := checkAcknowledgedWrites(options, acknowledged, counts)
	if err := writeAcknowledgedWriteSideArtifacts(options.caseDir, result, acknowledged, counts); err != nil {
		return false, err
	}
	if err := writeJSONFile(outputPath, result); err != nil {
		return false, err
	}

	return result.Valid, nil
}

func readSortedUniqueLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read acknowledged op ids %s: %w", path, err)
	}
	defer file.Close()

	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		seen[line] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan acknowledged op ids %s: %w", path, err)
	}

	return sortedKeys(seen), nil
}

func readOpCounts(path string) ([]opCount, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read final primary op counts %s: %w", path, err)
	}
	defer file.Close()

	var counts []opCount
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}
		count, err := strconv.Atoi(strings.TrimSpace(fields[1]))
		if err != nil {
			return nil, fmt.Errorf("parse final primary op count %s:%d: %w", path, lineNumber, err)
		}
		opID := strings.TrimSpace(fields[0])
		if opID == "" {
			continue
		}
		counts = append(counts, opCount{OpID: opID, Count: count})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan final primary op counts %s: %w", path, err)
	}

	return counts, nil
}

func checkAcknowledgedWrites(options acknowledgedWriteCheckerOptions, acknowledged []string, counts []opCount) acknowledgedWriteCheckerResult {
	actualSet := make(map[string]struct{})
	observedOnceSet := make(map[string]struct{})
	duplicateSet := make(map[string]struct{})
	for _, count := range counts {
		actualSet[count.OpID] = struct{}{}
		if count.Count == 1 {
			observedOnceSet[count.OpID] = struct{}{}
		} else {
			duplicateSet[count.OpID] = struct{}{}
		}
	}

	actual := sortedKeys(actualSet)
	observedOnce := sortedKeys(observedOnceSet)
	duplicates := sortedKeys(duplicateSet)
	missing := difference(acknowledged, actual)
	duplicateAcknowledged := intersection(acknowledged, duplicates)
	unexpected := difference(actual, acknowledged)
	observedAcknowledgedOnce := intersection(acknowledged, observedOnce)

	valid := len(acknowledged) > 0 && len(duplicateAcknowledged) == 0 && (len(missing) == 0 || options.asyncLossAllowed)

	return acknowledgedWriteCheckerResult{
		Checker:                     acknowledgedWriteCheckerName,
		Valid:                       valid,
		Workload:                    options.workload,
		RunID:                       options.runID,
		FinalPrimary:                options.finalPrimary,
		FinalPrimaryService:         options.finalPrimaryService,
		Table:                       options.table,
		AsyncLossAllowed:            options.asyncLossAllowed,
		ExpectedAcknowledged:        len(acknowledged),
		ObservedExactlyOnce:         len(observedAcknowledgedOnce),
		MissingAcknowledged:         len(missing),
		DuplicateAcknowledged:       len(duplicateAcknowledged),
		UnacknowledgedObserved:      len(unexpected),
		MissingOpIDs:                missing,
		DuplicateOpIDs:              duplicateAcknowledged,
		UnacknowledgedObservedOpIDs: unexpected,
	}
}

func writeAcknowledgedWriteSideArtifacts(caseDir string, result acknowledgedWriteCheckerResult, acknowledged []string, counts []opCount) error {
	actualSet := make(map[string]struct{})
	observedOnceSet := make(map[string]struct{})
	duplicateSet := make(map[string]struct{})
	for _, count := range counts {
		actualSet[count.OpID] = struct{}{}
		if count.Count == 1 {
			observedOnceSet[count.OpID] = struct{}{}
		} else {
			duplicateSet[count.OpID] = struct{}{}
		}
	}

	files := map[string][]string{
		"acknowledged-op-ids.sorted":                acknowledged,
		"final-primary-op-ids.sorted":               sortedKeys(actualSet),
		"final-primary-observed-once-op-ids.sorted": sortedKeys(observedOnceSet),
		"final-primary-duplicate-op-ids.sorted":     sortedKeys(duplicateSet),
		"missing-acknowledged-op-ids.txt":           result.MissingOpIDs,
		"duplicate-acknowledged-op-ids.txt":         result.DuplicateOpIDs,
		"unacknowledged-observed-op-ids.txt":        result.UnacknowledgedObservedOpIDs,
	}

	for name, lines := range files {
		if err := writeLinesFile(filepath.Join(caseDir, name), lines); err != nil {
			return err
		}
	}
	return nil
}

func writeLinesFile(path string, lines []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory for %s: %w", path, err)
	}
	content := ""
	if len(lines) > 0 {
		content = strings.Join(lines, "\n") + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func sortedKeys(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func intersection(left, right []string) []string {
	rightSet := stringSet(right)
	result := make([]string, 0)
	for _, value := range left {
		if _, ok := rightSet[value]; ok {
			result = append(result, value)
		}
	}
	return result
}

func difference(left, right []string) []string {
	rightSet := stringSet(right)
	result := make([]string, 0)
	for _, value := range left {
		if _, ok := rightSet[value]; !ok {
			result = append(result, value)
		}
	}
	return result
}
