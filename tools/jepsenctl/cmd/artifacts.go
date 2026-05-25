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

var artifactIndexExactNames = map[string]struct{}{
	"history.edn":                                     {},
	"jepsen-history.edn":                              {},
	"results.edn":                                     {},
	"nemesis-schedule.edn":                            {},
	"case-results.jsonl":                              {},
	"nightly-failures.txt":                            {},
	"docker-compose-after-destroy.txt":                {},
	"checker.json":                                    {},
	"single-primary-checker.json":                     {},
	"acknowledged-write-checker.json":                 {},
	"timeline-checker.json":                           {},
	"old-primary-rejoin-checker.json":                 {},
	"manual-switchover-checker.json":                  {},
	"client-traffic-during-nemesis-checker.json":      {},
	"client-traffic-during-nemesis.jsonl":             {},
	"replication-traffic-during-nemesis-checker.json": {},
	"replication-traffic-during-nemesis.jsonl":        {},
	"dcs-traffic-during-nemesis-checker.json":         {},
	"dcs-traffic-during-nemesis.jsonl":                {},
	"dcs-quorum-checker.json":                         {},
	"dcs-quorum-during-nemesis.jsonl":                 {},
	"failover-chain-checker.json":                     {},
	"failover-chain.jsonl":                            {},
	"open-transaction-checker.json":                   {},
	"open-transaction.json":                           {},
	"vip-routing-checker.json":                        {},
	"vip-routing.jsonl":                               {},
	"primary-observations.jsonl":                      {},
	"pacman-cluster-snapshots.jsonl":                  {},
	"pg-stat-wal-receiver.jsonl":                      {},
	"pg-stat-replication.json":                        {},
}

type artifactSummaryOptions struct {
	campaign          string
	caseName          string
	status            int
	statusLabel       string
	harness           string
	store             string
	runner            string
	commit            string
	githubRunID       string
	repoRoot          string
	summaryPath       string
	artifactIndexPath string
	summaryNote       string
}

type caseResult struct {
	Workload string `json:"workload"`
	Nemesis  string `json:"nemesis"`
	Valid    bool   `json:"valid"`
	Details  string `json:"details"`
}

type checkerResult struct {
	Checker    string `json:"checker"`
	Valid      *bool  `json:"valid"`
	Applicable *bool  `json:"applicable"`
	Error      string `json:"error"`
}

func newArtifactsCommand() *cobra.Command {
	artifacts := &cobra.Command{
		Use:   "artifacts",
		Short: "summarize Jepsen artifacts",
	}

	artifacts.AddCommand(newArtifactsSummarizeCommand())

	return artifacts
}

func newArtifactsSummarizeCommand() *cobra.Command {
	options := artifactSummaryOptions{}

	summarize := &cobra.Command{
		Use:   "summarize",
		Short: "write Jepsen summary and artifact index",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("artifacts summarize does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return writeArtifactSummary(options)
		},
	}

	summarize.Flags().StringVar(&options.campaign, "campaign", "", "Jepsen campaign name")
	summarize.Flags().StringVar(&options.caseName, "case", "", "Jepsen case name for case campaigns")
	summarize.Flags().IntVar(&options.status, "status", 0, "campaign exit status")
	summarize.Flags().StringVar(&options.statusLabel, "status-label", "", "summary status label")
	summarize.Flags().StringVar(&options.harness, "harness", "", "Jepsen harness path")
	summarize.Flags().StringVar(&options.store, "store", "", "Jepsen artifact store path")
	summarize.Flags().StringVar(&options.runner, "runner", "", "Jepsen runner path")
	summarize.Flags().StringVar(&options.commit, "commit", "", "commit identifier")
	summarize.Flags().StringVar(&options.githubRunID, "github-run-id", "", "GitHub Actions run id")
	summarize.Flags().StringVar(&options.repoRoot, "repo-root", "", "repository root used to shorten artifact paths")
	summarize.Flags().StringVar(&options.summaryPath, "summary-path", "", "path to write summary markdown")
	summarize.Flags().StringVar(&options.artifactIndexPath, "artifact-index-path", "", "path to write artifact index")
	summarize.Flags().StringVar(&options.summaryNote, "summary-note", "", "explicit summary note")

	if err := summarize.MarkFlagRequired("campaign"); err != nil {
		panic(err)
	}
	if err := summarize.MarkFlagRequired("store"); err != nil {
		panic(err)
	}
	if err := summarize.MarkFlagRequired("summary-path"); err != nil {
		panic(err)
	}
	if err := summarize.MarkFlagRequired("artifact-index-path"); err != nil {
		panic(err)
	}

	return summarize
}

func writeArtifactSummary(options artifactSummaryOptions) error {
	statusLabel := options.statusLabel
	if statusLabel == "" {
		if options.status == 0 {
			statusLabel = "passed"
		} else {
			statusLabel = "failed"
		}
	}

	if err := os.MkdirAll(filepath.Dir(options.summaryPath), 0o755); err != nil {
		return fmt.Errorf("create summary directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(options.artifactIndexPath), 0o755); err != nil {
		return fmt.Errorf("create artifact index directory: %w", err)
	}

	artifactPaths, err := collectArtifactIndex(options.store)
	if err != nil {
		return err
	}
	if err := os.WriteFile(options.artifactIndexPath, []byte(strings.Join(artifactPaths, "\n")+lineSuffix(artifactPaths)), 0o644); err != nil {
		return fmt.Errorf("write artifact index: %w", err)
	}

	failures := collectFailureSummary(options.store)
	summary := renderArtifactSummary(options, statusLabel, artifactPaths, failures)
	if err := os.WriteFile(options.summaryPath, []byte(summary), 0o644); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}

	return nil
}

func collectArtifactIndex(store string) ([]string, error) {
	info, err := os.Stat(store)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat artifact store %s: %w", store, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("artifact store %s is not a directory", store)
	}

	var paths []string
	err = filepath.WalkDir(store, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if artifactIndexIncludes(filepath.Base(path)) {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("collect artifact index: %w", err)
	}

	sort.Strings(paths)
	return paths, nil
}

func artifactIndexIncludes(base string) bool {
	if _, ok := artifactIndexExactNames[base]; ok {
		return true
	}
	return strings.HasSuffix(base, ".html") ||
		strings.Contains(base, "history") && strings.HasSuffix(base, ".edn") ||
		strings.HasSuffix(base, ".log") ||
		strings.HasSuffix(base, ".json")
}

func collectFailureSummary(store string) []string {
	var failures []string

	_ = filepath.WalkDir(store, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}

		switch filepath.Base(path) {
		case "case-results.jsonl":
			failures = append(failures, readCaseResultFailures(path)...)
		case "nightly-failures.txt":
			failures = append(failures, readNightlyFailures(path)...)
		default:
			if strings.HasSuffix(filepath.Base(path), "checker.json") || filepath.Base(path) == "checker.json" {
				failures = append(failures, readCheckerFailure(path)...)
			}
		}

		return nil
	})

	sort.Strings(failures)
	return compactStrings(failures)
}

func readCaseResultFailures(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()

	var failures []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var result caseResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			failures = append(failures, fmt.Sprintf("%s: invalid case result JSON: %v", path, err))
			continue
		}
		if !result.Valid {
			failures = append(failures, fmt.Sprintf("%s:%s failed: %s", result.Workload, result.Nemesis, result.Details))
		}
	}

	return failures
}

func readNightlyFailures(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var failures []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			failures = append(failures, line)
		}
	}
	return failures
}

func readCheckerFailure(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var result checkerResult
	if err := json.Unmarshal(data, &result); err != nil {
		return []string{fmt.Sprintf("%s: invalid checker JSON: %v", path, err)}
	}
	if result.Valid == nil || *result.Valid {
		return nil
	}

	checker := result.Checker
	if checker == "" {
		checker = strings.TrimSuffix(filepath.Base(path), ".json")
	}
	reason := result.Error
	if reason == "" && result.Applicable != nil && !*result.Applicable {
		reason = "not applicable"
	}
	if reason == "" {
		reason = "valid=false"
	}

	return []string{fmt.Sprintf("%s checker failed: %s", checker, reason)}
}

func renderArtifactSummary(options artifactSummaryOptions, statusLabel string, artifactPaths, failures []string) string {
	var builder strings.Builder

	fmt.Fprintf(&builder, "# Jepsen %s %s\n\n", options.campaign, statusLabel)
	fmt.Fprintf(&builder, "- Campaign: `%s`\n", options.campaign)
	if options.campaign == "case" && options.caseName != "" {
		fmt.Fprintf(&builder, "- Case: `%s`\n", options.caseName)
	}
	fmt.Fprintf(&builder, "- Status: `%s`\n", statusLabel)
	fmt.Fprintf(&builder, "- Harness: `%s`\n", options.harness)
	fmt.Fprintf(&builder, "- Store: `%s`\n", options.store)
	fmt.Fprintf(&builder, "- Runner: `%s`\n", options.runner)
	fmt.Fprintf(&builder, "- Commit: `%s`\n", options.commit)
	if options.githubRunID != "" {
		fmt.Fprintf(&builder, "- GitHub run: `%s`\n", options.githubRunID)
	}
	builder.WriteString("\n")

	if options.summaryNote != "" {
		builder.WriteString("## Summary\n\n")
		fmt.Fprintf(&builder, "%s\n\n", options.summaryNote)
	} else if options.status != 0 {
		builder.WriteString("## Summary\n\n")
		fmt.Fprintf(&builder, "The Jepsen campaign exited with status %d. Inspect the HTML report, history, checker output, and node logs in the uploaded artifacts.\n\n", options.status)
	}

	if len(failures) > 0 {
		builder.WriteString("## Failure Summary\n\n")
		for _, failure := range firstN(failures, 20) {
			fmt.Fprintf(&builder, "- %s\n", failure)
		}
		if len(failures) > 20 {
			fmt.Fprintf(&builder, "- ... %d more failure detail(s) omitted from summary\n", len(failures)-20)
		}
		builder.WriteString("\n")
	}

	builder.WriteString("## Review Checklist\n\n")
	builder.WriteString("1. Open this summary first.\n")
	builder.WriteString("2. Inspect Jepsen HTML reports and checker output.\n")
	builder.WriteString("3. Inspect `jepsen-history.edn`, `results.edn`, and `nemesis-schedule.edn` around failure windows.\n")
	builder.WriteString("4. Compare PACMAN cluster/history snapshots with PostgreSQL and DCS logs.\n")
	builder.WriteString("5. Preserve the seed and full artifact path in any regression issue.\n\n")
	builder.WriteString("## Artifact Index\n\n")

	if len(artifactPaths) == 0 {
		if _, err := os.Stat(options.store); os.IsNotExist(err) {
			builder.WriteString("- Jepsen store path was not created.\n")
		} else {
			builder.WriteString("- No Jepsen report, history, log, or JSON artifacts found under the store path.\n")
		}
		return builder.String()
	}

	for _, path := range artifactPaths {
		fmt.Fprintf(&builder, "- `%s`\n", shortenArtifactPath(path, options.repoRoot))
	}

	return builder.String()
}

func shortenArtifactPath(path, repoRoot string) string {
	if repoRoot == "" {
		return path
	}
	cleanRoot := filepath.Clean(repoRoot)
	cleanPath := filepath.Clean(path)
	if rel, err := filepath.Rel(cleanRoot, cleanPath); err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." {
		return rel
	}
	return path
}

func lineSuffix(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return "\n"
}

func firstN(values []string, n int) []string {
	if len(values) <= n {
		return values
	}
	return values[:n]
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	compacted := values[:0]
	var previous string
	for _, value := range values {
		if value == previous {
			continue
		}
		compacted = append(compacted, value)
		previous = value
	}

	return compacted
}
