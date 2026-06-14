package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

type runOptions struct {
	campaign string
	caseName string
	target   jepsenTarget
}

type commandSpec struct {
	name   string
	args   []string
	dir    string
	env    []string
	stdout io.Writer
	stderr io.Writer
}

type commandRunner interface {
	Run(context.Context, commandSpec) (int, error)
}

type osCommandRunner struct{}

func newRunCommand(stdout, stderr io.Writer) *cobra.Command {
	run := &cobra.Command{
		Use:   "run",
		Short: "run Jepsen campaigns",
	}

	run.AddCommand(newRunCICommand(stdout, stderr, osCommandRunner{}))
	run.AddCommand(newRunDockerCommand(stdout, stderr, osCommandRunner{}))

	return run
}

func newRunCICommand(stdout, stderr io.Writer, runner commandRunner) *cobra.Command {
	command := &cobra.Command{
		Use:   "ci smoke|nightly|case [case-name|workload:nemesis]",
		Short: "run a Jepsen campaign on the current host",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseRunOptions(args)
			if err != nil {
				return err
			}

			status, err := runCICampaign(cmd.Context(), options, stdout, stderr, runner)
			if err != nil {
				return err
			}
			if status != 0 {
				return commandExitError{code: status}
			}
			return nil
		},
	}

	return command
}

func newRunDockerCommand(stdout, stderr io.Writer, runner commandRunner) *cobra.Command {
	command := &cobra.Command{
		Use:   "docker smoke|nightly|case [case-name|workload:nemesis]",
		Short: "run a Jepsen campaign from the Docker control-node image",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := parseRunOptions(args)
			if err != nil {
				return err
			}

			status, err := runDockerCampaign(cmd.Context(), options, stdout, stderr, runner)
			if err != nil {
				return err
			}
			if status != 0 {
				return commandExitError{code: status}
			}
			return nil
		},
	}

	return command
}

func parseRunOptions(args []string) (runOptions, error) {
	if len(args) == 0 {
		return runOptions{}, fmt.Errorf("campaign is required")
	}

	target, err := resolveJepsenTarget(envOrDefault("PACMAN_JEPSEN_TARGET", defaultJepsenTarget))
	if err != nil {
		return runOptions{}, err
	}

	options := runOptions{campaign: args[0], target: target}
	switch options.campaign {
	case "smoke", "nightly":
		if len(args) > 1 {
			return runOptions{}, fmt.Errorf("%s campaign does not accept a case argument", options.campaign)
		}
	case "case":
		if len(args) > 1 {
			options.caseName = args[1]
		} else {
			options.caseName = os.Getenv("PACMAN_JEPSEN_CASE")
		}
		if options.caseName == "" {
			return runOptions{}, fmt.Errorf("case campaign requires a case-name or PACMAN_JEPSEN_CASE")
		}
	default:
		return runOptions{}, fmt.Errorf("unsupported Jepsen campaign %q", options.campaign)
	}

	return options, nil
}

func runCICampaign(ctx context.Context, options runOptions, stdout, stderr io.Writer, runner commandRunner) (int, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return 1, err
	}

	jepsenDir := envOrDefault("PACMAN_JEPSEN_DIR", filepath.Join(repoRoot, "jepsen"))
	artifactDir := envOrDefault("PACMAN_JEPSEN_ARTIFACT_DIR", filepath.Join(jepsenDir, "store"))
	ciArtifactDir := envOrDefault("PACMAN_JEPSEN_CI_ARTIFACT_DIR", filepath.Join(repoRoot, "bin", "jepsen-ci", options.campaign))
	summaryPath := envOrDefault("PACMAN_JEPSEN_SUMMARY_PATH", filepath.Join(ciArtifactDir, "summary.md"))
	artifactIndexPath := filepath.Join(ciArtifactDir, "artifact-index.txt")
	runnerPath := "jepsenctl run ci " + options.campaign

	summary := ciSummary{
		options:           options,
		repoRoot:          repoRoot,
		jepsenDir:         jepsenDir,
		artifactDir:       artifactDir,
		ciArtifactDir:     ciArtifactDir,
		summaryPath:       summaryPath,
		artifactIndexPath: artifactIndexPath,
		runnerPath:        runnerPath,
		stdout:            stdout,
	}

	if info, err := os.Stat(jepsenDir); err != nil || !info.IsDir() {
		notice(stdout, fmt.Sprintf("Jepsen harness directory is not present at %s; skipping %s campaign.", jepsenDir, options.campaign))
		summary.statusLabel = "skipped"
		summary.note = "Skipped because the Jepsen harness directory is not present yet."
		return 0, summary.write(0)
	}

	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return 1, fmt.Errorf("create Jepsen artifact directory: %w", err)
	}

	env := append(os.Environ(),
		"PACMAN_JEPSEN_ARTIFACT_DIR="+artifactDir,
		"PACMAN_JEPSEN_CI_ARTIFACT_DIR="+ciArtifactDir,
		"PACMAN_JEPSEN_SUMMARY_PATH="+summaryPath,
		"PACMAN_JEPSEN_TARGET="+options.target.Name,
	)
	if options.caseName != "" {
		env = append(env, "PACMAN_JEPSEN_CASE="+options.caseName)
	}

	status, runErr := runHarnessCampaign(ctx, harnessOptions{
		runOptions:  options,
		repoRoot:    repoRoot,
		jepsenDir:   jepsenDir,
		artifactDir: artifactDir,
		env:         env,
		stdout:      stdout,
		stderr:      stderr,
		runner:      runner,
	})
	if runErr != nil {
		return status, runErr
	}
	if err := summary.write(status); err != nil {
		return status, err
	}

	return status, nil
}

type ciSummary struct {
	options           runOptions
	repoRoot          string
	jepsenDir         string
	artifactDir       string
	ciArtifactDir     string
	summaryPath       string
	artifactIndexPath string
	runnerPath        string
	statusLabel       string
	note              string
	stdout            io.Writer
}

func (summary ciSummary) write(status int) error {
	statusLabel := summary.statusLabel
	if statusLabel == "" {
		if status == 0 {
			statusLabel = "passed"
		} else {
			statusLabel = "failed"
		}
	}

	if err := os.MkdirAll(summary.ciArtifactDir, 0o755); err != nil {
		return fmt.Errorf("create CI artifact directory: %w", err)
	}

	err := writeArtifactSummary(artifactSummaryOptions{
		campaign:          summary.options.campaign,
		caseName:          summary.options.caseName,
		status:            status,
		statusLabel:       statusLabel,
		harness:           summary.jepsenDir,
		store:             summary.artifactDir,
		runner:            summary.runnerPath,
		commit:            commitRef(summary.repoRoot),
		githubRunID:       os.Getenv("GITHUB_RUN_ID"),
		repoRoot:          summary.repoRoot,
		summaryPath:       summary.summaryPath,
		artifactIndexPath: summary.artifactIndexPath,
		summaryNote:       summary.note,
	})
	if err != nil {
		return err
	}

	if githubSummary := os.Getenv("GITHUB_STEP_SUMMARY"); githubSummary != "" {
		data, err := os.ReadFile(summary.summaryPath)
		if err != nil {
			return fmt.Errorf("read Jepsen summary for GitHub step summary: %w", err)
		}
		file, err := os.OpenFile(githubSummary, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open GitHub step summary: %w", err)
		}
		defer file.Close()
		if _, err := file.Write(data); err != nil {
			return fmt.Errorf("append GitHub step summary: %w", err)
		}
	}

	return nil
}

type harnessOptions struct {
	runOptions
	repoRoot    string
	jepsenDir   string
	artifactDir string
	env         []string
	stdout      io.Writer
	stderr      io.Writer
	runner      commandRunner
}

func runHarnessCampaign(ctx context.Context, options harnessOptions) (int, error) {
	switch options.campaign {
	case "smoke":
		return runHarnessSmoke(ctx, options)
	case "nightly":
		return runHarnessNightly(ctx, options)
	case "case":
		return runHarnessCase(ctx, options)
	default:
		return 1, fmt.Errorf("unsupported Jepsen campaign %q", options.campaign)
	}
}

func runHarnessSmoke(ctx context.Context, options harnessOptions) (status int, err error) {
	cases := campaignCases("smoke")
	runDir := runDirFor(options.artifactDir, "smoke", options.target)
	historyFile := filepath.Join(runDir, "jepsen-history.edn")
	scheduleFile := filepath.Join(runDir, "nemesis-schedule.edn")

	if err := createHarnessFiles(runDir, historyFile, scheduleFile); err != nil {
		return 1, err
	}
	defer finishHarnessCampaign(ctx, options, runDir, historyFile, "smoke", &status, &err)

	if status, err = writeEDNEvent(historyFile, "bootstrap", "invoke", `"docker-lab"`); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "bootstrap_lab"); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "bootstrap", "ok", `"docker-lab"`); err != nil || status != 0 {
		return status, err
	}

	if status, err = writeEDNEvent(historyFile, "cluster", "invoke", `"three-data-node"`); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "verify_three_data_node_cluster "+shellLiteral(filepath.Join(runDir, "pacman-cluster-before.json"))); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "cluster", "ok", `"three-data-node"`); err != nil || status != 0 {
		return status, err
	}

	if status, err = writeEDNEvent(historyFile, "verify", "invoke", `"demo-verify"`); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "verify_lab"); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "verify", "ok", `"demo-verify"`); err != nil || status != 0 {
		return status, err
	}

	casesValue := strings.Join(cases, " ")
	if status, err = writeEDNEvent(historyFile, "cases", "invoke", fmt.Sprintf("%q", casesValue)); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "run_jepsen_cases "+shellLiteral(casesValue)+" "+shellLiteral(runDir)+" "+shellLiteral(historyFile)+" "+shellLiteral(scheduleFile)); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "cases", "ok", fmt.Sprintf("%q", casesValue)); err != nil || status != 0 {
		return status, err
	}

	if status, err = writeEDNEvent(historyFile, "verify", "invoke", `"demo-verify-after-cases"`); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "verify_lab"); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "verify", "ok", `"demo-verify-after-cases"`); err != nil || status != 0 {
		return status, err
	}

	return 0, nil
}

func runHarnessCase(ctx context.Context, options harnessOptions) (status int, err error) {
	caseSpec, err := resolveCaseSpec(options.caseName)
	if err != nil {
		return 2, err
	}

	runDir := runDirFor(options.artifactDir, "case-"+caseSlug(caseSpec), options.target)
	historyFile := filepath.Join(runDir, "jepsen-history.edn")
	scheduleFile := filepath.Join(runDir, "nemesis-schedule.edn")

	if err := createHarnessFiles(runDir, historyFile, scheduleFile); err != nil {
		return 1, err
	}
	options.env = append(options.env, "PACMAN_JEPSEN_CAMPAIGN=case-"+caseSlug(caseSpec))
	defer finishHarnessCampaign(ctx, options, runDir, historyFile, "case", &status, &err)

	if envOrDefault("PACMAN_JEPSEN_BOOTSTRAP_LAB", "true") == "true" {
		if status, err = writeEDNEvent(historyFile, "bootstrap", "invoke", `"docker-lab"`); err != nil || status != 0 {
			return status, err
		}
		if status, err = options.callHarness(ctx, "bootstrap_lab_with_retries "+shellLiteral("case:"+caseSpec)); err != nil || status != 0 {
			return status, err
		}
		if status, err = writeEDNEvent(historyFile, "bootstrap", "ok", `"docker-lab"`); err != nil || status != 0 {
			return status, err
		}
	} else if status, err = writeEDNEvent(historyFile, "bootstrap", "ok", `"existing-docker-lab"`); err != nil || status != 0 {
		return status, err
	}

	if status, err = writeEDNEvent(historyFile, "cluster", "invoke", `"three-data-node"`); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "verify_three_data_node_cluster "+shellLiteral(filepath.Join(runDir, "pacman-cluster-before.json"))); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "cluster", "ok", `"three-data-node"`); err != nil || status != 0 {
		return status, err
	}

	if envOrDefault("PACMAN_JEPSEN_VERIFY_LAB", "true") == "true" {
		if status, err = writeEDNEvent(historyFile, "verify", "invoke", `"demo-verify-before"`); err != nil || status != 0 {
			return status, err
		}
		if status, err = options.callHarness(ctx, "verify_lab"); err != nil || status != 0 {
			return status, err
		}
		if status, err = writeEDNEvent(historyFile, "verify", "ok", `"demo-verify-before"`); err != nil || status != 0 {
			return status, err
		}
	}

	if status, err = writeEDNEvent(historyFile, "case", "invoke", fmt.Sprintf("%q", caseSpec)); err != nil || status != 0 {
		return status, err
	}
	if status, err = options.callHarness(ctx, "run_jepsen_cases "+shellLiteral(caseSpec)+" "+shellLiteral(runDir)+" "+shellLiteral(historyFile)+" "+shellLiteral(scheduleFile)); err != nil || status != 0 {
		return status, err
	}
	if status, err = writeEDNEvent(historyFile, "case", "ok", fmt.Sprintf("%q", caseSpec)); err != nil || status != 0 {
		return status, err
	}

	if envOrDefault("PACMAN_JEPSEN_VERIFY_LAB", "true") == "true" {
		if status, err = writeEDNEvent(historyFile, "verify", "invoke", `"demo-verify-after-case"`); err != nil || status != 0 {
			return status, err
		}
		if status, err = options.callHarness(ctx, "verify_lab"); err != nil || status != 0 {
			return status, err
		}
		if status, err = writeEDNEvent(historyFile, "verify", "ok", `"demo-verify-after-case"`); err != nil || status != 0 {
			return status, err
		}
	}

	return 0, nil
}

func runHarnessNightly(ctx context.Context, options harnessOptions) (status int, err error) {
	cases := campaignCases("nightly")
	runDir := runDirFor(options.artifactDir, "nightly", options.target)
	historyFile := filepath.Join(runDir, "jepsen-history.edn")
	scheduleFile := filepath.Join(runDir, "nemesis-schedule.edn")
	failuresFile := filepath.Join(runDir, "nightly-failures.txt")
	caseResultsFile := filepath.Join(runDir, "case-results.jsonl")

	if err := createHarnessFiles(runDir, historyFile, scheduleFile, failuresFile, caseResultsFile); err != nil {
		return 1, err
	}
	if err := os.MkdirAll(filepath.Join(runDir, "cases"), 0o755); err != nil {
		return 1, fmt.Errorf("create nightly cases directory: %w", err)
	}
	options.env = append(options.env, "PACMAN_JEPSEN_CAMPAIGN=nightly")
	defer finishHarnessCampaign(ctx, options, runDir, historyFile, "nightly", &status, &err)

	failed := false
	var failedCases []string
	casesValue := strings.Join(cases, " ")
	if status, err = writeEDNEvent(historyFile, "cases", "invoke", fmt.Sprintf("%q", casesValue)); err != nil || status != 0 {
		return status, err
	}

	for _, spec := range cases {
		workload, nemesis := splitCaseSpec(spec)
		caseLabel := workload + ":" + nemesis
		slug := caseSlug(workload + "__" + nemesis)

		if status, err = writeEDNEvent(historyFile, "bootstrap", "invoke", fmt.Sprintf("%q", "docker-lab:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}
		status, err = options.callHarness(ctx, "bootstrap_lab_with_retries "+shellLiteral(caseLabel))
		if err != nil {
			return status, err
		}
		if status != 0 {
			_, _ = writeEDNEvent(historyFile, "bootstrap", "fail", fmt.Sprintf("%q", "docker-lab:"+caseLabel))
			appendLine(failuresFile, fmt.Sprintf("%s bootstrap failed", caseLabel))
			failedCases = append(failedCases, caseLabel+":bootstrap")
			failed = true
			continue
		}
		if status, err = writeEDNEvent(historyFile, "bootstrap", "ok", fmt.Sprintf("%q", "docker-lab:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}

		if status, err = writeEDNEvent(historyFile, "cluster", "invoke", fmt.Sprintf("%q", "three-data-node:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}
		status, err = options.callHarness(ctx, "verify_three_data_node_cluster "+shellLiteral(filepath.Join(runDir, "pacman-cluster-before-"+slug+".json")))
		if err != nil {
			return status, err
		}
		if status != 0 {
			_, _ = writeEDNEvent(historyFile, "cluster", "fail", fmt.Sprintf("%q", "three-data-node:"+caseLabel))
			appendLine(failuresFile, fmt.Sprintf("%s cluster shape failed", caseLabel))
			failedCases = append(failedCases, caseLabel+":cluster")
			failed = true
			continue
		}
		if status, err = writeEDNEvent(historyFile, "cluster", "ok", fmt.Sprintf("%q", "three-data-node:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}

		if status, err = writeEDNEvent(historyFile, "verify", "invoke", fmt.Sprintf("%q", "demo-verify-before:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}
		status, err = options.callHarness(ctx, "verify_lab")
		if err != nil {
			return status, err
		}
		if status != 0 {
			_, _ = writeEDNEvent(historyFile, "verify", "fail", fmt.Sprintf("%q", "demo-verify-before:"+caseLabel))
			appendLine(failuresFile, fmt.Sprintf("%s verify-before failed", caseLabel))
			failedCases = append(failedCases, caseLabel+":verify-before")
			failed = true
			continue
		}
		if status, err = writeEDNEvent(historyFile, "verify", "ok", fmt.Sprintf("%q", "demo-verify-before:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}

		if status, err = options.callHarness(ctx, "ensure_workload_schema"); err != nil || status != 0 {
			return status, err
		}
		status, err = options.callHarness(ctx, "run_jepsen_case "+shellLiteral(workload)+" "+shellLiteral(nemesis)+" "+shellLiteral(runDir)+" "+shellLiteral(historyFile)+" "+shellLiteral(scheduleFile)+" "+shellLiteral(caseResultsFile))
		if err != nil {
			return status, err
		}
		if status != 0 {
			appendLine(failuresFile, fmt.Sprintf("%s case failed", caseLabel))
			failedCases = append(failedCases, caseLabel+":case")
			failed = true
		}

		if status, err = writeEDNEvent(historyFile, "verify", "invoke", fmt.Sprintf("%q", "demo-verify-after:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}
		status, err = options.callHarness(ctx, "verify_lab")
		if err != nil {
			return status, err
		}
		if status != 0 {
			_, _ = writeEDNEvent(historyFile, "verify", "fail", fmt.Sprintf("%q", "demo-verify-after:"+caseLabel))
			appendLine(failuresFile, fmt.Sprintf("%s verify-after failed", caseLabel))
			failedCases = append(failedCases, caseLabel+":verify-after")
			failed = true
			continue
		}
		if status, err = writeEDNEvent(historyFile, "verify", "ok", fmt.Sprintf("%q", "demo-verify-after:"+caseLabel)); err != nil || status != 0 {
			return status, err
		}
	}

	if failed {
		if status, err = writeEDNEvent(historyFile, "cases", "fail", fmt.Sprintf("%q", casesValue)); err != nil || status != 0 {
			return status, err
		}
	} else if status, err = writeEDNEvent(historyFile, "cases", "ok", fmt.Sprintf("%q", casesValue)); err != nil || status != 0 {
		return status, err
	}

	postFailed, postFailures, postStatus, postErr := runNightlyPostCampaign(ctx, options, runDir, historyFile, failuresFile)
	if postErr != nil {
		return postStatus, postErr
	}
	if postFailed {
		failed = true
		failedCases = append(failedCases, postFailures...)
	}

	if failed {
		fmt.Fprintln(options.stderr, "PACMAN Jepsen nightly failed. Failure summary:")
		if len(failedCases) == 0 {
			fmt.Fprintf(options.stderr, "  - unknown failure; inspect %s\n", runDir)
		} else {
			for _, failedCase := range failedCases {
				fmt.Fprintf(options.stderr, "  - %s\n", failedCase)
			}
		}
		fmt.Fprintf(options.stderr, "Artifacts: %s\n", runDir)
		return 1, nil
	}

	return 0, nil
}

func runNightlyPostCampaign(ctx context.Context, options harnessOptions, runDir, historyFile, failuresFile string) (bool, []string, int, error) {
	var failures []string

	if status, err := writeEDNEvent(historyFile, "bootstrap", "invoke", `"docker-lab:post-campaign"`); err != nil || status != 0 {
		return false, nil, status, err
	}
	status, err := options.callHarness(ctx, "bootstrap_lab_with_retries post-campaign")
	if err != nil {
		return false, nil, status, err
	}
	if status != 0 {
		_, _ = writeEDNEvent(historyFile, "bootstrap", "fail", `"docker-lab:post-campaign"`)
		appendLine(failuresFile, "post-campaign bootstrap failed")
		return true, []string{"post-campaign:bootstrap"}, 0, nil
	}
	if status, err = writeEDNEvent(historyFile, "bootstrap", "ok", `"docker-lab:post-campaign"`); err != nil || status != 0 {
		return false, nil, status, err
	}

	if status, err = writeEDNEvent(historyFile, "switchover", "invoke", `"auto"`); err != nil || status != 0 {
		return false, nil, status, err
	}
	status, err = options.runner.Run(ctx, commandSpec{
		name:   filepath.Join(options.repoRoot, "deploy", "lab", "scripts", "demo.sh"),
		args:   []string{"switchover"},
		dir:    options.repoRoot,
		env:    options.env,
		stdout: options.stdout,
		stderr: options.stderr,
	})
	if err != nil {
		return false, nil, status, err
	}
	if status != 0 {
		_, _ = writeEDNEvent(historyFile, "switchover", "fail", `"auto"`)
		appendLine(failuresFile, "post-campaign switchover failed")
		failures = append(failures, "post-campaign:switchover")
	} else if status, err = writeEDNEvent(historyFile, "switchover", "ok", `"auto"`); err != nil || status != 0 {
		return false, nil, status, err
	}

	if status, err = writeEDNEvent(historyFile, "verify", "invoke", `"demo-verify-after"`); err != nil || status != 0 {
		return false, nil, status, err
	}
	status, err = options.callHarness(ctx, "verify_lab")
	if err != nil {
		return false, nil, status, err
	}
	if status != 0 {
		_, _ = writeEDNEvent(historyFile, "verify", "fail", `"demo-verify-after"`)
		appendLine(failuresFile, "post-campaign verify failed")
		failures = append(failures, "post-campaign:verify")
	} else if status, err = writeEDNEvent(historyFile, "verify", "ok", `"demo-verify-after"`); err != nil || status != 0 {
		return false, nil, status, err
	}

	return len(failures) > 0, failures, 0, nil
}

func finishHarnessCampaign(ctx context.Context, options harnessOptions, runDir, historyFile, label string, status *int, err *error) {
	valid := "false"
	if *status == 0 {
		valid = "true"
	}

	if finishStatus, finishErr := options.callHarness(ctx, "collect_artifacts "+shellLiteral(runDir)+" "+valid); finishErr != nil {
		if *err == nil {
			*err = finishErr
		}
		if *status == 0 {
			*status = finishStatus
		}
	}
	if finishStatus, finishErr := options.callHarness(ctx, "destroy_lab_after_suite "+shellLiteral(runDir)+" "+shellLiteral(historyFile)); finishErr != nil {
		if *err == nil {
			*err = finishErr
		}
		if *status == 0 {
			*status = finishStatus
		}
	} else if finishStatus != 0 {
		*status = 1
		_, _ = options.callHarness(ctx, "write_results_file "+shellLiteral(runDir)+" false")
	}

	if *status == 0 {
		fmt.Fprintf(options.stdout, "PACMAN Jepsen %s artifacts: %s\n", label, runDir)
	}
}

func (options harnessOptions) callHarness(ctx context.Context, body string) (int, error) {
	return newHarnessLab(options).dispatch(ctx, body)
}

func campaignCases(campaign string) []string {
	if override := os.Getenv("PACMAN_JEPSEN_CASES"); override != "" {
		return strings.Fields(override)
	}
	if campaign == "smoke" {
		return []string{"append-smoke:none"}
	}

	cases := defaultJepsenCases()
	specs := make([]string, 0, len(cases))
	for _, testCase := range cases {
		if testCase.PatroniOnly || testCase.NightlyUnsafe {
			continue
		}
		specs = append(specs, testCase.Spec)
	}
	return specs
}

func resolveCaseSpec(caseName string) (string, error) {
	for _, testCase := range defaultJepsenCases() {
		if caseName == testCase.Slug || caseName == testCase.Spec {
			return testCase.Spec, nil
		}
	}

	var supported []string
	for _, testCase := range defaultJepsenCases() {
		supported = append(supported, testCase.Slug, testCase.Spec)
	}
	return "", fmt.Errorf("unsupported Jepsen case %q; supported cases: %s", caseName, strings.Join(supported, ", "))
}

func splitCaseSpec(spec string) (string, string) {
	workload, nemesis, found := strings.Cut(spec, ":")
	if !found || workload == nemesis {
		return workload, "none"
	}
	return workload, nemesis
}

func runDirFor(artifactDir, campaign string, target jepsenTarget) string {
	runID := envOrDefault("PACMAN_JEPSEN_RUN_ID", time.Now().UTC().Format("20060102T150405Z"))
	return filepath.Join(artifactDir, target.StoreName, campaign, runID)
}

func createHarnessFiles(runDir string, files ...string) error {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create harness run directory: %w", err)
	}
	for _, file := range files {
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			return fmt.Errorf("create parent directory for %s: %w", file, err)
		}
		if err := os.WriteFile(file, nil, 0o644); err != nil {
			return fmt.Errorf("create %s: %w", file, err)
		}
	}
	return nil
}

func writeEDNEvent(historyFile, op, status, value string) (int, error) {
	line := fmt.Sprintf("{:time %q :process :bootstrap :type :%s :f :%s :value %s}\n", time.Now().UTC().Format(time.RFC3339), status, op, value)
	file, err := os.OpenFile(historyFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 1, fmt.Errorf("open history file %s: %w", historyFile, err)
	}
	defer file.Close()
	if _, err := file.WriteString(line); err != nil {
		return 1, fmt.Errorf("append history file %s: %w", historyFile, err)
	}
	return 0, nil
}

func appendLine(path, line string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.WriteString(line + "\n")
}

func caseSlug(value string) string {
	var builder strings.Builder
	for _, char := range strings.ReplaceAll(value, ",", "+") {
		switch {
		case char >= 'A' && char <= 'Z',
			char >= 'a' && char <= 'z',
			char >= '0' && char <= '9',
			char == '_',
			char == '.',
			char == '+',
			char == '-':
			builder.WriteRune(char)
		default:
			builder.WriteByte('-')
		}
	}
	return builder.String()
}

func runDockerCampaign(ctx context.Context, options runOptions, stdout, stderr io.Writer, runner commandRunner) (int, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return 1, err
	}

	image := envOrDefault("PACMAN_JEPSEN_DOCKER_IMAGE", "pacman-jepsen-runner:local")
	dockerfile := envOrDefault("PACMAN_JEPSEN_DOCKERFILE", filepath.Join(repoRoot, "deploy", "jepsen", "Dockerfile"))
	goBaseImage := envOrDefault("PACMAN_JEPSEN_GO_BASE_IMAGE", "golang:1.26.1-alpine")
	dockerBaseImage := envOrDefault("PACMAN_JEPSEN_DOCKER_BASE_IMAGE", "docker:27-cli")
	buildImage := envOrDefault("PACMAN_JEPSEN_DOCKER_BUILD", "true")
	dryRun := envOrDefault("PACMAN_JEPSEN_DOCKER_DRY_RUN", "false")
	dockerSock := envOrDefault("PACMAN_JEPSEN_DOCKER_SOCKET", "/var/run/docker.sock")
	pullAttempts := envInt("PACMAN_JEPSEN_DOCKER_PULL_ATTEMPTS", 5)
	pullRetryDelay := time.Duration(envInt("PACMAN_JEPSEN_DOCKER_PULL_RETRY_DELAY_SECONDS", 10)) * time.Second

	if _, err := exec.LookPath("docker"); err != nil {
		return 1, fmt.Errorf("docker is required to run local Jepsen campaigns in containers")
	}

	if buildImage != "false" {
		for _, baseImage := range []string{goBaseImage, dockerBaseImage} {
			status, err := pullDockerImageWithRetries(ctx, runner, dryRun, stdout, stderr, baseImage, pullAttempts, pullRetryDelay)
			if err != nil || status != 0 {
				return status, err
			}
		}
		status, err := runMaybeDry(ctx, runner, dryRun, stdout, stderr, commandSpec{
			name:   "docker",
			args:   []string{"build", "--build-arg", "PACMAN_JEPSEN_GO_BASE_IMAGE=" + goBaseImage, "--build-arg", "PACMAN_JEPSEN_DOCKER_BASE_IMAGE=" + dockerBaseImage, "-f", dockerfile, "-t", image, repoRoot},
			stdout: stdout,
			stderr: stderr,
		})
		if err != nil || status != 0 {
			return status, err
		}
	}

	dockerArgs := []string{
		"run",
		"--rm",
		"-t",
		"-v", repoRoot + ":" + repoRoot,
		"-w", repoRoot,
	}
	dockerArgs = append(dockerArgs, dockerEnvArgs(dockerCampaignEnv(repoRoot, options.campaign, options.caseName))...)

	if socketInfo, err := os.Stat(dockerSock); err == nil && socketInfo.Mode()&os.ModeSocket != 0 {
		dockerArgs = append(dockerArgs,
			"-v", dockerSock+":/var/run/docker.sock",
			"-e", "DOCKER_HOST=unix:///var/run/docker.sock",
		)
	} else {
		fmt.Fprintf(stderr, "warning: Docker socket %s was not found; nested lab control may not work\n", dockerSock)
	}

	if sshAuthSock := os.Getenv("SSH_AUTH_SOCK"); sshAuthSock != "" {
		if socketInfo, err := os.Stat(sshAuthSock); err == nil && socketInfo.Mode()&os.ModeSocket != 0 {
			dockerArgs = append(dockerArgs,
				"-v", sshAuthSock+":/ssh-agent",
				"-e", "SSH_AUTH_SOCK=/ssh-agent",
			)
		}
	}

	dockerArgs = append(dockerArgs,
		image,
		"jepsenctl", "run", "ci", options.campaign,
	)
	if options.campaign == "case" {
		dockerArgs = append(dockerArgs, options.caseName)
	}

	return runMaybeDry(ctx, runner, dryRun, stdout, stderr, commandSpec{
		name:   "docker",
		args:   dockerArgs,
		stdout: stdout,
		stderr: stderr,
	})
}

func pullDockerImageWithRetries(ctx context.Context, runner commandRunner, dryRun string, stdout, stderr io.Writer, image string, attempts int, delay time.Duration) (int, error) {
	if attempts < 1 {
		attempts = 1
	}
	var status int
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		status, err = runMaybeDry(ctx, runner, dryRun, stdout, stderr, commandSpec{
			name:   "docker",
			args:   []string{"pull", image},
			stdout: stdout,
			stderr: stderr,
		})
		if err == nil && status == 0 {
			return 0, nil
		}
		if dryRun == "true" || attempt == attempts {
			break
		}
		fmt.Fprintf(stderr, "pull %s failed on attempt %d/%d; retrying\n", image, attempt, attempts)
		select {
		case <-ctx.Done():
			return 1, ctx.Err()
		case <-time.After(delay):
		}
	}
	return status, err
}

func dockerCampaignEnv(repoRoot, campaign, caseName string) map[string]string {
	return map[string]string{
		"PACMAN_JEPSEN_DIR":                                  envOrDefault("PACMAN_JEPSEN_DIR", filepath.Join(repoRoot, "jepsen")),
		"PACMAN_JEPSEN_ARTIFACT_DIR":                         envOrDefault("PACMAN_JEPSEN_ARTIFACT_DIR", filepath.Join(repoRoot, "jepsen", "store")),
		"PACMAN_JEPSEN_CI_ARTIFACT_DIR":                      envOrDefault("PACMAN_JEPSEN_CI_ARTIFACT_DIR", filepath.Join(repoRoot, "bin", "jepsen-ci", campaign)),
		"PACMAN_JEPSEN_CASES":                                os.Getenv("PACMAN_JEPSEN_CASES"),
		"PACMAN_JEPSEN_CASE":                                 firstNonEmpty(caseName, os.Getenv("PACMAN_JEPSEN_CASE")),
		"PACMAN_JEPSEN_TARGET":                               envOrDefault("PACMAN_JEPSEN_TARGET", defaultJepsenTarget),
		"PACMAN_JEPSEN_WORKLOAD_OPS":                         os.Getenv("PACMAN_JEPSEN_WORKLOAD_OPS"),
		"PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS":            os.Getenv("PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS"),
		"PACMAN_JEPSEN_WORKLOAD_CLIENTS":                     os.Getenv("PACMAN_JEPSEN_WORKLOAD_CLIENTS"),
		"PACMAN_JEPSEN_WORKLOAD_KEYS":                        os.Getenv("PACMAN_JEPSEN_WORKLOAD_KEYS"),
		"PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS":                 os.Getenv("PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS"),
		"PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS":          os.Getenv("PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS"),
		"PACMAN_JEPSEN_APPEND_FAILOVER_OP_DELAY_SECONDS":     os.Getenv("PACMAN_JEPSEN_APPEND_FAILOVER_OP_DELAY_SECONDS"),
		"PACMAN_JEPSEN_APPEND_SWITCHOVER_OP_DELAY_SECONDS":   os.Getenv("PACMAN_JEPSEN_APPEND_SWITCHOVER_OP_DELAY_SECONDS"),
		"PACMAN_JEPSEN_DCS_KILL_SERVICE":                     os.Getenv("PACMAN_JEPSEN_DCS_KILL_SERVICE"),
		"PACMAN_JEPSEN_DCS_MAJORITY_KILL_SERVICES":           os.Getenv("PACMAN_JEPSEN_DCS_MAJORITY_KILL_SERVICES"),
		"PACMAN_JEPSEN_DCS_MAJORITY_PARTITION_SERVICES":      os.Getenv("PACMAN_JEPSEN_DCS_MAJORITY_PARTITION_SERVICES"),
		"PACMAN_JEPSEN_DCS_RESTART_SERVICES":                 os.Getenv("PACMAN_JEPSEN_DCS_RESTART_SERVICES"),
		"PACMAN_JEPSEN_DCS_SLOW_SERVICES":                    os.Getenv("PACMAN_JEPSEN_DCS_SLOW_SERVICES"),
		"PACMAN_JEPSEN_DCS_SLOW_MIN_LATENCY_MS":              os.Getenv("PACMAN_JEPSEN_DCS_SLOW_MIN_LATENCY_MS"),
		"PACMAN_JEPSEN_DCS_RECOVERY_TIMEOUT_SECONDS":         os.Getenv("PACMAN_JEPSEN_DCS_RECOVERY_TIMEOUT_SECONDS"),
		"PACMAN_JEPSEN_DCS_RECOVERY_INTERVAL_SECONDS":        os.Getenv("PACMAN_JEPSEN_DCS_RECOVERY_INTERVAL_SECONDS"),
		"PACMAN_JEPSEN_BOOTSTRAP_LAB":                        os.Getenv("PACMAN_JEPSEN_BOOTSTRAP_LAB"),
		"PACMAN_JEPSEN_VERIFY_LAB":                           os.Getenv("PACMAN_JEPSEN_VERIFY_LAB"),
		"PACMAN_JEPSEN_DESTROY_LAB":                          os.Getenv("PACMAN_JEPSEN_DESTROY_LAB"),
		"PACMAN_JEPSEN_BOOTSTRAP_ATTEMPTS":                   os.Getenv("PACMAN_JEPSEN_BOOTSTRAP_ATTEMPTS"),
		"PACMAN_JEPSEN_BOOTSTRAP_RETRY_DELAY_SECONDS":        os.Getenv("PACMAN_JEPSEN_BOOTSTRAP_RETRY_DELAY_SECONDS"),
		"PACMAN_JEPSEN_RESET_LAB":                            os.Getenv("PACMAN_JEPSEN_RESET_LAB"),
		"PACMAN_JEPSEN_ALLOW_ASYNC_LOSS":                     os.Getenv("PACMAN_JEPSEN_ALLOW_ASYNC_LOSS"),
		"PACMAN_JEPSEN_PRIMARY_SAMPLE_INTERVAL_SECONDS":      os.Getenv("PACMAN_JEPSEN_PRIMARY_SAMPLE_INTERVAL_SECONDS"),
		"PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_TIMEOUT_SECONDS":  os.Getenv("PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_TIMEOUT_SECONDS"),
		"PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_INTERVAL_SECONDS": os.Getenv("PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_INTERVAL_SECONDS"),
		"PACMAN_JEPSEN_STRICT_SYNC_PROBE_TIMEOUT_SECONDS":    os.Getenv("PACMAN_JEPSEN_STRICT_SYNC_PROBE_TIMEOUT_SECONDS"),
		"PACMAN_JEPSEN_PG_CLIENT_SERVICE":                    os.Getenv("PACMAN_JEPSEN_PG_CLIENT_SERVICE"),
		"PACMAN_JEPSEN_PG_HOST":                              os.Getenv("PACMAN_JEPSEN_PG_HOST"),
		"PACMAN_JEPSEN_PG_PORT":                              os.Getenv("PACMAN_JEPSEN_PG_PORT"),
		"PACMAN_JEPSEN_PG_USER":                              os.Getenv("PACMAN_JEPSEN_PG_USER"),
		"PACMAN_JEPSEN_PG_PASSWORD":                          os.Getenv("PACMAN_JEPSEN_PG_PASSWORD"),
		"PACMAN_JEPSEN_PG_DATABASE":                          os.Getenv("PACMAN_JEPSEN_PG_DATABASE"),
		"PACMAN_JEPSEN_VIP_INTERFACE":                        os.Getenv("PACMAN_JEPSEN_VIP_INTERFACE"),
		"PACMAN_ANSIBLE_INSTALL_RPM_DIR":                     os.Getenv("PACMAN_ANSIBLE_INSTALL_RPM_DIR"),
	}
}

func dockerEnvArgs(env map[string]string) []string {
	keys := []string{
		"PACMAN_JEPSEN_DIR",
		"PACMAN_JEPSEN_ARTIFACT_DIR",
		"PACMAN_JEPSEN_CI_ARTIFACT_DIR",
		"PACMAN_JEPSEN_CASES",
		"PACMAN_JEPSEN_CASE",
		"PACMAN_JEPSEN_TARGET",
		"PACMAN_JEPSEN_WORKLOAD_OPS",
		"PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS",
		"PACMAN_JEPSEN_WORKLOAD_CLIENTS",
		"PACMAN_JEPSEN_WORKLOAD_KEYS",
		"PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS",
		"PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS",
		"PACMAN_JEPSEN_APPEND_FAILOVER_OP_DELAY_SECONDS",
		"PACMAN_JEPSEN_APPEND_SWITCHOVER_OP_DELAY_SECONDS",
		"PACMAN_JEPSEN_DCS_KILL_SERVICE",
		"PACMAN_JEPSEN_DCS_MAJORITY_KILL_SERVICES",
		"PACMAN_JEPSEN_DCS_MAJORITY_PARTITION_SERVICES",
		"PACMAN_JEPSEN_DCS_RESTART_SERVICES",
		"PACMAN_JEPSEN_DCS_SLOW_SERVICES",
		"PACMAN_JEPSEN_DCS_SLOW_MIN_LATENCY_MS",
		"PACMAN_JEPSEN_DCS_RECOVERY_TIMEOUT_SECONDS",
		"PACMAN_JEPSEN_DCS_RECOVERY_INTERVAL_SECONDS",
		"PACMAN_JEPSEN_BOOTSTRAP_LAB",
		"PACMAN_JEPSEN_VERIFY_LAB",
		"PACMAN_JEPSEN_DESTROY_LAB",
		"PACMAN_JEPSEN_BOOTSTRAP_ATTEMPTS",
		"PACMAN_JEPSEN_BOOTSTRAP_RETRY_DELAY_SECONDS",
		"PACMAN_JEPSEN_RESET_LAB",
		"PACMAN_JEPSEN_ALLOW_ASYNC_LOSS",
		"PACMAN_JEPSEN_PRIMARY_SAMPLE_INTERVAL_SECONDS",
		"PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_TIMEOUT_SECONDS",
		"PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_INTERVAL_SECONDS",
		"PACMAN_JEPSEN_STRICT_SYNC_PROBE_TIMEOUT_SECONDS",
		"PACMAN_JEPSEN_PG_CLIENT_SERVICE",
		"PACMAN_JEPSEN_PG_HOST",
		"PACMAN_JEPSEN_PG_PORT",
		"PACMAN_JEPSEN_PG_USER",
		"PACMAN_JEPSEN_PG_PASSWORD",
		"PACMAN_JEPSEN_PG_DATABASE",
		"PACMAN_JEPSEN_VIP_INTERFACE",
		"PACMAN_ANSIBLE_INSTALL_RPM_DIR",
	}

	args := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, "-e", key+"="+env[key])
	}
	return args
}

func runMaybeDry(ctx context.Context, runner commandRunner, dryRun string, stdout, stderr io.Writer, spec commandSpec) (int, error) {
	if dryRun == "true" {
		fmt.Fprintf(stdout, "+ %s\n", shellQuote(append([]string{spec.name}, spec.args...)))
		return 0, nil
	}
	spec.stdout = stdout
	spec.stderr = stderr
	return runner.Run(ctx, spec)
}

func (osCommandRunner) Run(ctx context.Context, spec commandSpec) (int, error) {
	command := exec.CommandContext(ctx, spec.name, spec.args...)
	command.Dir = spec.dir
	command.Env = spec.env
	if len(command.Env) == 0 {
		command.Env = os.Environ()
	}
	command.Stdout = spec.stdout
	command.Stderr = spec.stderr

	err := command.Run()
	if err == nil {
		return 0, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return status.ExitStatus(), nil
		}
		return exitErr.ExitCode(), nil
	}

	return 1, fmt.Errorf("run %s: %w", spec.name, err)
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if fileExists(filepath.Join(dir, "go.mod")) && fileExists(filepath.Join(dir, "jepsen")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find repository root from %s", dir)
		}
		dir = parent
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func notice(stdout io.Writer, message string) {
	fmt.Fprintln(stdout, message)
	if os.Getenv("GITHUB_ACTIONS") != "" {
		fmt.Fprintf(stdout, "::notice::%s\n", message)
	}
}

func commitRef(repoRoot string) string {
	if githubSHA := os.Getenv("GITHUB_SHA"); githubSHA != "" {
		return githubSHA
	}

	command := exec.Command("git", "-C", repoRoot, "rev-parse", "--short", "HEAD")
	output, err := command.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

func shellLiteral(value string) string {
	return shellQuote([]string{value})
}

func shellQuote(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "" {
			quoted = append(quoted, "''")
			continue
		}
		if strings.ContainsAny(arg, " \t\n'\"\\$`") {
			quoted = append(quoted, "'"+strings.ReplaceAll(arg, "'", "'\"'\"'")+"'")
			continue
		}
		quoted = append(quoted, arg)
	}
	return strings.Join(quoted, " ")
}
