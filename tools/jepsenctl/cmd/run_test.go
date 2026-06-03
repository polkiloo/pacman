package cmd

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseRunOptions(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		envCase      string
		wantCampaign string
		wantCase     string
		wantErr      string
	}{
		{
			name:         "smoke",
			args:         []string{"smoke"},
			wantCampaign: "smoke",
		},
		{
			name:         "nightly",
			args:         []string{"nightly"},
			wantCampaign: "nightly",
		},
		{
			name:         "case argument",
			args:         []string{"case", "append-smoke-none"},
			wantCampaign: "case",
			wantCase:     "append-smoke-none",
		},
		{
			name:         "case environment fallback",
			args:         []string{"case"},
			envCase:      "append-failover:kill",
			wantCampaign: "case",
			wantCase:     "append-failover:kill",
		},
		{
			name:    "missing case",
			args:    []string{"case"},
			wantErr: "case campaign requires",
		},
		{
			name:    "unsupported campaign",
			args:    []string{"weekly"},
			wantErr: `unsupported Jepsen campaign "weekly"`,
		},
		{
			name:    "smoke rejects case argument",
			args:    []string{"smoke", "append-smoke-none"},
			wantErr: "smoke campaign does not accept",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.envCase != "" {
				t.Setenv("PACMAN_JEPSEN_CASE", test.envCase)
			} else {
				t.Setenv("PACMAN_JEPSEN_CASE", "")
			}

			got, err := parseRunOptions(test.args)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("err: got %v want fragment %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse run options: %v", err)
			}
			if got.campaign != test.wantCampaign {
				t.Fatalf("campaign: got %q want %q", got.campaign, test.wantCampaign)
			}
			if got.caseName != test.wantCase {
				t.Fatalf("case: got %q want %q", got.caseName, test.wantCase)
			}
		})
	}
}

func TestRunMaybeDryPrintsCommand(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status, err := runMaybeDry(context.Background(), recordingRunner{}, "true", &stdout, &stderr, commandSpec{
		name: "docker",
		args: []string{"run", "--rm", "-e", "PACMAN_JEPSEN_CASE=append-failover:packet,kill", "pacman-jepsen-runner:local"},
	})
	if err != nil {
		t.Fatalf("run dry command: %v", err)
	}
	if status != 0 {
		t.Fatalf("status: got %d want 0", status)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr: got %q want empty", stderr.String())
	}
	assertContainsAll(t, "stdout", stdout.String(), []string{
		"+ docker run --rm -e PACMAN_JEPSEN_CASE=append-failover:packet,kill pacman-jepsen-runner:local",
	})
}

func TestDockerCampaignEnvPrefersExplicitCase(t *testing.T) {
	t.Setenv("PACMAN_JEPSEN_CASE", "append-smoke-none")
	t.Setenv("PACMAN_JEPSEN_WORKLOAD_OPS", "3")

	env := dockerCampaignEnv("/repo", "case", "append-failover-kill")

	if env["PACMAN_JEPSEN_CASE"] != "append-failover-kill" {
		t.Fatalf("case env: got %q", env["PACMAN_JEPSEN_CASE"])
	}
	if env["PACMAN_JEPSEN_WORKLOAD_OPS"] != "3" {
		t.Fatalf("workload ops env: got %q", env["PACMAN_JEPSEN_WORKLOAD_OPS"])
	}
	if env["PACMAN_JEPSEN_DIR"] != string(os.PathSeparator)+"repo"+string(os.PathSeparator)+"jepsen" {
		t.Fatalf("jepsen dir env: got %q", env["PACMAN_JEPSEN_DIR"])
	}
}

func TestCampaignCasesExcludesPatroniOnlyProfilesFromNightly(t *testing.T) {
	t.Setenv("PACMAN_JEPSEN_CASES", "")

	cases := strings.Join(campaignCases("nightly"), " ")
	if strings.Contains(cases, "append-sync:") || strings.Contains(cases, "append-sync-two:") || strings.Contains(cases, "append-strict-sync:") || strings.Contains(cases, "append-max-lag:") || strings.Contains(cases, "append-check-timeline:") {
		t.Fatalf("nightly cases included opt-in Patroni profiles: %s", cases)
	}
}

func TestPullDockerImageWithRetries(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	runner := &retryRecordingRunner{statuses: []int{1, 0}}

	status, err := pullDockerImageWithRetries(context.Background(), runner, "false", &stdout, &stderr, "docker:27-cli", 2, time.Nanosecond)
	if err != nil {
		t.Fatalf("pull image: %v", err)
	}
	if status != 0 {
		t.Fatalf("status: got %d want 0", status)
	}
	if runner.calls != 2 {
		t.Fatalf("calls: got %d want 2", runner.calls)
	}
	if !strings.Contains(stderr.String(), "pull docker:27-cli failed on attempt 1/2") {
		t.Fatalf("stderr: got %q want retry message", stderr.String())
	}
}

type recordingRunner struct{}

func (recordingRunner) Run(context.Context, commandSpec) (int, error) {
	return 99, nil
}

type retryRecordingRunner struct {
	statuses []int
	calls    int
}

func (runner *retryRecordingRunner) Run(_ context.Context, spec commandSpec) (int, error) {
	runner.calls++
	if len(spec.args) < 2 || spec.args[0] != "pull" {
		return 1, nil
	}
	index := runner.calls - 1
	if index >= len(runner.statuses) {
		index = len(runner.statuses) - 1
	}
	return runner.statuses[index], nil
}
