package cmd

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestHarnessCommandParsing(t *testing.T) {
	t.Parallel()

	command, err := parseHarnessCommand(`run_jepsen_case append-failover packet "/tmp/run dir" history.edn schedule.edn results.jsonl`)
	if err != nil {
		t.Fatalf("parse harness command: %v", err)
	}
	if command.name != "run_jepsen_case" {
		t.Fatalf("name: got %q", command.name)
	}
	wantArgs := []string{"append-failover", "packet", "/tmp/run dir", "history.edn", "schedule.edn", "results.jsonl"}
	if !reflect.DeepEqual(command.args, wantArgs) {
		t.Fatalf("args: got %#v want %#v", command.args, wantArgs)
	}

	if _, err := parseHarnessCommand(`run_jepsen_case "unterminated`); err == nil {
		t.Fatalf("unterminated quote parsed without error")
	}
	if _, err := parseHarnessCommand("   "); err == nil {
		t.Fatalf("empty command parsed without error")
	}
}

func TestHarnessDispatchValidationAndResultsFile(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	target, err := resolveJepsenTarget(defaultJepsenTarget)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	lab := newHarnessLab(harnessOptions{runOptions: runOptions{campaign: "case", target: target}})

	status, err := lab.dispatch(context.Background(), "missing_command")
	if status != 1 || err == nil || !strings.Contains(err.Error(), "unsupported Go harness command") {
		t.Fatalf("unsupported dispatch: status=%d err=%v", status, err)
	}

	status, err = lab.dispatch(context.Background(), "run_jepsen_case too few args")
	if status != 1 || err == nil || !strings.Contains(err.Error(), "expects 6 args") {
		t.Fatalf("arity dispatch: status=%d err=%v", status, err)
	}

	status, err = lab.dispatch(context.Background(), "write_results_file "+shellLiteral(runDir)+" true")
	if status != 0 || err != nil {
		t.Fatalf("write results dispatch: status=%d err=%v", status, err)
	}
	data, err := os.ReadFile(filepath.Join(runDir, "results.edn"))
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	assertContainsAll(t, "results", string(data), []string{":valid? true", `:campaign "case"`, `:target "pacman-3-data"`, `:target-store "pacman"`})
}

func TestBootstrapRetryDestroysLabBetweenAttempts(t *testing.T) {
	t.Setenv("PACMAN_JEPSEN_BOOTSTRAP_ATTEMPTS", "2")
	t.Setenv("PACMAN_JEPSEN_BOOTSTRAP_RETRY_DELAY_SECONDS", "0")
	t.Setenv("PACMAN_JEPSEN_RESET_LAB", "false")

	target, err := resolveJepsenTarget(defaultJepsenTarget)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	var stderr strings.Builder
	runner := &scriptedRunner{statuses: []int{137, 0, 137}}
	lab := newHarnessLab(harnessOptions{
		repoRoot: t.TempDir(),
		runner:   runner,
		stderr:   &stderr,
		runOptions: runOptions{
			target: target,
		},
	})

	err = lab.bootstrapLabWithRetries(context.Background(), "case:append-smoke:none")
	if err == nil || !strings.Contains(err.Error(), "bootstrap lab exited with status 137") {
		t.Fatalf("bootstrap error: got %v want status 137", err)
	}
	if len(runner.specs) != 3 {
		t.Fatalf("runner calls: got %d want 3", len(runner.specs))
	}
	if got := filepath.Base(runner.specs[0].name); got != "bootstrap-cluster.sh" {
		t.Fatalf("first command: got %q want bootstrap-cluster.sh", got)
	}
	if got := filepath.Base(runner.specs[1].name); got != "destroy-cluster.sh" {
		t.Fatalf("second command: got %q want destroy-cluster.sh", got)
	}
	if got := filepath.Base(runner.specs[2].name); got != "bootstrap-cluster.sh" {
		t.Fatalf("third command: got %q want bootstrap-cluster.sh", got)
	}
}
