package cmd

import (
	"context"
	"encoding/json"
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
	lab := newHarnessLab(harnessOptions{runOptions: runOptions{campaign: "case"}})

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
	assertContainsAll(t, "results", string(data), []string{":valid? true", `:campaign "case"`, `:target "pacman-docker-lab"`})
}

func TestHarnessTopologyHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"service alpha-1", serviceForMember("alpha-1"), "pacman-primary"},
		{"service alpha-2", serviceForMember("alpha-2"), "pacman-replica"},
		{"service alpha-3", serviceForMember("alpha-3"), "pacman-replica-2"},
		{"member primary", memberForService("pacman-primary"), "alpha-1"},
		{"dcs service", dcsMemberForService("pacman-dcs-2"), "alpha-dcs-2"},
		{"service ip", serviceIP("pacman-replica-2"), "172.28.0.13"},
		{"sql literal", sqlLiteral("alpha's"), "'alpha''s'"},
	}
	for _, test := range tests {
		if test.got != test.want {
			t.Fatalf("%s: got %q want %q", test.name, test.got, test.want)
		}
	}

	if got := peerServicesForMember("alpha-1"); !reflect.DeepEqual(got, []string{"pacman-replica", "pacman-replica-2"}) {
		t.Fatalf("alpha-1 peers: got %#v", got)
	}
	if got := serviceForMember("unknown"); got != "" {
		t.Fatalf("unknown service: got %q want empty", got)
	}
}

func TestHarnessFileAndJSONHelpers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	linesPath := filepath.Join(dir, "lines.txt")
	if err := os.WriteFile(linesPath, []byte("one\n\n two \n"), 0o644); err != nil {
		t.Fatalf("write lines: %v", err)
	}
	if got := countLines(linesPath); got != 2 {
		t.Fatalf("count lines: got %d want 2", got)
	}
	if got := lastNonEmptyLine("a\n\n b \n"); got != "b" {
		t.Fatalf("last line: got %q want b", got)
	}
	if got := lastJSONObject("noise\n{\"first\":true}\ntext\n{\"last\":true}\n"); got != `{"last":true}` {
		t.Fatalf("last json object: got %q", got)
	}

	jsonlPath := filepath.Join(dir, "rows.jsonl")
	appendJSONL(jsonlPath, map[string]any{"ok": true})
	appendJSONL(jsonlPath, map[string]any{"ok": false})
	rows := readJSONL(jsonlPath)
	if len(rows) != 2 || rows[0]["ok"] != true || rows[1]["ok"] != false {
		t.Fatalf("jsonl rows: %#v", rows)
	}
	if got := countSamples(rows, func(row map[string]any) bool { return row["ok"] == true }); got != 1 {
		t.Fatalf("sample count: got %d want 1", got)
	}

	jsonPath := filepath.Join(dir, "value.json")
	writeJSON(jsonPath, map[string]any{"name": "alpha"})
	var decoded map[string]string
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if decoded["name"] != "alpha" {
		t.Fatalf("decoded json: %#v", decoded)
	}
}

func TestHarnessSmallProfileHelpers(t *testing.T) {
	t.Parallel()

	if boolStatus(true) != 0 {
		t.Fatalf("boolStatus(true) should be 0")
	}
	if boolStatus(false) != 1 {
		t.Fatalf("boolStatus(false) should be 1")
	}
	if got := workloadTable("append-failover"); got != "jepsen.append_values" {
		t.Fatalf("append table: got %q", got)
	}
	if got := workloadTable("serializable-txn"); got != "jepsen.txn_ops" {
		t.Fatalf("txn table: got %q", got)
	}
	if got := workloadTable("unknown"); got != "" {
		t.Fatalf("unknown table: got %q want empty", got)
	}
	if got := maxDuration(2, 1); got != 2 {
		t.Fatalf("max duration: got %s", got)
	}
}
