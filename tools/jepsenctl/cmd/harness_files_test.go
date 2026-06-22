package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

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
	clusterOutput := `{
  "clusterName": "alpha",
  "phase": "healthy",
  "currentPrimary": "alpha-1",
  "members": [
    {"name": "alpha-1", "role": "primary", "state": "running", "healthy": true},
    {"name": "alpha-2", "role": "replica", "state": "streaming", "healthy": true},
    {"name": "alpha-3", "role": "replica", "state": "streaming", "healthy": true}
  ]
}
{"time":"2026-05-27T20:32:48Z","msg":"completed pacmanctl command"}`
	clusterJSON := clusterStatusJSONObject(clusterOutput)
	var status clusterStatus
	if err := json.Unmarshal([]byte(clusterJSON), &status); err != nil {
		t.Fatalf("decode extracted cluster json: %v\n%s", err, clusterJSON)
	}
	if status.CurrentPrimary != "alpha-1" || len(status.Members) != 3 {
		t.Fatalf("extracted wrong json object: %#v", status)
	}
	if got := clusterStatusJSONObject(`{"time":"2026-05-27T20:32:48Z","msg":"completed pacmanctl command"}`); got != "" {
		t.Fatalf("log-only json should not be treated as cluster status: %s", got)
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

	schedulePath := filepath.Join(dir, "campaign-schedule.edn")
	caseSchedulePath := filepath.Join(dir, "case-schedule.edn")
	writeTestFile(t, schedulePath, "old\n")
	offset := fileSize(schedulePath)
	appendFile(schedulePath, "new\n")
	if err := copyScheduleTail(schedulePath, caseSchedulePath, offset); err != nil {
		t.Fatalf("copy schedule tail: %v", err)
	}
	if got := mustRead(caseSchedulePath); got != "new\n" {
		t.Fatalf("case schedule: got %q want only new entry", got)
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
