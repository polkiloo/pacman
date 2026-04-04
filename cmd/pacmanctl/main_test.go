package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestRunReturnsSuccessForHelp(t *testing.T) {
	exitCode, stdout, stderr := runWithCapturedIO(t, nil)

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if got, want := stdout, "pacmanctl commands: cluster status, cluster spec show, cluster switchover, cluster failover, cluster maintenance enable, cluster maintenance disable, members list, history list, node status, diagnostics show, patronictl-compatible: list, topology, history, show-config, pause, resume, switchover, failover\n"; got != want {
		t.Fatalf("unexpected stdout output: got %q, want %q", got, want)
	}

	if stderr != "" {
		t.Fatalf("expected no stderr output, got %q", stderr)
	}
}

func TestRunReturnsSuccessForClusterStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/cluster" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(map[string]any{
			"clusterName":    "alpha",
			"phase":          "healthy",
			"currentPrimary": "alpha-1",
			"currentEpoch":   1,
			"observedAt":     "2026-04-02T12:00:00Z",
			"maintenance": map[string]any{
				"enabled": false,
			},
			"members": []map[string]any{
				{
					"name":       "alpha-1",
					"role":       "primary",
					"state":      "running",
					"healthy":    true,
					"leader":     true,
					"timeline":   1,
					"lastSeenAt": "2026-04-02T12:00:00Z",
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	exitCode, stdout, stderr := runWithCapturedIO(t, []string{"-api-url", server.URL, "cluster", "status", "-o", "json"})

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d stderr=%q", exitCode, stderr)
	}

	if !strings.Contains(stdout, `"clusterName": "alpha"`) {
		t.Fatalf("expected cluster status json output, got %q", stdout)
	}

	if stderr != "" {
		t.Fatalf("expected no stderr output, got %q", stderr)
	}
}

func TestRunReturnsSuccessForMaintenanceEnable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", request.Method)
		}
		if request.URL.Path != "/api/v1/maintenance" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if enabled, _ := body["enabled"].(bool); !enabled {
			t.Fatalf("expected enabled=true in request body, got %v", body["enabled"])
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(map[string]any{
			"enabled":     true,
			"reason":      "upgrade",
			"requestedBy": "main-test",
			"updatedAt":   "2026-04-03T13:00:00Z",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	exitCode, stdout, stderr := runWithCapturedIO(t, []string{"-api-url", server.URL, "cluster", "maintenance", "enable", "-reason", "upgrade"})

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d stderr=%q", exitCode, stderr)
	}

	if !strings.Contains(stdout, "Enabled:") || !strings.Contains(stdout, "true") {
		t.Fatalf("expected maintenance text output, got %q", stdout)
	}

	if stderr != "" {
		t.Fatalf("expected no stderr output, got %q", stderr)
	}
}

func TestRunReturnsSuccessForNodeStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/nodes/alpha-1" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(map[string]any{
			"nodeName":   "alpha-1",
			"memberName": "alpha-1",
			"role":       "primary",
			"state":      "running",
			"postgres": map[string]any{
				"managed":       true,
				"checkedAt":     "2026-04-04T10:00:00Z",
				"up":            true,
				"role":          "primary",
				"recoveryKnown": true,
				"inRecovery":    false,
				"details":       map[string]any{},
				"wal":           map[string]any{},
				"errors":        map[string]any{},
			},
			"controlPlane": map[string]any{
				"clusterReachable": true,
			},
			"observedAt": "2026-04-04T10:00:00Z",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	exitCode, stdout, stderr := runWithCapturedIO(t, []string{"-api-url", server.URL, "node", "status", "alpha-1"})

	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d stderr=%q", exitCode, stderr)
	}

	if !strings.Contains(stdout, "Node Name:") || !strings.Contains(stdout, "alpha-1") {
		t.Fatalf("expected node status output, got %q", stdout)
	}

	if stderr != "" {
		t.Fatalf("expected no stderr output, got %q", stderr)
	}
}

func TestRunReturnsErrorForInvalidFlag(t *testing.T) {
	exitCode, _, stderr := runWithCapturedIO(t, []string{"-invalid"})

	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}

	assertContains(t, stderr, "flag provided but not defined")
	assertContains(t, stderr, `"msg":"app run failed"`)
}

func runWithCapturedIO(t *testing.T, args []string) (int, string, string) {
	t.Helper()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}

	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stderr pipe: %v", err)
	}

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	os.Args = append([]string{processName}, args...)
	os.Stdout = stdoutW
	os.Stderr = stderrW

	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	exitCode := run()

	if err := stdoutW.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}

	if err := stderrW.Close(); err != nil {
		t.Fatalf("close stderr writer: %v", err)
	}

	stdoutBytes, err := io.ReadAll(stdoutR)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}

	stderrBytes, err := io.ReadAll(stderrR)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}

	if err := stdoutR.Close(); err != nil {
		t.Fatalf("close stdout reader: %v", err)
	}

	if err := stderrR.Close(); err != nil {
		t.Fatalf("close stderr reader: %v", err)
	}

	return exitCode, string(stdoutBytes), string(stderrBytes)
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
