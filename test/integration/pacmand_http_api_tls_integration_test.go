//go:build integration

package integration_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestPacmandHTTPAPIServesHealthOverTLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	daemon := startSingleNodeDaemonTLS(t, "alpha-https")
	waitForProbeStatus(t, daemon.Client, daemon.Base+"/health", http.StatusOK, pacmandStartupTimeout)

	resp, err := daemon.Client.Get(daemon.Base + "/health")
	if err != nil {
		t.Fatalf("GET /health over tls: %v", err)
	}
	defer resp.Body.Close()

	if resp.TLS == nil {
		t.Fatal("expected tls connection details on https response")
	}

	var payload struct {
		State string `json:"state"`
		Role  string `json:"role"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode /health payload: %v", err)
	}

	if payload.State != "running" {
		t.Errorf("/health state: got %q, want %q", payload.State, "running")
	}

	if payload.Role != "primary" {
		t.Errorf("/health role: got %q, want %q", payload.Role, "primary")
	}
}
