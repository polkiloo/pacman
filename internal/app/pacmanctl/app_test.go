package pacmanctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/version"
)

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-version"}); err != nil {
		t.Fatalf("run pacmanctl version: %v", err)
	}

	if got, want := stdout.String(), version.String()+"\n"; got != want {
		t.Fatalf("unexpected version output: got %q, want %q", got, want)
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}
}

func TestRunWithoutCommandPrintsHelp(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), nil); err != nil {
		t.Fatalf("run pacmanctl help: %v", err)
	}

	const want = "pacmanctl commands: cluster status, cluster spec show, cluster switchover, cluster failover, cluster maintenance enable, cluster maintenance disable, members list, history list, node status, diagnostics show, patronictl-compatible: list, topology, history, show-config, pause, resume, switchover, failover\n"
	if got := stdout.String(); got != want {
		t.Fatalf("unexpected help output: got %q, want %q", got, want)
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}
}

func TestRunClusterStatusText(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/cluster" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName:    "alpha",
			Phase:          "healthy",
			CurrentPrimary: "alpha-1",
			CurrentEpoch:   3,
			ObservedAt:     time.Date(2026, time.April, 2, 20, 0, 0, 0, time.UTC),
			Maintenance:    maintenanceModeStatusJSON{Enabled: false},
			Members: []memberStatusJSON{
				{
					Name:       "alpha-1",
					Role:       "primary",
					State:      "running",
					Healthy:    true,
					Leader:     true,
					Timeline:   1,
					LastSeenAt: time.Date(2026, time.April, 2, 20, 0, 0, 0, time.UTC),
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status"}); err != nil {
		t.Fatalf("run cluster status: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Cluster Name:")
	assertContains(t, output, "alpha")
	assertContains(t, output, "Current Primary:")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "NAME")
	assertContains(t, output, "primary")
}

func TestRunClusterStatusJSONFromEnvironment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName:    "alpha",
			Phase:          "healthy",
			CurrentPrimary: "alpha-1",
			CurrentEpoch:   2,
			ObservedAt:     time.Date(2026, time.April, 2, 21, 0, 0, 0, time.UTC),
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	t.Setenv("PACMANCTL_API_URL", server.URL)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"cluster", "status", "-o", "json"}); err != nil {
		t.Fatalf("run cluster status json: %v", err)
	}

	var payload clusterStatusResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode stdout json: %v", err)
	}

	if payload.ClusterName != "alpha" || payload.CurrentPrimary != "alpha-1" {
		t.Fatalf("unexpected cluster status payload: %+v", payload)
	}
}

func TestRunClusterStatusUsesAPITokenFromEnvironment(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("authorization header: got %q, want %q", got, "Bearer secret-token")
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "alpha",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	t.Setenv("PACMANCTL_API_URL", server.URL)
	t.Setenv("PACMANCTL_API_TOKEN", "secret-token")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"cluster", "status", "-o", "json"}); err != nil {
		t.Fatalf("run cluster status json with api token: %v", err)
	}
}

func TestRunMembersListText(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/members" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(membersResponse{
			Items: []memberStatusJSON{
				{
					Name:       "alpha-1",
					Role:       "primary",
					State:      "running",
					Healthy:    true,
					Leader:     true,
					Timeline:   1,
					LastSeenAt: time.Date(2026, time.April, 2, 22, 0, 0, 0, time.UTC),
				},
				{
					Name:       "alpha-2",
					Role:       "replica",
					State:      "streaming",
					Healthy:    true,
					LagBytes:   64,
					LastSeenAt: time.Date(2026, time.April, 2, 22, 0, 1, 0, time.UTC),
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "members", "list"}); err != nil {
		t.Fatalf("run members list: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "NAME")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "alpha-2")
	assertContains(t, output, "streaming")
}

func TestRunMembersListJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(membersResponse{
			Items: []memberStatusJSON{{Name: "alpha-1", Role: "primary", State: "running", Healthy: true}},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "members", "list", "-format", "json"}); err != nil {
		t.Fatalf("run members list json: %v", err)
	}

	var payload membersResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode members json: %v", err)
	}

	if len(payload.Items) != 1 || payload.Items[0].Name != "alpha-1" {
		t.Fatalf("unexpected members payload: %+v", payload)
	}
}

func TestRunClusterSwitchoverText(t *testing.T) {
	t.Parallel()

	scheduledAt := time.Date(2026, time.April, 3, 9, 30, 0, 0, time.UTC)
	requestedAt := time.Date(2026, time.April, 3, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", request.Method)
		}
		if request.URL.Path != "/api/v1/operations/switchover" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body switchoverRequestJSON
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		if body.Candidate != "alpha-2" {
			t.Fatalf("candidate: got %q, want %q", body.Candidate, "alpha-2")
		}
		if body.Reason != "rotate primary" {
			t.Fatalf("reason: got %q, want %q", body.Reason, "rotate primary")
		}
		if body.RequestedBy != "ops-bot" {
			t.Fatalf("requestedBy: got %q, want %q", body.RequestedBy, "ops-bot")
		}
		if body.ScheduledAt == nil || !body.ScheduledAt.Equal(scheduledAt) {
			t.Fatalf("scheduledAt: got %v, want %v", body.ScheduledAt, scheduledAt)
		}

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
			Message: "switchover accepted",
			Operation: operationJSON{
				ID:          "sw-1",
				Kind:        "switchover",
				State:       "pending",
				RequestedBy: "ops-bot",
				RequestedAt: requestedAt,
				Reason:      "rotate primary",
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				ScheduledAt: &scheduledAt,
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	err := app.Run(context.Background(), []string{
		"-api-url", server.URL,
		"cluster", "switchover",
		"-candidate", "alpha-2",
		"-scheduled-at", scheduledAt.Format(time.RFC3339),
		"-reason", "rotate primary",
		"-requested-by", "ops-bot",
	})
	if err != nil {
		t.Fatalf("run cluster switchover: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Message:")
	assertContains(t, output, "switchover accepted")
	assertContains(t, output, "Operation ID:")
	assertContains(t, output, "sw-1")
	assertContains(t, output, "To Member:")
	assertContains(t, output, "alpha-2")
}

func TestRunClusterSwitchoverJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
			Operation: operationJSON{
				ID:       "sw-2",
				Kind:     "switchover",
				State:    "pending",
				ToMember: "alpha-3",
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "switchover", "-candidate", "alpha-3", "-o", "json"}); err != nil {
		t.Fatalf("run cluster switchover json: %v", err)
	}

	var payload operationAcceptedResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode switchover json: %v", err)
	}

	if payload.Operation.ID != "sw-2" || payload.Operation.ToMember != "alpha-3" {
		t.Fatalf("unexpected switchover payload: %+v", payload)
	}
}

func TestRunClusterSwitchoverRequiresCandidate(t *testing.T) {
	t.Parallel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"cluster", "switchover"})
	if !errors.Is(err, errCandidateRequired) {
		t.Fatalf("expected missing candidate error, got %v", err)
	}
}

func TestRunClusterSwitchoverRejectsInvalidSchedule(t *testing.T) {
	t.Parallel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"cluster", "switchover", "-candidate", "alpha-2", "-scheduled-at", "later"})
	if err == nil {
		t.Fatal("expected invalid schedule error")
	}

	assertContains(t, err.Error(), "invalid -scheduled-at value")
}

func TestRunClusterFailoverText(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, time.April, 3, 11, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", request.Method)
		}
		if request.URL.Path != "/api/v1/operations/failover" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body failoverRequestJSON
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		if body.Reason != "primary lost" {
			t.Fatalf("reason: got %q, want %q", body.Reason, "primary lost")
		}
		if body.RequestedBy != "ops-bot" {
			t.Fatalf("requestedBy: got %q, want %q", body.RequestedBy, "ops-bot")
		}

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
			Message: "failover accepted",
			Operation: operationJSON{
				ID:          "fo-1",
				Kind:        "failover",
				State:       "pending",
				RequestedBy: "ops-bot",
				RequestedAt: requestedAt,
				Reason:      "primary lost",
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "failover", "-reason", "primary lost", "-requested-by", "ops-bot"})
	if err != nil {
		t.Fatalf("run cluster failover: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Kind:")
	assertContains(t, output, "failover")
	assertContains(t, output, "Reason:")
	assertContains(t, output, "primary lost")
}

func TestRunClusterMaintenanceEnableText(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, time.April, 3, 12, 0, 0, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", request.Method)
		}
		if request.URL.Path != "/api/v1/maintenance" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body maintenanceModeUpdateRequestJSON
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		if !body.Enabled {
			t.Fatal("expected maintenance enable request")
		}
		if body.Reason != "rolling upgrade" {
			t.Fatalf("reason: got %q, want %q", body.Reason, "rolling upgrade")
		}
		if body.RequestedBy != "ops-bot" {
			t.Fatalf("requestedBy: got %q, want %q", body.RequestedBy, "ops-bot")
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{
			Enabled:     true,
			Reason:      "rolling upgrade",
			RequestedBy: "ops-bot",
			UpdatedAt:   &updatedAt,
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "maintenance", "enable", "-reason", "rolling upgrade", "-requested-by", "ops-bot"})
	if err != nil {
		t.Fatalf("run cluster maintenance enable: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Enabled:")
	assertContains(t, output, "true")
	assertContains(t, output, "rolling upgrade")
}

func TestRunClusterMaintenanceDisableJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body maintenanceModeUpdateRequestJSON
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		if body.Enabled {
			t.Fatal("expected maintenance disable request")
		}
		if body.Reason != "complete" {
			t.Fatalf("reason: got %q, want %q", body.Reason, "complete")
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{
			Enabled: false,
			Reason:  "complete",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "maintenance", "disable", "-reason", "complete", "-o", "json"})
	if err != nil {
		t.Fatalf("run cluster maintenance disable json: %v", err)
	}

	var payload maintenanceModeStatusJSON
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode maintenance json: %v", err)
	}

	if payload.Enabled || payload.Reason != "complete" {
		t.Fatalf("unexpected maintenance payload: %+v", payload)
	}
}

func TestRunHistoryListText(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/history" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(historyResponse{
			Items: []historyEntryJSON{
				{
					OperationID: "op-1",
					Kind:        "switchover",
					Timeline:    4,
					WALLSN:      "0/4000000",
					FromMember:  "alpha-1",
					ToMember:    "alpha-2",
					Reason:      "planned maintenance",
					Result:      "succeeded",
					FinishedAt:  time.Date(2026, time.April, 4, 9, 0, 0, 0, time.UTC),
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "history", "list"}); err != nil {
		t.Fatalf("run history list: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "OPERATION ID")
	assertContains(t, output, "op-1")
	assertContains(t, output, "planned maintenance")
}

func TestRunHistoryListEmpty(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/history" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(historyResponse{}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "history", "list"}); err != nil {
		t.Fatalf("run history list empty: %v", err)
	}

	assertContains(t, stdout.String(), "No history.")
}

func TestRunClusterSpecShowText(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/cluster/spec" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
			Generation:  7,
			Maintenance: maintenanceDesiredJSON{
				DefaultReason: "ops",
			},
			Failover: failoverPolicyJSON{
				Mode:            "automatic",
				MaximumLagBytes: 1048576,
				CheckTimeline:   true,
				RequireQuorum:   true,
			},
			Switchover: switchoverPolicyJSON{
				AllowScheduled: true,
				RequireSpecificCandidateDuringMaintenance: true,
			},
			Postgres: postgresPolicyJSON{
				SynchronousMode: "quorum",
				UsePgRewind:     true,
				Parameters: map[string]any{
					"max_connections": 200,
					"wal_level":       "replica",
				},
			},
			Members: []memberSpecJSON{
				{Name: "alpha-1", Priority: 100},
				{Name: "alpha-2", Priority: 90, NoFailover: true, Tags: map[string]any{"zone": "b"}},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "spec", "show"}); err != nil {
		t.Fatalf("run cluster spec show: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Cluster Name:")
	assertContains(t, output, "alpha")
	assertContains(t, output, "PostgreSQL Parameters:")
	assertContains(t, output, "max_connections")
	assertContains(t, output, "alpha-2")
}

func TestRunNodeStatusText(t *testing.T) {
	t.Parallel()

	replayTimestamp := time.Date(2026, time.April, 4, 10, 0, 30, 0, time.UTC)
	heartbeatAt := time.Date(2026, time.April, 4, 10, 1, 0, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/nodes/alpha-1" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(nodeStatusResponse{
			NodeName:   "alpha-1",
			MemberName: "alpha-1",
			Role:       "primary",
			State:      "running",
			ObservedAt: time.Date(2026, time.April, 4, 10, 2, 0, 0, time.UTC),
			Postgres: postgresLocalStatusJSON{
				Managed:       true,
				Address:       "127.0.0.1:5432",
				CheckedAt:     time.Date(2026, time.April, 4, 10, 2, 0, 0, time.UTC),
				Up:            true,
				Role:          "primary",
				RecoveryKnown: true,
				InRecovery:    false,
				Details: postgresDetailsJSON{
					ServerVersion:    170004,
					SystemIdentifier: "sys-1",
					Timeline:         2,
				},
				WAL: walProgressJSON{
					WriteLSN:        "0/5000000",
					ReplayTimestamp: &replayTimestamp,
				},
			},
			ControlPlane: controlPlaneLocalStatusJSON{
				ClusterReachable: true,
				Leader:           true,
				LastHeartbeatAt:  &heartbeatAt,
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "node", "status", "alpha-1"}); err != nil {
		t.Fatalf("run node status: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Node Name:")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "Cluster Reachable:")
	assertContains(t, output, "Write LSN:")
}

func TestRunNodeStatusRequiresName(t *testing.T) {
	t.Parallel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"node", "status"})
	if !errors.Is(err, errNodeNameRequired) {
		t.Fatalf("expected missing node name error, got %v", err)
	}
}

func TestRunDiagnosticsShowText(t *testing.T) {
	t.Parallel()

	quorumReachable := true

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/diagnostics" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		if got, want := request.URL.Query().Get("includeMembers"), "false"; got != want {
			t.Fatalf("includeMembers query: got %q, want %q", got, want)
		}

		// When includeMembers=false the API omits the members list.
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(diagnosticsSummaryJSON{
			ClusterName:        "alpha",
			GeneratedAt:        time.Date(2026, time.April, 4, 11, 5, 0, 0, time.UTC),
			ControlPlaneLeader: "alpha-1",
			QuorumReachable:    &quorumReachable,
			Warnings:           []string{"maintenance mode is enabled"},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "diagnostics", "show", "-include-members=false"}); err != nil {
		t.Fatalf("run diagnostics show: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Cluster Name:")
	assertContains(t, output, "alpha")
	assertContains(t, output, "Warnings:")
	assertContains(t, output, "maintenance mode is enabled")
}

func TestRunDiagnosticsShowWithMembers(t *testing.T) {
	t.Parallel()

	lastSeenAt := time.Date(2026, time.April, 4, 11, 0, 0, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/diagnostics" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		if got := request.URL.Query().Get("includeMembers"); got != "" {
			t.Fatalf("expected no includeMembers query param, got %q", got)
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(diagnosticsSummaryJSON{
			ClusterName:        "alpha",
			GeneratedAt:        time.Date(2026, time.April, 4, 11, 5, 0, 0, time.UTC),
			ControlPlaneLeader: "alpha-1",
			Members: []memberDiagnosticSummaryJSON{
				{
					Name:       "alpha-1",
					Role:       "primary",
					State:      "running",
					LastSeenAt: &lastSeenAt,
				},
				{
					Name:        "alpha-2",
					Role:        "replica",
					State:       "streaming",
					NeedsRejoin: true,
					LastSeenAt:  &lastSeenAt,
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "diagnostics", "show"}); err != nil {
		t.Fatalf("run diagnostics show with members: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Cluster Name:")
	assertContains(t, output, "Members:")
	assertContains(t, output, "NAME")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "alpha-2")
	assertContains(t, output, "primary")
}

func TestRunReturnsAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		if err := json.NewEncoder(writer).Encode(apiErrorResponse{
			Error:   "cluster_status_unavailable",
			Message: "cluster status unavailable",
		}); err != nil {
			t.Fatalf("encode error response: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status"})
	if err == nil {
		t.Fatal("expected API error")
	}

	assertContains(t, err.Error(), "GET /api/v1/cluster returned 503: cluster status unavailable")
}

func TestRunReturnsWriteAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		if err := json.NewEncoder(writer).Encode(apiErrorResponse{
			Error:   "switchover_conflict",
			Message: "another switchover is already running",
		}); err != nil {
			t.Fatalf("encode error response: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "switchover", "-candidate", "alpha-2"})
	if err == nil {
		t.Fatal("expected write API error")
	}

	assertContains(t, err.Error(), "POST /api/v1/operations/switchover returned 409: another switchover is already running")
}

func TestRunReturnsUnsupportedOutputFormatError(t *testing.T) {
	t.Parallel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"members", "list", "-o", "xml"})
	if err == nil {
		t.Fatal("expected unsupported output format error")
	}

	if !errors.Is(err, errUnsupportedOutputFormat) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunReturnsUnsupportedCommandError(t *testing.T) {
	t.Parallel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"unknown", "command"})
	if err == nil {
		t.Fatal("expected unsupported command error")
	}

	assertContains(t, err.Error(), "unsupported pacmanctl command")
}

func TestRunReturnsFlagError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	err := app.Run(context.Background(), []string{"-invalid"})
	if err == nil {
		t.Fatal("expected invalid flag error")
	}

	assertContains(t, err.Error(), "flag provided but not defined")
	assertContains(t, stderr.String(), "flag provided but not defined")
}

func TestRunReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(ctx, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}

func TestRunClusterStatusWithActiveOperationAndScheduledSwitchover(t *testing.T) {
	t.Parallel()

	scheduledAt := time.Date(2026, time.April, 10, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName:    "alpha",
			Phase:          "healthy",
			CurrentPrimary: "alpha-1",
			Maintenance:    maintenanceModeStatusJSON{Enabled: true, Reason: "upgrade"},
			ActiveOperation: &operationJSON{
				ID:       "op-active",
				Kind:     "switchover",
				State:    "running",
				ToMember: "alpha-2",
			},
			ScheduledSwitchover: &scheduledSwitchoverJSON{
				At:   scheduledAt,
				From: "alpha-1",
				To:   "alpha-2",
			},
			Members: []memberStatusJSON{
				{Name: "alpha-1", Role: "primary", State: "running", Healthy: true, Leader: true},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status"}); err != nil {
		t.Fatalf("run cluster status: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "enabled (upgrade)")
	assertContains(t, output, "switchover")
	assertContains(t, output, "alpha-2")
	assertContains(t, output, "2026-04-10T09:00:00Z")
}

func TestFormatMaintenance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status maintenanceModeStatusJSON
		want   string
	}{
		{name: "disabled", status: maintenanceModeStatusJSON{Enabled: false}, want: "disabled"},
		{name: "enabled no reason", status: maintenanceModeStatusJSON{Enabled: true}, want: "enabled"},
		{name: "enabled with reason", status: maintenanceModeStatusJSON{Enabled: true, Reason: "upgrade"}, want: "enabled (upgrade)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatMaintenance(tc.status)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatOperation(t *testing.T) {
	t.Parallel()

	if got := formatOperation(nil); got != "-" {
		t.Fatalf("nil: got %q, want %q", got, "-")
	}

	op := &operationJSON{Kind: "switchover", State: "running", ToMember: "alpha-2", FromMember: "alpha-1"}
	got := formatOperation(op)
	assertContains(t, got, "switchover")
	assertContains(t, got, "running")
	assertContains(t, got, "to=alpha-2")
	assertContains(t, got, "from=alpha-1")

	opNoMembers := &operationJSON{Kind: "failover", State: "pending"}
	got2 := formatOperation(opNoMembers)
	assertContains(t, got2, "failover")
	if strings.Contains(got2, "to=") {
		t.Fatalf("unexpected to= in operation without ToMember: %q", got2)
	}
}

func TestFormatScheduledSwitchover(t *testing.T) {
	t.Parallel()

	if got := formatScheduledSwitchover(nil); got != "-" {
		t.Fatalf("nil: got %q, want %q", got, "-")
	}

	at := time.Date(2026, time.May, 1, 9, 0, 0, 0, time.UTC)
	sw := &scheduledSwitchoverJSON{At: at, From: "alpha-1", To: "alpha-2"}
	got := formatScheduledSwitchover(sw)
	assertContains(t, got, "2026-05-01T09:00:00Z")
	assertContains(t, got, "from=alpha-1")
	assertContains(t, got, "to=alpha-2")

	swNoTo := &scheduledSwitchoverJSON{At: at, From: "alpha-1"}
	got2 := formatScheduledSwitchover(swNoTo)
	assertContains(t, got2, "from=alpha-1")
	if strings.Contains(got2, "to=") {
		t.Fatalf("unexpected to= for no-to switchover: %q", got2)
	}
}

func TestRunMembersListEmpty(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(membersResponse{Items: nil}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "members", "list"}); err != nil {
		t.Fatalf("run members list empty: %v", err)
	}
	assertContains(t, stdout.String(), "No members.")
}

func TestRunMembersListYAML(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(membersResponse{
			Items: []memberStatusJSON{{Name: "alpha-1", Role: "primary", State: "running", Healthy: true}},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "members", "list", "-o", "yaml"}); err != nil {
		t.Fatalf("run members list yaml: %v", err)
	}
	assertContains(t, stdout.String(), "alpha-1")
}

func TestRunClusterSpecShowJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "gamma",
			Postgres:    postgresPolicyJSON{SynchronousMode: "quorum"},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "spec", "show", "-o", "json"}); err != nil {
		t.Fatalf("run cluster spec show json: %v", err)
	}

	var payload clusterSpecResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode spec json: %v", err)
	}
	if payload.ClusterName != "gamma" {
		t.Fatalf("cluster name: got %q, want gamma", payload.ClusterName)
	}
}

func TestRunNodeStatusJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(nodeStatusResponse{
			NodeName:   "alpha-1",
			MemberName: "alpha-1",
			Role:       "primary",
			State:      "running",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "node", "status", "-node", "alpha-1", "-o", "json"}); err != nil {
		t.Fatalf("run node status json: %v", err)
	}

	var payload nodeStatusResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode node status json: %v", err)
	}
	if payload.NodeName != "alpha-1" {
		t.Fatalf("node name: got %q, want alpha-1", payload.NodeName)
	}
}

func TestRunNodeStatusExtraArgs(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"node", "status", "alpha-1", "extra"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

func TestRunNodeStatusBothFlagAndArg(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"node", "status", "-node", "alpha-1", "positional"})
	if err == nil {
		t.Fatal("expected error for both flag and positional arg")
	}
}

func TestRunDiagnosticsShowJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(diagnosticsSummaryJSON{
			ClusterName:        "alpha",
			ControlPlaneLeader: "alpha-1",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "diagnostics", "show", "-o", "json"}); err != nil {
		t.Fatalf("run diagnostics show json: %v", err)
	}

	var payload diagnosticsSummaryJSON
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode diagnostics json: %v", err)
	}
	if payload.ClusterName != "alpha" {
		t.Fatalf("cluster name: got %q, want alpha", payload.ClusterName)
	}
}

func TestRunUnsupportedSubcommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args []string
	}{
		{args: []string{"cluster"}},
		{args: []string{"cluster", "unknown"}},
		{args: []string{"cluster", "spec"}},
		{args: []string{"cluster", "spec", "unknown"}},
		{args: []string{"cluster", "maintenance"}},
		{args: []string{"cluster", "maintenance", "unknown"}},
		{args: []string{"members"}},
		{args: []string{"members", "unknown"}},
		{args: []string{"history", "unknown"}},
		{args: []string{"node"}},
		{args: []string{"node", "unknown"}},
		{args: []string{"diagnostics"}},
		{args: []string{"diagnostics", "unknown"}},
	}

	for _, tc := range tests {
		t.Run(strings.Join(tc.args, "_"), func(t *testing.T) {
			t.Parallel()
			app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
			if err := app.Run(context.Background(), tc.args); err == nil {
				t.Fatalf("expected error for args %v", tc.args)
			}
		})
	}
}

func TestRunHistoryListJSON(t *testing.T) {
	t.Parallel()

	finishedAt := time.Date(2026, time.April, 5, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(historyResponse{
			Items: []historyEntryJSON{
				{OperationID: "op-json-1", Kind: "failover", Result: "succeeded", FinishedAt: finishedAt},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "history", "list", "-o", "json"}); err != nil {
		t.Fatalf("run history list json: %v", err)
	}

	var payload historyResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode history json: %v", err)
	}
	if len(payload.Items) != 1 || payload.Items[0].OperationID != "op-json-1" {
		t.Fatalf("unexpected history payload: %+v", payload)
	}
}

func TestRunReturnsErrorForEmptyAPIErrorBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		// empty body
	}))
	defer server.Close()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status"})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	assertContains(t, err.Error(), "500")
}

func TestRunReturnsErrorForPlainTextAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusBadGateway)
		if _, err := writer.Write([]byte("upstream connection failed")); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status"})
	if err == nil {
		t.Fatal("expected error for 502 response")
	}
	assertContains(t, err.Error(), "upstream connection failed")
}

func TestRunInvalidAPIURL(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"-api-url", "not-a-url", "cluster", "status"})
	if err == nil {
		t.Fatal("expected error for invalid API URL")
	}
}

func TestRunMissingAPIURL(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	// Override default by passing empty string
	err := app.Run(context.Background(), []string{"-api-url", "  ", "cluster", "status"})
	if !errors.Is(err, errAPIURLRequired) {
		t.Fatalf("expected errAPIURLRequired, got %v", err)
	}
}

func TestRunClusterStatusYAML(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName:    "alpha",
			Phase:          "healthy",
			CurrentPrimary: "alpha-1",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status", "-o", "yaml"}); err != nil {
		t.Fatalf("run cluster status yaml: %v", err)
	}
	// YAML encoding uses lowercase field names (no yaml struct tags)
	assertContains(t, stdout.String(), "alpha")
}

func TestRunClusterMaintenanceEnableJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: true, Reason: "test"}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "maintenance", "enable", "-o", "json"}); err != nil {
		t.Fatalf("run cluster maintenance enable json: %v", err)
	}

	var payload maintenanceModeStatusJSON
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode maintenance json: %v", err)
	}
	if !payload.Enabled {
		t.Fatal("expected enabled=true in payload")
	}
}

func TestRunClusterSwitchoverYAML(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
			Operation: operationJSON{ID: "sw-yaml", Kind: "switchover"},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "switchover", "-candidate", "alpha-2", "-o", "yaml"}); err != nil {
		t.Fatalf("run cluster switchover yaml: %v", err)
	}
	assertContains(t, stdout.String(), "sw-yaml")
}

func TestRunClusterFailoverJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
			Operation: operationJSON{ID: "fo-json", Kind: "failover"},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "failover", "-o", "json"}); err != nil {
		t.Fatalf("run cluster failover json: %v", err)
	}

	var payload operationAcceptedResponse
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode failover json: %v", err)
	}
	if payload.Operation.ID != "fo-json" {
		t.Fatalf("operation id: got %q, want fo-json", payload.Operation.ID)
	}
}

func TestRunClusterFailoverWithCandidate(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body failoverRequestJSON
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.Candidate != "alpha-2" {
			t.Fatalf("candidate: got %q, want alpha-2", body.Candidate)
		}

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
			Operation: operationJSON{ID: "fo-candidate", Kind: "failover"},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "failover", "-candidate", "alpha-2"}); err != nil {
		t.Fatalf("run cluster failover with candidate: %v", err)
	}
}

func TestRunClusterStatusUnexpectedArgs(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"cluster", "status", "unexpected-arg"})
	if err == nil {
		t.Fatal("expected error for unexpected argument")
	}
}

func TestFormatAny(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "nil", value: nil, want: "-"},
		{name: "empty string", value: "", want: "-"},
		{name: "non-empty string", value: "hello", want: "hello"},
		{name: "bool true", value: true, want: "true"},
		{name: "bool false", value: false, want: "false"},
		{name: "int", value: 42, want: "42"},
		{name: "float64", value: float64(3.14), want: "3.14"},
		{name: "map", value: map[string]any{"k": "v"}, want: `{"k":"v"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatAny(tc.value)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatOptionalInt(t *testing.T) {
	t.Parallel()

	if got := formatOptionalInt(0); got != "-" {
		t.Fatalf("zero: got %q, want %q", got, "-")
	}
	if got := formatOptionalInt(42); got != "42" {
		t.Fatalf("non-zero: got %q, want %q", got, "42")
	}
}

func TestNormalizeOutputFormat(t *testing.T) {
	t.Parallel()

	if got := normalizeOutputFormat(""); got != defaultOutputFormat {
		t.Fatalf("empty: got %q, want %q", got, defaultOutputFormat)
	}
	if got := normalizeOutputFormat("  "); got != defaultOutputFormat {
		t.Fatalf("whitespace: got %q, want %q", got, defaultOutputFormat)
	}
	if got := normalizeOutputFormat("JSON"); got != "json" {
		t.Fatalf("uppercase: got %q, want %q", got, "json")
	}
}

func TestRunClusterSpecShowUnknown(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"cluster", "spec", "unknown"})
	if err == nil {
		t.Fatal("expected error for unknown cluster spec subcommand")
	}
}

func TestRunDiagnosticsShowWithMembersAndWarnings(t *testing.T) {
	t.Parallel()

	lastSeenAt := time.Date(2026, time.April, 5, 8, 0, 0, 0, time.UTC)
	quorumReachable := false

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(diagnosticsSummaryJSON{
			ClusterName:        "alpha",
			ControlPlaneLeader: "alpha-1",
			QuorumReachable:    &quorumReachable,
			Warnings:           []string{"quorum lost"},
			Members: []memberDiagnosticSummaryJSON{
				{
					Name:       "alpha-1",
					Role:       "primary",
					State:      "running",
					LastSeenAt: &lastSeenAt,
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "diagnostics", "show"}); err != nil {
		t.Fatalf("run diagnostics show: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "quorum lost")
	assertContains(t, output, "alpha-1")
}

func TestRunClusterSpecShowWithNoParameters(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "spec", "show"}); err != nil {
		t.Fatalf("run cluster spec show no params: %v", err)
	}
	assertContains(t, stdout.String(), "Cluster Name:")
}

func TestRunClusterStatusWithNoMembers(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "alpha",
			Phase:       "initializing",
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status"}); err != nil {
		t.Fatalf("run cluster status no members: %v", err)
	}
	assertContains(t, stdout.String(), "Members:")
}

func TestRunClusterSpecShowWithZeroPriorityMember(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
			Members: []memberSpecJSON{
				{Name: "alpha-1", Priority: 0},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "spec", "show"}); err != nil {
		t.Fatalf("run cluster spec zero priority: %v", err)
	}
	assertContains(t, stdout.String(), "alpha-1")
}

func TestRunDiagnosticsShowExtraArgs(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"diagnostics", "show", "extra"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}
