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

	if got, want := stdout.String(), "pacmanctl commands: cluster status, members list\n"; got != want {
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

func TestRunReturnsUnsupportedOutputFormatError(t *testing.T) {
	t.Parallel()

	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})

	err := app.Run(context.Background(), []string{"members", "list", "-o", "yaml"})
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

	err := app.Run(context.Background(), []string{"history", "list"})
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
