package pacmanctl

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestRunLogsPacmanctlCommandLifecycleAndAPIRequests(t *testing.T) {
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
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := New(Params{
		Stdout: &stdout,
		Stderr: &stderr,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "cluster", "status", "-o", "json"}); err != nil {
		t.Fatalf("run pacmanctl cluster status: %v", err)
	}

	logOutput := logs.String()
	for _, want := range []string{
		`"msg":"starting pacmanctl command"`,
		`"msg":"completed pacmanctl api request"`,
		`"msg":"completed pacmanctl command"`,
		`"component":"pacmanctl"`,
		`"command":"cluster status"`,
		`"api_url":"` + server.URL + `"`,
		`"api_token_configured":false`,
		`"method":"GET"`,
		`"path":"/api/v1/cluster"`,
		`"status":200`,
	} {
		assertContains(t, logOutput, want)
	}
}

func TestRunLogsPacmanctlFailuresWithoutLeakingSecrets(t *testing.T) {
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

	parsedURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	parsedURL.User = url.UserPassword("operator", "super-secret-password")

	var logs bytes.Buffer
	app := New(Params{
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})

	err = app.Run(context.Background(), []string{
		"-api-url", parsedURL.String(),
		"-api-token", "super-secret-token",
		"cluster", "status",
	})
	if err == nil {
		t.Fatal("expected pacmanctl API error")
	}

	logOutput := logs.String()
	for _, want := range []string{
		`"msg":"pacmanctl api request failed"`,
		`"msg":"pacmanctl command failed"`,
		`"command":"cluster status"`,
		`"api_url":"` + parsedURL.Scheme + `://` + parsedURL.Host + `"`,
		`"api_token_configured":true`,
		`"status":503`,
		`"error":"GET /api/v1/cluster returned 503: cluster status unavailable"`,
	} {
		assertContains(t, logOutput, want)
	}

	for _, secret := range []string{"super-secret-token", "super-secret-password", "operator@"} {
		if strings.Contains(logOutput, secret) {
			t.Fatalf("expected logs to redact secret value %q, got %q", secret, logOutput)
		}
	}
}

func TestInferCommandPathAndAPILogSanitizing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		showVersion bool
		args        []string
		want        string
	}{
		{name: "version", showVersion: true, want: "version"},
		{name: "help", args: nil, want: "help"},
		{name: "cluster status", args: []string{"cluster", "status", "-o", "json"}, want: "cluster status"},
		{name: "cluster spec", args: []string{"cluster", "spec", "show"}, want: "cluster spec show"},
		{name: "maintenance", args: []string{"cluster", "maintenance", "enable"}, want: "cluster maintenance enable"},
		{name: "patronictl alias", args: []string{"switchover", "alpha"}, want: "switchover"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := inferCommandPath(testCase.showVersion, testCase.args); got != testCase.want {
				t.Fatalf("unexpected command path: got %q want %q", got, testCase.want)
			}
		})
	}

	if got := sanitizeLogAPIURL("http://operator:secret@example.com:8080/path?token=secret#frag"); got != "http://example.com:8080/path" {
		t.Fatalf("unexpected sanitized api url: got %q want %q", got, "http://example.com:8080/path")
	}

	if got := sanitizeLogAPIURL("not-a-valid-url"); got != "" {
		t.Fatalf("expected invalid api url to be omitted from logs, got %q", got)
	}
}
