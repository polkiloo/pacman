package httpapi

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestPatroniInspiredProbeHEADResponsesMatchGETAndOmitBody(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	primary := primaryNodeStatus("alpha-1", now)
	primary.Postgres.Up = true
	replica := replicaNodeStatus("alpha-1", now, 0)

	testCases := []struct {
		path  string
		store testNodeStatusStore
	}{
		{path: "/health", store: testNodeStatusStore{nodeStatus: primary, hasNode: true}},
		{path: "/liveness", store: testNodeStatusStore{nodeStatus: primary, hasNode: true}},
		{path: "/readiness", store: testNodeStatusStore{nodeStatus: primary, hasNode: true}},
		{path: "/primary", store: testNodeStatusStore{nodeStatus: primary, hasNode: true}},
		{path: "/replica", store: testNodeStatusStore{nodeStatus: replica, hasNode: true}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.path, func(t *testing.T) {
			t.Parallel()

			srv := New("alpha-1", testCase.store, discardLogger(), Config{})

			get := performRequestMethod(t, srv, http.MethodGet, testCase.path)
			_, _ = io.Copy(io.Discard, get.Body)
			get.Body.Close()

			head := performRequestMethod(t, srv, http.MethodHead, testCase.path)
			headBody, err := io.ReadAll(head.Body)
			head.Body.Close()
			if err != nil {
				t.Fatalf("read HEAD %s body: %v", testCase.path, err)
			}

			if head.StatusCode != get.StatusCode {
				t.Fatalf("HEAD %s status: got %d, want GET status %d", testCase.path, head.StatusCode, get.StatusCode)
			}
			if len(headBody) != 0 {
				t.Fatalf("HEAD %s returned body %q", testCase.path, headBody)
			}
		})
	}
}

func TestPatroniInspiredProbeOPTIONSAdvertisesProbeMethods(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{})
	for _, path := range []string{"/health", "/liveness", "/readiness", "/primary", "/replica"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			response := performRequestMethod(t, srv, http.MethodOptions, path)
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()

			if response.StatusCode != http.StatusOK {
				t.Fatalf("OPTIONS %s status: got %d, want %d", path, response.StatusCode, http.StatusOK)
			}

			allow := response.Header.Get("Allow")
			for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
				if !containsWord(allow, method) {
					t.Fatalf("OPTIONS %s Allow header %q missing %s", path, allow, method)
				}
			}
		})
	}
}

func TestPatroniInspiredAPINamespaceWrongMethodsReturnJSONNotFound(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	srv := New("alpha-1", testNodeStatusStore{
		clusterStatus:    clusterStatusForPatroniInspiredTests(now),
		hasClusterStatus: true,
	}, discardLogger(), Config{})

	testCases := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/api/v1/cluster"},
		{method: http.MethodPatch, path: "/api/v1/members"},
		{method: http.MethodGet, path: "/api/v1/operations/switchover"},
		{method: http.MethodDelete, path: "/api/v1/operations/failover"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.method+" "+testCase.path, func(t *testing.T) {
			t.Parallel()

			response := performRequestMethod(t, srv, testCase.method, testCase.path)
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("%s %s status: got %d, want %d", testCase.method, testCase.path, response.StatusCode, http.StatusNotFound)
			}

			assertAPIResponseHeaders(t, response)

			var body errorResponseJSON
			decodeJSONResponse(t, response, &body)
			if body.Error != "not_found" {
				t.Fatalf("error: got %q, want %q", body.Error, "not_found")
			}
			if !strings.Contains(body.Message, testCase.path) {
				t.Fatalf("message %q does not mention path %q", body.Message, testCase.path)
			}
		})
	}
}

func TestPatroniInspiredInvalidOperationBodiesReturnJSONErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path      string
		wantError string
	}{
		{path: "/api/v1/operations/switchover", wantError: "invalid_switchover_request"},
		{path: "/api/v1/operations/failover", wantError: "invalid_failover_request"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.path, func(t *testing.T) {
			t.Parallel()

			response := performRequestBodyWithHeaders(
				t,
				New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}),
				http.MethodPost,
				testCase.path,
				[]byte(`{"candidate":`),
				map[string]string{"Content-Type": "application/json"},
			)

			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("POST %s status: got %d, want %d", testCase.path, response.StatusCode, http.StatusBadRequest)
			}

			assertAPIResponseHeaders(t, response)

			var body errorResponseJSON
			decodeJSONResponse(t, response, &body)
			if body.Error != testCase.wantError {
				t.Fatalf("error: got %q, want %q", body.Error, testCase.wantError)
			}
		})
	}
}

func TestPatroniInspiredOpenAPIHEADUsesCachedDocumentAndOmitBody(t *testing.T) {
	t.Parallel()

	requests := 0
	srv := New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{
		OpenAPIDocument: func() ([]byte, error) {
			requests++
			return []byte("openapi: 3.1.0\ninfo:\n  title: Patroni Inspired Test\n  version: 0.1.0\npaths: {}\n"), nil
		},
	})

	get := performRequestMethod(t, srv, http.MethodGet, "/openapi.yaml")
	getBody, err := io.ReadAll(get.Body)
	get.Body.Close()
	if err != nil {
		t.Fatalf("read GET /openapi.yaml: %v", err)
	}
	if get.StatusCode != http.StatusOK {
		t.Fatalf("GET /openapi.yaml status: got %d, want %d", get.StatusCode, http.StatusOK)
	}

	head := performRequestMethod(t, srv, http.MethodHead, "/openapi.yaml")
	headBody, err := io.ReadAll(head.Body)
	head.Body.Close()
	if err != nil {
		t.Fatalf("read HEAD /openapi.yaml: %v", err)
	}

	if head.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /openapi.yaml status: got %d, want %d", head.StatusCode, http.StatusOK)
	}
	if len(headBody) != 0 {
		t.Fatalf("HEAD /openapi.yaml returned body %q", headBody)
	}
	if requests != 1 {
		t.Fatalf("OpenAPI provider calls: got %d, want 1", requests)
	}
	if !strings.Contains(string(getBody), "Patroni Inspired Test") {
		t.Fatalf("unexpected GET /openapi.yaml body: %s", getBody)
	}
	if got := head.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/yaml") {
		t.Fatalf("HEAD /openapi.yaml Content-Type: got %q", got)
	}
	assertAPIResponseHeaders(t, head)
}

func TestPatroniInspiredOpenAPIHEADCachesProviderFailure(t *testing.T) {
	t.Parallel()

	requests := 0
	srv := New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{
		OpenAPIDocument: func() ([]byte, error) {
			requests++
			return nil, errors.New("document unavailable")
		},
	})

	first := performRequestMethod(t, srv, http.MethodHead, "/openapi.yaml")
	firstBody, err := io.ReadAll(first.Body)
	first.Body.Close()
	if err != nil {
		t.Fatalf("read first HEAD /openapi.yaml: %v", err)
	}

	second := performRequestMethod(t, srv, http.MethodGet, "/openapi.yaml")
	defer second.Body.Close()

	if first.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("first HEAD status: got %d, want %d", first.StatusCode, http.StatusServiceUnavailable)
	}
	if len(firstBody) != 0 {
		t.Fatalf("first HEAD returned body %q", firstBody)
	}
	if second.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second GET status: got %d, want %d", second.StatusCode, http.StatusServiceUnavailable)
	}
	if requests != 1 {
		t.Fatalf("OpenAPI provider calls: got %d, want 1", requests)
	}
}

func assertAPIResponseHeaders(t *testing.T, response *http.Response) {
	t.Helper()

	if got := response.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control: got %q, want %q", got, "no-store")
	}
	if got := response.Header.Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma: got %q, want %q", got, "no-cache")
	}
	if got := response.Header.Get(headerXContentTypeOption); got != "nosniff" {
		t.Fatalf("%s: got %q, want %q", headerXContentTypeOption, got, "nosniff")
	}
}

func clusterStatusForPatroniInspiredTests(observedAt time.Time) cluster.ClusterStatus {
	return cluster.ClusterStatus{
		ClusterName:    "alpha",
		Phase:          cluster.ClusterPhaseHealthy,
		CurrentPrimary: "alpha-1",
		CurrentEpoch:   3,
		ObservedAt:     observedAt,
		Members: []cluster.MemberStatus{
			{
				Name:       "alpha-1",
				Role:       cluster.MemberRolePrimary,
				State:      cluster.MemberStateRunning,
				Healthy:    true,
				Leader:     true,
				LastSeenAt: observedAt,
			},
		},
	}
}
