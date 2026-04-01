package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

func TestServerStartServesAndShutsDown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New("alpha-1", testNodeStatusStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})
	addr := reserveLoopbackAddress(t)

	if err := srv.Start(ctx, addr); err != nil {
		t.Fatalf("start server: %v", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	resp, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		t.Fatalf("call health endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected health status: got %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	cancel()

	if err := srv.Wait(); err != nil {
		t.Fatalf("wait for server shutdown: %v", err)
	}
}

func TestServerStartReturnsErrorForDoubleStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New("alpha-1", testNodeStatusStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})

	if err := srv.Start(ctx, reserveLoopbackAddress(t)); err != nil {
		t.Fatalf("first start: %v", err)
	}

	if err := srv.Start(ctx, reserveLoopbackAddress(t)); err == nil {
		t.Fatal("expected error on double start, got nil")
	}
}

func TestServerStartReturnsErrorForInvalidAddress(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{})
	if err := srv.Start(ctx, "not-a-valid-address"); err == nil {
		t.Fatal("expected error for invalid address, got nil")
	}
}

func TestIsClosedListenerError(t *testing.T) {
	t.Parallel()

	if !isClosedListenerError(nil) {
		t.Error("nil error should be treated as closed listener")
	}

	if !isClosedListenerError(net.ErrClosed) {
		t.Error("net.ErrClosed should be treated as closed listener")
	}

	if !isClosedListenerError(errors.New("use of closed network connection")) {
		t.Error("message-matched error should be treated as closed listener")
	}

	if isClosedListenerError(errors.New("some unrelated network error")) {
		t.Error("unrelated error must not be treated as closed listener")
	}
}

func TestWaitReturnsNilWhenServerNeverStarted(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})

	if err := srv.Wait(); err != nil {
		t.Fatalf("unexpected error from Wait on unstarted server: %v", err)
	}
}

func TestRequestIDMiddlewareGeneratesHeaderWhenMissing(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	requestID := response.Header.Get(headerRequestID)
	if strings.TrimSpace(requestID) == "" {
		t.Fatal("expected generated request ID header")
	}
}

func TestRequestIDMiddlewarePreservesProvidedHeader(t *testing.T) {
	t.Parallel()

	response := performRequestWithHeaders(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), http.MethodGet, "/health", map[string]string{
		headerRequestID: "req-123",
	})
	defer response.Body.Close()

	if got := response.Header.Get(headerRequestID); got != "req-123" {
		t.Fatalf("unexpected request ID header: got %q, want %q", got, "req-123")
	}
}

func TestAccessLogMiddlewareLogsRequest(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil))

	response := performRequestWithHeaders(t, New("alpha-1", testNodeStatusStore{}, logger, Config{}), http.MethodGet, "/health", map[string]string{
		headerRequestID: "req-456",
	})
	defer response.Body.Close()

	logs := buffer.String()
	if !strings.Contains(logs, `"msg":"handled http request"`) {
		t.Fatalf("expected access log entry, got %q", logs)
	}

	for _, want := range []string{
		`"component":"httpapi"`,
		`"request_id":"req-456"`,
		`"method":"GET"`,
		`"path":"/health"`,
		`"status":503`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("expected access log to contain %q, got %q", want, logs)
		}
	}
}

func TestAPICommonMiddlewareSetsNoStoreHeaders(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus: cluster.ClusterStatus{
			ClusterName:  "alpha",
			Phase:        cluster.ClusterPhaseHealthy,
			CurrentEpoch: 1,
			ObservedAt:   now,
			Members:      []cluster.MemberStatus{},
		},
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	if got := response.Header.Get(fiber.HeaderCacheControl); got != "no-store" {
		t.Fatalf("cache-control: got %q, want %q", got, "no-store")
	}

	if got := response.Header.Get(fiber.HeaderPragma); got != "no-cache" {
		t.Fatalf("pragma: got %q, want %q", got, "no-cache")
	}

	if got := response.Header.Get(headerXContentTypeOption); got != "nosniff" {
		t.Fatalf("x-content-type-options: got %q, want %q", got, "nosniff")
	}
}

func TestAuthMiddlewareRejectsUnauthorizedAPIRoute(t *testing.T) {
	t.Parallel()

	authorizer := &testAuthorizer{
		err: Unauthorized("missing bearer token"),
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{
		Authorizer: authorizer,
	}), "/api/v1/cluster")
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}

	if got := response.Header.Get(fiber.HeaderWWWAuthenticate); got != "Bearer" {
		t.Fatalf("www-authenticate: got %q, want %q", got, "Bearer")
	}

	if len(authorizer.scopes) != 1 || authorizer.scopes[0] != AccessScopeClusterRead {
		t.Fatalf("unexpected auth scopes: got %+v", authorizer.scopes)
	}

	var body errorResponseJSON
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.Error != "unauthorized" {
		t.Fatalf("error: got %q, want %q", body.Error, "unauthorized")
	}

	if body.Message != "missing bearer token" {
		t.Fatalf("message: got %q, want %q", body.Message, "missing bearer token")
	}
}

func TestAuthMiddlewareRejectsForbiddenWriteRoute(t *testing.T) {
	t.Parallel()

	authorizer := &testAuthorizer{
		err: Forbidden("operator role required"),
	}

	response := performRequestBodyWithHeaders(
		t,
		New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{Authorizer: authorizer}),
		http.MethodPut,
		"/api/v1/maintenance",
		[]byte(`{"enabled":true}`),
		map[string]string{"Content-Type": "application/json"},
	)
	defer response.Body.Close()

	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusForbidden)
	}

	if len(authorizer.scopes) != 1 || authorizer.scopes[0] != AccessScopeClusterWrite {
		t.Fatalf("unexpected auth scopes: got %+v", authorizer.scopes)
	}

	var body errorResponseJSON
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.Error != "forbidden" {
		t.Fatalf("error: got %q, want %q", body.Error, "forbidden")
	}

	if body.Message != "operator role required" {
		t.Fatalf("message: got %q, want %q", body.Message, "operator role required")
	}
}

func TestAuthMiddlewareStoresPrincipalInRequestContext(t *testing.T) {
	t.Parallel()

	authorizer := &testAuthorizer{
		principal: &Principal{
			Subject:   "ops@example",
			Mechanism: "bearer",
		},
	}

	srv := &Server{
		nodeName:   "alpha-1",
		authorizer: authorizer,
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(srv.requestIDMiddleware())
	app.Use(srv.apiCommonMiddleware())
	app.Use(srv.authMiddleware(AccessScopeClusterRead))
	app.Get("/api/v1/test-principal", func(c *fiber.Ctx) error {
		principal, ok := CurrentPrincipal(c)
		if !ok {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "principal_missing",
			})
		}

		return c.JSON(fiber.Map{
			"subject":    principal.Subject,
			"mechanism":  principal.Mechanism,
			"request_id": RequestID(c),
		})
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v1/test-principal", nil)
	response, err := app.Test(request, int(time.Second.Milliseconds()))
	if err != nil {
		t.Fatalf("perform GET %q: %v", "/api/v1/test-principal", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	if len(authorizer.scopes) != 1 || authorizer.scopes[0] != AccessScopeClusterRead {
		t.Fatalf("unexpected auth scopes: got %+v", authorizer.scopes)
	}

	var body struct {
		Subject   string `json:"subject"`
		Mechanism string `json:"mechanism"`
		RequestID string `json:"request_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.Subject != "ops@example" {
		t.Fatalf("subject: got %q, want %q", body.Subject, "ops@example")
	}

	if body.Mechanism != "bearer" {
		t.Fatalf("mechanism: got %q, want %q", body.Mechanism, "bearer")
	}

	if strings.TrimSpace(body.RequestID) == "" {
		t.Fatal("expected request ID to be available in handler context")
	}
}

func TestWriteAPIRoutesRequireJSONContentType(t *testing.T) {
	t.Parallel()

	response := performRequestBodyWithHeaders(
		t,
		New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}),
		http.MethodPut,
		"/api/v1/maintenance",
		[]byte(`{"enabled":true}`),
		map[string]string{"Content-Type": "text/plain"},
	)
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusUnsupportedMediaType)
	}

	var body errorResponseJSON
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.Error != "unsupported_media_type" {
		t.Fatalf("error: got %q, want %q", body.Error, "unsupported_media_type")
	}
}

func TestAPINotFoundReturnsJSONError(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/missing")
	defer response.Body.Close()

	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusNotFound)
	}

	var body errorResponseJSON
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.Error != "not_found" {
		t.Fatalf("error: got %q, want %q", body.Error, "not_found")
	}

	if body.Message != `path "/api/v1/missing" was not found` {
		t.Fatalf("message: got %q", body.Message)
	}
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

func TestHealthReturnsOKWhenPostgresUp(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	status := primaryNodeStatus("alpha-1", now)
	status.Postgres.Up = true

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus:  status,
		hasNode:     true,
		clusterSpec: cluster.ClusterSpec{ClusterName: "alpha"},
		hasSpec:     true,
	}, discardLogger(), Config{}), "/health")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.State != "running" {
		t.Fatalf("unexpected state: got %q, want %q", body.State, "running")
	}

	if body.Role != "primary" {
		t.Fatalf("unexpected role: got %q, want %q", body.Role, "primary")
	}

	if body.Patroni.Name != "alpha-1" {
		t.Fatalf("unexpected patroni name: got %q", body.Patroni.Name)
	}

	if body.Patroni.Scope != "alpha" {
		t.Fatalf("unexpected patroni scope: got %q", body.Patroni.Scope)
	}

	if body.ServerVersion != 170002 {
		t.Fatalf("unexpected server version: got %d", body.ServerVersion)
	}
}

// TestHealthReturnsPrimaryStatus is kept for backward compat with the original suite.
func TestHealthReturnsPrimaryStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	status := agentmodel.NodeStatus{
		NodeName:   "alpha-1",
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		ObservedAt: now,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				ServerVersion:    170002,
				SystemIdentifier: "7599025879359099984",
				Timeline:         1,
			},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus:  status,
		hasNode:     true,
		clusterSpec: cluster.ClusterSpec{ClusterName: "alpha"},
		hasSpec:     true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{}), "/health")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.State != "running" {
		t.Fatalf("unexpected state: got %q", body.State)
	}

	if body.Role != "primary" {
		t.Fatalf("unexpected role: got %q", body.Role)
	}

	if body.Patroni.Name != "alpha-1" {
		t.Fatalf("unexpected patroni name: got %q", body.Patroni.Name)
	}

	if body.Patroni.Scope != "alpha" {
		t.Fatalf("unexpected patroni scope: got %q", body.Patroni.Scope)
	}

	if body.ServerVersion != 170002 {
		t.Fatalf("unexpected server version: got %d", body.ServerVersion)
	}
}

func TestHealthReturnsServiceUnavailableWhenPostgresDown(t *testing.T) {
	t.Parallel()

	status := primaryNodeStatus("alpha-1", time.Now().UTC())
	status.Postgres.Up = false

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: status,
		hasNode:    true,
	}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestHealthReturnsServiceUnavailableWhenNodeAbsent(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

// ---------------------------------------------------------------------------
// GET /liveness
// ---------------------------------------------------------------------------

func TestLivenessReturnsOKWhenHeartbeatFresh(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: agentmodel.NodeStatus{
			NodeName:   "alpha-1",
			ObservedAt: time.Now(),
		},
		hasNode: true,
	}, discardLogger(), Config{LivenessWindow: 30 * time.Second}), "/liveness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestLivenessReturnsServiceUnavailableForStaleHeartbeat(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: agentmodel.NodeStatus{
			NodeName:   "alpha-1",
			ObservedAt: time.Now().Add(-time.Minute),
			Postgres: agentmodel.PostgresStatus{
				Managed: true,
				Up:      true,
			},
		},
		hasNode: true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{LivenessWindow: 5 * time.Second}), "/liveness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status code: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestLivenessReturnsServiceUnavailableWhenNodeAbsent(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/liveness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

// ---------------------------------------------------------------------------
// GET /readiness
// ---------------------------------------------------------------------------

func TestReadinessReturnsOKForPrimaryWhenPostgresUp(t *testing.T) {
	t.Parallel()

	status := primaryNodeStatus("alpha-1", time.Now().UTC())
	status.Postgres.Up = true

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: status,
		hasNode:    true,
	}, discardLogger(), Config{}), "/readiness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestReadinessReturnsServiceUnavailableWhenNodeAbsent(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/readiness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestReadinessReturnsOKForReplicaWithNoLagLimit(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 999999),
		hasNode:    true,
	}, discardLogger(), Config{}), "/readiness")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestReadinessHonorsReplicaLagThreshold(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: agentmodel.NodeStatus{
			NodeName:   "alpha-1",
			Role:       cluster.MemberRoleReplica,
			State:      cluster.MemberStateStreaming,
			ObservedAt: time.Now().UTC(),
			Postgres: agentmodel.PostgresStatus{
				Managed: true,
				Up:      true,
				Role:    cluster.MemberRoleReplica,
				Details: agentmodel.PostgresDetails{
					ReplicationLagBytes: 64,
				},
			},
		},
		hasNode: true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})

	response := performRequest(t, srv, "/readiness?lag=32B")
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status code for strict lag: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	response = performRequest(t, srv, "/readiness?lag=128B&mode=apply")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code for relaxed lag: got %d, want %d", response.StatusCode, http.StatusOK)
	}
}

// ---------------------------------------------------------------------------
// GET /primary
// ---------------------------------------------------------------------------

func TestPrimaryReturnsOKForPrimary(t *testing.T) {
	t.Parallel()

	status := primaryNodeStatus("alpha-1", time.Now().UTC())
	status.Postgres.Up = true

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus:  status,
		hasNode:     true,
		clusterSpec: cluster.ClusterSpec{ClusterName: "batman"},
		hasSpec:     true,
	}, discardLogger(), Config{}), "/primary")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.Role != "primary" {
		t.Fatalf("unexpected role: got %q, want %q", body.Role, "primary")
	}

	if body.State != "running" {
		t.Fatalf("unexpected state: got %q, want %q", body.State, "running")
	}

	if body.Patroni.Scope != "batman" {
		t.Fatalf("unexpected scope: got %q, want %q", body.Patroni.Scope, "batman")
	}
}

func TestPrimaryReturnsServiceUnavailableForReplica(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 0),
		hasNode:    true,
	}, discardLogger(), Config{}), "/primary")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestPrimaryReturnsServiceUnavailableWhenPostgresDown(t *testing.T) {
	t.Parallel()

	status := primaryNodeStatus("alpha-1", time.Now().UTC())
	status.Postgres.Up = false

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: status,
		hasNode:    true,
	}, discardLogger(), Config{}), "/primary")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestPrimaryReturnsServiceUnavailableWhenNodeAbsent(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/primary")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

// ---------------------------------------------------------------------------
// GET /replica
// ---------------------------------------------------------------------------

func TestReplicaReturnsOKForHealthyReplica(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 0),
		hasNode:    true,
	}, discardLogger(), Config{}), "/replica")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.Role != "replica" {
		t.Fatalf("unexpected role: got %q, want %q", body.Role, "replica")
	}
}

func TestReplicaReturnsServiceUnavailableForPrimary(t *testing.T) {
	t.Parallel()

	status := primaryNodeStatus("alpha-1", time.Now().UTC())
	status.Postgres.Up = true

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: status,
		hasNode:    true,
	}, discardLogger(), Config{}), "/replica")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestReplicaReturnsServiceUnavailableWhenPostgresDown(t *testing.T) {
	t.Parallel()

	status := replicaNodeStatus("alpha-1", time.Now().UTC(), 0)
	status.Postgres.Up = false

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: status,
		hasNode:    true,
	}, discardLogger(), Config{}), "/replica")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestReplicaReturnsServiceUnavailableWhenNodeAbsent(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/replica")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestReplicaHonorsLagThreshold(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 2*1024*1024),
		hasNode:    true,
	}, discardLogger(), Config{})

	// lag 2MB, limit 1MB → 503
	response := performRequest(t, srv, "/replica?lag=1MB")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when lag exceeds limit: got %d", response.StatusCode)
	}

	// lag 2MB, limit 4MB → 200
	response = performRequest(t, srv, "/replica?lag=4MB")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 when lag within limit: got %d", response.StatusCode)
	}
}

func TestReplicaHonorsReplicationState(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 0),
		hasNode:    true,
	}, discardLogger(), Config{})

	// streaming matches → 200
	response := performRequest(t, srv, "/replica?replication_state=streaming")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for streaming state: got %d", response.StatusCode)
	}

	// unknown replication state → 503
	response = performRequest(t, srv, "/replica?replication_state=catchup")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for non-streaming state: got %d", response.StatusCode)
	}
}

func TestReplicaHonorsTagFilters(t *testing.T) {
	t.Parallel()

	node := replicaNodeStatus("alpha-1", time.Now().UTC(), 0)
	node.Tags = map[string]any{"clonefrom": true, "env": "prod"}

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
	}, discardLogger(), Config{})

	cases := []struct {
		path   string
		status int
		label  string
	}{
		{"/replica?clonefrom=true", http.StatusOK, "bool true matches"},
		{"/replica?clonefrom=false", http.StatusServiceUnavailable, "bool false mismatch"},
		{"/replica?env=prod", http.StatusOK, "string match"},
		{"/replica?env=staging", http.StatusServiceUnavailable, "string mismatch"},
		{"/replica?clonefrom=true&env=prod", http.StatusOK, "multiple filters match"},
		{"/replica?clonefrom=true&env=staging", http.StatusServiceUnavailable, "multiple filters one mismatch"},
		{"/replica?missing=value", http.StatusServiceUnavailable, "absent tag treated as empty"},
	}

	for _, c := range cases {
		response := performRequest(t, srv, c.path)
		response.Body.Close()

		if response.StatusCode != c.status {
			t.Errorf("case %q (%s): got %d, want %d", c.path, c.label, response.StatusCode, c.status)
		}
	}
}

func TestReplicaIgnoresReservedQueryParamsInTagFilters(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 512),
		hasNode:    true,
	}, discardLogger(), Config{})

	// lag, replication_state, mode must not be treated as tag filters
	response := performRequest(t, srv, "/replica?lag=1kB&replication_state=streaming&mode=apply")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("reserved params must not act as tag filters: got %d", response.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// HEAD requests
// ---------------------------------------------------------------------------

func TestHeadPrimaryMatchesGetStatus(t *testing.T) {
	t.Parallel()

	status := primaryNodeStatus("alpha-1", time.Now().UTC())
	status.Postgres.Up = true

	srv := New("alpha-1", testNodeStatusStore{nodeStatus: status, hasNode: true}, discardLogger(), Config{})

	get := performRequest(t, srv, "/primary")
	get.Body.Close()

	head := performRequestMethod(t, srv, http.MethodHead, "/primary")
	head.Body.Close()

	if get.StatusCode != head.StatusCode {
		t.Fatalf("HEAD /primary status %d differs from GET status %d", head.StatusCode, get.StatusCode)
	}
}

func TestHeadReplicaMatchesGetStatus(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{
		nodeStatus: replicaNodeStatus("alpha-1", time.Now().UTC(), 0),
		hasNode:    true,
	}, discardLogger(), Config{})

	get := performRequest(t, srv, "/replica")
	get.Body.Close()

	head := performRequestMethod(t, srv, http.MethodHead, "/replica")
	head.Body.Close()

	if get.StatusCode != head.StatusCode {
		t.Fatalf("HEAD /replica status %d differs from GET status %d", head.StatusCode, get.StatusCode)
	}
}

func TestHeadHealthMatchesGetStatus(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{})

	get := performRequest(t, srv, "/health")
	get.Body.Close()

	head := performRequestMethod(t, srv, http.MethodHead, "/health")
	head.Body.Close()

	if get.StatusCode != head.StatusCode {
		t.Fatalf("HEAD /health status %d differs from GET status %d", head.StatusCode, get.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// OPTIONS requests
// ---------------------------------------------------------------------------

func TestOptionsReturnsAllowHeader(t *testing.T) {
	t.Parallel()

	srv := New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{})

	for _, path := range []string{"/health", "/liveness", "/readiness", "/primary", "/replica"} {
		response := performRequestMethod(t, srv, http.MethodOptions, path)
		response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Errorf("OPTIONS %s: got %d, want %d", path, response.StatusCode, http.StatusOK)
		}

		allow := response.Header.Get("Allow")
		for _, method := range []string{"GET", "HEAD", "OPTIONS"} {
			if !containsWord(allow, method) {
				t.Errorf("OPTIONS %s: Allow header %q missing %s", path, allow, method)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/cluster
// ---------------------------------------------------------------------------

func TestGetClusterStatusReturnsOKWithStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	status := cluster.ClusterStatus{
		ClusterName:    "alpha",
		Phase:          cluster.ClusterPhaseHealthy,
		CurrentPrimary: "alpha-1",
		CurrentEpoch:   3,
		ObservedAt:     now,
		Maintenance:    cluster.MaintenanceModeStatus{Enabled: false},
		Members: []cluster.MemberStatus{
			{
				Name:       "alpha-1",
				Role:       cluster.MemberRolePrimary,
				State:      cluster.MemberStateRunning,
				Healthy:    true,
				Leader:     true,
				Timeline:   2,
				Priority:   100,
				LastSeenAt: now,
			},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body clusterStatusResponse
	decodeJSONResponse(t, response, &body)

	if body.ClusterName != "alpha" {
		t.Errorf("clusterName: got %q, want %q", body.ClusterName, "alpha")
	}

	if body.Phase != "healthy" {
		t.Errorf("phase: got %q, want %q", body.Phase, "healthy")
	}

	if body.CurrentPrimary != "alpha-1" {
		t.Errorf("currentPrimary: got %q, want %q", body.CurrentPrimary, "alpha-1")
	}

	if body.CurrentEpoch != 3 {
		t.Errorf("currentEpoch: got %d, want %d", body.CurrentEpoch, 3)
	}

	if len(body.Members) != 1 {
		t.Fatalf("members: got %d, want 1", len(body.Members))
	}

	m := body.Members[0]
	if m.Name != "alpha-1" {
		t.Errorf("member name: got %q, want %q", m.Name, "alpha-1")
	}

	if m.Role != "primary" {
		t.Errorf("member role: got %q, want %q", m.Role, "primary")
	}

	if !m.Healthy {
		t.Error("member healthy: got false, want true")
	}

	if !m.Leader {
		t.Error("member leader: got false, want true")
	}
}

func TestGetClusterStatusReturns503WhenUnavailable(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestGetClusterStatusIncludesActiveOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(time.Second)
	completedAt := now.Add(2 * time.Second)

	op := cluster.Operation{
		ID:          "op-1",
		Kind:        cluster.OperationKindSwitchover,
		State:       cluster.OperationStateCompleted,
		RequestedBy: "admin",
		RequestedAt: now,
		Reason:      "planned",
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Result:      cluster.OperationResultSucceeded,
		Message:     "done",
	}

	status := cluster.ClusterStatus{
		ClusterName:     "alpha",
		Phase:           cluster.ClusterPhaseHealthy,
		CurrentEpoch:    1,
		ObservedAt:      now,
		ActiveOperation: &op,
		Members:         []cluster.MemberStatus{},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.ActiveOperation == nil {
		t.Fatal("expected activeOperation, got nil")
	}

	if body.ActiveOperation.ID != "op-1" {
		t.Errorf("operation id: got %q, want %q", body.ActiveOperation.ID, "op-1")
	}

	if body.ActiveOperation.Kind != "switchover" {
		t.Errorf("operation kind: got %q, want %q", body.ActiveOperation.Kind, "switchover")
	}

	if body.ActiveOperation.StartedAt == nil {
		t.Error("expected startedAt, got nil")
	}

	if body.ActiveOperation.CompletedAt == nil {
		t.Error("expected completedAt, got nil")
	}
}

func TestGetClusterStatusIncludesOperationScheduledAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(time.Hour)

	op := cluster.Operation{
		ID:          "op-scheduled",
		Kind:        cluster.OperationKindSwitchover,
		State:       cluster.OperationStateScheduled,
		RequestedAt: now,
		ScheduledAt: scheduledAt,
		// StartedAt and CompletedAt intentionally zero
	}

	status := cluster.ClusterStatus{
		ClusterName:     "alpha",
		Phase:           cluster.ClusterPhaseHealthy,
		CurrentEpoch:    1,
		ObservedAt:      now,
		ActiveOperation: &op,
		Members:         []cluster.MemberStatus{},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.ActiveOperation == nil {
		t.Fatal("expected activeOperation, got nil")
	}

	if body.ActiveOperation.ScheduledAt == nil {
		t.Error("expected scheduledAt to be non-nil")
	}

	if body.ActiveOperation.StartedAt != nil {
		t.Error("expected startedAt to be nil when zero")
	}
}

func TestGetClusterStatusOperationScheduledAtOmittedWhenZero(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	op := cluster.Operation{
		ID:          "op-2",
		Kind:        cluster.OperationKindFailover,
		State:       cluster.OperationStateRunning,
		RequestedAt: now,
		// ScheduledAt, StartedAt, CompletedAt intentionally zero
	}

	status := cluster.ClusterStatus{
		ClusterName:     "alpha",
		Phase:           cluster.ClusterPhaseFailingOver,
		CurrentEpoch:    2,
		ObservedAt:      now,
		ActiveOperation: &op,
		Members:         []cluster.MemberStatus{},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.ActiveOperation == nil {
		t.Fatal("expected activeOperation, got nil")
	}

	if body.ActiveOperation.ScheduledAt != nil {
		t.Error("expected scheduledAt to be nil when zero")
	}

	if body.ActiveOperation.StartedAt != nil {
		t.Error("expected startedAt to be nil when zero")
	}

	if body.ActiveOperation.CompletedAt != nil {
		t.Error("expected completedAt to be nil when zero")
	}
}

func TestGetClusterStatusIncludesScheduledSwitchover(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	scheduledAt := now.Add(time.Hour)

	sw := cluster.ScheduledSwitchover{
		At:   scheduledAt,
		From: "alpha-1",
		To:   "alpha-2",
	}

	status := cluster.ClusterStatus{
		ClusterName:         "alpha",
		Phase:               cluster.ClusterPhaseHealthy,
		CurrentEpoch:        1,
		ObservedAt:          now,
		ScheduledSwitchover: &sw,
		Members:             []cluster.MemberStatus{},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.ScheduledSwitchover == nil {
		t.Fatal("expected scheduledSwitchover, got nil")
	}

	if body.ScheduledSwitchover.From != "alpha-1" {
		t.Errorf("scheduled switchover from: got %q, want %q", body.ScheduledSwitchover.From, "alpha-1")
	}

	if body.ScheduledSwitchover.To != "alpha-2" {
		t.Errorf("scheduled switchover to: got %q, want %q", body.ScheduledSwitchover.To, "alpha-2")
	}
}

func TestGetClusterStatusMaintenanceUpdatedAtEmittedWhenSet(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	updatedAt := now.Add(-time.Minute)

	status := cluster.ClusterStatus{
		ClusterName:  "alpha",
		Phase:        cluster.ClusterPhaseMaintenance,
		CurrentEpoch: 1,
		ObservedAt:   now,
		Maintenance: cluster.MaintenanceModeStatus{
			Enabled:     true,
			Reason:      "weekly backup",
			RequestedBy: "ops",
			UpdatedAt:   updatedAt,
		},
		Members: []cluster.MemberStatus{},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if !body.Maintenance.Enabled {
		t.Error("maintenance.enabled: got false, want true")
	}

	if body.Maintenance.Reason != "weekly backup" {
		t.Errorf("maintenance.reason: got %q, want %q", body.Maintenance.Reason, "weekly backup")
	}

	if body.Maintenance.UpdatedAt == nil {
		t.Fatal("maintenance.updatedAt: got nil, want non-nil")
	}

	if !body.Maintenance.UpdatedAt.Equal(updatedAt) {
		t.Errorf("maintenance.updatedAt: got %v, want %v", body.Maintenance.UpdatedAt, updatedAt)
	}
}

func TestGetClusterStatusMaintenanceUpdatedAtOmittedWhenZero(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	status := cluster.ClusterStatus{
		ClusterName:  "alpha",
		Phase:        cluster.ClusterPhaseHealthy,
		CurrentEpoch: 1,
		ObservedAt:   now,
		Maintenance:  cluster.MaintenanceModeStatus{Enabled: false},
		Members:      []cluster.MemberStatus{},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body.Maintenance.UpdatedAt != nil {
		t.Errorf("maintenance.updatedAt: got %v, want nil", body.Maintenance.UpdatedAt)
	}
}

func TestGetClusterStatusMemberFullFields(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 8, 28, 12, 0, 0, 0, time.UTC)
	status := cluster.ClusterStatus{
		ClusterName:  "alpha",
		Phase:        cluster.ClusterPhaseHealthy,
		CurrentEpoch: 1,
		ObservedAt:   now,
		Members: []cluster.MemberStatus{
			{
				Name:        "alpha-2",
				APIURL:      "http://alpha-2:8080",
				Host:        "10.0.0.2",
				Port:        5432,
				Role:        cluster.MemberRoleReplica,
				State:       cluster.MemberStateStreaming,
				Healthy:     true,
				Timeline:    2,
				LagBytes:    1024,
				Priority:    50,
				NoFailover:  true,
				NeedsRejoin: false,
				Tags:        map[string]any{"zone": "us-east-1a"},
				LastSeenAt:  now,
			},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/cluster")
	defer response.Body.Close()

	var body clusterStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if len(body.Members) != 1 {
		t.Fatalf("members: got %d, want 1", len(body.Members))
	}

	m := body.Members[0]

	if m.APIURL != "http://alpha-2:8080" {
		t.Errorf("apiUrl: got %q", m.APIURL)
	}

	if m.Host != "10.0.0.2" {
		t.Errorf("host: got %q", m.Host)
	}

	if m.Port != 5432 {
		t.Errorf("port: got %d", m.Port)
	}

	if m.LagBytes != 1024 {
		t.Errorf("lagBytes: got %d", m.LagBytes)
	}

	if !m.NoFailover {
		t.Error("noFailover: got false, want true")
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/cluster/spec
// ---------------------------------------------------------------------------

func TestGetClusterSpecReturnsOKWithSpec(t *testing.T) {
	t.Parallel()

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  5,
		Failover: cluster.FailoverPolicy{
			Mode:            cluster.FailoverModeAutomatic,
			MaximumLagBytes: 1048576,
			RequireQuorum:   true,
			CheckTimeline:   true,
			FencingRequired: false,
		},
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Postgres: cluster.PostgresPolicy{
			SynchronousMode: cluster.SynchronousModeQuorum,
			UsePgRewind:     true,
			Parameters:      map[string]any{"max_connections": 200},
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1", Priority: 100},
			{Name: "alpha-2", Priority: 50, NoFailover: true},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: spec,
		hasSpec:     true,
	}, discardLogger(), Config{}), "/api/v1/cluster/spec")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body clusterSpecResponse
	decodeJSONResponse(t, response, &body)

	if body.ClusterName != "alpha" {
		t.Errorf("clusterName: got %q, want %q", body.ClusterName, "alpha")
	}

	if body.Generation != 5 {
		t.Errorf("generation: got %d, want 5", body.Generation)
	}

	if body.Failover.Mode != "automatic" {
		t.Errorf("failover.mode: got %q, want %q", body.Failover.Mode, "automatic")
	}

	if body.Failover.MaximumLagBytes != 1048576 {
		t.Errorf("failover.maximumLagBytes: got %d", body.Failover.MaximumLagBytes)
	}

	if !body.Failover.RequireQuorum {
		t.Error("failover.requireQuorum: got false, want true")
	}

	if !body.Failover.CheckTimeline {
		t.Error("failover.checkTimeline: got false, want true")
	}

	if !body.Switchover.AllowScheduled {
		t.Error("switchover.allowScheduled: got false, want true")
	}

	if body.Postgres.SynchronousMode != "quorum" {
		t.Errorf("postgres.synchronousMode: got %q, want %q", body.Postgres.SynchronousMode, "quorum")
	}

	if !body.Postgres.UsePgRewind {
		t.Error("postgres.usePgRewind: got false, want true")
	}

	if len(body.Members) != 2 {
		t.Fatalf("members: got %d, want 2", len(body.Members))
	}

	if body.Members[0].Name != "alpha-1" {
		t.Errorf("member[0].name: got %q", body.Members[0].Name)
	}

	if body.Members[1].NoFailover != true {
		t.Error("member[1].noFailover: got false, want true")
	}
}

func TestGetClusterSpecReturns503WhenUnavailable(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/cluster/spec")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestGetClusterSpecWithMemberTags(t *testing.T) {
	t.Parallel()

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  1,
		Members: []cluster.MemberSpec{
			{
				Name: "alpha-1",
				Tags: map[string]any{"clonefrom": true, "zone": "us-east-1a"},
			},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: spec,
		hasSpec:     true,
	}, discardLogger(), Config{}), "/api/v1/cluster/spec")
	defer response.Body.Close()
	defer response.Body.Close()

	var body clusterSpecResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if len(body.Members) != 1 {
		t.Fatalf("members: got %d, want 1", len(body.Members))
	}

	if body.Members[0].Tags == nil {
		t.Fatal("member tags: got nil, want non-nil")
	}
}

func TestGetClusterSpecMaintenance(t *testing.T) {
	t.Parallel()

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  2,
		Maintenance: cluster.MaintenanceDesiredState{
			Enabled:       true,
			DefaultReason: "weekly backup",
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: spec,
		hasSpec:     true,
	}, discardLogger(), Config{}), "/api/v1/cluster/spec")
	defer response.Body.Close()

	var body clusterSpecResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if !body.Maintenance.Enabled {
		t.Error("maintenance.enabled: got false, want true")
	}

	if body.Maintenance.DefaultReason != "weekly backup" {
		t.Errorf("maintenance.defaultReason: got %q", body.Maintenance.DefaultReason)
	}
}

func TestGetClusterSpecSwitchoverRequireSpecificCandidate(t *testing.T) {
	t.Parallel()

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  1,
		Switchover: cluster.SwitchoverPolicy{
			RequireSpecificCandidateDuringMaintenance: true,
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: spec,
		hasSpec:     true,
	}, discardLogger(), Config{}), "/api/v1/cluster/spec")
	defer response.Body.Close()

	var body clusterSpecResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if !body.Switchover.RequireSpecificCandidateDuringMaintenance {
		t.Error("switchover.requireSpecificCandidateDuringMaintenance: got false, want true")
	}
}

func TestGetClusterSpecFencingRequired(t *testing.T) {
	t.Parallel()

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Generation:  1,
		Failover: cluster.FailoverPolicy{
			FencingRequired: true,
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: spec,
		hasSpec:     true,
	}, discardLogger(), Config{}), "/api/v1/cluster/spec")
	defer response.Body.Close()

	var body clusterSpecResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if !body.Failover.FencingRequired {
		t.Error("failover.fencingRequired: got false, want true")
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/nodes/{nodeName}
// ---------------------------------------------------------------------------

func TestGetNodeStatusReturnsOKWithNode(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	checkedAt := now.Add(-2 * time.Second)
	postmasterAt := now.Add(-time.Hour)
	replayAt := now.Add(-time.Second)
	heartbeatAt := now.Add(-3 * time.Second)
	dcsSeenAt := now.Add(-4 * time.Second)

	node := agentmodel.NodeStatus{
		NodeName:       "alpha-2",
		MemberName:     "alpha-2",
		Role:           cluster.MemberRoleReplica,
		State:          cluster.MemberStateStreaming,
		PendingRestart: true,
		NeedsRejoin:    true,
		Tags:           map[string]any{"zone": "us-east-1a"},
		Postgres: agentmodel.PostgresStatus{
			Managed:       true,
			Address:       "alpha-2-postgres:5432",
			CheckedAt:     checkedAt,
			Up:            true,
			Role:          cluster.MemberRoleReplica,
			RecoveryKnown: true,
			InRecovery:    true,
			Details: agentmodel.PostgresDetails{
				ServerVersion:       170002,
				PendingRestart:      true,
				SystemIdentifier:    "7599025879359099984",
				Timeline:            7,
				PostmasterStartAt:   postmasterAt,
				ReplicationLagBytes: 128,
			},
			WAL: agentmodel.WALProgress{
				WriteLSN:        "0/7000200",
				FlushLSN:        "0/7000200",
				ReceiveLSN:      "0/7000200",
				ReplayLSN:       "0/7000100",
				ReplayTimestamp: replayAt,
			},
			Errors: agentmodel.PostgresErrors{
				Availability: "stale replica sample",
			},
		},
		ControlPlane: agentmodel.ControlPlaneStatus{
			ClusterReachable: true,
			LastHeartbeatAt:  heartbeatAt,
			LastDCSSeenAt:    dcsSeenAt,
			PublishError:     "republish scheduled",
		},
		ObservedAt: now,
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
	}, discardLogger(), Config{}), "/api/v1/nodes/alpha-2")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body nodeStatusResponse
	decodeJSONResponse(t, response, &body)

	if body.NodeName != "alpha-2" {
		t.Fatalf("nodeName: got %q, want %q", body.NodeName, "alpha-2")
	}

	if body.Role != "replica" {
		t.Errorf("role: got %q, want %q", body.Role, "replica")
	}

	if body.State != "streaming" {
		t.Errorf("state: got %q, want %q", body.State, "streaming")
	}

	if !body.PendingRestart {
		t.Error("pendingRestart: got false, want true")
	}

	if !body.NeedsRejoin {
		t.Error("needsRejoin: got false, want true")
	}

	if body.Tags["zone"] != "us-east-1a" {
		t.Errorf("tags.zone: got %#v", body.Tags["zone"])
	}

	if !body.Postgres.Managed {
		t.Error("postgres.managed: got false, want true")
	}

	if body.Postgres.Address != "alpha-2-postgres:5432" {
		t.Errorf("postgres.address: got %q", body.Postgres.Address)
	}

	if !body.Postgres.CheckedAt.Equal(checkedAt) {
		t.Errorf("postgres.checkedAt: got %v, want %v", body.Postgres.CheckedAt, checkedAt)
	}

	if body.Postgres.Role != "replica" {
		t.Errorf("postgres.role: got %q, want %q", body.Postgres.Role, "replica")
	}

	if !body.Postgres.Details.PendingRestart {
		t.Error("postgres.details.pendingRestart: got false, want true")
	}

	if body.Postgres.Details.SystemIdentifier != "7599025879359099984" {
		t.Errorf("postgres.details.systemIdentifier: got %q", body.Postgres.Details.SystemIdentifier)
	}

	if body.Postgres.Details.PostmasterStartAt == nil || !body.Postgres.Details.PostmasterStartAt.Equal(postmasterAt) {
		t.Errorf("postgres.details.postmasterStartAt: got %v, want %v", body.Postgres.Details.PostmasterStartAt, postmasterAt)
	}

	if body.Postgres.WAL.ReplayTimestamp == nil || !body.Postgres.WAL.ReplayTimestamp.Equal(replayAt) {
		t.Errorf("postgres.wal.replayTimestamp: got %v, want %v", body.Postgres.WAL.ReplayTimestamp, replayAt)
	}

	if body.Postgres.Errors.Availability != "stale replica sample" {
		t.Errorf("postgres.errors.availability: got %q", body.Postgres.Errors.Availability)
	}

	if !body.ControlPlane.ClusterReachable {
		t.Error("controlPlane.clusterReachable: got false, want true")
	}

	if body.ControlPlane.LastHeartbeatAt == nil || !body.ControlPlane.LastHeartbeatAt.Equal(heartbeatAt) {
		t.Errorf("controlPlane.lastHeartbeatAt: got %v, want %v", body.ControlPlane.LastHeartbeatAt, heartbeatAt)
	}

	if body.ControlPlane.LastDCSSeenAt == nil || !body.ControlPlane.LastDCSSeenAt.Equal(dcsSeenAt) {
		t.Errorf("controlPlane.lastDcsSeenAt: got %v, want %v", body.ControlPlane.LastDCSSeenAt, dcsSeenAt)
	}

	if body.ControlPlane.PublishError != "republish scheduled" {
		t.Errorf("controlPlane.publishError: got %q", body.ControlPlane.PublishError)
	}
}

func TestGetNodeStatusReturns404WhenMissing(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/nodes/missing")

	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusNotFound)
	}

	var body errorResponseJSON
	decodeJSONResponse(t, response, &body)

	if body.Error != "node_not_found" {
		t.Errorf("error: got %q, want %q", body.Error, "node_not_found")
	}

	if body.Message != `node "missing" was not found` {
		t.Errorf("message: got %q", body.Message)
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/members
// ---------------------------------------------------------------------------

func TestGetMembersReturnsOKWithClusterMembers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	status := cluster.ClusterStatus{
		ClusterName:  "alpha",
		Phase:        cluster.ClusterPhaseHealthy,
		CurrentEpoch: 3,
		ObservedAt:   now,
		Members: []cluster.MemberStatus{
			{
				Name:       "alpha-1",
				APIURL:     "http://alpha-1:8080",
				Host:       "10.0.0.1",
				Port:       5432,
				Role:       cluster.MemberRolePrimary,
				State:      cluster.MemberStateRunning,
				Healthy:    true,
				Leader:     true,
				Timeline:   3,
				Priority:   100,
				LastSeenAt: now,
			},
			{
				Name:        "alpha-2",
				APIURL:      "http://alpha-2:8080",
				Host:        "10.0.0.2",
				Port:        5432,
				Role:        cluster.MemberRoleReplica,
				State:       cluster.MemberStateStreaming,
				Healthy:     true,
				Timeline:    3,
				LagBytes:    256,
				Priority:    50,
				NoFailover:  true,
				NeedsRejoin: true,
				Tags:        map[string]any{"zone": "us-east-1a"},
				LastSeenAt:  now,
			},
		},
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterStatus:    status,
		hasClusterStatus: true,
	}, discardLogger(), Config{}), "/api/v1/members")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body membersResponse
	decodeJSONResponse(t, response, &body)

	if len(body.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(body.Items))
	}

	if body.Items[0].Name != "alpha-1" || body.Items[0].Role != "primary" {
		t.Errorf("items[0]: got %+v", body.Items[0])
	}

	if body.Items[1].LagBytes != 256 {
		t.Errorf("items[1].lagBytes: got %d, want 256", body.Items[1].LagBytes)
	}

	if !body.Items[1].NoFailover {
		t.Error("items[1].noFailover: got false, want true")
	}

	if !body.Items[1].NeedsRejoin {
		t.Error("items[1].needsRejoin: got false, want true")
	}

	if body.Items[1].Tags["zone"] != "us-east-1a" {
		t.Errorf("items[1].tags.zone: got %#v", body.Items[1].Tags["zone"])
	}
}

func TestGetMembersReturns503WhenClusterStatusUnavailable(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/members")

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	var body errorResponseJSON
	decodeJSONResponse(t, response, &body)

	if body.Error != "cluster_status_unavailable" {
		t.Errorf("error: got %q, want %q", body.Error, "cluster_status_unavailable")
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/history
// ---------------------------------------------------------------------------

func TestGetHistoryReturnsEntries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 2, 12, 0, 0, 0, time.UTC)
	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		history: []cluster.HistoryEntry{
			{
				OperationID: "maintenance-1",
				Kind:        cluster.OperationKindMaintenanceChange,
				Timeline:    7,
				WALLSN:      "0/7000100",
				FromMember:  "alpha-1",
				Reason:      "weekly backup",
				Result:      cluster.OperationResultSucceeded,
				FinishedAt:  now,
			},
		},
	}, discardLogger(), Config{}), "/api/v1/history")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body historyResponse
	decodeJSONResponse(t, response, &body)

	if len(body.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(body.Items))
	}

	if body.Items[0].OperationID != "maintenance-1" {
		t.Errorf("operationId: got %q", body.Items[0].OperationID)
	}

	if body.Items[0].Kind != "maintenance_change" {
		t.Errorf("kind: got %q", body.Items[0].Kind)
	}

	if body.Items[0].WALLSN != "0/7000100" {
		t.Errorf("walLsn: got %q", body.Items[0].WALLSN)
	}
}

func TestGetHistoryReturnsEmptyListWhenNoEntries(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/history")
	defer response.Body.Close()

	var body historyResponse
	decodeJSONResponse(t, response, &body)

	if len(body.Items) != 0 {
		t.Fatalf("items: got %d, want 0", len(body.Items))
	}
}

// ---------------------------------------------------------------------------
// GET/PUT /api/v1/maintenance
// ---------------------------------------------------------------------------

func TestGetMaintenanceReturnsCurrentStatus(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, time.April, 2, 12, 5, 0, 0, time.UTC)
	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		maintenance: cluster.MaintenanceModeStatus{
			Enabled:     true,
			Reason:      "weekly backup",
			RequestedBy: "ops",
			UpdatedAt:   updatedAt,
		},
	}, discardLogger(), Config{}), "/api/v1/maintenance")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body maintenanceModeStatusJSON
	decodeJSONResponse(t, response, &body)

	if !body.Enabled {
		t.Fatal("enabled: got false, want true")
	}

	if body.Reason != "weekly backup" {
		t.Errorf("reason: got %q", body.Reason)
	}

	if body.UpdatedAt == nil || !body.UpdatedAt.Equal(updatedAt) {
		t.Errorf("updatedAt: got %v, want %v", body.UpdatedAt, updatedAt)
	}
}

func TestPutMaintenanceReturnsUpdatedStatus(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, time.April, 2, 12, 10, 0, 0, time.UTC)
	updated := cluster.MaintenanceModeStatus{
		Enabled:     true,
		Reason:      "weekly backup",
		RequestedBy: "ops",
		UpdatedAt:   updatedAt,
	}

	response := performRequestBodyWithHeaders(
		t,
		New("alpha-1", testNodeStatusStore{
			maintenanceUpdate: &updated,
		}, discardLogger(), Config{}),
		http.MethodPut,
		"/api/v1/maintenance",
		[]byte(`{"enabled":true,"reason":"weekly backup","requestedBy":"ops"}`),
		map[string]string{"Content-Type": "application/json"},
	)

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body maintenanceModeStatusJSON
	decodeJSONResponse(t, response, &body)

	if !body.Enabled || body.Reason != "weekly backup" || body.RequestedBy != "ops" {
		t.Fatalf("unexpected maintenance response: %+v", body)
	}

	if body.UpdatedAt == nil || !body.UpdatedAt.Equal(updatedAt) {
		t.Errorf("updatedAt: got %v, want %v", body.UpdatedAt, updatedAt)
	}
}

func TestPutMaintenanceReturns400OnInvalidJSON(t *testing.T) {
	t.Parallel()

	response := performRequestBodyWithHeaders(
		t,
		New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}),
		http.MethodPut,
		"/api/v1/maintenance",
		[]byte(`{"enabled":`),
		map[string]string{"Content-Type": "application/json"},
	)

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusBadRequest)
	}

	var body errorResponseJSON
	decodeJSONResponse(t, response, &body)

	if body.Error != "invalid_maintenance_request" {
		t.Errorf("error: got %q", body.Error)
	}
}

func TestPutMaintenanceReturns400OnUpdateError(t *testing.T) {
	t.Parallel()

	response := performRequestBodyWithHeaders(
		t,
		New("alpha-1", testNodeStatusStore{
			maintenanceErr: errors.New("cluster spec required"),
		}, discardLogger(), Config{}),
		http.MethodPut,
		"/api/v1/maintenance",
		[]byte(`{"enabled":true}`),
		map[string]string{"Content-Type": "application/json"},
	)

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusBadRequest)
	}

	var body errorResponseJSON
	decodeJSONResponse(t, response, &body)

	if body.Message != "cluster spec required" {
		t.Errorf("message: got %q", body.Message)
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/diagnostics
// ---------------------------------------------------------------------------

func TestGetDiagnosticsReturnsSummaryWithMembersAndWarnings(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 2, 13, 0, 0, 0, time.UTC)
	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: cluster.ClusterSpec{
			ClusterName: "alpha",
			Members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2"},
			},
		},
		hasSpec: true,
		clusterStatus: cluster.ClusterStatus{
			ClusterName:    "alpha",
			Phase:          cluster.ClusterPhaseDegraded,
			CurrentPrimary: "",
			Maintenance: cluster.MaintenanceModeStatus{
				Enabled: true,
			},
			ActiveOperation: &cluster.Operation{
				ID:          "op-1",
				Kind:        cluster.OperationKindFailover,
				State:       cluster.OperationStateRunning,
				RequestedAt: now,
				Result:      cluster.OperationResultPending,
			},
			Members: []cluster.MemberStatus{
				{
					Name:       "alpha-1",
					Role:       cluster.MemberRolePrimary,
					State:      cluster.MemberStateRunning,
					Healthy:    true,
					LastSeenAt: now,
				},
				{
					Name:        "alpha-2",
					Role:        cluster.MemberRoleReplica,
					State:       cluster.MemberStateNeedsRejoin,
					Healthy:     false,
					LagBytes:    256,
					NeedsRejoin: true,
					LastSeenAt:  now,
				},
			},
			ObservedAt: now,
		},
		hasClusterStatus: true,
		nodeStatuses: []agentmodel.NodeStatus{
			{
				NodeName: "alpha-1",
				ControlPlane: agentmodel.ControlPlaneStatus{
					Leader: true,
				},
			},
		},
	}, discardLogger(), Config{}), "/api/v1/diagnostics")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body diagnosticsSummaryJSON
	decodeJSONResponse(t, response, &body)

	if body.ClusterName != "alpha" {
		t.Errorf("clusterName: got %q", body.ClusterName)
	}

	if body.ControlPlaneLeader != "alpha-1" {
		t.Errorf("controlPlaneLeader: got %q", body.ControlPlaneLeader)
	}

	if body.QuorumReachable == nil || *body.QuorumReachable {
		t.Fatalf("quorumReachable: got %v, want false", body.QuorumReachable)
	}

	if len(body.Members) != 2 {
		t.Fatalf("members: got %d, want 2", len(body.Members))
	}

	for _, want := range []string{
		"cluster phase is degraded",
		"maintenance mode is enabled",
		"active operation failover is running",
		"no writable primary observed",
		"member alpha-2 is unhealthy",
		"member alpha-2 requires rejoin",
		"quorum is not reachable",
	} {
		if !containsString(body.Warnings, want) {
			t.Fatalf("expected warning %q in %+v", want, body.Warnings)
		}
	}
}

func TestGetDiagnosticsSupportsIncludeMembersFalse(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 2, 13, 30, 0, 0, time.UTC)
	response := performRequestMethod(t, New("alpha-1", testNodeStatusStore{
		clusterSpec: cluster.ClusterSpec{ClusterName: "alpha"},
		hasSpec:     true,
		clusterStatus: cluster.ClusterStatus{
			ClusterName: "alpha",
			Phase:       cluster.ClusterPhaseHealthy,
			Members: []cluster.MemberStatus{
				{
					Name:       "alpha-1",
					Role:       cluster.MemberRolePrimary,
					State:      cluster.MemberStateRunning,
					Healthy:    true,
					LastSeenAt: now,
				},
			},
			ObservedAt: now,
		},
		hasClusterStatus: true,
	}, discardLogger(), Config{}), http.MethodGet, "/api/v1/diagnostics?includeMembers=false")

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var body diagnosticsSummaryJSON
	decodeJSONResponse(t, response, &body)

	if len(body.Members) != 0 {
		t.Fatalf("members: got %d, want 0", len(body.Members))
	}
}

func TestGetDiagnosticsReturns503WhenUnavailable(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/api/v1/diagnostics")

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	var body errorResponseJSON
	decodeJSONResponse(t, response, &body)

	if body.Error != "diagnostics_unavailable" {
		t.Errorf("error: got %q", body.Error)
	}
}

// ---------------------------------------------------------------------------
// buildNodeStatus — full field coverage
// ---------------------------------------------------------------------------

func TestBuildNodeStatusEmitsPostmasterStartTime(t *testing.T) {
	t.Parallel()

	postmasterAt := time.Date(2024, 8, 28, 19, 39, 26, 0, time.UTC)
	node := primaryNodeStatus("alpha-1", time.Now().UTC())
	node.Postgres.Up = true
	node.Postgres.Details.PostmasterStartAt = postmasterAt

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
	}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.PostmasterStartTime == nil {
		t.Fatal("expected PostmasterStartTime to be set")
	}

	if !body.PostmasterStartTime.Equal(postmasterAt) {
		t.Fatalf("unexpected postmaster time: got %v, want %v", *body.PostmasterStartTime, postmasterAt)
	}
}

func TestBuildNodeStatusEmitsDCSLastSeen(t *testing.T) {
	t.Parallel()

	dcsAt := time.Date(2024, 8, 28, 20, 0, 0, 0, time.UTC)
	node := primaryNodeStatus("alpha-1", time.Now().UTC())
	node.Postgres.Up = true
	node.ControlPlane.LastDCSSeenAt = dcsAt

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
	}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.DCSLastSeen != dcsAt.Unix() {
		t.Fatalf("unexpected dcs_last_seen: got %d, want %d", body.DCSLastSeen, dcsAt.Unix())
	}
}

func TestBuildNodeStatusEmitsPauseWhenMaintenanceEnabled(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus:  primaryNodeStatus("alpha-1", time.Now().UTC()),
		hasNode:     true,
		maintenance: cluster.MaintenanceModeStatus{Enabled: true},
	}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if !body.Pause {
		t.Fatal("expected pause=true when maintenance enabled")
	}
}

func TestBuildNodeStatusWithNoClusterSpec(t *testing.T) {
	t.Parallel()

	node := primaryNodeStatus("alpha-1", time.Now().UTC())
	node.Postgres.Up = true

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
		// hasSpec deliberately false
	}, discardLogger(), Config{}), "/primary")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.Patroni.Scope != "" {
		t.Fatalf("expected empty scope when no cluster spec, got %q", body.Patroni.Scope)
	}
}

func TestBuildNodeStatusWhenNodeAbsentReturnsStoppedUnknown(t *testing.T) {
	t.Parallel()

	response := performRequest(t, New("alpha-1", testNodeStatusStore{}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.State != "stopped" {
		t.Fatalf("expected state=stopped for absent node, got %q", body.State)
	}

	if body.Role != "unknown" {
		t.Fatalf("expected role=unknown for absent node, got %q", body.Role)
	}
}

func TestBuildNodeStatusEmitsXLogFields(t *testing.T) {
	t.Parallel()

	replayAt := time.Date(2024, 8, 28, 19, 40, 0, 0, time.UTC)
	node := replicaNodeStatus("alpha-1", time.Now().UTC(), 0)
	node.Postgres.Up = true
	node.Postgres.WAL = agentmodel.WALProgress{
		WriteLSN:        "0/1000000",
		ReceiveLSN:      "0/2000000",
		ReplayLSN:       "0/3000000",
		ReplayTimestamp: replayAt,
	}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
	}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.XLog == nil {
		t.Fatal("expected non-nil xlog")
	}

	if body.XLog.Location != parseLSN("0/1000000") {
		t.Fatalf("unexpected xlog.location: got %d", body.XLog.Location)
	}

	if body.XLog.ReceivedLocation != parseLSN("0/2000000") {
		t.Fatalf("unexpected xlog.received_location: got %d", body.XLog.ReceivedLocation)
	}

	if body.XLog.ReplayedLocation != parseLSN("0/3000000") {
		t.Fatalf("unexpected xlog.replayed_location: got %d", body.XLog.ReplayedLocation)
	}

	if body.XLog.ReplayedTimestamp == nil || !body.XLog.ReplayedTimestamp.Equal(replayAt) {
		t.Fatalf("unexpected xlog.replayed_timestamp: got %v, want %v", body.XLog.ReplayedTimestamp, replayAt)
	}
}

func TestBuildNodeStatusXLogNilWhenWALEmpty(t *testing.T) {
	t.Parallel()

	node := replicaNodeStatus("alpha-1", time.Now().UTC(), 0)
	node.Postgres.WAL = agentmodel.WALProgress{}

	response := performRequest(t, New("alpha-1", testNodeStatusStore{
		nodeStatus: node,
		hasNode:    true,
	}, discardLogger(), Config{}), "/health")
	defer response.Body.Close()

	var body patroniNodeStatus
	decodeJSONResponse(t, response, &body)

	if body.XLog != nil {
		t.Fatalf("expected nil xlog for empty WAL, got %+v", body.XLog)
	}
}

// ---------------------------------------------------------------------------
// postgresState helper
// ---------------------------------------------------------------------------

func TestPostgresStateMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		node agentmodel.NodeStatus
		want string
	}{
		{agentmodel.NodeStatus{Postgres: agentmodel.PostgresStatus{Up: true}}, "running"},
		{agentmodel.NodeStatus{State: cluster.MemberStateStarting}, "starting"},
		{agentmodel.NodeStatus{State: cluster.MemberStateStopping}, "stopping"},
		{agentmodel.NodeStatus{State: cluster.MemberStateFailed}, "stopped"},
		{agentmodel.NodeStatus{State: cluster.MemberStateUnknown}, "stopped"},
		{agentmodel.NodeStatus{State: cluster.MemberStateUnreachable}, "stopped"},
	}

	for _, c := range cases {
		got := postgresState(c.node)
		if got != c.want {
			t.Errorf("postgresState(%q up=%v): got %q, want %q",
				c.node.State, c.node.Postgres.Up, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// patroniRole helper
// ---------------------------------------------------------------------------

func TestPatroniRoleMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		role cluster.MemberRole
		want string
	}{
		{cluster.MemberRolePrimary, "primary"},
		{cluster.MemberRoleReplica, "replica"},
		{cluster.MemberRoleStandbyLeader, "standby_leader"},
		{cluster.MemberRoleWitness, "unknown"},
		{cluster.MemberRoleUnknown, "unknown"},
		{"", "unknown"},
	}

	for _, c := range cases {
		got := patroniRole(c.role)
		if got != c.want {
			t.Errorf("patroniRole(%q): got %q, want %q", c.role, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// parseLSN helper
// ---------------------------------------------------------------------------

func TestParseLSN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input string
		want  int64
	}{
		{"", 0},
		{"noslash", 0},
		{"G/16B6BB0", 0},
		{"0/ZZZ", 0},
		{"0/0", 0},
		{"0/1", 1},
		{"0/16B6BB0", 0x16B6BB0},
		{"1/00000000", 1 << 32},
		{"0/FFFFFFFF", 0xFFFFFFFF},
	}

	for _, c := range cases {
		got := parseLSN(c.input)
		if got != c.want {
			t.Errorf("parseLSN(%q): got %d (0x%X), want %d (0x%X)", c.input, got, got, c.want, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// parseLagBytes helper
// ---------------------------------------------------------------------------

func TestParseLagBytes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input string
		want  int64
	}{
		{"", 0},
		{"0", 0},
		{"1024", 1024},
		{"1B", 1},
		{"1kB", 1024},
		{"1MB", 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"1TB", 1 << 40},
		{"32MB", 32 * 1024 * 1024},
		{"notanumberMB", 0},
		{"-5MB", 0},
		{"notanumber", 0},
		{"-100", 0},
		{"  16MB  ", 16 * 1024 * 1024},
	}

	for _, c := range cases {
		got := parseLagBytes(c.input)
		if got != c.want {
			t.Errorf("parseLagBytes(%q): got %d, want %d", c.input, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// tagFiltersMatch and helpers
// ---------------------------------------------------------------------------

func TestTagFiltersMatch(t *testing.T) {
	t.Parallel()

	tags := map[string]any{
		"clonefrom": true,
		"env":       "prod",
		"count":     42,
	}

	cases := []struct {
		filters map[string]string
		want    bool
		label   string
	}{
		{nil, true, "nil filters match"},
		{map[string]string{}, true, "empty filters match"},
		{map[string]string{"clonefrom": "true"}, true, "bool true match"},
		{map[string]string{"clonefrom": "false"}, false, "bool true ≠ false"},
		{map[string]string{"env": "prod"}, true, "string exact match"},
		{map[string]string{"env": "PROD"}, true, "string case-insensitive match"},
		{map[string]string{"env": "staging"}, false, "string mismatch"},
		{map[string]string{"count": "42"}, true, "int match via fmt"},
		{map[string]string{"missing": "value"}, false, "absent key treated as empty"},
		{map[string]string{"missing": ""}, true, "absent key matches empty string"},
	}

	for _, c := range cases {
		got := tagFiltersMatch(tags, c.filters)
		if got != c.want {
			t.Errorf("tagFiltersMatch %s: got %v, want %v", c.label, got, c.want)
		}
	}
}

func TestTagValueString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		v    any
		want string
	}{
		{nil, ""},
		{true, "true"},
		{false, "false"},
		{"hello", "hello"},
		{"UPPER", "upper"},
		{42, "42"},
		{3.14, "3.14"},
	}

	for _, c := range cases {
		got := tagValueString(c.v)
		if got != c.want {
			t.Errorf("tagValueString(%v): got %q, want %q", c.v, got, c.want)
		}
	}
}

func TestReplicaTagFiltersExcludesReservedParams(t *testing.T) {
	t.Parallel()

	queries := map[string]string{
		"lag":               "16MB",
		"replication_state": "streaming",
		"mode":              "apply",
		"clonefrom":         "true",
		"env":               "prod",
	}

	got := replicaTagFilters(queries)

	if _, ok := got["lag"]; ok {
		t.Error("lag must not appear in tag filters")
	}

	if _, ok := got["replication_state"]; ok {
		t.Error("replication_state must not appear in tag filters")
	}

	if _, ok := got["mode"]; ok {
		t.Error("mode must not appear in tag filters")
	}

	if got["clonefrom"] != "true" {
		t.Errorf("expected clonefrom=true in tag filters, got %q", got["clonefrom"])
	}

	if got["env"] != "prod" {
		t.Errorf("expected env=prod in tag filters, got %q", got["env"])
	}
}

func TestReplicaLagOK(t *testing.T) {
	t.Parallel()

	cases := []struct {
		lag   int64
		param string
		want  bool
	}{
		{0, "", true},
		{1024, "", true},
		{1024, "2kB", true},
		{1025, "1kB", false},
		{0, "0", true},
	}

	for _, c := range cases {
		got := replicaLagOK(c.lag, c.param)
		if got != c.want {
			t.Errorf("replicaLagOK(%d, %q): got %v, want %v", c.lag, c.param, got, c.want)
		}
	}
}

func TestReplicaStateOK(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state string
		want  bool
	}{
		{"", true},
		{"streaming", true},
		{"catchup", false},
		{"initializing", false},
		{"Streaming", false}, // case-sensitive match
	}

	for _, c := range cases {
		got := replicaStateOK(c.state)
		if got != c.want {
			t.Errorf("replicaStateOK(%q): got %v, want %v", c.state, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type testNodeStatusStore struct {
	nodeStatus        agentmodel.NodeStatus
	nodeStatuses      []agentmodel.NodeStatus
	hasNode           bool
	clusterSpec       cluster.ClusterSpec
	hasSpec           bool
	clusterStatus     cluster.ClusterStatus
	hasClusterStatus  bool
	maintenance       cluster.MaintenanceModeStatus
	maintenanceUpdate *cluster.MaintenanceModeStatus
	maintenanceErr    error
	history           []cluster.HistoryEntry
}

func (store testNodeStatusStore) NodeStatus(nodeName string) (agentmodel.NodeStatus, bool) {
	if len(store.nodeStatuses) > 0 {
		for _, nodeStatus := range store.nodeStatuses {
			if nodeStatus.NodeName == nodeName {
				return nodeStatus.Clone(), true
			}
		}

		return agentmodel.NodeStatus{}, false
	}

	if !store.hasNode || nodeName != store.nodeStatus.NodeName {
		return agentmodel.NodeStatus{}, false
	}

	return store.nodeStatus.Clone(), true
}

func (store testNodeStatusStore) NodeStatuses() []agentmodel.NodeStatus {
	if len(store.nodeStatuses) > 0 {
		items := make([]agentmodel.NodeStatus, len(store.nodeStatuses))
		for i, nodeStatus := range store.nodeStatuses {
			items[i] = nodeStatus.Clone()
		}
		return items
	}

	if store.hasNode {
		return []agentmodel.NodeStatus{store.nodeStatus.Clone()}
	}

	return nil
}

func (store testNodeStatusStore) ClusterSpec() (cluster.ClusterSpec, bool) {
	if !store.hasSpec {
		return cluster.ClusterSpec{}, false
	}

	return store.clusterSpec.Clone(), true
}

func (store testNodeStatusStore) ClusterStatus() (cluster.ClusterStatus, bool) {
	if !store.hasClusterStatus {
		return cluster.ClusterStatus{}, false
	}

	return store.clusterStatus, true
}

func (store testNodeStatusStore) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return store.maintenance
}

func (store testNodeStatusStore) UpdateMaintenanceMode(ctx context.Context, request cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	if err := ctx.Err(); err != nil {
		return cluster.MaintenanceModeStatus{}, err
	}

	if store.maintenanceErr != nil {
		return cluster.MaintenanceModeStatus{}, store.maintenanceErr
	}

	if store.maintenanceUpdate != nil {
		return *store.maintenanceUpdate, nil
	}

	return cluster.MaintenanceModeStatus{
		Enabled:     request.Enabled,
		Reason:      request.Reason,
		RequestedBy: request.RequestedBy,
	}, nil
}

func (store testNodeStatusStore) History() []cluster.HistoryEntry {
	if store.history == nil {
		return nil
	}

	items := make([]cluster.HistoryEntry, len(store.history))
	copy(items, store.history)
	return items
}

type testAuthorizer struct {
	principal *Principal
	err       error
	scopes    []AccessScope
}

func (auth *testAuthorizer) Authorize(_ *fiber.Ctx, scope AccessScope) (*Principal, error) {
	auth.scopes = append(auth.scopes, scope)
	return auth.principal, auth.err
}

func performRequest(t *testing.T, srv *Server, path string) *http.Response {
	t.Helper()
	return performRequestMethod(t, srv, http.MethodGet, path)
}

func performRequestMethod(t *testing.T, srv *Server, method, path string) *http.Response {
	t.Helper()

	return performRequestWithHeaders(t, srv, method, path, nil)
}

func performRequestBodyWithHeaders(t *testing.T, srv *Server, method, path string, body []byte, headers map[string]string) *http.Response {
	t.Helper()

	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	response, err := srv.app.Test(request, int(time.Second.Milliseconds()))
	if err != nil {
		t.Fatalf("perform %s %q: %v", method, path, err)
	}

	return response
}

func performRequestWithHeaders(t *testing.T, srv *Server, method, path string, headers map[string]string) *http.Response {
	t.Helper()

	request := httptest.NewRequest(method, path, nil)
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	response, err := srv.app.Test(request, int(time.Second.Milliseconds()))
	if err != nil {
		t.Fatalf("perform %s %q: %v", method, path, err)
	}

	return response
}

func decodeJSONResponse(t *testing.T, response *http.Response, target any) {
	t.Helper()
	defer response.Body.Close()

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}

	return address
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}

	return false
}

func containsWord(s, word string) bool {
	for _, part := range splitWords(s) {
		if part == word {
			return true
		}
	}
	return false
}

func splitWords(s string) []string {
	var words []string
	for _, part := range splitByCommaAndSpace(s) {
		if part != "" {
			words = append(words, part)
		}
	}
	return words
}

func splitByCommaAndSpace(s string) []string {
	result := []string{}
	current := ""
	for _, r := range s {
		if r == ',' || r == ' ' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(r)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func primaryNodeStatus(nodeName string, now time.Time) agentmodel.NodeStatus {
	return agentmodel.NodeStatus{
		NodeName:   nodeName,
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		ObservedAt: now,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Role:    cluster.MemberRolePrimary,
			Details: agentmodel.PostgresDetails{
				ServerVersion: 170002,
				Timeline:      1,
			},
		},
	}
}

func replicaNodeStatus(nodeName string, now time.Time, lagBytes int64) agentmodel.NodeStatus {
	return agentmodel.NodeStatus{
		NodeName:   nodeName,
		Role:       cluster.MemberRoleReplica,
		State:      cluster.MemberStateStreaming,
		ObservedAt: now,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Role:    cluster.MemberRoleReplica,
			Details: agentmodel.PostgresDetails{
				Timeline:            1,
				ReplicationLagBytes: lagBytes,
			},
		},
	}
}
