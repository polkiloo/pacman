package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

type httpResponseSnapshot struct {
	statusCode int
	headers    http.Header
	body       []byte
}

func TestPrometheusExporterMiddlewareServesMetricsOnGetAndHead(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(fiber.Handler(PrometheusExporterMiddleware(testStateReader{})))

	getResponse := performFiberTestRequest(t, app, fiber.MethodGet, "/metrics")
	if getResponse.statusCode != fiber.StatusOK {
		t.Fatalf("GET /metrics status: got %d, want %d", getResponse.statusCode, fiber.StatusOK)
	}

	if contentType := getResponse.headers.Get(fiber.HeaderContentType); !strings.HasPrefix(contentType, "text/plain;") {
		t.Fatalf("GET /metrics content type: got %q", contentType)
	}

	if !strings.Contains(string(getResponse.body), "pacman_cluster_maintenance_mode") {
		t.Fatalf("expected metrics output, got %q", string(getResponse.body))
	}

	headResponse := performFiberTestRequest(t, app, fiber.MethodHead, "/metrics")
	if headResponse.statusCode != fiber.StatusOK {
		t.Fatalf("HEAD /metrics status: got %d, want %d", headResponse.statusCode, fiber.StatusOK)
	}
}

func TestPrometheusExporterMiddlewareDelegatesRequestsItDoesNotHandle(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(fiber.Handler(PrometheusExporterMiddleware(testStateReader{})))
	app.All("/metrics", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusCreated).SendString("delegated metrics")
	})
	app.All("/health", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusAccepted).SendString("delegated health")
	})

	postResponse := performFiberTestRequest(t, app, fiber.MethodPost, "/metrics")
	if postResponse.statusCode != fiber.StatusCreated {
		t.Fatalf("POST /metrics status: got %d, want %d", postResponse.statusCode, fiber.StatusCreated)
	}

	if string(postResponse.body) != "delegated metrics" {
		t.Fatalf("POST /metrics body: got %q", string(postResponse.body))
	}

	healthResponse := performFiberTestRequest(t, app, fiber.MethodGet, "/health")
	if healthResponse.statusCode != fiber.StatusAccepted {
		t.Fatalf("GET /health status: got %d, want %d", healthResponse.statusCode, fiber.StatusAccepted)
	}

	if string(healthResponse.body) != "delegated health" {
		t.Fatalf("GET /health body: got %q", string(healthResponse.body))
	}
}

func performFiberTestRequest(t *testing.T, app *fiber.App, method, path string) httpResponseSnapshot {
	t.Helper()

	response, err := app.Test(httptest.NewRequest(method, path, nil))
	if err != nil {
		t.Fatalf("perform %s %s: %v", method, path, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s body: %v", method, path, err)
	}

	return httpResponseSnapshot{
		statusCode: response.StatusCode,
		headers:    response.Header.Clone(),
		body:       body,
	}
}

func (reader testStateReader) NodeStatus(nodeName string) (agentmodel.NodeStatus, bool) {
	for _, status := range reader.nodeStatuses {
		if status.NodeName == nodeName {
			return status.Clone(), true
		}
	}

	return agentmodel.NodeStatus{}, false
}

func (reader testStateReader) UpdateMaintenanceMode(context.Context, cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	return reader.maintenance, nil
}

func (reader testStateReader) History() []cluster.HistoryEntry {
	return nil
}

func (reader testStateReader) CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error) {
	return controlplane.SwitchoverIntent{}, nil
}

func (reader testStateReader) CancelSwitchover(context.Context) (cluster.Operation, error) {
	return cluster.Operation{}, nil
}

func (reader testStateReader) CreateFailoverIntent(context.Context, controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error) {
	return controlplane.FailoverIntent{}, nil
}
