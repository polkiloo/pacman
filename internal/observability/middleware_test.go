package observability

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

func TestPrometheusExporterMiddlewareServesMetricsOnGetAndHead(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(fiber.Handler(PrometheusExporterMiddleware(testStateReader{})))

	getResponse, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/metrics", nil))
	if err != nil {
		t.Fatalf("perform GET /metrics: %v", err)
	}

	if getResponse.StatusCode != fiber.StatusOK {
		t.Fatalf("GET /metrics status: got %d, want %d", getResponse.StatusCode, fiber.StatusOK)
	}

	if contentType := getResponse.Header.Get(fiber.HeaderContentType); !strings.HasPrefix(contentType, "text/plain;") {
		t.Fatalf("GET /metrics content type: got %q", contentType)
	}

	getBody, err := io.ReadAll(getResponse.Body)
	if err != nil {
		t.Fatalf("read GET /metrics body: %v", err)
	}

	if !strings.Contains(string(getBody), "pacman_cluster_maintenance_mode") {
		t.Fatalf("expected metrics output, got %q", string(getBody))
	}

	headResponse, err := app.Test(httptest.NewRequest(fiber.MethodHead, "/metrics", nil))
	if err != nil {
		t.Fatalf("perform HEAD /metrics: %v", err)
	}

	if headResponse.StatusCode != fiber.StatusOK {
		t.Fatalf("HEAD /metrics status: got %d, want %d", headResponse.StatusCode, fiber.StatusOK)
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

	postResponse, err := app.Test(httptest.NewRequest(fiber.MethodPost, "/metrics", nil))
	if err != nil {
		t.Fatalf("perform POST /metrics: %v", err)
	}

	if postResponse.StatusCode != fiber.StatusCreated {
		t.Fatalf("POST /metrics status: got %d, want %d", postResponse.StatusCode, fiber.StatusCreated)
	}

	postBody, err := io.ReadAll(postResponse.Body)
	if err != nil {
		t.Fatalf("read POST /metrics body: %v", err)
	}

	if string(postBody) != "delegated metrics" {
		t.Fatalf("POST /metrics body: got %q", string(postBody))
	}

	healthResponse, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/health", nil))
	if err != nil {
		t.Fatalf("perform GET /health: %v", err)
	}

	if healthResponse.StatusCode != fiber.StatusAccepted {
		t.Fatalf("GET /health status: got %d, want %d", healthResponse.StatusCode, fiber.StatusAccepted)
	}

	healthBody, err := io.ReadAll(healthResponse.Body)
	if err != nil {
		t.Fatalf("read GET /health body: %v", err)
	}

	if string(healthBody) != "delegated health" {
		t.Fatalf("GET /health body: got %q", string(healthBody))
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
