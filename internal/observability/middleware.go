package observability

import (
	"net/http"

	"github.com/gofiber/fiber/v2"

	"github.com/polkiloo/pacman/internal/httpapi"
)

// PrometheusExporterMiddleware serves Prometheus text exposition on
// GET/HEAD /metrics and delegates every other request to the rest of the HTTP
// API stack.
func PrometheusExporterMiddleware(state httpapi.NodeStatusReader) httpapi.Middleware {
	gatherer := NewPrometheusGatherer(state)

	return func(c *fiber.Ctx) error {
		if c.Path() != "/metrics" {
			return c.Next()
		}

		switch c.Method() {
		case fiber.MethodGet:
		case fiber.MethodHead:
		default:
			return c.Next()
		}

		body, contentType, err := EncodePrometheusText(gatherer)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(http.StatusText(fiber.StatusInternalServerError))
		}

		c.Set(fiber.HeaderContentType, contentType)
		if c.Method() == fiber.MethodHead {
			return c.SendStatus(fiber.StatusOK)
		}

		return c.Send(body)
	}
}
