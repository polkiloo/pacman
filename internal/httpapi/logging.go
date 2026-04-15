package httpapi

import (
	"context"
	"log/slog"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/polkiloo/pacman/internal/cluster"
	paclog "github.com/polkiloo/pacman/internal/logging"
)

func (srv *Server) requestContext(c *fiber.Ctx) context.Context {
	ctx := c.UserContext()
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func (srv *Server) requestLogAttrs(c *fiber.Ctx) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("component", "httpapi"),
		slog.String("method", c.Method()),
		slog.String("path", c.Path()),
		slog.String("route", currentRoutePattern(c)),
	}
	attrs = append(attrs, paclog.AttrsFromContext(srv.requestContext(c))...)

	return attrs
}

func (srv *Server) logRequest(c *fiber.Ctx, level slog.Level, msg string, attrs ...slog.Attr) {
	if srv.logger == nil {
		return
	}

	base := srv.requestLogAttrs(c)
	base = append(base, attrs...)
	srv.logger.LogAttrs(srv.requestContext(c), level, msg, base...)
}

func auditLogAttrs(action string) []slog.Attr {
	return []slog.Attr{
		slog.String("event_category", "audit"),
		slog.String("audit_action", action),
	}
}

func operationLogAttrs(operation cluster.Operation) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("operation_id", operation.ID),
		slog.String("operation_kind", string(operation.Kind)),
		slog.String("operation_state", string(operation.State)),
	}
	if !operation.Result.IsZero() {
		attrs = append(attrs, slog.String("operation_result", string(operation.Result)))
	}
	if fromMember := strings.TrimSpace(operation.FromMember); fromMember != "" {
		attrs = append(attrs, slog.String("from_member", fromMember))
	}
	if toMember := strings.TrimSpace(operation.ToMember); toMember != "" {
		attrs = append(attrs, slog.String("to_member", toMember), slog.String("member", toMember))
	} else if fromMember := strings.TrimSpace(operation.FromMember); fromMember != "" {
		attrs = append(attrs, slog.String("member", fromMember))
	}

	return attrs
}
