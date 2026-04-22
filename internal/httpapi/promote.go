package httpapi

import (
	"context"
	"log/slog"

	"github.com/gofiber/fiber/v2"
)

// LocalPromoter promotes the local PostgreSQL instance to primary via pg_ctl.
type LocalPromoter interface {
	PromoteLocal(context.Context) error
}

func (srv *Server) handlePromote(c *fiber.Ctx) error {
	if srv.localPromoter == nil {
		return writeAPIError(c, fiber.StatusServiceUnavailable, "promote_unavailable", "local promotion is not configured on this node")
	}

	if err := srv.localPromoter.PromoteLocal(c.UserContext()); err != nil {
		srv.logRequest(c, slog.LevelError, "local postgres promotion failed", slog.String("error", err.Error()))
		return writeAPIError(c, fiber.StatusInternalServerError, "promote_failed", "failed to promote local postgres")
	}

	srv.logRequest(c, slog.LevelInfo, "promoted local postgres to primary")
	return c.JSON(map[string]string{"message": "promoted local postgres to primary"})
}
