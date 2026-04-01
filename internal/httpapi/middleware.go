package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

const (
	headerRequestID   = "X-Request-ID"
	requestIDLocalKey = "request_id"
)

func (srv *Server) requestIDMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		requestID := strings.TrimSpace(c.Get(headerRequestID))
		if requestID == "" {
			requestID = srv.nextRequestID()
		}

		c.Locals(requestIDLocalKey, requestID)
		c.Set(headerRequestID, requestID)

		return c.Next()
	}
}

func (srv *Server) accessLogMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		startedAt := time.Now().UTC()
		err := c.Next()
		duration := time.Since(startedAt)

		if srv.logger == nil {
			return err
		}

		status := c.Response().StatusCode()
		if err != nil {
			var fiberError *fiber.Error
			if errors.As(err, &fiberError) {
				status = fiberError.Code
			} else if status < fiber.StatusBadRequest {
				status = fiber.StatusInternalServerError
			}
		}

		level := slog.LevelInfo
		switch {
		case status >= fiber.StatusInternalServerError:
			level = slog.LevelError
		case status >= fiber.StatusBadRequest:
			level = slog.LevelWarn
		}

		requestID, _ := c.Locals(requestIDLocalKey).(string)
		srv.logger.LogAttrs(
			context.Background(),
			level,
			"handled http request",
			slog.String("component", "httpapi"),
			slog.String("request_id", requestID),
			slog.String("method", c.Method()),
			slog.String("path", c.Path()),
			slog.String("remote_addr", c.IP()),
			slog.Int("status", status),
			slog.Duration("duration", duration),
		)

		return err
	}
}

func (srv *Server) nextRequestID() string {
	sequence := srv.requestSeq.Add(1)
	return fmt.Sprintf("%s-%d-%d", srv.nodeName, time.Now().UTC().UnixNano(), sequence)
}
