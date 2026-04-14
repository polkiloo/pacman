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
	headerRequestID          = "X-Request-ID"
	headerXContentTypeOption = "X-Content-Type-Options"
	requestIDLocalKey        = "request_id"
	principalLocalKey        = "auth_principal"
)

// AccessScope declares the authorization level required by an API route.
type AccessScope string

const (
	AccessScopeClusterRead  AccessScope = "cluster.read"
	AccessScopeClusterWrite AccessScope = "cluster.write"
)

// Principal captures the authenticated caller identity accepted by the API.
type Principal struct {
	Subject   string
	Mechanism string
}

// Authorizer optionally authenticates and authorizes control-plane API
// requests.
type Authorizer interface {
	Authorize(*fiber.Ctx, AccessScope) (*Principal, error)
}

// AuthorizerFunc adapts a function to the Authorizer interface.
type AuthorizerFunc func(*fiber.Ctx, AccessScope) (*Principal, error)

func (fn AuthorizerFunc) Authorize(c *fiber.Ctx, scope AccessScope) (*Principal, error) {
	return fn(c, scope)
}

// AuthorizationError signals an authentication or authorization denial.
type AuthorizationError struct {
	StatusCode int
	Message    string
}

func (err *AuthorizationError) Error() string {
	return err.Message
}

// Unauthorized returns a standardized authentication failure for API routes.
func Unauthorized(message string) error {
	return &AuthorizationError{
		StatusCode: fiber.StatusUnauthorized,
		Message:    defaultAuthorizationMessage(fiber.StatusUnauthorized, message),
	}
}

// Forbidden returns a standardized authorization failure for API routes.
func Forbidden(message string) error {
	return &AuthorizationError{
		StatusCode: fiber.StatusForbidden,
		Message:    defaultAuthorizationMessage(fiber.StatusForbidden, message),
	}
}

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

		attributes := []slog.Attr{
			slog.String("component", "httpapi"),
			slog.String("node", srv.nodeName),
			slog.String("request_id", RequestID(c)),
			slog.String("method", c.Method()),
			slog.String("path", c.Path()),
			slog.String("route", currentRoutePattern(c)),
			slog.String("remote_addr", c.IP()),
			slog.String("user_agent", c.Get(fiber.HeaderUserAgent)),
			slog.Int("status", status),
			slog.Duration("duration", duration),
			slog.Int("response_bytes", len(c.Response().Body())),
		}
		if principal, ok := CurrentPrincipal(c); ok {
			attributes = append(
				attributes,
				slog.String("principal_subject", principal.Subject),
				slog.String("principal_mechanism", principal.Mechanism),
			)
		}
		if err != nil {
			attributes = append(attributes, slog.String("error", err.Error()))
		}

		srv.logger.LogAttrs(context.Background(), level, "handled http request", attributes...)

		return err
	}
}

func currentRoutePattern(c *fiber.Ctx) string {
	route := c.Route()
	if route == nil {
		return c.Path()
	}

	trimmed := strings.TrimSpace(route.Path)
	if trimmed == "" {
		return c.Path()
	}

	return trimmed
}

func (srv *Server) apiCommonMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		applyAPIResponseHeaders(c)
		return c.Next()
	}
}

func (srv *Server) authMiddleware(scope AccessScope) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if srv.authorizer == nil {
			return c.Next()
		}

		principal, err := srv.authorizer.Authorize(c, scope)
		if err != nil {
			return srv.writeAuthorizationError(c, scope, err)
		}

		if principal != nil {
			c.Locals(principalLocalKey, principal)
		}

		return c.Next()
	}
}

func (srv *Server) requireJSONContentTypeMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		if len(c.Body()) == 0 {
			return c.Next()
		}

		contentType := strings.ToLower(strings.TrimSpace(c.Get(fiber.HeaderContentType)))
		if !strings.HasPrefix(contentType, fiber.MIMEApplicationJSON) {
			return writeAPIError(c, fiber.StatusUnsupportedMediaType, "unsupported_media_type", "request content type must be application/json")
		}

		return c.Next()
	}
}

func (srv *Server) apiNotFoundMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		applyAPIResponseHeaders(c)
		return writeAPIError(c, fiber.StatusNotFound, "not_found", fmt.Sprintf("path %q was not found", c.Path()))
	}
}

func CurrentPrincipal(c *fiber.Ctx) (*Principal, bool) {
	principal, ok := c.Locals(principalLocalKey).(*Principal)
	if !ok || principal == nil {
		return nil, false
	}

	return principal, true
}

func RequestID(c *fiber.Ctx) string {
	requestID, _ := c.Locals(requestIDLocalKey).(string)
	return requestID
}

func (srv *Server) nextRequestID() string {
	sequence := srv.requestSeq.Add(1)
	return fmt.Sprintf("%s-%d-%d", srv.nodeName, time.Now().UTC().UnixNano(), sequence)
}

func (srv *Server) writeAuthorizationError(c *fiber.Ctx, scope AccessScope, err error) error {
	var authErr *AuthorizationError
	if errors.As(err, &authErr) {
		return writeAPIError(c, authErr.StatusCode, authorizationErrorCode(authErr.StatusCode), authErr.Message)
	}

	if srv.logger != nil {
		srv.logger.Error(
			"authorization hook failed",
			slog.String("component", "httpapi"),
			slog.String("request_id", RequestID(c)),
			slog.String("method", c.Method()),
			slog.String("path", c.Path()),
			slog.String("scope", string(scope)),
			slog.String("error", err.Error()),
		)
	}

	return writeAPIError(c, fiber.StatusInternalServerError, "authorization_error", "authorization hook failed")
}

func applyAPIResponseHeaders(c *fiber.Ctx) {
	c.Set(fiber.HeaderCacheControl, "no-store")
	c.Set(fiber.HeaderPragma, "no-cache")
	c.Set(headerXContentTypeOption, "nosniff")
}

func writeAPIError(c *fiber.Ctx, status int, code, message string) error {
	if status == fiber.StatusUnauthorized {
		c.Set(fiber.HeaderWWWAuthenticate, "Bearer")
	}

	return c.Status(status).JSON(errorResponseJSON{
		Error:   code,
		Message: message,
	})
}

func defaultAuthorizationMessage(status int, message string) string {
	if trimmed := strings.TrimSpace(message); trimmed != "" {
		return trimmed
	}

	switch status {
	case fiber.StatusUnauthorized:
		return "request is missing valid API authentication"
	case fiber.StatusForbidden:
		return "authenticated principal is not allowed to perform this operation"
	default:
		return "request authorization failed"
	}
}

func authorizationErrorCode(status int) string {
	switch status {
	case fiber.StatusUnauthorized:
		return "unauthorized"
	case fiber.StatusForbidden:
		return "forbidden"
	default:
		return "authorization_error"
	}
}
