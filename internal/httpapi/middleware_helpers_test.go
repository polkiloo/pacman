package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

type testLocalPromoter struct {
	calls int
	err   error
}

type httpResponseSnapshot struct {
	statusCode int
	headers    http.Header
	body       []byte
}

func (promoter *testLocalPromoter) PromoteLocal(context.Context) error {
	promoter.calls++
	return promoter.err
}

func TestAuthorizerHelpers(t *testing.T) {
	t.Parallel()

	authorizer := AuthorizerFunc(func(_ *fiber.Ctx, scope AccessScope) (*Principal, error) {
		if scope != AccessScopeClusterWrite {
			t.Fatalf("unexpected access scope %q", scope)
		}

		return &Principal{Subject: "ops@example", Mechanism: "bearer"}, nil
	})

	principal, err := authorizer.Authorize(nil, AccessScopeClusterWrite)
	if err != nil {
		t.Fatalf("authorize via adapter: %v", err)
	}

	if principal == nil || principal.Subject != "ops@example" || principal.Mechanism != "bearer" {
		t.Fatalf("unexpected principal: %+v", principal)
	}

	authErr := Unauthorized("missing bearer token")
	if got := authErr.Error(); got != "missing bearer token" {
		t.Fatalf("unexpected unauthorized error string: %q", got)
	}

	forbiddenErr := Forbidden("operator role required")
	if got := forbiddenErr.Error(); got != "operator role required" {
		t.Fatalf("unexpected forbidden error string: %q", got)
	}
}

func TestDefaultAuthorizationMessage(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		status  int
		message string
		want    string
	}{
		{
			name:   "unauthorized default",
			status: fiber.StatusUnauthorized,
			want:   "request is missing valid API authentication",
		},
		{
			name:   "forbidden default",
			status: fiber.StatusForbidden,
			want:   "authenticated principal is not allowed to perform this operation",
		},
		{
			name:   "generic default",
			status: fiber.StatusInternalServerError,
			want:   "request authorization failed",
		},
		{
			name:    "explicit message wins",
			status:  fiber.StatusUnauthorized,
			message: "custom message",
			want:    "custom message",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := defaultAuthorizationMessage(testCase.status, testCase.message); got != testCase.want {
				t.Fatalf("authorization message: got %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestWriteAuthorizationErrorMapsResponses(t *testing.T) {
	t.Parallel()

	t.Run("authorization error", func(t *testing.T) {
		t.Parallel()

		response := exerciseAuthErrorResponse(t, Unauthorized(""))
		if response.statusCode != fiber.StatusUnauthorized {
			t.Fatalf("status: got %d, want %d", response.statusCode, fiber.StatusUnauthorized)
		}

		if got := response.headers.Get(fiber.HeaderWWWAuthenticate); got != "Bearer" {
			t.Fatalf("www-authenticate header: got %q", got)
		}

		assertErrorBody(t, response.body, "unauthorized", "request is missing valid API authentication")
	})

	t.Run("generic error", func(t *testing.T) {
		t.Parallel()

		response := exerciseAuthErrorResponse(t, errors.New("boom"))
		if response.statusCode != fiber.StatusInternalServerError {
			t.Fatalf("status: got %d, want %d", response.statusCode, fiber.StatusInternalServerError)
		}

		assertErrorBody(t, response.body, "authorization_error", "authorization hook failed")
	})
}

func TestHandlePromoteResponses(t *testing.T) {
	t.Parallel()

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		response, calls := exercisePromote(t, nil)
		if response.statusCode != fiber.StatusServiceUnavailable {
			t.Fatalf("status: got %d, want %d", response.statusCode, fiber.StatusServiceUnavailable)
		}

		if calls != 0 {
			t.Fatalf("expected promoter not to be called, got %d calls", calls)
		}

		assertErrorBody(t, response.body, "promote_unavailable", "local promotion is not configured on this node")
	})

	t.Run("promoter failure", func(t *testing.T) {
		t.Parallel()

		response, calls := exercisePromote(t, &testLocalPromoter{err: errors.New("promote failed")})
		if response.statusCode != fiber.StatusInternalServerError {
			t.Fatalf("status: got %d, want %d", response.statusCode, fiber.StatusInternalServerError)
		}

		if calls != 1 {
			t.Fatalf("expected promoter to be called once, got %d", calls)
		}

		assertErrorBody(t, response.body, "promote_failed", "failed to promote local postgres")
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		response, calls := exercisePromote(t, &testLocalPromoter{})
		if response.statusCode != fiber.StatusOK {
			t.Fatalf("status: got %d, want %d", response.statusCode, fiber.StatusOK)
		}

		if calls != 1 {
			t.Fatalf("expected promoter to be called once, got %d", calls)
		}

		var payload map[string]string
		if err := json.Unmarshal(response.body, &payload); err != nil {
			t.Fatalf("decode success body: %v", err)
		}

		if got := payload["message"]; got != "promoted local postgres to primary" {
			t.Fatalf("unexpected success message: %q", got)
		}
	})
}

func exerciseAuthErrorResponse(t *testing.T, err error) httpResponseSnapshot {
	t.Helper()

	app := fiber.New()
	srv := &Server{}
	app.Get("/", func(c *fiber.Ctx) error {
		return srv.writeAuthorizationError(c, AccessScopeClusterWrite, err)
	})

	return performFiberRequest(t, app, fiber.MethodGet, "/")
}

func exercisePromote(t *testing.T, promoter *testLocalPromoter) (httpResponseSnapshot, int) {
	t.Helper()

	app := fiber.New()
	srv := &Server{}
	if promoter != nil {
		srv.localPromoter = promoter
	}
	app.Post("/promote", srv.handlePromote)

	response := performFiberRequest(t, app, fiber.MethodPost, "/promote")
	calls := 0
	if promoter != nil {
		calls = promoter.calls
	}

	return response, calls
}

func performFiberRequest(t *testing.T, app *fiber.App, method, path string) httpResponseSnapshot {
	t.Helper()

	response, err := app.Test(httptest.NewRequest(method, path, nil))
	if err != nil {
		t.Fatalf("perform %s %s: %v", method, path, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s response body: %v", method, path, err)
	}

	return httpResponseSnapshot{
		statusCode: response.StatusCode,
		headers:    response.Header.Clone(),
		body:       body,
	}
}

func assertErrorBody(t *testing.T, payload []byte, wantCode, wantMessage string) {
	t.Helper()

	var response errorResponseJSON
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode error body: %v", err)
	}

	if response.Error != wantCode {
		t.Fatalf("error code: got %q, want %q", response.Error, wantCode)
	}

	if response.Message != wantMessage {
		t.Fatalf("error message: got %q, want %q", response.Message, wantMessage)
	}
}
