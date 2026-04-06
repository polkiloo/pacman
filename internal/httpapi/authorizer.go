package httpapi

import (
	"crypto/subtle"
	"strings"

	"github.com/gofiber/fiber/v2"
)

const (
	adminPrincipalSubject = "admin"
	errMissingBearerToken = "missing bearer token"
)

type adminBearerTokenAuthorizer struct {
	expectedToken string
}

// NewAdminBearerTokenAuthorizer constructs an authorizer that grants
// administrative API access to callers presenting the configured bearer token.
func NewAdminBearerTokenAuthorizer(token string) Authorizer {
	return adminBearerTokenAuthorizer{
		expectedToken: strings.TrimSpace(token),
	}
}

func (auth adminBearerTokenAuthorizer) Authorize(c *fiber.Ctx, _ AccessScope) (*Principal, error) {
	token, err := parseBearerAuthorizationHeader(c.Get(fiber.HeaderAuthorization))
	if err != nil {
		return nil, err
	}

	if auth.expectedToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(auth.expectedToken)) != 1 {
		return nil, Unauthorized("invalid bearer token")
	}

	return &Principal{
		Subject:   adminPrincipalSubject,
		Mechanism: "bearer",
	}, nil
}

func parseBearerAuthorizationHeader(header string) (string, error) {
	trimmed := strings.TrimSpace(header)
	if trimmed == "" {
		return "", Unauthorized(errMissingBearerToken)
	}

	scheme, credentials, found := strings.Cut(trimmed, " ")
	if !strings.EqualFold(strings.TrimSpace(scheme), "Bearer") {
		return "", Unauthorized("authorization scheme must be Bearer")
	}

	if !found {
		return "", Unauthorized(errMissingBearerToken)
	}

	token := strings.TrimSpace(credentials)
	if token == "" {
		return "", Unauthorized(errMissingBearerToken)
	}

	if strings.ContainsAny(token, " \t\r\n") {
		return "", Unauthorized("bearer token is malformed")
	}

	return token, nil
}
