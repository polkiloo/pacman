package config

import (
	"fmt"
	"os"
	"strings"
)

// AdminAuthEnabled reports whether administrative API authentication is
// configured.
func (security *SecurityConfig) AdminAuthEnabled() bool {
	if security == nil {
		return false
	}

	return strings.TrimSpace(security.AdminBearerToken) != "" || strings.TrimSpace(security.AdminBearerTokenFile) != ""
}

// PeerMTLSEnabled reports whether peer mTLS is enabled for cluster-member
// traffic on the control-plane listener.
func (security *SecurityConfig) PeerMTLSEnabled() bool {
	if security == nil {
		return false
	}

	return security.MemberMTLSEnabled
}

// ResolveAdminBearerToken returns the configured admin bearer token, loading it
// from disk when a token file is configured. File-backed secrets are trimmed so
// newline-terminated secret mounts work without extra configuration.
func (security SecurityConfig) ResolveAdminBearerToken(readFile func(string) ([]byte, error)) (string, error) {
	if trimmed := strings.TrimSpace(security.AdminBearerToken); trimmed != "" {
		return trimmed, nil
	}

	path := strings.TrimSpace(security.AdminBearerTokenFile)
	if path == "" {
		return "", nil
	}

	if readFile == nil {
		readFile = os.ReadFile
	}

	payload, err := readFile(path)
	if err != nil {
		return "", fmt.Errorf("read admin bearer token file %q: %w", path, err)
	}

	token := strings.TrimSpace(string(payload))
	if token == "" {
		return "", fmt.Errorf("read admin bearer token file %q: token is empty", path)
	}

	return token, nil
}
