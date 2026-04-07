package config

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/internal/dcs"
)

func TestSecurityAdminAuthEnabled(t *testing.T) {
	t.Parallel()

	var nilSecurity *SecurityConfig
	if nilSecurity.AdminAuthEnabled() {
		t.Fatal("expected nil security config to report auth disabled")
	}

	if (&SecurityConfig{}).AdminAuthEnabled() {
		t.Fatal("expected empty security config to report auth disabled")
	}

	if !(&SecurityConfig{AdminBearerToken: "secret-token"}).AdminAuthEnabled() {
		t.Fatal("expected inline token to enable admin auth")
	}

	if !(&SecurityConfig{AdminBearerTokenFile: "/run/secrets/pacman-admin-token"}).AdminAuthEnabled() {
		t.Fatal("expected token file to enable admin auth")
	}
}

func TestSecurityPeerMTLSEnabled(t *testing.T) {
	t.Parallel()

	var nilSecurity *SecurityConfig
	if nilSecurity.PeerMTLSEnabled() {
		t.Fatal("expected nil security config to report member mTLS disabled")
	}

	if (&SecurityConfig{}).PeerMTLSEnabled() {
		t.Fatal("expected empty security config to report member mTLS disabled")
	}

	if !(&SecurityConfig{MemberMTLSEnabled: true}).PeerMTLSEnabled() {
		t.Fatal("expected member mTLS flag to report enabled")
	}
}

func TestSecurityResolveAdminBearerToken(t *testing.T) {
	t.Parallel()

	token, err := (SecurityConfig{AdminBearerToken: " secret-token "}).ResolveAdminBearerToken(nil)
	if err != nil {
		t.Fatalf("resolve inline token: %v", err)
	}

	if token != "secret-token" {
		t.Fatalf("unexpected inline token: got %q, want %q", token, "secret-token")
	}
}

func TestSecurityResolveAdminBearerTokenFromFile(t *testing.T) {
	t.Parallel()

	token, err := (SecurityConfig{AdminBearerTokenFile: "/run/secrets/pacman-admin-token"}).ResolveAdminBearerToken(func(path string) ([]byte, error) {
		if path != "/run/secrets/pacman-admin-token" {
			t.Fatalf("unexpected token file path: %q", path)
		}

		return []byte("secret-token\n"), nil
	})
	if err != nil {
		t.Fatalf("resolve token file: %v", err)
	}

	if token != "secret-token" {
		t.Fatalf("unexpected token file value: got %q, want %q", token, "secret-token")
	}
}

func TestSecurityResolveAdminBearerTokenRejectsEmptyFile(t *testing.T) {
	t.Parallel()

	_, err := (SecurityConfig{AdminBearerTokenFile: "/run/secrets/pacman-admin-token"}).ResolveAdminBearerToken(func(string) ([]byte, error) {
		return []byte("\n"), nil
	})
	if err == nil {
		t.Fatal("expected empty token file error")
	}
}

func TestSecurityResolveAdminBearerTokenPropagatesReadError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("permission denied")
	_, err := (SecurityConfig{AdminBearerTokenFile: "/run/secrets/pacman-admin-token"}).ResolveAdminBearerToken(func(string) ([]byte, error) {
		return nil, readErr
	})
	if err == nil {
		t.Fatal("expected token file read error")
	}

	if !errors.Is(err, readErr) {
		t.Fatalf("expected wrapped read error, got %v", err)
	}
}

func TestConfigRedactedMasksSecuritySecretsWithoutMutatingOriginal(t *testing.T) {
	t.Parallel()

	cfg := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name: "alpha-1",
		},
		Security: &SecurityConfig{
			AdminBearerToken:     "secret-token",
			AdminBearerTokenFile: "/run/secrets/pacman-admin-token",
			MemberMTLSEnabled:    true,
		},
	}

	redacted := cfg.Redacted()

	if redacted.Security == nil {
		t.Fatal("expected redacted security config")
	}

	if redacted.Security.AdminBearerToken != redactedSecretValue {
		t.Fatalf("inline token redaction: got %q, want %q", redacted.Security.AdminBearerToken, redactedSecretValue)
	}

	if redacted.Security.AdminBearerTokenFile != redactedSecretValue {
		t.Fatalf("token file redaction: got %q, want %q", redacted.Security.AdminBearerTokenFile, redactedSecretValue)
	}

	if !redacted.Security.MemberMTLSEnabled {
		t.Fatal("expected non-secret security fields to be preserved")
	}

	if cfg.Security.AdminBearerToken != "secret-token" {
		t.Fatalf("expected original inline token to remain unchanged, got %q", cfg.Security.AdminBearerToken)
	}

	if cfg.Security.AdminBearerTokenFile != "/run/secrets/pacman-admin-token" {
		t.Fatalf("expected original token file to remain unchanged, got %q", cfg.Security.AdminBearerTokenFile)
	}
}

func TestConfigRedactedMasksDCSSecretsWithoutMutatingOriginal(t *testing.T) {
	t.Parallel()

	cfg := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name: "alpha-1",
		},
		DCS: &dcs.Config{
			Backend:      dcs.BackendEtcd,
			ClusterName:  "alpha",
			TTL:          dcs.DefaultTTL,
			RetryTimeout: dcs.DefaultRetryTimeout,
			Etcd: &dcs.EtcdConfig{
				Endpoints: []string{"https://127.0.0.1:2379"},
				Username:  "pacman",
				Password:  "secret-password",
			},
		},
	}

	redacted := cfg.Redacted()

	if redacted.DCS == nil || redacted.DCS.Etcd == nil {
		t.Fatal("expected redacted dcs config")
	}

	if redacted.DCS.Etcd.Password != redactedSecretValue {
		t.Fatalf("unexpected dcs password redaction: got %q, want %q", redacted.DCS.Etcd.Password, redactedSecretValue)
	}

	if redacted.DCS.Etcd.Username != "pacman" {
		t.Fatalf("expected non-secret dcs fields to be preserved, got %q", redacted.DCS.Etcd.Username)
	}

	if cfg.DCS == nil || cfg.DCS.Etcd == nil || cfg.DCS.Etcd.Password != "secret-password" {
		t.Fatalf("expected original dcs password to remain unchanged, got %+v", cfg.DCS)
	}
}

func TestConfigStringAndLogValueRedactSecuritySecrets(t *testing.T) {
	t.Parallel()

	cfg := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name: "alpha-1",
		},
		Security: &SecurityConfig{
			AdminBearerToken:     "secret-token",
			AdminBearerTokenFile: "/run/secrets/pacman-admin-token",
		},
	}

	formatted := cfg.String()
	if strings.Contains(formatted, "secret-token") {
		t.Fatalf("expected String output to redact inline token, got %q", formatted)
	}

	if strings.Contains(formatted, "/run/secrets/pacman-admin-token") {
		t.Fatalf("expected String output to redact token file path, got %q", formatted)
	}

	if !strings.Contains(formatted, redactedSecretValue) {
		t.Fatalf("expected String output to contain redaction marker, got %q", formatted)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	logger.Info("loaded node configuration", slog.Any("config", cfg))

	if strings.Contains(logs.String(), "secret-token") {
		t.Fatalf("expected slog output to redact inline token, got %q", logs.String())
	}

	if strings.Contains(logs.String(), "/run/secrets/pacman-admin-token") {
		t.Fatalf("expected slog output to redact token file path, got %q", logs.String())
	}

	if !strings.Contains(logs.String(), "redacted") {
		t.Fatalf("expected slog output to contain redaction marker, got %q", logs.String())
	}
}

func TestConfigGoStringRedactsSecuritySecrets(t *testing.T) {
	t.Parallel()

	cfg := Config{
		APIVersion: APIVersionV1Alpha1,
		Kind:       KindNodeConfig,
		Node: NodeConfig{
			Name: "alpha-1",
		},
		Security: &SecurityConfig{
			AdminBearerToken: "secret-token",
		},
	}

	formatted := cfg.GoString()
	if strings.Contains(formatted, "secret-token") {
		t.Fatalf("expected GoString output to redact inline token, got %q", formatted)
	}

	if !strings.Contains(formatted, redactedSecretValue) {
		t.Fatalf("expected GoString output to contain redaction marker, got %q", formatted)
	}
}

func TestConfigHasInlineSecretsIncludesDCS(t *testing.T) {
	t.Parallel()

	cfg := Config{
		DCS: &dcs.Config{
			Backend:      dcs.BackendEtcd,
			ClusterName:  "alpha",
			TTL:          dcs.DefaultTTL,
			RetryTimeout: dcs.DefaultRetryTimeout,
			Etcd: &dcs.EtcdConfig{
				Endpoints: []string{"https://127.0.0.1:2379"},
				Password:  "secret-password",
			},
		},
	}

	if !cfg.HasInlineSecrets() {
		t.Fatal("expected DCS inline password to count as a secret")
	}
}
