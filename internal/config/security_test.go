package config

import (
	"errors"
	"testing"
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
