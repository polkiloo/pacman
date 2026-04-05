package security

import (
	"crypto/tls"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/security/tlstesting"
)

type resolvedAPIServerTLSConfig struct {
	fx.In

	TLSConfig *tls.Config `name:"api_server_tls" optional:"true"`
}

func TestTLSModuleProvidesAPIServerTLSConfig(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)
	cfg := config.Config{
		TLS: &config.TLSConfig{
			Enabled:  true,
			CAFile:   fixture.CAFile,
			CertFile: fixture.CertFile,
			KeyFile:  fixture.KeyFile,
		},
	}

	var resolved resolvedAPIServerTLSConfig
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() *config.Config { return &cfg }),
		TLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	if resolved.TLSConfig == nil {
		t.Fatal("expected tls config to be provided")
	}

	if resolved.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minVersion: got %v, want %v", resolved.TLSConfig.MinVersion, tls.VersionTLS12)
	}
}

func TestTLSModuleAllowsMissingConfig(t *testing.T) {
	t.Parallel()

	var resolved resolvedAPIServerTLSConfig
	app := fx.New(
		fx.NopLogger,
		TLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	if resolved.TLSConfig != nil {
		t.Fatalf("expected nil tls config, got %#v", resolved.TLSConfig)
	}
}

func TestTLSModuleRejectsUnreadableTLSFiles(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		TLS: &config.TLSConfig{
			Enabled:  true,
			CertFile: filepath.Join(t.TempDir(), "missing.crt"),
			KeyFile:  filepath.Join(t.TempDir(), "missing.key"),
		},
	}

	var resolved resolvedAPIServerTLSConfig
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() *config.Config { return &cfg }),
		TLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected tls module error")
	} else if !strings.Contains(err.Error(), "load api server tls config") {
		t.Fatalf("unexpected error: %v", err)
	}
}
