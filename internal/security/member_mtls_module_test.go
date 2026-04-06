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

type resolvedMemberMTLSConfigs struct {
	fx.In

	ServerTLSConfig *tls.Config `name:"member_peer_server_tls" optional:"true"`
	ClientTLSConfig *tls.Config `name:"member_peer_client_tls" optional:"true"`
}

func TestMemberMTLSModuleProvidesPeerTLSConfigs(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)
	cfg := config.Config{
		TLS: &config.TLSConfig{
			Enabled:  true,
			CAFile:   fixture.CAFile,
			CertFile: fixture.CertFile,
			KeyFile:  fixture.KeyFile,
		},
		Security: &config.SecurityConfig{
			MemberMTLSEnabled: true,
		},
	}

	var resolved resolvedMemberMTLSConfigs
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() *config.Config { return &cfg }),
		MemberMTLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	if resolved.ServerTLSConfig == nil {
		t.Fatal("expected member peer server tls config to be provided")
	}

	if resolved.ServerTLSConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("clientAuth: got %v, want %v", resolved.ServerTLSConfig.ClientAuth, tls.RequireAndVerifyClientCert)
	}

	if resolved.ClientTLSConfig == nil {
		t.Fatal("expected member peer client tls config to be provided")
	}

	if resolved.ClientTLSConfig.RootCAs == nil {
		t.Fatal("expected member peer client tls config to include root CAs")
	}
}

func TestMemberMTLSModuleAllowsMissingEnablement(t *testing.T) {
	t.Parallel()

	var resolved resolvedMemberMTLSConfigs
	app := fx.New(
		fx.NopLogger,
		MemberMTLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	if resolved.ServerTLSConfig != nil {
		t.Fatalf("expected nil member peer server tls config, got %#v", resolved.ServerTLSConfig)
	}

	if resolved.ClientTLSConfig != nil {
		t.Fatalf("expected nil member peer client tls config, got %#v", resolved.ClientTLSConfig)
	}
}

func TestMemberMTLSModuleRejectsUnreadableTLSFiles(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		TLS: &config.TLSConfig{
			Enabled:  true,
			CAFile:   filepath.Join(t.TempDir(), "missing-ca.crt"),
			CertFile: filepath.Join(t.TempDir(), "missing.crt"),
			KeyFile:  filepath.Join(t.TempDir(), "missing.key"),
		},
		Security: &config.SecurityConfig{
			MemberMTLSEnabled: true,
		},
	}

	var resolved resolvedMemberMTLSConfigs
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() *config.Config { return &cfg }),
		MemberMTLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected member mtls module error")
	} else if !strings.Contains(err.Error(), "load member peer server tls config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMemberMTLSModuleRejectsEnabledPeerMTLSWithoutTLS(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		Security: &config.SecurityConfig{
			MemberMTLSEnabled: true,
		},
	}

	var resolved resolvedMemberMTLSConfigs
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() *config.Config { return &cfg }),
		MemberMTLSModule(),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected member mtls tls-enable error")
	} else if !strings.Contains(err.Error(), config.ErrSecurityMemberMTLSRequiresTLS.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}
