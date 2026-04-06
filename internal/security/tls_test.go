package security

import (
	"crypto/tls"
	"path/filepath"
	"testing"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/security/tlstesting"
)

func TestLoadServerTLSConfigDisabledReturnsNil(t *testing.T) {
	t.Parallel()

	tlsConfig, err := LoadServerTLSConfig(config.TLSConfig{}, tls.NoClientCert)
	if err != nil {
		t.Fatalf("load disabled server tls config: %v", err)
	}

	if tlsConfig != nil {
		t.Fatalf("expected nil tls config when disabled, got %#v", tlsConfig)
	}
}

func TestLoadServerTLSConfigLoadsKeyPairAndClientCAs(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)

	tlsConfig, err := LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.CertFile,
		KeyFile:  fixture.KeyFile,
	}, tls.RequireAndVerifyClientCert)
	if err != nil {
		t.Fatalf("load server tls config: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("expected non-nil tls config")
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("certificates: got %d, want 1", len(tlsConfig.Certificates))
	}

	if tlsConfig.ClientCAs == nil {
		t.Fatal("expected client ca pool to be loaded")
	}

	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("clientAuth: got %v, want %v", tlsConfig.ClientAuth, tls.RequireAndVerifyClientCert)
	}
}

func TestLoadServerTLSConfigRejectsMissingKeyPair(t *testing.T) {
	t.Parallel()

	_, err := LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CertFile: filepath.Join(t.TempDir(), "missing.crt"),
		KeyFile:  filepath.Join(t.TempDir(), "missing.key"),
	}, tls.NoClientCert)
	if err == nil {
		t.Fatal("expected missing key pair error")
	}
}

func TestLoadClientTLSConfigLoadsRootsAndServerName(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)

	tlsConfig, err := LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("load client tls config: %v", err)
	}

	if tlsConfig.RootCAs == nil {
		t.Fatal("expected root ca pool to be loaded")
	}

	if tlsConfig.ServerName != "localhost" {
		t.Fatalf("serverName: got %q, want %q", tlsConfig.ServerName, "localhost")
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minVersion: got %v, want %v", tlsConfig.MinVersion, tls.VersionTLS12)
	}
}

func TestLoadClientTLSConfigLoadsClientCertificateAndInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)

	tlsConfig, err := LoadClientTLSConfig(config.TLSConfig{
		CAFile:             fixture.CAFile,
		CertFile:           fixture.CertFile,
		KeyFile:            fixture.KeyFile,
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("load client tls config with certificate: %v", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("certificates: got %d, want %d", len(tlsConfig.Certificates), 1)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Fatal("expected insecure skip verify to be enabled")
	}
}

func TestLoadCertPoolRejectsEmptyPath(t *testing.T) {
	t.Parallel()

	if _, err := LoadCertPool(""); err == nil {
		t.Fatal("expected empty path error")
	}
}
