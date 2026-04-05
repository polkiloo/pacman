package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/polkiloo/pacman/internal/config"
)

// LoadServerTLSConfig builds the TLS configuration for inbound PACMAN servers.
// When TLS is disabled in the node config, it returns nil.
func LoadServerTLSConfig(cfg config.TLSConfig, clientAuth tls.ClientAuthType) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	certificate, err := loadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   clientAuth,
	}

	if clientAuth >= tls.VerifyClientCertIfGiven {
		clientCAs, err := LoadCertPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("load tls client ca pool: %w", err)
		}

		tlsConfig.ClientCAs = clientCAs
	}

	return tlsConfig, nil
}

// LoadClientTLSConfig builds a client-side TLS configuration from PACMAN node
// TLS settings so future peer/client transports can share certificate-loading
// behavior.
func LoadClientTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: strings.TrimSpace(cfg.ServerName),
	}

	if strings.TrimSpace(cfg.CAFile) != "" {
		rootCAs, err := LoadCertPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("load tls root ca pool: %w", err)
		}

		tlsConfig.RootCAs = rootCAs
	}

	if strings.TrimSpace(cfg.CertFile) != "" || strings.TrimSpace(cfg.KeyFile) != "" {
		certificate, err := loadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}

		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	if cfg.InsecureSkipVerify {
		//nolint:gosec // Explicit operator opt-in for non-production or pinned-endpoint testing.
		tlsConfig.InsecureSkipVerify = true
	}

	return tlsConfig, nil
}

// LoadCertPool reads PEM-encoded CA certificates from disk and returns a cert
// pool suitable for TLS server/client verification.
func LoadCertPool(path string) (*x509.CertPool, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, fmt.Errorf("tls caFile is required")
	}

	payload, err := os.ReadFile(trimmed)
	if err != nil {
		return nil, fmt.Errorf("read tls ca file %q: %w", trimmed, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(payload) {
		return nil, fmt.Errorf("parse tls ca file %q: no PEM certificates found", trimmed)
	}

	return pool, nil
}

func loadX509KeyPair(certFile, keyFile string) (tls.Certificate, error) {
	certPath := strings.TrimSpace(certFile)
	keyPath := strings.TrimSpace(keyFile)

	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load tls key pair cert=%q key=%q: %w", certPath, keyPath, err)
	}

	return certificate, nil
}
