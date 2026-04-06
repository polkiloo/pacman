// Package tlstesting provides self-signed TLS certificate fixtures for use in
// tests that need real cert/key files on disk.
package tlstesting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Fixture holds file paths to a temporary CA certificate, server certificate,
// and server private key. All files are cleaned up automatically by t.Cleanup.
type Fixture struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// Write generates a self-signed CA and a server certificate signed by that CA,
// writes all three PEM files into a temporary directory, and returns their
// paths. The server certificate is valid for localhost (DNS and 127.0.0.1) and
// carries both ServerAuth and ClientAuth extended key usages.
func Write(t *testing.T) Fixture {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstesting: generate ca key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "pacman-test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatalf("tlstesting: create ca certificate: %v", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstesting: generate server key: %v", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, serverKey.Public(), caKey)
	if err != nil {
		t.Fatalf("tlstesting: create server certificate: %v", err)
	}

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")

	writePEMFile(t, caFile, "CERTIFICATE", caDER)
	writePEMFile(t, certFile, "CERTIFICATE", serverDER)

	keyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatalf("tlstesting: marshal server key: %v", err)
	}
	writePEMFile(t, keyFile, "EC PRIVATE KEY", keyDER)

	return Fixture{
		CAFile:   caFile,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
}

func writePEMFile(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("tlstesting: write %s: %v", path, err)
	}
}
