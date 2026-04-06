// Package tlstesting provides self-signed TLS certificate fixtures for use in
// tests that need real cert/key files on disk.
package tlstesting

import (
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
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

	dir := t.TempDir()
	caTemplate, caKey, caDER := writeTestCA(t)
	caFile := filepath.Join(dir, "ca.crt")
	writePEMFile(t, caFile, "CERTIFICATE", caDER)
	server := writeLeaf(t, dir, "server", caTemplate, caKey, "localhost")

	return Fixture{
		CAFile:   caFile,
		CertFile: server.CertFile,
		KeyFile:  server.KeyFile,
	}
}

func writePEMFile(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("tlstesting: write %s: %v", path, err)
	}
}
