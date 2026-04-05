package tlstesting_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/polkiloo/pacman/internal/security/tlstesting"
)

func TestWriteGeneratesValidPEMFiles(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)

	for _, path := range []string{fixture.CAFile, fixture.CertFile, fixture.KeyFile} {
		if path == "" {
			t.Fatalf("expected non-empty file path")
		}

		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected fixture file to exist at %q: %v", path, err)
		}
	}

	caPayload, err := os.ReadFile(fixture.CAFile)
	if err != nil {
		t.Fatalf("read ca file: %v", err)
	}

	block, _ := pem.Decode(caPayload)
	if block == nil {
		t.Fatal("expected PEM-encoded CA certificate")
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse ca certificate: %v", err)
	}

	if !caCert.IsCA {
		t.Fatal("expected CA certificate to have IsCA=true")
	}
}

func TestWriteIsIndependentBetweenCalls(t *testing.T) {
	t.Parallel()

	a := tlstesting.Write(t)
	b := tlstesting.Write(t)

	if a.CAFile == b.CAFile {
		t.Fatal("expected independent fixtures to use different file paths")
	}
}
