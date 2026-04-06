package tlstesting

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"
)

func TestWriteCreatesCAAndLeafFiles(t *testing.T) {
	t.Parallel()

	fixture := Write(t)

	if _, err := os.Stat(fixture.CAFile); err != nil {
		t.Fatalf("stat ca file: %v", err)
	}

	if cert := mustReadCertificate(t, fixture.CertFile); cert.Subject.CommonName != "localhost" {
		t.Fatalf("commonName: got %q, want %q", cert.Subject.CommonName, "localhost")
	}
}

func TestWriteMutualCreatesDistinctPeerCertificates(t *testing.T) {
	t.Parallel()

	fixture := WriteMutual(t, "alpha-1", "beta-1")

	serverCert := mustReadCertificate(t, fixture.Server.CertFile)
	if serverCert.Subject.CommonName != "alpha-1" {
		t.Fatalf("server commonName: got %q, want %q", serverCert.Subject.CommonName, "alpha-1")
	}

	clientCert := mustReadCertificate(t, fixture.Client.CertFile)
	if clientCert.Subject.CommonName != "beta-1" {
		t.Fatalf("client commonName: got %q, want %q", clientCert.Subject.CommonName, "beta-1")
	}

	if fixture.Server.CertFile == fixture.Client.CertFile {
		t.Fatal("expected distinct server and client certificate files")
	}
}

func TestWriteMutualClientsCreatesMultipleClientCertificates(t *testing.T) {
	t.Parallel()

	fixture := WriteMutualClients(t, "alpha-1", "beta-1", "gamma-1")

	if len(fixture.Clients) != 2 {
		t.Fatalf("unexpected client count: got %d, want %d", len(fixture.Clients), 2)
	}

	betaCert := mustReadCertificate(t, fixture.Clients["beta-1"].CertFile)
	if betaCert.Subject.CommonName != "beta-1" {
		t.Fatalf("beta commonName: got %q, want %q", betaCert.Subject.CommonName, "beta-1")
	}

	gammaCert := mustReadCertificate(t, fixture.Clients["gamma-1"].CertFile)
	if gammaCert.Subject.CommonName != "gamma-1" {
		t.Fatalf("gamma commonName: got %q, want %q", gammaCert.Subject.CommonName, "gamma-1")
	}

	if fixture.Client.CertFile != fixture.Clients["beta-1"].CertFile {
		t.Fatal("expected primary client fixture to match the first client")
	}
}

func mustReadCertificate(t *testing.T, path string) *x509.Certificate {
	t.Helper()

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read certificate %q: %v", path, err)
	}

	block, _ := pem.Decode(payload)
	if block == nil {
		t.Fatalf("decode pem certificate %q", path)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate %q: %v", path, err)
	}

	return cert
}
