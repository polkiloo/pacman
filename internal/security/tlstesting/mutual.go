package tlstesting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// MutualFixture holds CA, server, and client certificate files signed by the
// same temporary test CA.
type MutualFixture struct {
	CAFile  string
	Server  LeafFixture
	Client  LeafFixture
	Clients map[string]LeafFixture
}

// LeafFixture holds a PEM certificate and private key written to disk.
type LeafFixture struct {
	CertFile string
	KeyFile  string
}

// WriteMutual generates server and client certificates signed by the same test
// CA. Both leaf certificates are valid for localhost and 127.0.0.1 so callers
// can bind loopback listeners without extra SAN configuration.
func WriteMutual(t *testing.T, serverCommonName, clientCommonName string) MutualFixture {
	t.Helper()

	return WriteMutualClients(t, serverCommonName, clientCommonName)
}

// WriteMutualClients generates one server certificate and one or more client
// certificates signed by the same test CA. The first client certificate is also
// exposed through MutualFixture.Client for compatibility with existing callers.
func WriteMutualClients(t *testing.T, serverCommonName string, clientCommonNames ...string) MutualFixture {
	t.Helper()

	if len(clientCommonNames) == 0 {
		t.Fatal("tlstesting: at least one client common name is required")
	}

	caTemplate, caKey, caDER := writeTestCA(t)
	dir := t.TempDir()

	caFile := filepath.Join(dir, "ca.crt")
	writePEMFile(t, caFile, "CERTIFICATE", caDER)

	server := writeLeaf(t, dir, "server", caTemplate, caKey, serverCommonName)
	clients := make(map[string]LeafFixture, len(clientCommonNames))
	for index, commonName := range clientCommonNames {
		clients[commonName] = writeLeaf(t, dir, fmt.Sprintf("client-%d", index+1), caTemplate, caKey, commonName)
	}

	client := clients[clientCommonNames[0]]

	return MutualFixture{
		CAFile:  caFile,
		Server:  server,
		Client:  client,
		Clients: clients,
	}
}

func writeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
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

	return caTemplate, caKey, caDER
}

func writeLeaf(t *testing.T, dir, prefix string, caTemplate *x509.Certificate, caKey *ecdsa.PrivateKey, commonName string) LeafFixture {
	t.Helper()

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlstesting: generate %s key: %v", prefix, err)
	}

	serialNumber := big.NewInt(time.Now().UnixNano())
	leafTemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, leafKey.Public(), caKey)
	if err != nil {
		t.Fatalf("tlstesting: create %s certificate: %v", prefix, err)
	}

	certFile := filepath.Join(dir, prefix+".crt")
	keyFile := filepath.Join(dir, prefix+".key")
	writePEMFile(t, certFile, "CERTIFICATE", leafDER)

	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("tlstesting: marshal %s key: %v", prefix, err)
	}
	writePEMFile(t, keyFile, "EC PRIVATE KEY", keyDER)

	return LeafFixture{
		CertFile: certFile,
		KeyFile:  keyFile,
	}
}
