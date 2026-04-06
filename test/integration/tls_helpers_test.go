//go:build integration

package integration_test

import (
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/security"
	"github.com/polkiloo/pacman/internal/security/tlstesting"
)

type integrationTLSFixture struct {
	tlstesting.Fixture
}

type integrationMutualTLSFixture struct {
	tlstesting.MutualFixture
}

func writeIntegrationTLSFixture(t *testing.T) integrationTLSFixture {
	t.Helper()
	return integrationTLSFixture{Fixture: tlstesting.Write(t)}
}

func writeIntegrationMutualTLSFixture(t *testing.T, serverCommonName, clientCommonName string) integrationMutualTLSFixture {
	t.Helper()
	return integrationMutualTLSFixture{MutualFixture: tlstesting.WriteMutual(t, serverCommonName, clientCommonName)}
}

func writeIntegrationMutualTLSClientsFixture(t *testing.T, serverCommonName string, clientCommonNames ...string) integrationMutualTLSFixture {
	t.Helper()
	return integrationMutualTLSFixture{MutualFixture: tlstesting.WriteMutualClients(t, serverCommonName, clientCommonNames...)}
}

func (fixture integrationTLSFixture) containerFiles(certPath, keyPath string) []testcontainers.ContainerFile {
	return []testcontainers.ContainerFile{
		{
			HostFilePath:      fixture.CertFile,
			ContainerFilePath: certPath,
			FileMode:          0o600,
		},
		{
			HostFilePath:      fixture.KeyFile,
			ContainerFilePath: keyPath,
			FileMode:          0o600,
		},
	}
}

func (fixture integrationTLSFixture) client(t *testing.T, serverName string) *http.Client {
	t.Helper()

	tlsConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		ServerName: serverName,
	})
	if err != nil {
		t.Fatalf("load tls client config: %v", err)
	}

	return newIntegrationTLSClient(tlsConfig)
}

func (fixture integrationMutualTLSFixture) serverContainerFiles(caPath, certPath, keyPath string) []testcontainers.ContainerFile {
	return []testcontainers.ContainerFile{
		{
			HostFilePath:      fixture.CAFile,
			ContainerFilePath: caPath,
			FileMode:          0o600,
		},
		{
			HostFilePath:      fixture.Server.CertFile,
			ContainerFilePath: certPath,
			FileMode:          0o600,
		},
		{
			HostFilePath:      fixture.Server.KeyFile,
			ContainerFilePath: keyPath,
			FileMode:          0o600,
		},
	}
}

func (fixture integrationMutualTLSFixture) client(t *testing.T, serverName string) *http.Client {
	t.Helper()

	return fixture.clientFor(t, serverName, fixture.Client)
}

func (fixture integrationMutualTLSFixture) clientNamed(t *testing.T, serverName, clientCommonName string) *http.Client {
	t.Helper()

	leaf, ok := fixture.Clients[clientCommonName]
	if !ok {
		t.Fatalf("load mutual tls client %q: fixture is missing that client certificate", clientCommonName)
	}

	return fixture.clientFor(t, serverName, leaf)
}

func (fixture integrationMutualTLSFixture) clientFor(t *testing.T, serverName string, leaf tlstesting.LeafFixture) *http.Client {
	t.Helper()

	tlsConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		CertFile:   leaf.CertFile,
		KeyFile:    leaf.KeyFile,
		ServerName: serverName,
	})
	if err != nil {
		t.Fatalf("load mutual tls client config: %v", err)
	}

	return newIntegrationTLSClient(tlsConfig)
}

func (fixture integrationMutualTLSFixture) rootOnlyClient(t *testing.T, serverName string) *http.Client {
	t.Helper()

	tlsConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		ServerName: serverName,
	})
	if err != nil {
		t.Fatalf("load root-only tls client config: %v", err)
	}

	return newIntegrationTLSClient(tlsConfig)
}

func newIntegrationTLSClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}
