//go:build integration

package integration_test

import (
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

func writeIntegrationTLSFixture(t *testing.T) integrationTLSFixture {
	t.Helper()
	return integrationTLSFixture{Fixture: tlstesting.Write(t)}
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

	return &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}
