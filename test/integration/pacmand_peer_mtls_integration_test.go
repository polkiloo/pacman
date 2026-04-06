//go:build integration

package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gopkg.in/yaml.v3"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/peerapi"
	"github.com/polkiloo/pacman/test/testenv"
)

const (
	memberMTLSAPIAddress     = "0.0.0.0:8080"
	memberMTLSControlAddress = "0.0.0.0:9090"
	memberMTLSCAPath         = "/etc/pacman/tls/ca.crt"
	memberMTLSServerCertPath = "/etc/pacman/tls/server.crt"
	memberMTLSServerKeyPath  = "/etc/pacman/tls/server.key"
)

func TestPacmandPeerIdentityAcceptsAllowedMemberCertificate(t *testing.T) {
	t.Parallel()

	fixture := writeIntegrationMutualTLSFixture(t, "alpha-mtls", "beta-1")
	daemon := startWitnessDaemonMemberMTLS(t, "alpha-mtls", []string{"alpha-mtls", "beta-1"}, fixture)
	client := fixture.client(t, "localhost")
	peerURL := "https://" + daemon.Address(t, "9090") + "/peer/v1/identity"

	waitForProbeStatus(t, client, peerURL, http.StatusOK, pacmandStartupTimeout)

	response, err := client.Get(peerURL)
	if err != nil {
		t.Fatalf("get peer identity over mTLS: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var payload peerapi.IdentityResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode peer identity response: %v", err)
	}

	if payload.NodeName != "alpha-mtls" {
		t.Fatalf("nodeName: got %q, want %q", payload.NodeName, "alpha-mtls")
	}

	if payload.Peer.Subject != "beta-1" {
		t.Fatalf("peer subject: got %q, want %q", payload.Peer.Subject, "beta-1")
	}

	if payload.Peer.Mechanism != "mtls" {
		t.Fatalf("peer mechanism: got %q, want %q", payload.Peer.Mechanism, "mtls")
	}

	rootOnlyClient := fixture.rootOnlyClient(t, "localhost")
	if _, err := rootOnlyClient.Get(peerURL); err == nil {
		t.Fatal("expected mTLS handshake failure without client certificate")
	}
}

func TestPacmandPeerIdentityRejectsUnexpectedMemberCertificate(t *testing.T) {
	t.Parallel()

	fixture := writeIntegrationMutualTLSClientsFixture(t, "alpha-mtls", "beta-1", "gamma-1")
	daemon := startWitnessDaemonMemberMTLS(t, "alpha-mtls", []string{"alpha-mtls", "beta-1"}, fixture)
	readyClient := fixture.clientNamed(t, "localhost", "beta-1")
	client := fixture.clientNamed(t, "localhost", "gamma-1")
	peerURL := "https://" + daemon.Address(t, "9090") + "/peer/v1/identity"

	waitForProbeStatus(t, readyClient, peerURL, http.StatusOK, pacmandStartupTimeout)

	response, err := client.Get(peerURL)
	if err != nil {
		t.Fatalf("get peer identity with unexpected certificate: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read unauthorized response body: %v", err)
	}

	if !strings.Contains(string(body), "peer certificate subject is not allowed") {
		t.Fatalf("unexpected unauthorized body: %s", string(body))
	}
}

func startWitnessDaemonMemberMTLS(t *testing.T, nodeName string, expectedMembers []string, fixture integrationMutualTLSFixture) *testenv.Service {
	t.Helper()

	env := testenv.New(t)
	prefix := sanitizeIntegrationName(t.Name())
	testenv.RequireLocalImage(t, pacmanTestImage())

	configBody := marshalMemberMTLSDaemonConfig(t, nodeName, expectedMembers)

	files := []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)}
	files = append(files, fixture.serverContainerFiles(memberMTLSCAPath, memberMTLSServerCertPath, memberMTLSServerKeyPath)...)

	return env.StartService(t, testenv.ServiceConfig{
		Name:         prefix + "-peer-mtls",
		Image:        pacmanTestImage(),
		Aliases:      []string{prefix + "-peer-mtls"},
		Files:        files,
		ExposedPorts: []string{"8080/tcp", "9090/tcp"},
		Cmd: []string{
			"/bin/sh",
			"-lc",
			"pacmand -config " + daemonConfigPath,
		},
		WaitStrategy: wait.ForListeningPort("9090/tcp").WithStartupTimeout(pacmandStartupTimeout),
	})
}

func marshalMemberMTLSDaemonConfig(t *testing.T, nodeName string, expectedMembers []string) string {
	t.Helper()

	cfg := config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name:           nodeName,
			Role:           cluster.NodeRoleWitness,
			APIAddress:     memberMTLSAPIAddress,
			ControlAddress: memberMTLSControlAddress,
		},
		TLS: &config.TLSConfig{
			Enabled:  true,
			CAFile:   memberMTLSCAPath,
			CertFile: memberMTLSServerCertPath,
			KeyFile:  memberMTLSServerKeyPath,
		},
		Security: &config.SecurityConfig{
			MemberMTLSEnabled: true,
		},
		Bootstrap: &config.ClusterBootstrapConfig{
			ClusterName:     "alpha",
			InitialPrimary:  nodeName,
			SeedAddresses:   []string{memberMTLSControlAddress},
			ExpectedMembers: append([]string(nil), expectedMembers...),
		},
	}

	payload, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal member mTLS daemon config: %v", err)
	}

	return string(payload)
}
