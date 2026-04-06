package peerapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/security"
	"github.com/polkiloo/pacman/internal/security/tlstesting"
)

func TestServerIdentityAcceptsAllowedPeer(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, clientTLSConfig := mustMutualTLSConfigs(t, fixture)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := New("alpha-1", slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		TLSConfig:    serverTLSConfig,
		AllowedPeers: []string{"beta-1"},
	})
	addr := reserveLoopbackAddress(t)
	if err := server.Start(ctx, addr); err != nil {
		t.Fatalf("start peer server: %v", err)
	}

	client := newTLSClient(clientTLSConfig)
	waitForServer(t, client, "https://"+addr+"/peer/v1/identity")

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/peer/v1/identity", nil)
	if err != nil {
		t.Fatalf("build identity request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call peer identity endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var payload IdentityResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode identity response: %v", err)
	}

	if payload.NodeName != "alpha-1" {
		t.Fatalf("nodeName: got %q, want %q", payload.NodeName, "alpha-1")
	}

	if payload.Peer.Subject != "beta-1" {
		t.Fatalf("peer subject: got %q, want %q", payload.Peer.Subject, "beta-1")
	}

	if payload.Peer.Mechanism != "mtls" {
		t.Fatalf("peer mechanism: got %q, want %q", payload.Peer.Mechanism, "mtls")
	}

	cancel()
	if err := server.Wait(); err != nil {
		t.Fatalf("wait for peer server shutdown: %v", err)
	}
}

func TestServerIdentityRejectsUnexpectedPeerSubject(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, clientTLSConfig := mustMutualTLSConfigs(t, fixture)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := New("alpha-1", slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		TLSConfig:    serverTLSConfig,
		AllowedPeers: []string{"gamma-1"},
	})
	addr := reserveLoopbackAddress(t)
	if err := server.Start(ctx, addr); err != nil {
		t.Fatalf("start peer server: %v", err)
	}

	client := newTLSClient(clientTLSConfig)
	waitForServer(t, client, "https://"+addr+"/peer/v1/identity")

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/peer/v1/identity", nil)
	if err != nil {
		t.Fatalf("build identity request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call peer identity endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	cancel()
	if err := server.Wait(); err != nil {
		t.Fatalf("wait for peer server shutdown: %v", err)
	}
}

func TestServerIdentityRejectsMissingClientCertificate(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, err := security.LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.Server.CertFile,
		KeyFile:  fixture.Server.KeyFile,
	}, tls.RequireAndVerifyClientCert)
	if err != nil {
		t.Fatalf("load server tls config: %v", err)
	}

	clientTLSConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("load client tls config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := New("alpha-1", slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		TLSConfig:    serverTLSConfig,
		AllowedPeers: []string{"beta-1"},
	})
	addr := reserveLoopbackAddress(t)
	if err := server.Start(ctx, addr); err != nil {
		t.Fatalf("start peer server: %v", err)
	}

	client := newTLSClient(clientTLSConfig)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/peer/v1/identity", nil)
	if err != nil {
		t.Fatalf("build identity request: %v", err)
	}

	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("expected tls handshake error without client certificate")
	}

	cancel()
	if err := server.Wait(); err != nil {
		t.Fatalf("wait for peer server shutdown: %v", err)
	}
}

func TestServerWaitReturnsNilWhenNotStarted(t *testing.T) {
	t.Parallel()

	server := New("alpha-1", slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})
	if err := server.Wait(); err != nil {
		t.Fatalf("wait on not-started server: %v", err)
	}
}

func TestServerStartRejectsSecondStart(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, _ := mustMutualTLSConfigs(t, fixture)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := New("alpha-1", slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		TLSConfig:    serverTLSConfig,
		AllowedPeers: []string{"beta-1"},
	})
	addr := reserveLoopbackAddress(t)
	if err := server.Start(ctx, addr); err != nil {
		t.Fatalf("start peer server: %v", err)
	}

	if err := server.Start(ctx, reserveLoopbackAddress(t)); err == nil {
		t.Fatal("expected second start to fail")
	}

	cancel()
	if err := server.Wait(); err != nil {
		t.Fatalf("wait for peer server shutdown: %v", err)
	}
}

func TestHandleIdentityRejectsMissingPrincipal(t *testing.T) {
	t.Parallel()

	server := New("alpha-1", slog.New(slog.NewTextHandler(io.Discard, nil)), Config{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/peer/v1/identity", nil)

	server.handleIdentity(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status: got %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
}

func mustMutualTLSConfigs(t *testing.T, fixture tlstesting.MutualFixture) (*tls.Config, *tls.Config) {
	t.Helper()

	serverTLSConfig, err := security.LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.Server.CertFile,
		KeyFile:  fixture.Server.KeyFile,
	}, tls.RequireAndVerifyClientCert)
	if err != nil {
		t.Fatalf("load server tls config: %v", err)
	}

	clientTLSConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		CertFile:   fixture.Client.CertFile,
		KeyFile:    fixture.Client.KeyFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("load client tls config: %v", err)
	}

	return serverTLSConfig, clientTLSConfig
}

func newTLSClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}

func waitForServer(t *testing.T, client *http.Client, rawURL string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
		if err != nil {
			t.Fatalf("build wait request: %v", err)
		}

		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("peer server at %q did not become ready", rawURL)
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close loopback listener: %v", err)
	}

	return address
}
