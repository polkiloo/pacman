package httpapi

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/security"
	"github.com/polkiloo/pacman/internal/security/tlstesting"
)

func TestServerStartServesAndShutsDownWithTLS(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.Write(t)

	serverTLSConfig, err := security.LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CertFile: fixture.CertFile,
		KeyFile:  fixture.KeyFile,
	}, tls.NoClientCert)
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

	srv := New("alpha-1", testNodeStatusStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		TLSConfig: serverTLSConfig,
	})
	addr := reserveLoopbackAddress(t)

	if err := srv.Start(ctx, addr); err != nil {
		t.Fatalf("start server: %v", err)
	}

	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: clientTLSConfig,
		},
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/health", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call health endpoint over tls: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unexpected health status: got %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	cancel()

	if err := srv.Wait(); err != nil {
		t.Fatalf("wait for server shutdown: %v", err)
	}
}
