package peerapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Config carries optional peer listener parameters.
type Config struct {
	TLSConfig    *tls.Config
	AllowedPeers []string
}

// Principal captures the authenticated peer identity.
type Principal struct {
	Subject   string `json:"subject"`
	Mechanism string `json:"mechanism"`
}

// IdentityResponse reports the local node serving the request and the
// authenticated peer principal observed on the request connection.
type IdentityResponse struct {
	NodeName string    `json:"nodeName"`
	Peer     Principal `json:"peer"`
}

// Server serves internal peer endpoints on the control-plane listener.
type Server struct {
	nodeName     string
	logger       *slog.Logger
	tlsConfig    *tls.Config
	allowedPeers map[string]struct{}
	httpServer   *http.Server

	mu       sync.Mutex
	runDone  chan struct{}
	runErr   error
	listener net.Listener
	stopping bool
}

// New constructs a peer server with mTLS middleware for cluster-member
// traffic.
func New(nodeName string, logger *slog.Logger, cfg Config) *Server {
	srv := &Server{
		nodeName:     strings.TrimSpace(nodeName),
		logger:       logger,
		tlsConfig:    cfg.TLSConfig,
		allowedPeers: normalizeAllowedPeers(cfg.AllowedPeers),
	}

	mux := http.NewServeMux()
	mux.Handle("/peer/v1/identity", srv.peerAuthMiddleware(http.HandlerFunc(srv.handleIdentity)))

	srv.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	return srv
}

// Start binds addr, serves requests in the background, and shuts down when ctx
// is cancelled.
func (srv *Server) Start(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv.mu.Lock()
	if srv.runDone != nil {
		srv.mu.Unlock()
		_ = listener.Close()
		return errors.New("peer api server is already started")
	}
	srv.runDone = make(chan struct{})
	srv.runErr = nil
	srv.listener = listener
	srv.stopping = false
	done := srv.runDone
	srv.mu.Unlock()

	go func() {
		<-ctx.Done()

		srv.mu.Lock()
		srv.stopping = true
		listener := srv.listener
		srv.mu.Unlock()

		if listener != nil {
			_ = listener.Close()
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.httpServer.Shutdown(shutdownCtx)
	}()

	go func() {
		serveListener := listener
		if srv.tlsConfig != nil {
			serveListener = tls.NewListener(listener, srv.tlsConfig.Clone())
		}

		err := srv.httpServer.Serve(serveListener)

		srv.mu.Lock()
		srv.listener = nil
		if srv.stopping && isClosedListenerError(err) {
			srv.runErr = nil
		} else {
			srv.runErr = err
		}
		srv.mu.Unlock()

		close(done)
	}()

	return nil
}

// Wait blocks until the server stops and returns the terminal serve error, if
// any.
func (srv *Server) Wait() error {
	srv.mu.Lock()
	done := srv.runDone
	srv.mu.Unlock()

	if done == nil {
		return nil
	}

	<-done

	srv.mu.Lock()
	defer srv.mu.Unlock()

	return srv.runErr
}

func (srv *Server) handleIdentity(w http.ResponseWriter, r *http.Request) {
	principal, ok := CurrentPrincipal(r.Context())
	if !ok {
		writeError(w, http.StatusInternalServerError, "peer principal missing from request context")
		return
	}

	writeJSON(w, http.StatusOK, IdentityResponse{
		NodeName: srv.nodeName,
		Peer:     *principal,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"code":    http.StatusText(status),
		"message": message,
	})
}

func normalizeAllowedPeers(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}

	normalized := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		normalized[trimmed] = struct{}{}
	}

	if len(normalized) == 0 {
		return nil
	}

	return normalized
}

func isClosedListenerError(err error) bool {
	return err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) || strings.Contains(err.Error(), "use of closed network connection")
}
