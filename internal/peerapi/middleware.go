package peerapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"net/http"
	"strings"
)

type principalContextKey struct{}

// CurrentPrincipal returns the peer principal attached by the mTLS middleware,
// if present.
func CurrentPrincipal(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(*Principal)
	if !ok || principal == nil {
		return nil, false
	}

	return principal, true
}

func (srv *Server) peerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := srv.resolvePeerPrincipal(r.TLS)
		if err != nil {
			if srv.logger != nil {
				srv.logger.Warn(
					"rejected peer request",
					slog.String("component", "peerapi"),
					slog.String("remote_addr", r.RemoteAddr),
					slog.String("path", r.URL.Path),
					slog.String("error", err.Error()),
				)
			}

			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal)))
	})
}

func (srv *Server) resolvePeerPrincipal(state *tls.ConnectionState) (*Principal, error) {
	if state == nil {
		return nil, errPeerTLSRequired
	}

	if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return nil, errPeerCertificateRequired
	}

	leaf := state.PeerCertificates[0]
	subject := peerSubject(leaf)
	if subject == "" {
		return nil, errPeerSubjectRequired
	}

	if len(srv.allowedPeers) > 0 {
		if _, ok := srv.allowedPeers[subject]; !ok {
			return nil, errPeerSubjectUnauthorized
		}
	}

	return &Principal{
		Subject:   subject,
		Mechanism: "mtls",
	}, nil
}

func peerSubject(leaf *x509.Certificate) string {
	if leaf == nil {
		return ""
	}

	return strings.TrimSpace(leaf.Subject.CommonName)
}
