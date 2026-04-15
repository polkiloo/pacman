package agent

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/postgres"
)

// Option customizes daemon construction for tests and future runtime hooks.
type Option func(*Daemon)

type postgresAvailabilityProbe func(context.Context, string) error
type postgresStateProbe func(context.Context, string) (postgres.Observation, error)

// WithNoAPIServer disables the HTTP API server for the daemon. Use this in
// tests that verify control-plane state without needing a bound network address.
func WithNoAPIServer() Option {
	return func(daemon *Daemon) {
		daemon.apiServerDisabled = true
		daemon.apiServer = nil
	}
}

// WithAPIServerTLSConfig overrides the TLS configuration used by the daemon's
// external HTTP API server.
func WithAPIServerTLSConfig(tlsConfig *tls.Config) Option {
	return func(daemon *Daemon) {
		daemon.apiTLSConfig = tlsConfig
	}
}

// WithHTTPAPIMiddlewareFactory registers an HTTP API middleware builder that
// will be evaluated against the daemon's live state store when the API server
// is constructed.
func WithHTTPAPIMiddlewareFactory(factory httpapi.MiddlewareFactory) Option {
	return func(daemon *Daemon) {
		if factory != nil {
			daemon.apiMiddlewares = append(daemon.apiMiddlewares, factory)
		}
	}
}

// WithPeerServerTLSConfig overrides the TLS configuration used by the daemon's
// internal control-plane peer listener.
func WithPeerServerTLSConfig(tlsConfig *tls.Config) Option {
	return func(daemon *Daemon) {
		daemon.peerServerTLSConfig = tlsConfig
	}
}

// WithPeerClientTLSConfig overrides the TLS configuration used by the daemon's
// outbound control-plane peer client.
func WithPeerClientTLSConfig(tlsConfig *tls.Config) Option {
	return func(daemon *Daemon) {
		daemon.peerClientTLSConfig = tlsConfig
	}
}

// WithControlPlanePublisher overrides the control-plane publisher used by the
// daemon to publish local observed state.
func WithControlPlanePublisher(publisher controlplane.NodeStatePublisher) Option {
	return func(daemon *Daemon) {
		if publisher != nil {
			daemon.statePublisher = publisher
		}
	}
}

func withNow(now func() time.Time) Option {
	return func(daemon *Daemon) {
		if now != nil {
			daemon.now = now
		}
	}
}

func withHeartbeatInterval(interval time.Duration) Option {
	return func(daemon *Daemon) {
		if interval > 0 {
			daemon.heartbeatInterval = interval
		}
	}
}

func withPostgresProbe(probe postgresAvailabilityProbe) Option {
	return func(daemon *Daemon) {
		if probe != nil {
			daemon.postgresProbe = probe
		}
	}
}

func withPostgresStateProbe(probe postgresStateProbe) Option {
	return func(daemon *Daemon) {
		if probe != nil {
			daemon.postgresStateProbe = probe
		}
	}
}
