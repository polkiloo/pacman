package agent

import (
	"context"
	"time"

	"github.com/polkiloo/pacman/internal/postgres"
)

// Option customizes daemon construction for tests and future runtime hooks.
type Option func(*Daemon)

type postgresAvailabilityProbe func(context.Context, string) error
type postgresStateProbe func(context.Context, string) (postgres.Observation, error)

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

func withProbeTimeout(timeout time.Duration) Option {
	return func(daemon *Daemon) {
		if timeout > 0 {
			daemon.probeTimeout = timeout
		}
	}
}
