package postgres

import (
	"context"
	"time"
)

// Lag describes the latest locally estimated PostgreSQL replication lag.
type Lag struct {
	Bytes       int64
	ReplayDelay time.Duration
}

var lagNow = time.Now

// EstimateLag derives a lag estimate from the latest PostgreSQL observation.
func EstimateLag(observation Observation, observedAt time.Time) Lag {
	lag := Lag{
		Bytes: maxInt64(observation.Details.ReplicationLagBytes, 0),
	}

	if !observation.InRecovery || observation.WAL.ReplayTimestamp.IsZero() {
		return lag
	}

	replayDelay := observedAt.UTC().Sub(observation.WAL.ReplayTimestamp.UTC())
	lag.ReplayDelay = maxDuration(replayDelay, 0)
	return lag
}

// QueryLag estimates the latest replication lag for the PostgreSQL instance at
// the given address.
func QueryLag(ctx context.Context, address string) (Lag, error) {
	client, err := Connect(address)
	if err != nil {
		return Lag{}, err
	}
	defer client.Close()

	return client.QueryLag(ctx)
}

// QueryLag estimates the latest replication lag through the connected
// PostgreSQL client.
func (client *Client) QueryLag(ctx context.Context) (Lag, error) {
	observation, err := client.QueryObservation(ctx)
	if err != nil {
		return Lag{}, err
	}

	return EstimateLag(observation, lagNow().UTC()), nil
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}

	return right
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}

	return right
}
