package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

func TestEstimateLagReturnsZeroForPrimary(t *testing.T) {
	t.Parallel()

	lag := EstimateLag(Observation{
		Role:       "primary",
		InRecovery: false,
		Details: Details{
			ReplicationLagBytes: 512,
		},
		WAL: WALProgress{
			ReplayTimestamp: time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC),
		},
	}, time.Date(2026, time.March, 25, 12, 1, 0, 0, time.UTC))

	if lag.Bytes != 512 {
		t.Fatalf("unexpected lag bytes: got %d", lag.Bytes)
	}

	if lag.ReplayDelay != 0 {
		t.Fatalf("expected zero replay delay, got %s", lag.ReplayDelay)
	}
}

func TestEstimateLagReturnsReplicaBytesAndDelay(t *testing.T) {
	t.Parallel()

	lag := EstimateLag(Observation{
		InRecovery: true,
		Details: Details{
			ReplicationLagBytes: 256,
		},
		WAL: WALProgress{
			ReplayTimestamp: time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC),
		},
	}, time.Date(2026, time.March, 25, 12, 5, 0, 0, time.UTC))

	if lag.Bytes != 256 {
		t.Fatalf("unexpected lag bytes: got %d", lag.Bytes)
	}

	if lag.ReplayDelay != 5*time.Minute {
		t.Fatalf("unexpected replay delay: got %s", lag.ReplayDelay)
	}
}

func TestEstimateLagClampsNegativeValues(t *testing.T) {
	t.Parallel()

	lag := EstimateLag(Observation{
		InRecovery: true,
		Details: Details{
			ReplicationLagBytes: -1,
		},
		WAL: WALProgress{
			ReplayTimestamp: time.Date(2026, time.March, 25, 12, 5, 0, 0, time.UTC),
		},
	}, time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC))

	if lag.Bytes != 0 {
		t.Fatalf("expected lag bytes to be clamped to zero, got %d", lag.Bytes)
	}

	if lag.ReplayDelay != 0 {
		t.Fatalf("expected replay delay to be clamped to zero, got %s", lag.ReplayDelay)
	}
}

func TestQueryLagReturnsConnectError(t *testing.T) {
	restoreOpen := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restoreOpen()

	lag, err := QueryLag(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected connect error")
	}

	if lag != (Lag{}) {
		t.Fatalf("expected zero lag on error, got %+v", lag)
	}
}

func TestClientQueryLagReturnsEstimate(t *testing.T) {
	restoreNow := replaceLagNow(t, func() time.Time {
		return time.Date(2026, time.March, 25, 12, 5, 0, 0, time.UTC)
	})
	defer restoreNow()

	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			row: []driver.Value{
				true,
				int64(170002),
				time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC),
				false,
				"7599025879359099984",
				int64(7),
				"",
				"0/7000200",
				"0/7000200",
				"0/7000100",
				time.Date(2026, time.March, 25, 12, 4, 30, 0, time.UTC),
				int64(128),
			},
		}),
	}

	lag, err := client.QueryLag(context.Background())
	if err != nil {
		t.Fatalf("query lag: %v", err)
	}

	if lag.Bytes != 128 {
		t.Fatalf("unexpected lag bytes: got %d", lag.Bytes)
	}

	if lag.ReplayDelay != 30*time.Second {
		t.Fatalf("unexpected replay delay: got %s", lag.ReplayDelay)
	}
}

func TestClientQueryLagReturnsObservationError(t *testing.T) {
	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			err: errors.New("query failed"),
		}),
	}

	lag, err := client.QueryLag(context.Background())
	if err == nil {
		t.Fatal("expected query error")
	}

	if lag != (Lag{}) {
		t.Fatalf("expected zero lag on error, got %+v", lag)
	}
}

func replaceLagNow(t *testing.T, replacement func() time.Time) func() {
	t.Helper()

	previous := lagNow
	lagNow = replacement
	return func() {
		lagNow = previous
	}
}
