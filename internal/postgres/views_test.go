package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestQueryRoleReturnsDetectedRole(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
			row: []driver.Value{
				false,
				int64(170002),
				time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC),
				false,
				"7599025879359099984",
				int64(3),
				"0/5000200",
				"0/5000200",
				"",
				"",
				nil,
				int64(1048576),
				int64(0),
			},
		}), nil
	})
	defer restore()

	role, err := QueryRole(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query role: %v", err)
	}

	if role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected role: got %q", role)
	}
}

func TestQueryRoleReturnsConnectError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	role, err := QueryRole(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected connect error")
	}

	if role != cluster.MemberRoleUnknown {
		t.Fatalf("expected unknown role on connect error, got %q", role)
	}
}

func TestClientQueryRoleReturnsObservationError(t *testing.T) {
	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			err: errors.New("query failed"),
		}),
	}

	role, err := client.QueryRole(context.Background())
	if err == nil {
		t.Fatal("expected query error")
	}

	if role != cluster.MemberRoleUnknown {
		t.Fatalf("expected unknown role on error, got %q", role)
	}
}

func TestQueryRecoveryStateReturnsReplicaState(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
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
				time.Date(2026, time.March, 25, 12, 1, 0, 0, time.UTC),
				int64(1048576),
				int64(128),
			},
		}), nil
	})
	defer restore()

	inRecovery, err := QueryRecoveryState(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query recovery state: %v", err)
	}

	if !inRecovery {
		t.Fatal("expected PostgreSQL to be in recovery")
	}
}

func TestQueryRecoveryStateReturnsConnectError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	inRecovery, err := QueryRecoveryState(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected connect error")
	}

	if inRecovery {
		t.Fatal("expected false recovery state on connect error")
	}
}

func TestClientQueryRecoveryStateReturnsObservationError(t *testing.T) {
	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			err: errors.New("query failed"),
		}),
	}

	inRecovery, err := client.QueryRecoveryState(context.Background())
	if err == nil {
		t.Fatal("expected query error")
	}

	if inRecovery {
		t.Fatal("expected false recovery state on error")
	}
}

func TestQuerySystemIdentifierReturnsValue(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
			row: []driver.Value{
				false,
				int64(170002),
				time.Date(2026, time.March, 25, 12, 0, 0, 0, time.UTC),
				false,
				"7599025879359099984",
				int64(3),
				"0/5000200",
				"0/5000200",
				"",
				"",
				nil,
				int64(1048576),
				int64(0),
			},
		}), nil
	})
	defer restore()

	systemIdentifier, err := QuerySystemIdentifier(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query system identifier: %v", err)
	}

	if systemIdentifier != "7599025879359099984" {
		t.Fatalf("unexpected system identifier: got %q", systemIdentifier)
	}
}

func TestQuerySystemIdentifierReturnsConnectError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	systemIdentifier, err := QuerySystemIdentifier(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected connect error")
	}

	if systemIdentifier != "" {
		t.Fatalf("expected empty system identifier on connect error, got %q", systemIdentifier)
	}
}

func TestClientQuerySystemIdentifierReturnsObservationError(t *testing.T) {
	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			err: errors.New("query failed"),
		}),
	}

	systemIdentifier, err := client.QuerySystemIdentifier(context.Background())
	if err == nil {
		t.Fatal("expected query error")
	}

	if systemIdentifier != "" {
		t.Fatalf("expected empty system identifier on error, got %q", systemIdentifier)
	}
}

func TestQueryWALProgressReturnsValue(t *testing.T) {
	replayAt := time.Date(2026, time.March, 25, 12, 1, 0, 0, time.UTC)

	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
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
				replayAt,
				int64(1048576),
				int64(128),
			},
		}), nil
	})
	defer restore()

	wal, err := QueryWALProgress(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query wal progress: %v", err)
	}

	if wal.FlushLSN != "0/7000200" || wal.ReceiveLSN != "0/7000200" || wal.ReplayLSN != "0/7000100" {
		t.Fatalf("unexpected wal progress: got %+v", wal)
	}

	if !wal.ReplayTimestamp.Equal(replayAt) {
		t.Fatalf("unexpected replay timestamp: got %v", wal.ReplayTimestamp)
	}
}

func TestQueryWALProgressReturnsConnectError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	wal, err := QueryWALProgress(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected connect error")
	}

	if wal != (WALProgress{}) {
		t.Fatalf("expected zero wal progress on connect error, got %+v", wal)
	}
}

func TestClientQueryWALProgressReturnsObservationError(t *testing.T) {
	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			err: errors.New("query failed"),
		}),
	}

	wal, err := client.QueryWALProgress(context.Background())
	if err == nil {
		t.Fatal("expected query error")
	}

	if wal != (WALProgress{}) {
		t.Fatalf("expected zero wal progress on error, got %+v", wal)
	}
}
