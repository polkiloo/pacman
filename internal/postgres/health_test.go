package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

func TestQueryHealthReturnsOpenError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	_, err := QueryHealth(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected open error")
	}
}

func TestQueryHealthReturnsServerVersion(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
			columns: []string{"server_version"},
			row:     []driver.Value{int64(170002)},
		}), nil
	})
	defer restore()

	health, err := QueryHealth(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query health: %v", err)
	}

	if health.ServerVersion != 170002 {
		t.Fatalf("unexpected server version: got %d", health.ServerVersion)
	}
}

func TestClientQueryHealthReturnsQueryError(t *testing.T) {
	client := &Client{
		db: newProbeTestDB(t, probeTestResponse{
			columns: []string{"server_version"},
			err:     errors.New("query failed"),
		}),
	}

	_, err := client.QueryHealth(context.Background())
	if err == nil {
		t.Fatal("expected query error")
	}
}
