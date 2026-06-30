package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestEnsurePhysicalReplicationSlotUsesPostgresDatabaseAndReplicationCredentials(t *testing.T) {
	db := newProbeTestDB(t, probeTestResponse{
		columns: []string{"ensured"},
		row:     []driver.Value{true},
	})
	var connection string
	restore := replaceOpenDB(t, func(driverName, dataSourceName string) (*sql.DB, error) {
		if driverName != "postgres" {
			t.Fatalf("unexpected driver: %q", driverName)
		}
		connection = dataSourceName
		return db, nil
	})
	defer restore()

	if err := EnsurePhysicalReplicationSlot(context.Background(), "primary.example:5432", "replicator", "secret", "pacman_alpha_1"); err != nil {
		t.Fatalf("ensure physical replication slot: %v", err)
	}
	for _, expected := range []string{"host='primary.example'", "port='5432'", "dbname='postgres'", "user='replicator'", "password='secret'"} {
		if !strings.Contains(connection, expected) {
			t.Fatalf("connection string %q does not contain %q", connection, expected)
		}
	}
}

func TestClientEnsurePhysicalReplicationSlot(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		response    probeTestResponse
		wantErrText string
	}{
		{
			name: "creates or reuses slot on writable primary",
			response: probeTestResponse{
				columns: []string{"ensured"},
				row:     []driver.Value{true},
			},
		},
		{
			name: "rejects standby source",
			response: probeTestResponse{
				columns: []string{"ensured"},
				row:     []driver.Value{false},
			},
			wantErrText: "on a standby",
		},
		{
			name:        "reports query failure",
			response:    probeTestResponse{err: errors.New("slot query failed")},
			wantErrText: "slot query failed",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			client := &Client{db: newProbeTestDB(t, testCase.response)}
			err := client.EnsurePhysicalReplicationSlot(context.Background(), "pacman_alpha_1")
			if testCase.wantErrText == "" {
				if err != nil {
					t.Fatalf("ensure physical replication slot: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.wantErrText) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantErrText, err)
			}
		})
	}
}
