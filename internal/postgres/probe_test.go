package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestConnectionStringUsesDefaults(t *testing.T) {
	t.Parallel()

	got := connectionString("127.0.0.1:5432")

	wantParts := []string{
		"host='127.0.0.1'",
		"port='5432'",
		"sslmode='disable'",
		"application_name=pacmand",
	}

	for _, wantPart := range wantParts {
		if !strings.Contains(got, wantPart) {
			t.Fatalf("connection string %q does not contain %q", got, wantPart)
		}
	}
}

func TestConnectionStringReturnsEmptyForInvalidAddress(t *testing.T) {
	t.Parallel()

	if got := connectionString("invalid-address"); got != "" {
		t.Fatalf("expected empty connection string for invalid address, got %q", got)
	}
}

func TestQueryObservationReturnsOpenError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	})
	defer restore()

	observation, err := QueryObservation(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected open error")
	}

	if observation.Role != cluster.MemberRoleUnknown {
		t.Fatalf("expected unknown role, got %q", observation.Role)
	}
}

func TestQueryObservationReturnsQueryError(t *testing.T) {
	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{err: errors.New("query failed")}), nil
	})
	defer restore()

	observation, err := QueryObservation(context.Background(), "127.0.0.1:5432")
	if err == nil {
		t.Fatal("expected query error")
	}

	if observation.Role != cluster.MemberRoleUnknown {
		t.Fatalf("expected unknown role, got %q", observation.Role)
	}
}

func TestQueryObservationMapsPrimaryObservation(t *testing.T) {
	postmasterStartAt := time.Date(2026, time.March, 25, 10, 0, 0, 0, time.UTC)

	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
			row: []driver.Value{
				false,
				int64(170002),
				postmasterStartAt,
				false,
				"7599025879359099984",
				int64(3),
				"0/5000200",
				"0/5000200",
				"",
				"",
				nil,
				int64(0),
			},
		}), nil
	})
	defer restore()

	observation, err := QueryObservation(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query observation: %v", err)
	}

	if observation.Role != cluster.MemberRolePrimary {
		t.Fatalf("expected primary role, got %q", observation.Role)
	}

	if observation.InRecovery {
		t.Fatalf("expected primary observation, got %+v", observation)
	}

	if observation.Details.ServerVersion != 170002 {
		t.Fatalf("unexpected server version: got %d", observation.Details.ServerVersion)
	}

	if observation.Details.SystemIdentifier != "7599025879359099984" {
		t.Fatalf("unexpected system identifier: got %q", observation.Details.SystemIdentifier)
	}

	if observation.Details.Timeline != 3 {
		t.Fatalf("unexpected timeline: got %d", observation.Details.Timeline)
	}

	if !observation.Details.PostmasterStartAt.Equal(postmasterStartAt) {
		t.Fatalf("unexpected postmaster start time: got %v", observation.Details.PostmasterStartAt)
	}

	if observation.WAL.WriteLSN != "0/5000200" || observation.WAL.FlushLSN != "0/5000200" {
		t.Fatalf("unexpected WAL progress: got %+v", observation.WAL)
	}
}

func TestQueryObservationMapsReplicaObservation(t *testing.T) {
	postmasterStartAt := time.Date(2026, time.March, 25, 10, 0, 0, 0, time.UTC)
	replayAt := time.Date(2026, time.March, 25, 10, 5, 0, 0, time.UTC)

	restore := replaceOpenDB(t, func(string, string) (*sql.DB, error) {
		return newProbeTestDB(t, probeTestResponse{
			row: []driver.Value{
				true,
				int64(170002),
				postmasterStartAt,
				true,
				"7599025879359099984",
				int64(7),
				"",
				"0/7000200",
				"0/7000200",
				"0/7000100",
				replayAt,
				int64(256),
			},
		}), nil
	})
	defer restore()

	observation, err := QueryObservation(context.Background(), "127.0.0.1:5432")
	if err != nil {
		t.Fatalf("query observation: %v", err)
	}

	if observation.Role != cluster.MemberRoleReplica {
		t.Fatalf("expected replica role, got %q", observation.Role)
	}

	if !observation.InRecovery {
		t.Fatalf("expected replica observation, got %+v", observation)
	}

	if !observation.Details.PendingRestart {
		t.Fatalf("expected pending restart, got %+v", observation.Details)
	}

	if observation.Details.ReplicationLagBytes != 256 {
		t.Fatalf("unexpected replication lag: got %d", observation.Details.ReplicationLagBytes)
	}

	if observation.WAL.ReceiveLSN != "0/7000200" || observation.WAL.ReplayLSN != "0/7000100" {
		t.Fatalf("unexpected WAL progress: got %+v", observation.WAL)
	}

	if !observation.WAL.ReplayTimestamp.Equal(replayAt) {
		t.Fatalf("unexpected replay timestamp: got %v", observation.WAL.ReplayTimestamp)
	}
}

func replaceOpenDB(t *testing.T, replacement func(string, string) (*sql.DB, error)) func() {
	t.Helper()

	previous := openDB
	openDB = replacement
	return func() {
		openDB = previous
	}
}

type probeTestResponse struct {
	columns []string
	row     []driver.Value
	err     error
}

var (
	probeTestDriverOnce sync.Once
	probeTestRegistry   = struct {
		sync.Mutex
		sequence  int
		responses map[string]probeTestResponse
	}{
		responses: make(map[string]probeTestResponse),
	}
)

func newProbeTestDB(t *testing.T, response probeTestResponse) *sql.DB {
	t.Helper()

	probeTestDriverOnce.Do(func() {
		sql.Register("probe-test", probeTestDriver{})
	})

	probeTestRegistry.Lock()
	probeTestRegistry.sequence++
	name := fmt.Sprintf("probe-test-%d", probeTestRegistry.sequence)
	probeTestRegistry.responses[name] = response
	probeTestRegistry.Unlock()

	t.Cleanup(func() {
		probeTestRegistry.Lock()
		delete(probeTestRegistry.responses, name)
		probeTestRegistry.Unlock()
	})

	db, err := sql.Open("probe-test", name)
	if err != nil {
		t.Fatalf("open probe test db: %v", err)
	}

	return db
}

type probeTestDriver struct{}

func (probeTestDriver) Open(name string) (driver.Conn, error) {
	probeTestRegistry.Lock()
	response, ok := probeTestRegistry.responses[name]
	probeTestRegistry.Unlock()
	if !ok {
		return nil, fmt.Errorf("probe test response %q not found", name)
	}

	return probeTestConn{response: response}, nil
}

type probeTestConn struct {
	response probeTestResponse
}

func (conn probeTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented")
}

func (conn probeTestConn) Close() error {
	return nil
}

func (conn probeTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not implemented")
}

func (conn probeTestConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if conn.response.err != nil {
		return nil, conn.response.err
	}

	return &probeTestRows{
		columns: conn.response.columns,
		values:  conn.response.row,
	}, nil
}

type probeTestRows struct {
	sent    bool
	columns []string
	values  []driver.Value
}

func (rows *probeTestRows) Columns() []string {
	if len(rows.columns) > 0 {
		return rows.columns
	}

	return []string{
		"in_recovery",
		"server_version",
		"postmaster_start_at",
		"pending_restart",
		"system_identifier",
		"timeline_id",
		"write_lsn",
		"flush_lsn",
		"receive_lsn",
		"replay_lsn",
		"replay_timestamp",
		"replication_lag_bytes",
	}
}

func (rows *probeTestRows) Close() error {
	return nil
}

func (rows *probeTestRows) Next(dest []driver.Value) error {
	if rows.sent {
		return io.EOF
	}

	copy(dest, rows.values)
	rows.sent = true
	return nil
}

func TestConnectionStringUsesEnvironmentOverrides(t *testing.T) {
	t.Setenv("PGSSLMODE", "require")
	t.Setenv("PGDATABASE", "app-db")
	t.Setenv("PGUSER", "app-user")
	t.Setenv("PGPASSWORD", `p@ss'word\`)

	got := connectionString("db.internal:6432")

	wantParts := []string{
		"host='db.internal'",
		"port='6432'",
		"sslmode='require'",
		"dbname='app-db'",
		"user='app-user'",
		`password='p@ss\'word\\'`,
	}

	for _, wantPart := range wantParts {
		if !strings.Contains(got, wantPart) {
			t.Fatalf("connection string %q does not contain %q", got, wantPart)
		}
	}
}
