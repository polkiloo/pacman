package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/polkiloo/pacman/internal/cluster"
)

var openDB = sql.Open

// Observation describes the latest PostgreSQL state collected from a direct
// SQL connection.
type Observation struct {
	Role       cluster.MemberRole
	InRecovery bool
	Details    Details
	WAL        WALProgress
}

// Details describes the latest local PostgreSQL instance details.
type Details struct {
	ServerVersion       int
	PendingRestart      bool
	SystemIdentifier    string
	Timeline            int64
	PostmasterStartAt   time.Time
	ReplicationLagBytes int64
}

// WALProgress describes the latest locally observed PostgreSQL WAL positions.
type WALProgress struct {
	WriteLSN        string
	FlushLSN        string
	ReceiveLSN      string
	ReplayLSN       string
	ReplayTimestamp time.Time
}

// QueryObservation determines the local PostgreSQL runtime observation by
// querying recovery state, system identifier, and timeline over a direct SQL
// connection.
func QueryObservation(ctx context.Context, address string) (Observation, error) {
	client, err := Connect(address)
	if err != nil {
		return unknownObservation(), err
	}
	defer client.Close()

	return client.QueryObservation(ctx)
}

// QueryObservation determines the local PostgreSQL runtime observation through
// the connected PostgreSQL client.
func (client *Client) QueryObservation(ctx context.Context) (Observation, error) {
	row, err := queryObservationRow(ctx, client.db)
	if err != nil {
		return unknownObservation(), err
	}

	return row.observation(), nil
}

type observationRow struct {
	inRecovery          bool
	serverVersion       int
	postmasterStartAt   time.Time
	pendingRestart      bool
	systemIdentifier    string
	timeline            int64
	writeLSN            string
	flushLSN            string
	receiveLSN          string
	replayLSN           string
	replayTimestamp     sql.NullTime
	replicationLagBytes int64
}

func queryObservationRow(ctx context.Context, db *sql.DB) (observationRow, error) {
	var row observationRow
	err := db.QueryRowContext(ctx, queryObservationSQL).Scan(
		&row.inRecovery,
		&row.serverVersion,
		&row.postmasterStartAt,
		&row.pendingRestart,
		&row.systemIdentifier,
		&row.timeline,
		&row.writeLSN,
		&row.flushLSN,
		&row.receiveLSN,
		&row.replayLSN,
		&row.replayTimestamp,
		&row.replicationLagBytes,
	)
	return row, err
}

func (row observationRow) observation() Observation {
	observation := Observation{
		Role:       observationRole(row.inRecovery),
		InRecovery: row.inRecovery,
		Details: Details{
			ServerVersion:       row.serverVersion,
			PendingRestart:      row.pendingRestart,
			SystemIdentifier:    row.systemIdentifier,
			Timeline:            row.timeline,
			PostmasterStartAt:   row.postmasterStartAt,
			ReplicationLagBytes: row.replicationLagBytes,
		},
		WAL: WALProgress{
			WriteLSN:   row.writeLSN,
			FlushLSN:   row.flushLSN,
			ReceiveLSN: row.receiveLSN,
			ReplayLSN:  row.replayLSN,
		},
	}

	if row.replayTimestamp.Valid {
		observation.WAL.ReplayTimestamp = row.replayTimestamp.Time.UTC()
	}

	return observation
}

func unknownObservation() Observation {
	return Observation{Role: cluster.MemberRoleUnknown}
}

func observationRole(inRecovery bool) cluster.MemberRole {
	if inRecovery {
		return cluster.MemberRoleReplica
	}

	return cluster.MemberRolePrimary
}

func connectionString(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}

	parts := []string{
		fmt.Sprintf("host=%s", quoteConnectionValue(host)),
		fmt.Sprintf("port=%s", quoteConnectionValue(port)),
		fmt.Sprintf("sslmode=%s", quoteConnectionValue(envOrDefault("PGSSLMODE", "disable"))),
		"application_name=pacmand",
	}

	if database := strings.TrimSpace(os.Getenv("PGDATABASE")); database != "" {
		parts = append(parts, fmt.Sprintf("dbname=%s", quoteConnectionValue(database)))
	}

	if user := strings.TrimSpace(os.Getenv("PGUSER")); user != "" {
		parts = append(parts, fmt.Sprintf("user=%s", quoteConnectionValue(user)))
	}

	if password := os.Getenv("PGPASSWORD"); password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", quoteConnectionValue(password)))
	}

	return strings.Join(parts, " ")
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}

	return fallback
}

func quoteConnectionValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}
