package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"

	_ "github.com/lib/pq"

	"github.com/polkiloo/pacman/internal/cluster"
)

// Observation describes the latest PostgreSQL state collected from a direct
// SQL connection.
type Observation struct {
	Role             cluster.MemberRole
	InRecovery       bool
	SystemIdentifier string
	Timeline         int64
}

// QueryObservation determines the local PostgreSQL runtime observation by
// querying recovery state, system identifier, and timeline over a direct SQL
// connection.
func QueryObservation(ctx context.Context, address string) (Observation, error) {
	db, err := sql.Open("postgres", connectionString(address))
	if err != nil {
		return Observation{Role: cluster.MemberRoleUnknown}, err
	}
	defer db.Close()

	var observation Observation
	if err := db.QueryRowContext(
		ctx,
		`select
			pg_is_in_recovery(),
			system.system_identifier::text,
			checkpoint.timeline_id
		from pg_control_system() as system
		cross join pg_control_checkpoint() as checkpoint`,
	).Scan(&observation.InRecovery, &observation.SystemIdentifier, &observation.Timeline); err != nil {
		return Observation{Role: cluster.MemberRoleUnknown}, err
	}

	if observation.InRecovery {
		observation.Role = cluster.MemberRoleReplica
		return observation, nil
	}

	observation.Role = cluster.MemberRolePrimary
	return observation, nil
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
