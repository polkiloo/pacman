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

// QueryRoleAndRecoveryState determines the local PostgreSQL member role by
// querying pg_is_in_recovery() over a direct SQL connection.
func QueryRoleAndRecoveryState(ctx context.Context, address string) (cluster.MemberRole, bool, error) {
	db, err := sql.Open("postgres", connectionString(address))
	if err != nil {
		return cluster.MemberRoleUnknown, false, err
	}
	defer db.Close()

	var inRecovery bool
	if err := db.QueryRowContext(ctx, "select pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return cluster.MemberRoleUnknown, false, err
	}

	if inRecovery {
		return cluster.MemberRoleReplica, true, nil
	}

	return cluster.MemberRolePrimary, false, nil
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
