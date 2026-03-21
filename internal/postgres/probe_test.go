package postgres

import (
	"strings"
	"testing"
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
