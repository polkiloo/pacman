package agent

import (
	"testing"

	"github.com/polkiloo/pacman/internal/config"
)

func TestPatroniInspiredBootstrapPostgresParametersAreCopied(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		Postgres: &config.PostgresLocalConfig{
			Parameters: map[string]string{
				"max_connections": "200",
				"wal_level":       "replica",
			},
		},
	}

	parameters := bootstrapPostgresParameters(cfg)
	if parameters["max_connections"] != "200" || parameters["wal_level"] != "replica" {
		t.Fatalf("unexpected bootstrap postgres parameters: got %+v", parameters)
	}

	cfg.Postgres.Parameters["max_connections"] = "50"
	parameters["wal_level"] = "minimal"

	if parameters["max_connections"] != "200" {
		t.Fatalf("expected bootstrap parameters to be isolated from config mutation, got %+v", parameters)
	}
	if cfg.Postgres.Parameters["wal_level"] != "replica" {
		t.Fatalf("expected config parameters to be isolated from bootstrap mutation, got %+v", cfg.Postgres.Parameters)
	}
}

func TestPatroniInspiredBootstrapPostgresParametersOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		cfg  config.Config
	}{
		{name: "nil postgres"},
		{name: "empty parameters", cfg: config.Config{Postgres: &config.PostgresLocalConfig{}}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := bootstrapPostgresParameters(testCase.cfg); got != nil {
				t.Fatalf("expected nil bootstrap postgres parameters, got %+v", got)
			}
		})
	}
}

func TestPatroniInspiredBootstrapMemberSpecsPreserveExpectedMemberOrder(t *testing.T) {
	t.Parallel()

	members := bootstrapMemberSpecs([]string{"alpha-1", "alpha-2", "alpha-witness"})

	if len(members) != 3 {
		t.Fatalf("unexpected member count: got %+v", members)
	}

	for index, wantName := range []string{"alpha-1", "alpha-2", "alpha-witness"} {
		if members[index].Name != wantName {
			t.Fatalf("unexpected member at %d: got %+v, want %q", index, members[index], wantName)
		}
	}
}
