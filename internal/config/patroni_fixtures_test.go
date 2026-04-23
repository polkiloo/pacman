package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPatroniUpstreamFixturesDecodeWithWarnings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		fixture           string
		wantNodeName      string
		wantAPIAddress    string
		wantListenAddress string
		wantPort          int
		wantWarnings      []string
	}{
		{
			name:              "postgres0",
			fixture:           "postgres0.yml",
			wantNodeName:      "postgresql0",
			wantAPIAddress:    "127.0.0.1:8008",
			wantListenAddress: "127.0.0.1",
			wantPort:          5432,
			wantWarnings: []string{
				`bootstrap.dcs.loop_wait`,
				`bootstrap.dcs.maximum_lag_on_failover`,
				`bootstrap.dcs.postgresql.use_pg_rewind`,
				`bootstrap.dcs.postgresql.pg_hba`,
				`bootstrap.initdb`,
				`postgresql.pgpass`,
				`postgresql.authentication`,
				`Patroni key "tags"`,
			},
		},
		{
			name:              "postgres1",
			fixture:           "postgres1.yml",
			wantNodeName:      "postgresql1",
			wantAPIAddress:    "127.0.0.1:8009",
			wantListenAddress: "127.0.0.1",
			wantPort:          5433,
			wantWarnings: []string{
				`bootstrap.dcs.loop_wait`,
				`bootstrap.dcs.maximum_lag_on_failover`,
				`bootstrap.dcs.postgresql.use_pg_rewind`,
				`bootstrap.dcs.postgresql.pg_hba`,
				`bootstrap.initdb`,
				`postgresql.pgpass`,
				`postgresql.authentication`,
				`postgresql.basebackup`,
				`Patroni key "tags"`,
			},
		},
		{
			name:              "postgres2",
			fixture:           "postgres2.yml",
			wantNodeName:      "postgresql2",
			wantAPIAddress:    "127.0.0.1:8010",
			wantListenAddress: "127.0.0.1",
			wantPort:          5434,
			wantWarnings: []string{
				`restapi.authentication`,
				`bootstrap.dcs.loop_wait`,
				`bootstrap.dcs.maximum_lag_on_failover`,
				`bootstrap.dcs.postgresql.use_pg_rewind`,
				`bootstrap.dcs.postgresql.pg_hba`,
				`bootstrap.initdb`,
				`postgresql.pgpass`,
				`postgresql.authentication`,
				`Patroni key "tags"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body, err := os.ReadFile(filepath.Join("testdata", "patroni", tt.fixture))
			if err != nil {
				t.Fatalf("read Patroni fixture %q: %v", tt.fixture, err)
			}

			report, err := DecodeWithReport(strings.NewReader(string(body)))
			if err != nil {
				t.Fatalf("decode Patroni fixture %q: %v", tt.fixture, err)
			}

			if report.Format != DocumentFormatPatroni {
				t.Fatalf("unexpected fixture format: got %q, want %q", report.Format, DocumentFormatPatroni)
			}

			if report.Config.Node.Name != tt.wantNodeName {
				t.Fatalf("unexpected translated node name: got %q, want %q", report.Config.Node.Name, tt.wantNodeName)
			}

			if report.Config.Node.APIAddress != tt.wantAPIAddress {
				t.Fatalf("unexpected translated apiAddress: got %q, want %q", report.Config.Node.APIAddress, tt.wantAPIAddress)
			}

			if report.Config.Postgres == nil {
				t.Fatal("expected translated postgres config")
			}

			if report.Config.Postgres.ListenAddress != tt.wantListenAddress || report.Config.Postgres.Port != tt.wantPort {
				t.Fatalf("unexpected translated postgres listen config: %+v", report.Config.Postgres)
			}

			if got := report.Config.Postgres.Parameters["unix_socket_directories"]; got != ".." {
				t.Fatalf("expected unix_socket_directories to be translated, got %q", got)
			}

			warnings := strings.Join(report.Warnings, "\n")
			for _, wantWarning := range tt.wantWarnings {
				assertContains(t, warnings, wantWarning)
			}
		})
	}
}
