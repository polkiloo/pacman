package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPGRewindRunExecutesExpectedCommand(t *testing.T) {
	t.Parallel()

	var gotName string
	var gotArgs []string
	rewind := PGRewind{
		BinDir:       "/usr/lib/postgresql/17/bin",
		DataDir:      "/srv/postgres",
		SourceServer: "host=primary port=5432 user=rewind",
		runner: func(_ context.Context, name string, args ...string) (commandResult, error) {
			gotName = name
			gotArgs = append([]string(nil), args...)
			return commandResult{}, nil
		},
	}

	if err := rewind.Run(context.Background()); err != nil {
		t.Fatalf("run pg_rewind: %v", err)
	}

	if gotName != filepath.Join("/usr/lib/postgresql/17/bin", "pg_rewind") {
		t.Fatalf("unexpected command name: got %q", gotName)
	}

	wantArgs := []string{
		"--target-pgdata", "/srv/postgres",
		"--source-server", "host=primary port=5432 user=rewind",
		"--progress",
	}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("unexpected args: got %v, want %v", gotArgs, wantArgs)
	}
}

func TestPGRewindRunRequiresDataDir(t *testing.T) {
	t.Parallel()

	err := (PGRewind{SourceServer: "host=primary"}).Run(context.Background())
	if !errors.Is(err, ErrDataDirRequired) {
		t.Fatalf("expected missing data dir error, got %v", err)
	}
}

func TestPGRewindRunRequiresSourceServer(t *testing.T) {
	t.Parallel()

	err := (PGRewind{DataDir: "/srv/postgres"}).Run(context.Background())
	if !errors.Is(err, ErrSourceServerRequired) {
		t.Fatalf("expected missing source server error, got %v", err)
	}
}

func TestPGRewindRunWrapsCommandError(t *testing.T) {
	t.Parallel()

	rewind := PGRewind{
		DataDir:      "/srv/postgres",
		SourceServer: "host=primary",
		runner: func(_ context.Context, _ string, _ ...string) (commandResult, error) {
			return commandResult{
				output:   "timeline fork not found",
				exitCode: 1,
			}, errors.New("exit status 1")
		},
	}

	err := rewind.Run(context.Background())
	if err == nil {
		t.Fatal("expected rewind error")
	}

	if !strings.Contains(err.Error(), "timeline fork not found") {
		t.Fatalf("unexpected wrapped error: %v", err)
	}
}
