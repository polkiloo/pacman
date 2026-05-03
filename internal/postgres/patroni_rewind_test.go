package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPatroniInspiredPGRewindTrimsInputsAndUsesDefaultBinary(t *testing.T) {
	t.Parallel()

	var gotName string
	var gotArgs []string
	rewind := PGRewind{
		DataDir:      " /srv/postgres ",
		SourceServer: " host=primary port=5432 user=rewind ",
		runner: func(_ context.Context, name string, args ...string) (commandResult, error) {
			gotName = name
			gotArgs = append([]string(nil), args...)
			return commandResult{}, nil
		},
	}

	if err := rewind.Run(context.Background()); err != nil {
		t.Fatalf("run pg_rewind: %v", err)
	}

	if gotName != "pg_rewind" {
		t.Fatalf("unexpected default pg_rewind binary: got %q", gotName)
	}

	wantArgs := []string{
		"--target-pgdata", "/srv/postgres",
		"--source-server", "host=primary port=5432 user=rewind",
		"--progress",
	}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("unexpected trimmed pg_rewind args: got %v, want %v", gotArgs, wantArgs)
	}
}

func TestPatroniInspiredPGRewindDoesNotCallRunnerWhenInputsAreInvalid(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		rewind PGRewind
		want   error
	}{
		{
			name: "missing data dir",
			rewind: PGRewind{
				SourceServer: "host=primary port=5432",
			},
			want: ErrDataDirRequired,
		},
		{
			name: "missing source server",
			rewind: PGRewind{
				DataDir: "/srv/postgres",
			},
			want: ErrSourceServerRequired,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			called := false
			rewind := testCase.rewind
			rewind.runner = func(context.Context, string, ...string) (commandResult, error) {
				called = true
				return commandResult{}, nil
			}

			err := rewind.Run(context.Background())
			if !errors.Is(err, testCase.want) {
				t.Fatalf("unexpected pg_rewind validation error: got %v, want %v", err, testCase.want)
			}
			if called {
				t.Fatal("expected invalid pg_rewind inputs to skip command runner")
			}
		})
	}
}

func TestPatroniInspiredPGRewindWrapsFailureWithoutOutput(t *testing.T) {
	t.Parallel()

	rewind := PGRewind{
		BinDir:       "/usr/lib/postgresql/17/bin",
		DataDir:      "/srv/postgres",
		SourceServer: "host=primary",
		runner: func(_ context.Context, name string, _ ...string) (commandResult, error) {
			if name != filepath.Join("/usr/lib/postgresql/17/bin", "pg_rewind") {
				t.Fatalf("unexpected pg_rewind path: got %q", name)
			}
			return commandResult{exitCode: 1}, errors.New("exit status 1")
		},
	}

	err := rewind.Run(context.Background())
	if err == nil {
		t.Fatal("expected pg_rewind failure")
	}

	if !strings.Contains(err.Error(), "run pg_rewind: exit status 1") {
		t.Fatalf("unexpected pg_rewind error without output: %v", err)
	}
}
