package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPGCtlStartRunsExpectedCommand(t *testing.T) {
	t.Parallel()

	var gotName string
	var gotArgs []string
	ctl := PGCtl{
		BinDir:  "/usr/lib/postgresql/17/bin",
		DataDir: "/srv/postgres",
		runner: func(_ context.Context, name string, args ...string) (commandResult, error) {
			gotName = name
			gotArgs = append([]string(nil), args...)
			return commandResult{}, nil
		},
	}

	if err := ctl.Start(context.Background()); err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	if gotName != filepath.Join("/usr/lib/postgresql/17/bin", "pg_ctl") {
		t.Fatalf("unexpected command name: got %q", gotName)
	}

	wantArgs := []string{"start", "-D", "/srv/postgres", "-w"}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("unexpected args: got %v, want %v", gotArgs, wantArgs)
	}
}

func TestPGCtlStopRunsExpectedCommand(t *testing.T) {
	t.Parallel()

	var gotArgs []string
	ctl := PGCtl{
		DataDir: "/srv/postgres",
		runner: func(_ context.Context, _ string, args ...string) (commandResult, error) {
			gotArgs = append([]string(nil), args...)
			return commandResult{}, nil
		},
	}

	if err := ctl.Stop(context.Background(), ShutdownModeFast); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}

	wantArgs := []string{"stop", "-D", "/srv/postgres", "-w", "-m", "fast"}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("unexpected args: got %v, want %v", gotArgs, wantArgs)
	}
}

func TestPGCtlStopRejectsInvalidMode(t *testing.T) {
	t.Parallel()

	err := (PGCtl{DataDir: "/srv/postgres"}).Stop(context.Background(), "broken")
	if !errors.Is(err, ErrShutdownModeInvalid) {
		t.Fatalf("expected invalid shutdown mode error, got %v", err)
	}
}

func TestPGCtlStartRequiresDataDir(t *testing.T) {
	t.Parallel()

	err := (PGCtl{}).Start(context.Background())
	if !errors.Is(err, ErrDataDirRequired) {
		t.Fatalf("expected missing data dir error, got %v", err)
	}
}

func TestPGCtlStatusReturnsRunning(t *testing.T) {
	t.Parallel()

	ctl := PGCtl{
		DataDir: "/srv/postgres",
		runner: func(_ context.Context, _ string, args ...string) (commandResult, error) {
			wantArgs := []string{"status", "-D", "/srv/postgres"}
			if !slices.Equal(args, wantArgs) {
				t.Fatalf("unexpected args: got %v, want %v", args, wantArgs)
			}
			return commandResult{output: "server is running"}, nil
		},
	}

	running, err := ctl.Status(context.Background())
	if err != nil {
		t.Fatalf("query status: %v", err)
	}

	if !running {
		t.Fatal("expected postgres to be running")
	}
}

func TestPGCtlStatusReturnsFalseWhenServerIsStopped(t *testing.T) {
	t.Parallel()

	ctl := PGCtl{
		DataDir: "/srv/postgres",
		runner: func(_ context.Context, _ string, _ ...string) (commandResult, error) {
			return commandResult{
				output:   "no server running",
				exitCode: 3,
			}, errors.New("exit status 3")
		},
	}

	running, err := ctl.Status(context.Background())
	if err != nil {
		t.Fatalf("query status: %v", err)
	}

	if running {
		t.Fatal("expected postgres to be reported as stopped")
	}
}

func TestPGCtlStatusWrapsUnexpectedError(t *testing.T) {
	t.Parallel()

	ctl := PGCtl{
		DataDir: "/srv/postgres",
		runner: func(_ context.Context, _ string, _ ...string) (commandResult, error) {
			return commandResult{
				output:   "permission denied",
				exitCode: 1,
			}, errors.New("exit status 1")
		},
	}

	_, err := ctl.Status(context.Background())
	if err == nil {
		t.Fatal("expected status error")
	}

	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unexpected wrapped error: %v", err)
	}
}
