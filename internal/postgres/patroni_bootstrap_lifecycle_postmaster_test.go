package postgres

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestPatroniInspiredStandbyBootstrapRejectsWhitespacePrimaryConnInfo(t *testing.T) {
	t.Parallel()

	_, err := RenderStandbyFiles(" /srv/postgres ", StandbyConfig{
		PrimaryConnInfo: " \t\n",
	})
	if !errors.Is(err, ErrPrimaryConnInfoRequired) {
		t.Fatalf("expected primary conninfo error, got %v", err)
	}
}

func TestPatroniInspiredStandbyBootstrapTrimsDataDir(t *testing.T) {
	t.Parallel()

	rendered, err := RenderStandbyFiles(" /srv/postgres ", StandbyConfig{
		PrimaryConnInfo: "host=primary port=5432",
	})
	if err != nil {
		t.Fatalf("render standby files: %v", err)
	}

	if rendered.PostgresAutoConfPath != "/srv/postgres/postgresql.auto.conf" {
		t.Fatalf("unexpected auto conf path: got %q", rendered.PostgresAutoConfPath)
	}
	if rendered.StandbySignalPath != "/srv/postgres/standby.signal" {
		t.Fatalf("unexpected standby signal path: got %q", rendered.StandbySignalPath)
	}
}

func TestPatroniInspiredPGCtlLifecycleAcceptsAllShutdownModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []ShutdownMode{ShutdownModeSmart, ShutdownModeFast, ShutdownModeImmediate} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			if !mode.IsValid() {
				t.Fatalf("expected shutdown mode %q to be valid", mode)
			}

			var calls [][]string
			ctl := PGCtl{
				DataDir: "/srv/postgres",
				runner: func(_ context.Context, _ string, args ...string) (commandResult, error) {
					calls = append(calls, append([]string(nil), args...))
					return commandResult{}, nil
				},
			}

			if err := ctl.Stop(context.Background(), mode); err != nil {
				t.Fatalf("stop postgres with %s: %v", mode, err)
			}
			if err := ctl.Restart(context.Background(), mode); err != nil {
				t.Fatalf("restart postgres with %s: %v", mode, err)
			}

			wantStop := []string{"stop", "-D", "/srv/postgres", "-w", "-m", string(mode)}
			wantRestart := []string{"restart", "-D", "/srv/postgres", "-w", "-m", string(mode)}
			if len(calls) != 2 || !slices.Equal(calls[0], wantStop) || !slices.Equal(calls[1], wantRestart) {
				t.Fatalf("unexpected lifecycle calls: got %v, want %v then %v", calls, wantStop, wantRestart)
			}
		})
	}
}

func TestPatroniInspiredPGCtlStartNoWaitTreatsAlreadyRunningRaceAsSuccess(t *testing.T) {
	t.Parallel()

	var calls [][]string
	ctl := PGCtl{
		DataDir: "/srv/postgres",
		runner: func(_ context.Context, _ string, args ...string) (commandResult, error) {
			calls = append(calls, append([]string(nil), args...))
			if args[0] == "status" {
				return commandResult{exitCode: 3}, errors.New("server not running")
			}

			return commandResult{
				output:   "pg_ctl: another server might be running; server is already running",
				exitCode: 1,
			}, errors.New("exit status 1")
		},
	}

	if err := ctl.StartNoWait(context.Background()); err != nil {
		t.Fatalf("start no-wait during already-running race: %v", err)
	}

	if len(calls) != 2 {
		t.Fatalf("expected status and start calls, got %v", calls)
	}
}

func TestPatroniInspiredPGCtlLifecycleActionsRequireDataDir(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		run  func(PGCtl) error
	}{
		{name: "status", run: func(ctl PGCtl) error {
			_, err := ctl.Status(context.Background())
			return err
		}},
		{name: "promote", run: func(ctl PGCtl) error { return ctl.Promote(context.Background()) }},
		{name: "reload", run: func(ctl PGCtl) error { return ctl.Reload(context.Background()) }},
		{name: "stop", run: func(ctl PGCtl) error { return ctl.Stop(context.Background(), ShutdownModeFast) }},
		{name: "restart", run: func(ctl PGCtl) error { return ctl.Restart(context.Background(), ShutdownModeFast) }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.run(PGCtl{})
			if !errors.Is(err, ErrDataDirRequired) {
				t.Fatalf("expected data dir error, got %v", err)
			}
		})
	}
}

func TestPatroniInspiredObservationRowMapsPostmasterStartTime(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("pg-local", 3*60*60)
	postmasterStartAt := time.Date(2026, time.May, 2, 12, 30, 0, 0, location)

	observation := (observationRow{
		inRecovery:        true,
		serverVersion:     170005,
		postmasterStartAt: postmasterStartAt,
		systemIdentifier:  "7599025879359099984",
		timeline:          9,
		receiveLSN:        "0/9000200",
		replayLSN:         "0/9000100",
		replayTimestamp:   sql.NullTime{},
	}).observation()

	if observation.Role != cluster.MemberRoleReplica || !observation.InRecovery {
		t.Fatalf("unexpected observation role: got %+v", observation)
	}
	if !observation.Details.PostmasterStartAt.Equal(postmasterStartAt) {
		t.Fatalf("unexpected postmaster start time: got %v, want %v", observation.Details.PostmasterStartAt, postmasterStartAt)
	}
	if !observation.WAL.ReplayTimestamp.IsZero() {
		t.Fatalf("expected zero replay timestamp when PostgreSQL reports NULL, got %v", observation.WAL.ReplayTimestamp)
	}
}

func TestPatroniInspiredObservationSQLIncludesPostmasterSignals(t *testing.T) {
	t.Parallel()

	wantFragments := []string{
		"pg_postmaster_start_time() as postmaster_start_at",
		"pg_control_system() as system",
		"pg_control_checkpoint() as checkpoint",
		"pg_control_recovery() as recovery",
		"pg_stat_wal_receiver",
	}

	for _, wantFragment := range wantFragments {
		if !strings.Contains(queryObservationSQL, wantFragment) {
			t.Fatalf("observation query does not include %q:\n%s", wantFragment, queryObservationSQL)
		}
	}
}
