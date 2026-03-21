package agent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/logging"
)

func TestNewDaemonRejectsNilLogger(t *testing.T) {
	t.Parallel()

	_, err := NewDaemon(validDataConfig(), nil)
	if err != ErrLoggerRequired {
		t.Fatalf("expected nil logger error, got %v", err)
	}
}

func TestNewDaemonRejectsMissingPostgresConfigForDataNode(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name: "alpha-1",
			Role: cluster.NodeRoleData,
		},
	}

	_, err := NewDaemon(cfg, logging.New("pacmand", &bytes.Buffer{}))
	if err != ErrPostgresConfigRequired {
		t.Fatalf("expected postgres config error, got %v", err)
	}
}

func TestNewDaemonAllowsWitnessWithoutPostgresConfig(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name: "alpha-3",
			Role: cluster.NodeRoleWitness,
		},
	}

	daemon, err := NewDaemon(cfg, logging.New("pacmand", &bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	startup := daemon.Startup()
	if !startup.StartedAt.IsZero() {
		t.Fatalf("expected zero startup state before start, got %+v", startup)
	}
}

func TestDaemonStartRecordsStartupStateAndHeartbeat(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	now := time.Date(2026, time.March, 21, 10, 30, 0, 0, time.UTC)

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withNow(func() time.Time { return now }),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (cluster.MemberRole, bool, error) {
			return cluster.MemberRolePrimary, false, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	startup := daemon.Startup()
	if startup.NodeName != "alpha-1" {
		t.Fatalf("unexpected node name: got %q, want %q", startup.NodeName, "alpha-1")
	}

	if startup.NodeRole != cluster.NodeRoleData {
		t.Fatalf("unexpected node role: got %q, want %q", startup.NodeRole, cluster.NodeRoleData)
	}

	if startup.APIAddress != config.DefaultAPIAddress {
		t.Fatalf("unexpected api address: got %q, want %q", startup.APIAddress, config.DefaultAPIAddress)
	}

	if startup.ControlAddress != config.DefaultControlAddress {
		t.Fatalf("unexpected control address: got %q, want %q", startup.ControlAddress, config.DefaultControlAddress)
	}

	if !startup.ManagesPostgres {
		t.Fatal("expected data node daemon to manage postgres")
	}

	if !startup.StartedAt.Equal(now) {
		t.Fatalf("unexpected startedAt: got %v, want %v", startup.StartedAt, now)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.Sequence != 1 {
		t.Fatalf("unexpected heartbeat sequence: got %d, want %d", heartbeat.Sequence, 1)
	}

	if !heartbeat.ObservedAt.Equal(now) {
		t.Fatalf("unexpected heartbeat observedAt: got %v, want %v", heartbeat.ObservedAt, now)
	}

	if !heartbeat.Postgres.Managed {
		t.Fatal("expected heartbeat to manage postgres")
	}

	if !heartbeat.Postgres.Up {
		t.Fatalf("expected postgres to be available, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected postgres role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRolePrimary)
	}

	if !heartbeat.Postgres.RecoveryKnown {
		t.Fatalf("expected known recovery state, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.InRecovery {
		t.Fatalf("expected primary recovery state, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Address != "127.0.0.1:5432" {
		t.Fatalf("unexpected postgres probe address: got %q", heartbeat.Postgres.Address)
	}

	assertContains(t, logs.String(), `"msg":"started local agent daemon"`)
	assertContains(t, logs.String(), `"msg":"observed PostgreSQL availability"`)
	assertContains(t, logs.String(), `"heartbeat_sequence":1`)
	assertContains(t, logs.String(), `"postgres_up":true`)
	assertContains(t, logs.String(), `"member_role":"primary"`)
	assertContains(t, logs.String(), `"in_recovery":false`)

	cancel()
	daemon.Wait()
}

func TestDaemonStartRecordsWitnessHeartbeatWithoutLocalPostgres(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	now := time.Date(2026, time.March, 21, 10, 30, 0, 0, time.UTC)

	daemon, err := NewDaemon(
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name: "witness-1",
				Role: cluster.NodeRoleWitness,
			},
		},
		logging.New("pacmand", &logs),
		withNow(func() time.Time { return now }),
		withHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.Sequence != 1 {
		t.Fatalf("unexpected heartbeat sequence: got %d, want %d", heartbeat.Sequence, 1)
	}

	if heartbeat.Postgres.Managed {
		t.Fatalf("expected witness heartbeat without postgres, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Role != cluster.MemberRoleWitness {
		t.Fatalf("unexpected witness role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRoleWitness)
	}

	if heartbeat.Postgres.Up {
		t.Fatalf("expected postgres availability to be false, got %+v", heartbeat.Postgres)
	}

	assertContains(t, logs.String(), `"msg":"observed heartbeat without local PostgreSQL"`)
	assertContains(t, logs.String(), `"postgres_managed":false`)

	cancel()
	daemon.Wait()
}

func TestDaemonStartDetectsReplicaRecoveryState(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &bytes.Buffer{}),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (cluster.MemberRole, bool, error) {
			return cluster.MemberRoleReplica, true, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.Postgres.Role != cluster.MemberRoleReplica {
		t.Fatalf("unexpected postgres role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRoleReplica)
	}

	if !heartbeat.Postgres.RecoveryKnown {
		t.Fatalf("expected known recovery state, got %+v", heartbeat.Postgres)
	}

	if !heartbeat.Postgres.InRecovery {
		t.Fatalf("expected in-recovery state, got %+v", heartbeat.Postgres)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonStartReportsRoleDetectionFailureWhileAvailabilityIsUp(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (cluster.MemberRole, bool, error) {
			return cluster.MemberRoleUnknown, false, errors.New("pq: password authentication failed")
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if !heartbeat.Postgres.Up {
		t.Fatalf("expected availability to remain up, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Role != cluster.MemberRoleUnknown {
		t.Fatalf("unexpected postgres role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRoleUnknown)
	}

	if heartbeat.Postgres.RecoveryKnown {
		t.Fatalf("expected unknown recovery state, got %+v", heartbeat.Postgres)
	}

	assertContains(t, logs.String(), `"msg":"observed PostgreSQL availability without role state"`)
	assertContains(t, logs.String(), `"postgres_state_error":"pq: password authentication failed"`)

	cancel()
	daemon.Wait()
}

func TestDaemonHeartbeatLoopTicksAndTracksAvailabilityChanges(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	var (
		mu    sync.Mutex
		calls int
	)

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withHeartbeatInterval(10*time.Millisecond),
		withPostgresProbe(func(context.Context, string) error {
			mu.Lock()
			defer mu.Unlock()

			calls++
			if calls == 1 {
				return errors.New("dial tcp 127.0.0.1:5432: connect: connection refused")
			}

			return nil
		}),
		withPostgresStateProbe(func(context.Context, string) (cluster.MemberRole, bool, error) {
			return cluster.MemberRolePrimary, false, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	waitForHeartbeat(t, daemon, func(heartbeat agentmodel.Heartbeat) bool {
		return heartbeat.Sequence >= 2 && heartbeat.Postgres.Up
	})

	cancel()
	daemon.Wait()

	heartbeat := daemon.Heartbeat()
	if heartbeat.Sequence < 2 {
		t.Fatalf("expected at least two heartbeats, got %d", heartbeat.Sequence)
	}

	if !heartbeat.Postgres.Up {
		t.Fatalf("expected postgres to recover, got %+v", heartbeat.Postgres)
	}

	assertContains(t, logs.String(), `"msg":"observed PostgreSQL unavailability"`)
	assertContains(t, logs.String(), `"msg":"observed PostgreSQL availability"`)
	assertContains(t, logs.String(), `"postgres_up":false`)
	assertContains(t, logs.String(), `"postgres_up":true`)
	assertContains(t, logs.String(), `"member_role":"primary"`)
}

func TestDaemonStartRejectsSecondStart(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(validDataConfig(), logging.New("pacmand", &bytes.Buffer{}), withHeartbeatInterval(time.Hour))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	if err := daemon.Start(ctx); err != ErrDaemonAlreadyStarted {
		t.Fatalf("expected second start error, got %v", err)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonStartRejectsConcurrentSecondStart(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &bytes.Buffer{}),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (cluster.MemberRole, bool, error) {
			return cluster.MemberRolePrimary, false, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			errs <- daemon.Start(ctx)
		}()
	}

	var startedCount int
	var alreadyStartedCount int
	for range 2 {
		err := <-errs
		switch err {
		case nil:
			startedCount++
		case ErrDaemonAlreadyStarted:
			alreadyStartedCount++
		default:
			t.Fatalf("unexpected start error: %v", err)
		}
	}

	if startedCount != 1 {
		t.Fatalf("expected exactly one successful start, got %d", startedCount)
	}

	if alreadyStartedCount != 1 {
		t.Fatalf("expected exactly one already-started error, got %d", alreadyStartedCount)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonStartReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	daemon, err := NewDaemon(validDataConfig(), logging.New("pacmand", &bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	err = daemon.Start(ctx)
	if err != context.Canceled {
		t.Fatalf("expected canceled context, got %v", err)
	}
}

func validDataConfig() config.Config {
	return config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name: "alpha-1",
			Role: cluster.NodeRoleData,
		},
		Postgres: &config.PostgresLocalConfig{
			DataDir: "/var/lib/postgresql/data",
		},
	}
}

func waitForHeartbeat(t *testing.T, daemon *Daemon, predicate func(agentmodel.Heartbeat) bool) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if predicate(daemon.Heartbeat()) {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("heartbeat condition was not met, last heartbeat: %+v", daemon.Heartbeat())
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
