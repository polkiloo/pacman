package agent

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

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

func TestDaemonStartRecordsStartupState(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	now := time.Date(2026, time.March, 21, 10, 30, 0, 0, time.UTC)

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withNow(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	if err := daemon.Start(context.Background()); err != nil {
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

	assertContains(t, logs.String(), `"msg":"started local agent daemon"`)
	assertContains(t, logs.String(), `"component":"agent"`)
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"manages_postgres":true`)
}

func TestDaemonStartRejectsSecondStart(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(validDataConfig(), logging.New("pacmand", &bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	if err := daemon.Start(context.Background()); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	if err := daemon.Start(context.Background()); err != ErrDaemonAlreadyStarted {
		t.Fatalf("expected second start error, got %v", err)
	}
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

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}
