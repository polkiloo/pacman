package localagent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/logging"
)

func TestRunReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Run(ctx, logging.New("pacmand", &bytes.Buffer{}), config.Config{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRunWrapsDaemonConstructionError(t *testing.T) {
	t.Parallel()

	err := Run(context.Background(), logging.New("pacmand", &bytes.Buffer{}), config.Config{
		Node: config.NodeConfig{
			Name: "alpha-1",
			Role: cluster.NodeRoleData,
		},
	})
	if !errors.Is(err, agent.ErrPostgresConfigRequired) {
		t.Fatalf("expected ErrPostgresConfigRequired, got %v", err)
	}

	if !strings.Contains(err.Error(), "construct local agent daemon") {
		t.Fatalf("expected error to contain wrap prefix, got %q", err)
	}
}

func TestRunStartsAndWaitsForWitnessDaemon(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var logs bytes.Buffer

	err := Run(
		ctx,
		logging.New("pacmand", &logs),
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name: "alpha-witness",
				Role: cluster.NodeRoleWitness,
			},
		},
		agent.WithNoAPIServer(),
	)
	if err != nil {
		t.Fatalf("run witness daemon: %v", err)
	}

	if !strings.Contains(logs.String(), `"msg":"stopped local agent daemon"`) {
		t.Fatalf("expected stop log entry, got %q", logs.String())
	}

	if !strings.Contains(logs.String(), `"node":"alpha-witness"`) {
		t.Fatalf("expected stop log to include node identity, got %q", logs.String())
	}

	if !strings.Contains(logs.String(), `"node_role":"witness"`) {
		t.Fatalf("expected stop log to include node role, got %q", logs.String())
	}
}

func TestRunWrapsDaemonStartError(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	defer listener.Close()

	err = Run(
		context.Background(),
		logging.New("pacmand", &bytes.Buffer{}),
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name:       "alpha-witness",
				Role:       cluster.NodeRoleWitness,
				APIAddress: listener.Addr().String(),
			},
		},
	)
	if err == nil {
		t.Fatal("expected daemon start error")
	}

	if !strings.Contains(err.Error(), "start local agent daemon") {
		t.Fatalf("expected error to contain start wrap prefix, got %q", err)
	}
}

func TestLocalPostgresOptionsReturnNilWithoutPostgresConfig(t *testing.T) {
	t.Parallel()

	if opts := localPostgresOptions(config.Config{}); len(opts) != 0 {
		t.Fatalf("expected no local postgres options without postgres config, got %d", len(opts))
	}
}

func TestLocalPostgresOptionsConfigurePgCtlAndAdminToken(t *testing.T) {
	t.Parallel()

	opts := localPostgresOptions(config.Config{
		Postgres: &config.PostgresLocalConfig{
			BinDir:  "/usr/pgsql-17/bin",
			DataDir: "/var/lib/pgsql/17/data",
		},
		Security: &config.SecurityConfig{
			AdminBearerToken: " secret-token ",
		},
	})
	if len(opts) != 2 {
		t.Fatalf("expected pg_ctl and admin token options, got %d", len(opts))
	}

	daemon := &agent.Daemon{}
	for _, option := range opts {
		option(daemon)
	}

	fields := reflect.ValueOf(daemon).Elem()
	pgCtl := fields.FieldByName("pgCtl")
	if pgCtl.IsNil() {
		t.Fatal("expected local postgres options to configure pg_ctl")
	}

	if got := pgCtl.Elem().FieldByName("BinDir").String(); got != "/usr/pgsql-17/bin" {
		t.Fatalf("pg_ctl bin dir: got %q, want %q", got, "/usr/pgsql-17/bin")
	}

	if got := pgCtl.Elem().FieldByName("DataDir").String(); got != "/var/lib/pgsql/17/data" {
		t.Fatalf("pg_ctl data dir: got %q, want %q", got, "/var/lib/pgsql/17/data")
	}

	if got := fields.FieldByName("adminToken").String(); got != "secret-token" {
		t.Fatalf("admin token: got %q, want %q", got, "secret-token")
	}
}

func TestLocalPostgresOptionsSkipUnreadableAdminTokenFile(t *testing.T) {
	t.Parallel()

	opts := localPostgresOptions(config.Config{
		Postgres: &config.PostgresLocalConfig{
			BinDir:  "/usr/pgsql-17/bin",
			DataDir: "/var/lib/pgsql/17/data",
		},
		Security: &config.SecurityConfig{
			AdminBearerTokenFile: filepath.Join(t.TempDir(), "missing-token"),
		},
	})
	if len(opts) != 1 {
		t.Fatalf("expected unreadable token file to leave only pg_ctl option, got %d", len(opts))
	}
}
