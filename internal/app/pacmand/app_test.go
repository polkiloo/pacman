package pacmand

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/pgext"
	"github.com/polkiloo/pacman/internal/version"
)

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-version"}, &stdout, &stderr, &logs)

	if err := app.Run(context.Background()); err != nil {
		t.Fatalf("run pacmand version: %v", err)
	}

	if got, want := stdout.String(), version.String()+"\n"; got != want {
		t.Fatalf("unexpected version output: got %q, want %q", got, want)
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}

	if logs.Len() != 0 {
		t.Fatalf("expected no logs for version output, got %q", logs.String())
	}
}

func TestRunRequiresConfigPath(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, nil, &stdout, &stderr, &logs)

	err := app.Run(context.Background())
	if err == nil {
		t.Fatal("expected config path error")
	}

	if stdout.Len() != 0 {
		t.Fatalf("expected no stdout output, got %q", stdout.String())
	}

	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}

	if !errors.Is(err, errConfigPathRequired) {
		t.Fatalf("unexpected error: got %v, want %v", err, errConfigPathRequired)
	}

	if logs.Len() != 0 {
		t.Fatalf("expected no logs for missing config path, got %q", logs.String())
	}
}

func TestRunStartsLocalDaemonFromPostgresExtensionEnvironment(t *testing.T) {
	apiAddress := reserveLoopbackAddress(t)
	controlAddress := reserveLoopbackAddress(t)
	postgresAddress := reserveLoopbackAddress(t)
	postgresHost, postgresPort := splitHostPort(t, postgresAddress)

	for key, value := range (pgext.Snapshot{
		NodeName:              "alpha-1",
		NodeRole:              "data",
		APIAddress:            apiAddress,
		ControlAddress:        controlAddress,
		PostgresDataDir:       "/var/lib/postgresql/data",
		PostgresListenAddress: postgresHost,
		PostgresPort:          postgresPort,
		ClusterName:           "alpha",
		InitialPrimary:        "alpha-1",
		SeedAddresses:         "alpha-1:9090",
		ExpectedMembers:       "alpha-1",
	}).Environment() {
		t.Setenv(key, value)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-pgext-env"}, &stdout, &stderr, &logs)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := app.Run(ctx); err != nil {
		t.Fatalf("run pacmand from pgext env: %v", err)
	}

	assertContains(t, logs.String(), `"msg":"loaded node configuration"`)
	assertContains(t, logs.String(), `"source":"pgext-env"`)
	assertContains(t, logs.String(), `"runtime_mode":"embedded_worker"`)
	assertContains(t, logs.String(), `"failure_isolation":"helper_process"`)
	assertContains(t, logs.String(), `"error_propagation":"structured_stderr_and_exit_status"`)
	assertContains(t, logs.String(), `"msg":"starting embedded worker runtime"`)
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"msg":"started local agent daemon"`)
	assertContains(t, logs.String(), `"postgres_up":false`)
}

func TestRunRejectsConfigPathAndPostgresExtensionEnvironmentTogether(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-config", "node.yaml", "-pgext-env"}, &stdout, &stderr, &logs)

	err := app.Run(context.Background())
	if !errors.Is(err, errConfigSourceConflict) {
		t.Fatalf("unexpected error: got %v want %v", err, errConfigSourceConflict)
	}
}

func TestRunReturnsPostgresExtensionEnvironmentError(t *testing.T) {
	t.Setenv(pgext.EnvNodeRole, "witness")
	t.Setenv(pgext.EnvNodeName, "alpha-witness")
	t.Setenv(pgext.EnvPostgresDataDir, "/var/lib/postgresql/data")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-pgext-env"}, &stdout, &stderr, &logs)

	err := app.Run(context.Background())
	if !errors.Is(err, pgext.ErrPostgresManagedNodeRequired) {
		t.Fatalf("expected ErrPostgresManagedNodeRequired, got %v", err)
	}

	assertContains(t, logs.String(), `"msg":"embedded worker runtime failed"`)
	assertContains(t, logs.String(), `"runtime_mode":"embedded_worker"`)
	assertContains(t, logs.String(), `"failure_isolation":"helper_process"`)
	assertContains(t, logs.String(), `"error_propagation":"structured_stderr_and_exit_status"`)
	assertContains(t, logs.String(), `"error":"postgres background worker mode requires a postgres-managing node role"`)
}

func TestRunStartsLocalDaemonAndHeartbeatLoopWhenProvided(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
  apiAddress: "%s"
postgres:
  dataDir: /var/lib/postgresql/data
`, reserveLoopbackAddress(t))

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-config", path}, &stdout, &stderr, &logs)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := app.Run(ctx); err != nil {
		t.Fatalf("run pacmand with config: %v", err)
	}

	assertContains(t, logs.String(), `"msg":"loaded node configuration"`)
	assertContains(t, logs.String(), `"component":"config"`)
	assertContains(t, logs.String(), `"runtime_mode":"process"`)
	assertContains(t, logs.String(), `"path":"`+path+`"`)
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"node_role":"data"`)
	assertContains(t, logs.String(), `"msg":"started local agent daemon"`)
	assertContains(t, logs.String(), `"component":"agent"`)
	assertContains(t, logs.String(), `"manages_postgres":true`)
	assertContains(t, logs.String(), `"msg":"observed PostgreSQL unavailability"`)
	assertContains(t, logs.String(), `"msg":"published local state to control plane"`)
	assertContains(t, logs.String(), `"msg":"stopped local agent daemon"`)
	assertContains(t, logs.String(), `"postgres_up":false`)
}

func TestRunLogsAdminAuthStateWithoutLeakingToken(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
  apiAddress: "%s"
security:
  adminBearerToken: super-secret-token
postgres:
  dataDir: /var/lib/postgresql/data
`, reserveLoopbackAddress(t))

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-config", path}, &stdout, &stderr, &logs)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := app.Run(ctx); err != nil {
		t.Fatalf("run pacmand with authenticated config: %v", err)
	}

	assertContains(t, logs.String(), `"admin_auth_enabled":true`)
	if strings.Contains(logs.String(), "super-secret-token") {
		t.Fatalf("expected logs to avoid token leakage, got %q", logs.String())
	}
}

func TestRunReturnsConfigLoadError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-config", filepath.Join(t.TempDir(), "missing.yaml")}, &stdout, &stderr, &logs)

	err := app.Run(context.Background())
	if err == nil {
		t.Fatal("expected config load error")
	}

	assertContains(t, err.Error(), "open config file")
}

func TestRunReturnsDaemonConstructionError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	payload := `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
`

	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-config", path}, &stdout, &stderr, &logs)

	err := app.Run(context.Background())
	if err == nil {
		t.Fatal("expected daemon construction error")
	}

	assertContains(t, err.Error(), "construct local agent daemon")
	assertContains(t, err.Error(), "agent postgres config is required for data nodes")
}

func TestRunReturnsFlagError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var logs bytes.Buffer

	app := newTestApp(t, []string{"-invalid"}, &stdout, &stderr, &logs)

	err := app.Run(context.Background())
	if err == nil {
		t.Fatal("expected invalid flag error")
	}

	assertContains(t, err.Error(), "flag provided but not defined")
	assertContains(t, stderr.String(), "flag provided but not defined")
}

func TestRunReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app := newTestApp(t, nil, &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{})

	err := app.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

type resolvedTestApp struct {
	fx.In

	App *App
}

func newTestApp(t *testing.T, args []string, stdout, stderr, logs io.Writer) *App {
	t.Helper()

	var resolved resolvedTestApp

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() context.Context { return context.Background() }),
		Module("pacmand", args, stdout, stderr),
		fx.Decorate(func(_ *slog.Logger) *slog.Logger {
			return logging.New("pacmand", logs)
		}),
		fx.Populate(&resolved),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build pacmand fx app: %v", err)
	}

	return resolved.App
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}

	return address
}

func splitHostPort(t *testing.T, address string) (string, int) {
	t.Helper()

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}

	value, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	return host, value
}
