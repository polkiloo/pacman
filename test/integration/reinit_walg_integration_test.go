//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/config"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestReinitWALGBackupFetchCommandUsesLatestWithRealWALG(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	walGBinary := requireWALGBinary(t)
	repositoryDir := t.TempDir()
	restoreParent := t.TempDir()
	dataDir := filepath.Join(restoreParent, "pgdata")

	walg := config.WALGConfig{
		Binary: walGBinary,
		Repository: config.WALGRepositoryConfig{
			Provider: config.WALGRepositoryProviderFilesystem,
			Prefix:   repositoryDir,
		},
	}.WithDefaults()

	binary, args, err := walg.BackupFetchCommand(dataDir)
	if err != nil {
		t.Fatalf("build WAL-G backup-fetch command: %v", err)
	}
	if got := args[len(args)-1]; got != config.DefaultWALGRestoreBackupName {
		t.Fatalf("backup-fetch source: got %q, want %q", got, config.DefaultWALGRestoreBackupName)
	}

	env, err := walg.RestoreEnvironment(nil, nil)
	if err != nil {
		t.Fatalf("build WAL-G restore environment: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), mapToEnv(env)...)

	output, runErr := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("WAL-G backup-fetch timed out: %v", ctx.Err())
	}
	if runErr == nil {
		t.Fatal("expected WAL-G backup-fetch against empty repository to fail")
	}

	renderedOutput := string(output)
	for _, forbidden := range []string{"unknown command", "accepts ", "Usage:"} {
		if strings.Contains(renderedOutput, forbidden) {
			t.Fatalf("WAL-G rejected generated backup-fetch command shape; err=%v output=%q", runErr, renderedOutput)
		}
	}
}

func TestReinitWALGBackupFetchCommandUsesLatestWithRealWALGInDocker(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	image := strings.TrimSpace(os.Getenv("PACMAN_TEST_WALG_IMAGE"))
	if image == "" {
		t.Skip("set PACMAN_TEST_WALG_IMAGE to a Docker image containing /bin/sh and wal-g")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := testcontainers.Run(ctx, image,
		testcontainers.WithEntrypoint("/bin/sh"),
		testcontainers.WithCmd("-lc", "sleep infinity"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"/bin/sh", "-lc", "command -v wal-g"}).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("start WAL-G test container %q: %v", image, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("terminate WAL-G test container: %v", err)
		}
	})

	repositoryDir := "/tmp/pacman-walg-repository"
	dataDir := "/tmp/pacman-walg-restore/pgdata"
	requireContainerExec(t, ctx, container, "/bin/sh", "-lc", "rm -rf /tmp/pacman-walg-repository /tmp/pacman-walg-restore && mkdir -p /tmp/pacman-walg-repository /tmp/pacman-walg-restore")

	walg := config.WALGConfig{
		Binary: "wal-g",
		Repository: config.WALGRepositoryConfig{
			Provider: config.WALGRepositoryProviderFilesystem,
			Prefix:   repositoryDir,
		},
	}.WithDefaults()

	binary, args, err := walg.BackupFetchCommand(dataDir)
	if err != nil {
		t.Fatalf("build WAL-G backup-fetch command: %v", err)
	}
	if got := args[len(args)-1]; got != config.DefaultWALGRestoreBackupName {
		t.Fatalf("backup-fetch source: got %q, want %q", got, config.DefaultWALGRestoreBackupName)
	}

	env, err := walg.RestoreEnvironment(nil, nil)
	if err != nil {
		t.Fatalf("build WAL-G restore environment: %v", err)
	}

	command := shellCommandWithEnv(binary, args, env)
	exitCode, output := containerExec(t, ctx, container, "/bin/sh", "-lc", command)
	if exitCode == 0 {
		t.Fatal("expected WAL-G backup-fetch against empty repository to fail")
	}

	for _, forbidden := range []string{"unknown command", "accepts ", "Usage:"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("WAL-G rejected generated backup-fetch command shape; exit=%d output=%q", exitCode, output)
		}
	}
}

func requireWALGBinary(t *testing.T) string {
	t.Helper()

	if configured := strings.TrimSpace(os.Getenv("PACMAN_TEST_WALG_BINARY")); configured != "" {
		if _, err := os.Stat(configured); err != nil {
			t.Fatalf("PACMAN_TEST_WALG_BINARY=%q is not usable: %v", configured, err)
		}
		return configured
	}

	path, err := exec.LookPath(config.DefaultWALGBinary)
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			t.Skip("set PACMAN_TEST_WALG_BINARY or install wal-g on PATH to run real WAL-G reinit integration test")
		}
		t.Fatalf("look up wal-g binary: %v", err)
	}

	return path
}

func mapToEnv(values map[string]string) []string {
	env := make([]string, 0, len(values))
	for name, value := range values {
		env = append(env, name+"="+value)
	}
	return env
}

func requireContainerExec(t *testing.T, ctx context.Context, container testcontainers.Container, cmd ...string) string {
	t.Helper()

	exitCode, output := containerExec(t, ctx, container, cmd...)
	if exitCode != 0 {
		t.Fatalf("exec %q in WAL-G test container returned %d: %s", strings.Join(cmd, " "), exitCode, output)
	}

	return output
}

func containerExec(t *testing.T, ctx context.Context, container testcontainers.Container, cmd ...string) (int, string) {
	t.Helper()

	exitCode, reader, err := container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec %q in WAL-G test container: %v", strings.Join(cmd, " "), err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read exec output for %q in WAL-G test container: %v", strings.Join(cmd, " "), err)
	}

	return exitCode, string(output)
}

func shellCommandWithEnv(binary string, args []string, env map[string]string) string {
	parts := make([]string, 0, len(env)+1+len(args))
	for _, assignment := range mapToEnv(env) {
		parts = append(parts, shellQuote(assignment))
	}
	parts = append(parts, shellQuote(binary))
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}

	return "env " + strings.Join(parts, " ")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}

	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
