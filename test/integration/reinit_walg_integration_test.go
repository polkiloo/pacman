//go:build integration

package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/config"
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
