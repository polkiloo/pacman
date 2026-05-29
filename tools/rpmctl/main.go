package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type config struct {
	workspace           string
	outputDir           string
	version             string
	release             string
	commit              string
	sourceDateEpoch     int64
	goRetryAttempts     int
	goRetryDelay        time.Duration
	nfpmArch            string
	rpmArch             string
	sourceDir           string
	stageMain           string
	stagePGExt          string
	buildDate           string
	pacmandLDFlags      string
	pacmanctlLDFlags    string
	pacmanRPM           string
	pacmanPGExtRPM      string
	pacmanConfig        string
	pacmanPGExtConfig   string
	pgConfig            string
	goBuildEnvironment  []string
	nfpmEnvironment     []string
	workspaceSkipExact  map[string]struct{}
	workspaceSkipPrefix []string
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, cleanup, err := loadConfig()
	if err != nil {
		return err
	}
	defer cleanup()

	if err := os.MkdirAll(cfg.sourceDir, 0o755); err != nil {
		return fmt.Errorf("create source dir: %w", err)
	}
	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	if err := copyWorkspace(cfg.workspace, cfg.sourceDir, cfg); err != nil {
		return fmt.Errorf("copy workspace: %w", err)
	}
	if err := retry(ctx, cfg.goRetryAttempts, cfg.goRetryDelay, func() error {
		return runCommand(ctx, cfg.sourceDir, cfg.goBuildEnvironment, "go", "mod", "download")
	}); err != nil {
		return fmt.Errorf("go mod download: %w", err)
	}
	if err := buildGoBinaries(ctx, cfg); err != nil {
		return err
	}
	if err := buildPostgresExtension(ctx, cfg); err != nil {
		return err
	}
	if err := packageRPMs(ctx, cfg); err != nil {
		return err
	}
	return nil
}

func loadConfig() (config, func(), error) {
	goArch, err := commandOutput("go", "env", "GOARCH")
	if err != nil {
		return config{}, func() {}, fmt.Errorf("detect GOARCH: %w", err)
	}

	nfpmArch, rpmArch, err := archPair(strings.TrimSpace(goArch))
	if err != nil {
		return config{}, func() {}, err
	}

	sourceDateEpoch := envInt64("SOURCE_DATE_EPOCH", time.Now().Unix())
	version := envOrDefault("RPM_VERSION", "0.1.0")
	release := envOrDefault("RPM_RELEASE", "1")
	commit := envOrDefault("RPM_COMMIT", "none")
	buildDate := time.Unix(sourceDateEpoch, 0).UTC().Format("2006-01-02T15:04:05Z")

	topDir, err := os.MkdirTemp("", "pacman-nfpm.")
	if err != nil {
		return config{}, func() {}, fmt.Errorf("create temp dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(topDir) }

	sourceDir := filepath.Join(topDir, "src", "pacman-"+version)
	stageMain := filepath.Join("build", "nfpm", "pacman")
	stagePGExt := filepath.Join("build", "nfpm", "pacman-postgresql17-agent")
	outputDir := envOrDefault("OUTPUT_DIR", "/out")

	pacmandLDFlags := ldflags(version, commit, buildDate)
	cfg := config{
		workspace:          envOrDefault("WORKSPACE", "/workspace"),
		outputDir:          outputDir,
		version:            version,
		release:            release,
		commit:             commit,
		sourceDateEpoch:    sourceDateEpoch,
		goRetryAttempts:    envInt("RPM_BUILDER_GO_RETRY_ATTEMPTS", 5),
		goRetryDelay:       time.Duration(envInt("RPM_BUILDER_GO_RETRY_DELAY_SECONDS", 10)) * time.Second,
		nfpmArch:           nfpmArch,
		rpmArch:            rpmArch,
		sourceDir:          sourceDir,
		stageMain:          stageMain,
		stagePGExt:         stagePGExt,
		buildDate:          buildDate,
		pacmandLDFlags:     pacmandLDFlags,
		pacmanctlLDFlags:   pacmandLDFlags,
		pacmanRPM:          filepath.Join(outputDir, fmt.Sprintf("pacman-%s-%s.%s.rpm", version, release, rpmArch)),
		pacmanPGExtRPM:     filepath.Join(outputDir, fmt.Sprintf("pacman-postgresql17-agent-%s-%s.%s.rpm", version, release, rpmArch)),
		pacmanConfig:       filepath.Join("packaging", "nfpm", "pacman.yaml"),
		pacmanPGExtConfig:  filepath.Join("packaging", "nfpm", "pacman-postgresql17-agent.yaml"),
		pgConfig:           envOrDefault("PG_CONFIG", "/usr/pgsql-17/bin/pg_config"),
		goBuildEnvironment: append(os.Environ(), "GOFLAGS=-buildvcs=false -mod=readonly -trimpath", "CGO_ENABLED=0"),
		nfpmEnvironment: append(os.Environ(),
			"NFPM_ARCH="+nfpmArch,
			"RPM_VERSION="+version,
			"RPM_RELEASE="+release,
			"SOURCE_DATE_EPOCH="+strconv.FormatInt(sourceDateEpoch, 10),
		),
		workspaceSkipExact: map[string]struct{}{
			".git":              {},
			".github":           {},
			".gocache":          {},
			"bin":               {},
			"coverage.out":      {},
			"deploy/lab/.local": {},
			"jepsen/store":      {},
		},
		workspaceSkipPrefix: []string{
			".git/",
			".github/",
			".gocache/",
			"bin/",
			"deploy/lab/.local/",
			"jepsen/store/",
		},
	}
	return cfg, cleanup, nil
}

func ldflags(version, commit, buildDate string) string {
	prefix := "github.com/polkiloo/pacman/internal/version."
	return strings.Join([]string{
		"-X " + prefix + "Version=" + version,
		"-X " + prefix + "Commit=" + commit,
		"-X " + prefix + "BuildDate=" + buildDate,
	}, " ")
}

func archPair(goArch string) (string, string, error) {
	switch goArch {
	case "amd64":
		return "amd64", "x86_64", nil
	case "arm64":
		return "arm64", "aarch64", nil
	default:
		return "", "", fmt.Errorf("unsupported GOARCH: %s", goArch)
	}
}

func buildGoBinaries(ctx context.Context, cfg config) error {
	binDir := filepath.Join(cfg.sourceDir, cfg.stageMain, "usr", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return fmt.Errorf("create binary stage: %w", err)
	}
	if err := runCommand(ctx, cfg.sourceDir, cfg.goBuildEnvironment, "go", "build", "-ldflags", cfg.pacmandLDFlags, "-o", filepath.Join(binDir, "pacmand"), "./cmd/pacmand"); err != nil {
		return fmt.Errorf("build pacmand: %w", err)
	}
	if err := runCommand(ctx, cfg.sourceDir, cfg.goBuildEnvironment, "go", "build", "-ldflags", cfg.pacmanctlLDFlags, "-o", filepath.Join(binDir, "pacmanctl"), "./cmd/pacmanctl"); err != nil {
		return fmt.Errorf("build pacmanctl: %w", err)
	}
	return nil
}

func buildPostgresExtension(ctx context.Context, cfg config) error {
	if err := runCommand(ctx, cfg.sourceDir, nil, "make", "-C", "postgresql/pacman_agent", "PG_CONFIG="+cfg.pgConfig); err != nil {
		return fmt.Errorf("build PostgreSQL extension: %w", err)
	}
	destDir := filepath.Join(cfg.sourceDir, cfg.stagePGExt)
	if err := runCommand(ctx, cfg.sourceDir, nil, "make", "-C", "postgresql/pacman_agent", "PG_CONFIG="+cfg.pgConfig, "DESTDIR="+destDir, "install"); err != nil {
		return fmt.Errorf("install PostgreSQL extension: %w", err)
	}
	return nil
}

func packageRPMs(ctx context.Context, cfg config) error {
	if err := runCommand(ctx, cfg.sourceDir, cfg.nfpmEnvironment, "nfpm", "package", "--packager", "rpm", "--config", cfg.pacmanConfig, "--target", cfg.pacmanRPM); err != nil {
		return fmt.Errorf("package pacman RPM: %w", err)
	}
	if err := runCommand(ctx, cfg.sourceDir, cfg.nfpmEnvironment, "nfpm", "package", "--packager", "rpm", "--config", cfg.pacmanPGExtConfig, "--target", cfg.pacmanPGExtRPM); err != nil {
		return fmt.Errorf("package PostgreSQL extension RPM: %w", err)
	}
	return nil
}

func copyWorkspace(src, dst string, cfg config) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == src {
			return nil
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if shouldSkip(rel, entry.IsDir(), cfg) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		target := filepath.Join(dst, filepath.FromSlash(rel))
		info, err := entry.Info()
		if err != nil {
			return err
		}
		mode := info.Mode()
		switch {
		case mode.IsDir():
			return os.MkdirAll(target, mode.Perm())
		case mode.Type()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		case mode.IsRegular():
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			return copyFile(path, target, mode.Perm())
		default:
			return nil
		}
	})
}

func shouldSkip(rel string, isDir bool, cfg config) bool {
	if _, ok := cfg.workspaceSkipExact[rel]; ok {
		return true
	}
	if !isDir {
		for _, prefix := range cfg.workspaceSkipPrefix {
			if strings.HasPrefix(rel, prefix) {
				return true
			}
		}
	}
	return false
}

func copyFile(src, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	return errors.Join(copyErr, closeErr)
}

func retry(ctx context.Context, attempts int, delay time.Duration, fn func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			if attempt == attempts {
				break
			}
			fmt.Fprintf(os.Stderr, "attempt %d/%d failed: %v; retrying\n", attempt, attempts, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}
		return nil
	}
	return lastErr
}

func runCommand(ctx context.Context, dir string, env []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func commandOutput(name string, args ...string) (string, error) {
	output, err := exec.Command(name, args...).Output()
	return string(output), err
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
