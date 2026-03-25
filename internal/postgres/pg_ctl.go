package postgres

import (
	"context"
	"strings"
)

// ShutdownMode describes how PostgreSQL should stop when controlled through
// pg_ctl.
type ShutdownMode string

const (
	ShutdownModeSmart     ShutdownMode = "smart"
	ShutdownModeFast      ShutdownMode = "fast"
	ShutdownModeImmediate ShutdownMode = "immediate"
)

// IsValid reports whether the shutdown mode is supported by PACMAN.
func (mode ShutdownMode) IsValid() bool {
	switch mode {
	case ShutdownModeSmart, ShutdownModeFast, ShutdownModeImmediate:
		return true
	default:
		return false
	}
}

// PGCtl wraps a local pg_ctl binary for node-local PostgreSQL lifecycle
// actions.
type PGCtl struct {
	BinDir  string
	DataDir string
	runner  commandRunner
}

// Start waits for PostgreSQL to report successful startup through pg_ctl.
func (ctl PGCtl) Start(ctx context.Context) error {
	result, err := ctl.run(ctx, "start", "-w")
	if err != nil {
		return wrapCommandError("start postgres via pg_ctl", result, err)
	}

	return nil
}

// Stop waits for PostgreSQL to stop through pg_ctl using the provided shutdown
// mode.
func (ctl PGCtl) Stop(ctx context.Context, mode ShutdownMode) error {
	if !mode.IsValid() {
		return ErrShutdownModeInvalid
	}

	result, err := ctl.run(ctx, "stop", "-w", "-m", string(mode))
	if err != nil {
		return wrapCommandError("stop postgres via pg_ctl", result, err)
	}

	return nil
}

// Status reports whether PostgreSQL is currently running according to pg_ctl.
func (ctl PGCtl) Status(ctx context.Context) (bool, error) {
	result, err := ctl.run(ctx, "status")
	if err == nil {
		return true, nil
	}

	if result.exitCode == 3 {
		return false, nil
	}

	return false, wrapCommandError("query postgres status via pg_ctl", result, err)
}

func (ctl PGCtl) run(ctx context.Context, action string, extraArgs ...string) (commandResult, error) {
	args, err := ctl.commandArgs(action, extraArgs...)
	if err != nil {
		return commandResult{}, err
	}

	return ctl.commandRunner()(ctx, binaryPath(ctl.BinDir, "pg_ctl"), args...)
}

func (ctl PGCtl) commandArgs(action string, extraArgs ...string) ([]string, error) {
	dataDir := strings.TrimSpace(ctl.DataDir)
	if dataDir == "" {
		return nil, ErrDataDirRequired
	}

	args := []string{action, "-D", dataDir}
	return append(args, extraArgs...), nil
}

func (ctl PGCtl) commandRunner() commandRunner {
	if ctl.runner != nil {
		return ctl.runner
	}

	return runCommand
}
