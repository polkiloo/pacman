package postgres

import (
	"context"
	"strings"
)

// PGRewind wraps a local pg_rewind binary for former-primary repair during
// rejoin.
type PGRewind struct {
	BinDir       string
	DataDir      string
	SourceServer string
	runner       commandRunner
}

// Run executes pg_rewind against the local data directory using the provided
// source server connection string.
func (rewind PGRewind) Run(ctx context.Context) error {
	args, err := rewind.commandArgs()
	if err != nil {
		return err
	}

	result, err := rewind.commandRunner()(ctx, binaryPath(rewind.BinDir, "pg_rewind"), args...)
	if err != nil {
		return wrapCommandError("run pg_rewind", result, err)
	}

	return nil
}

func (rewind PGRewind) commandArgs() ([]string, error) {
	dataDir := strings.TrimSpace(rewind.DataDir)
	if dataDir == "" {
		return nil, ErrDataDirRequired
	}

	sourceServer := strings.TrimSpace(rewind.SourceServer)
	if sourceServer == "" {
		return nil, ErrSourceServerRequired
	}

	return []string{
		"--target-pgdata", dataDir,
		"--source-server", sourceServer,
		"--progress",
	}, nil
}

func (rewind PGRewind) commandRunner() commandRunner {
	if rewind.runner != nil {
		return rewind.runner
	}

	return runCommand
}
