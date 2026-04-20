package postgres

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

type commandRunner func(ctx context.Context, name string, args ...string) (commandResult, error)

type commandResult struct {
	output   string
	exitCode int
}

var runCommand commandRunner = executeCommand
var runPassthroughCommand commandRunner = executePassthroughCommand

func executeCommand(ctx context.Context, name string, args ...string) (commandResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	result := commandResult{
		output:   string(output),
		exitCode: 0,
	}
	if err == nil {
		return result, nil
	}

	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.exitCode = exitError.ExitCode()
	} else {
		result.exitCode = -1
	}

	return result, err
}

func executePassthroughCommand(ctx context.Context, name string, args ...string) (commandResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	err := cmd.Run()
	result := commandResult{
		exitCode: 0,
	}
	if err == nil {
		return result, nil
	}

	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.exitCode = exitError.ExitCode()
	} else {
		result.exitCode = -1
	}

	return result, err
}

func binaryPath(binDir, binaryName string) string {
	trimmed := strings.TrimSpace(binDir)
	if trimmed == "" {
		return binaryName
	}

	return filepath.Join(trimmed, binaryName)
}

func wrapCommandError(action string, result commandResult, err error) error {
	output := strings.TrimSpace(result.output)
	if output == "" {
		return fmt.Errorf("%s: %w", action, err)
	}

	return fmt.Errorf("%s: %w: %s", action, err, output)
}
