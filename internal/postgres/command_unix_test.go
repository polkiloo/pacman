//go:build unix

package postgres

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureCommandContextCancelBeforeStartReturnsProcessDone(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(context.Background(), "sh", "-c", "true")
	configureCommandContextCancel(cmd)

	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("expected command cancellation to isolate process group, got %+v", cmd.SysProcAttr)
	}
	if err := cmd.Cancel(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("expected cancel before start to report process done, got %v", err)
	}
}

func TestConfigureCommandContextCancelAfterExitReturnsProcessDone(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(context.Background(), "sh", "-c", "true")
	configureCommandContextCancel(cmd)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start command: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait for command: %v", err)
	}

	err := cmd.Cancel()
	if err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("expected cancel after exit to report process done, got %v", err)
	}
}
