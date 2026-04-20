package postgres

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestExecutePassthroughCommandReturnsWithoutWaitingForBackgroundChild(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("shell background process semantics differ on windows")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err := executePassthroughCommand(ctx, "sh", "-c", "sleep 1 &")
	if err != nil {
		t.Fatalf("execute passthrough command: %v", err)
	}

	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("passthrough command blocked for background child: elapsed=%s", elapsed)
	}
}
