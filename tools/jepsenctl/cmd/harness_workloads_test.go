package cmd

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOpenTransactionQueryTimeoutIncludesSleepAndGrace(t *testing.T) {
	const sleepSeconds = 18
	want := 18*time.Second + openTransactionQueryGrace
	if got := openTransactionQueryTimeout(sleepSeconds); got != want {
		t.Fatalf("open transaction query timeout = %s, want %s", got, want)
	}
}

func TestRunOpenTransactionQueryCancelsBlockedQuery(t *testing.T) {
	started := time.Now()
	_, err := runOpenTransactionQuery(context.Background(), 10*time.Millisecond, func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run open transaction query error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked query cancellation took %s, want at most 1s", elapsed)
	}
}
