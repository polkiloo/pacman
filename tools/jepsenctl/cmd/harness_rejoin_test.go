package cmd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOldPrimaryRejoinWaitStateAcceptsDelayedRejoin(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	observationFile := filepath.Join(dir, primaryObservationFile)
	writeTestFile(t, observationFile, oldPrimaryDelayedRejoinObservations(false))

	state, err := oldPrimaryRejoinWaitState(observationFile, "primary-dcs-majority-partition")
	if err != nil {
		t.Fatalf("wait state before rejoin: %v", err)
	}
	if !state.applicable || state.valid || state.unsafeAfterPromotion {
		t.Fatalf("state before rejoin: %+v, want applicable invalid safe-to-wait", state)
	}

	appendFile(observationFile, oldPrimaryDelayedRejoinFinalSample())
	state, err = oldPrimaryRejoinWaitState(observationFile, "primary-dcs-majority-partition")
	if err != nil {
		t.Fatalf("wait state after rejoin: %v", err)
	}
	if !state.applicable || !state.valid || state.unsafeAfterPromotion {
		t.Fatalf("state after rejoin: %+v, want applicable valid safe", state)
	}
}

func TestWaitForOldPrimaryRejoinWaitsForDelayedObservation(t *testing.T) {
	dir := t.TempDir()
	observationFile := filepath.Join(dir, primaryObservationFile)
	writeTestFile(t, observationFile, oldPrimaryDelayedRejoinObservations(false))
	lab := &harnessLab{cfg: harnessConfig{
		oldPrimaryRejoinTimeout:  200 * time.Millisecond,
		oldPrimaryRejoinInterval: 5 * time.Millisecond,
	}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(20 * time.Millisecond)
		appendFile(observationFile, oldPrimaryDelayedRejoinFinalSample())
	}()

	if !lab.waitForOldPrimaryRejoin(context.Background(), observationFile, dir, "primary-dcs-majority-partition") {
		t.Fatalf("old primary rejoin wait returned false")
	}
	<-done
	log := mustRead(filepath.Join(dir, "nemesis.log"))
	assertContainsAll(t, "rejoin wait log", log, []string{
		"waiting up to 200ms for old primary rejoin after primary-dcs-majority-partition",
		"old primary rejoin observed after primary-dcs-majority-partition",
	})
}

func oldPrimaryDelayedRejoinObservations(includeFinal bool) string {
	text := strings.Join([]string{
		`{"sampleId":1,"observedAt":"2026-06-17T23:25:28Z","member":"alpha-1","service":"pacman-primary","reachable":true,"writable":true,"inRecovery":false,"timeline":1,"lsn":"0/5093730"}`,
		`{"sampleId":1,"observedAt":"2026-06-17T23:25:28Z","member":"alpha-2","service":"pacman-replica","reachable":true,"writable":false,"inRecovery":true,"timeline":1,"lsn":"0/5093730"}`,
		`{"sampleId":1,"observedAt":"2026-06-17T23:25:28Z","member":"alpha-3","service":"pacman-replica-2","reachable":true,"writable":false,"inRecovery":true,"timeline":1,"lsn":"0/5093730"}`,
		`{"sampleId":2,"observedAt":"2026-06-17T23:26:02Z","member":"alpha-1","service":"pacman-primary","reachable":false,"writable":false,"inRecovery":null,"error":"connection refused"}`,
		`{"sampleId":2,"observedAt":"2026-06-17T23:26:02Z","member":"alpha-2","service":"pacman-replica","reachable":true,"writable":true,"inRecovery":false,"timeline":2,"lsn":"0/5094DE8"}`,
		`{"sampleId":2,"observedAt":"2026-06-17T23:26:02Z","member":"alpha-3","service":"pacman-replica-2","reachable":true,"writable":false,"inRecovery":true,"timeline":2,"lsn":"0/5094DE8"}`,
	}, "\n") + "\n"
	if includeFinal {
		text += oldPrimaryDelayedRejoinFinalSample()
	}
	return text
}

func oldPrimaryDelayedRejoinFinalSample() string {
	return strings.Join([]string{
		`{"sampleId":3,"observedAt":"2026-06-17T23:26:07Z","member":"alpha-1","service":"pacman-primary","reachable":true,"writable":false,"inRecovery":true,"timeline":2,"lsn":"0/5094DE8"}`,
		`{"sampleId":3,"observedAt":"2026-06-17T23:26:07Z","member":"alpha-2","service":"pacman-replica","reachable":true,"writable":true,"inRecovery":false,"timeline":2,"lsn":"0/5094DE8"}`,
		`{"sampleId":3,"observedAt":"2026-06-17T23:26:07Z","member":"alpha-3","service":"pacman-replica-2","reachable":true,"writable":false,"inRecovery":true,"timeline":2,"lsn":"0/5094DE8"}`,
	}, "\n") + "\n"
}
