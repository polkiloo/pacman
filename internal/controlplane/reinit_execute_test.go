package controlplane

import (
	"context"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func seededReinitIntentStore(t *testing.T, now time.Time) *MemoryStateStore {
	t.Helper()

	store := seededReinitStore(t, now, []agentmodel.NodeStatus{
		reinitNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0, "10.0.0.1:5432"),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(time.Second), true, 21, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })
	if _, err := store.CreateReinitIntent(context.Background(), ReinitRequest{Member: "alpha-2"}); err != nil {
		t.Fatalf("create reinit intent: %v", err)
	}

	return store
}

func seededStoppedReinitStore(t *testing.T, now time.Time) *MemoryStateStore {
	t.Helper()

	store := seededReinitIntentStore(t, now)
	if _, err := store.ExecuteReinitStopPostgres(context.Background(), "alpha-2", &recordingReinitPostgresStopper{}); err != nil {
		t.Fatalf("stop postgres for reinit: %v", err)
	}

	return store
}

func seededArchivedReinitStore(t *testing.T, now time.Time) *MemoryStateStore {
	t.Helper()

	store := seededStoppedReinitStore(t, now)
	if _, err := store.ExecuteReinitArchiveDataDir(context.Background(), "alpha-2", &recordingReinitDataDirArchiver{}); err != nil {
		t.Fatalf("archive data dir for reinit: %v", err)
	}

	return store
}

func seededWALGRestoredReinitStore(t *testing.T, now time.Time) *MemoryStateStore {
	t.Helper()

	store := seededArchivedReinitStore(t, now)
	if _, err := store.ExecuteReinitWALGRestore(context.Background(), "alpha-2", &recordingReinitWALGRestorer{}); err != nil {
		t.Fatalf("restore from WAL-G for reinit: %v", err)
	}

	return store
}

func seededRecoveryConfiguredReinitStore(t *testing.T, now time.Time) *MemoryStateStore {
	t.Helper()

	store := seededWALGRestoredReinitStore(t, now)
	if _, err := store.ExecuteReinitRecoveryConfig(context.Background(), "alpha-2", &recordingReinitRecoveryConfigurator{}); err != nil {
		t.Fatalf("configure recovery for reinit: %v", err)
	}

	return store
}

type recordingReinitPostgresStopper struct {
	requests []ReinitPostgresStopRequest
	err      error
}

func (stopper *recordingReinitPostgresStopper) StopPostgres(_ context.Context, request ReinitPostgresStopRequest) error {
	stopper.requests = append(stopper.requests, request)
	return stopper.err
}

type recordingReinitDataDirArchiver struct {
	requests []ReinitDataDirArchiveRequest
	result   ReinitDataDirArchiveResult
	err      error
}

func (archiver *recordingReinitDataDirArchiver) ArchiveDataDir(_ context.Context, request ReinitDataDirArchiveRequest) (ReinitDataDirArchiveResult, error) {
	archiver.requests = append(archiver.requests, request)
	return archiver.result, archiver.err
}

type recordingReinitWALGRestorer struct {
	requests []ReinitWALGRestoreRequest
	result   ReinitWALGRestoreResult
	err      error
}

func (restorer *recordingReinitWALGRestorer) RestoreFromWALG(_ context.Context, request ReinitWALGRestoreRequest) (ReinitWALGRestoreResult, error) {
	restorer.requests = append(restorer.requests, request)
	return restorer.result, restorer.err
}

type recordingReinitRecoveryConfigurator struct {
	requests []ReinitRecoveryConfigRequest
	result   ReinitRecoveryConfigResult
	err      error
}

func (configurator *recordingReinitRecoveryConfigurator) ConfigureReinitRecovery(_ context.Context, request ReinitRecoveryConfigRequest) (ReinitRecoveryConfigResult, error) {
	configurator.requests = append(configurator.requests, request)
	return configurator.result, configurator.err
}

type recordingReinitStandbyRestarter struct {
	requests []ReinitStandbyRestartRequest
	err      error
}

func (restarter *recordingReinitStandbyRestarter) RestartReinitStandby(_ context.Context, request ReinitStandbyRestartRequest) error {
	restarter.requests = append(restarter.requests, request)
	return restarter.err
}
