package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestMemoryStateStoreCreateReinitIntentCreatesDistinctOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 16, 9, 0, 0, 0, time.UTC)
	store := seededReinitStore(t, now, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
		failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, true, 0, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

	intent, err := store.CreateReinitIntent(context.Background(), ReinitRequest{
		Member:      " alpha-2 ",
		RequestedBy: " ops ",
		Reason:      " reclone from WAL-G ",
	})
	if err != nil {
		t.Fatalf("create reinit intent: %v", err)
	}

	if intent.Operation.Kind != cluster.OperationKindReinit || intent.Operation.Kind == cluster.OperationKindRejoin {
		t.Fatalf("unexpected reinit operation kind: %+v", intent.Operation)
	}

	if intent.Operation.State != cluster.OperationStateAccepted || intent.Operation.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected reinit operation lifecycle: %+v", intent.Operation)
	}

	if intent.Operation.FromMember != "alpha-1" || intent.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected reinit operation members: %+v", intent.Operation)
	}

	if intent.Operation.RequestedBy != "ops" || intent.Operation.Reason != "reclone from WAL-G" {
		t.Fatalf("unexpected normalized reinit operation metadata: %+v", intent.Operation)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.ID != intent.Operation.ID || active.Kind != cluster.OperationKindReinit {
		t.Fatalf("expected active reinit operation, got ok=%v operation=%+v", ok, active)
	}
}

func TestMemoryStateStoreExecuteReinitStopPostgresStopsTargetBeforeRestore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 9, 0, 0, 0, time.UTC)
	store := seededReinitStore(t, now, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(time.Second), true, 21, 0),
	})
	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 12
	store.mu.Unlock()
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

	intent, err := store.CreateReinitIntent(context.Background(), ReinitRequest{
		Member:      "alpha-2",
		RequestedBy: "ops",
		Reason:      "reclone from WAL-G",
	})
	if err != nil {
		t.Fatalf("create reinit intent: %v", err)
	}

	stopper := &recordingReinitPostgresStopper{}
	execution, err := store.ExecuteReinitStopPostgres(context.Background(), " alpha-2 ", stopper)
	if err != nil {
		t.Fatalf("execute reinit PostgreSQL stop: %v", err)
	}

	if !execution.PostgresStopped || execution.CurrentEpoch != 12 {
		t.Fatalf("unexpected reinit execution result: %+v", execution)
	}
	if execution.Operation.ID != intent.Operation.ID || execution.Operation.Kind != cluster.OperationKindReinit {
		t.Fatalf("unexpected reinit operation payload: %+v", execution.Operation)
	}
	if execution.Operation.State != cluster.OperationStateRunning || execution.Operation.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected reinit operation lifecycle: %+v", execution.Operation)
	}
	if len(stopper.requests) != 1 {
		t.Fatalf("expected one PostgreSQL stop request, got %+v", stopper.requests)
	}

	request := stopper.requests[0]
	if request.Operation.ID != intent.Operation.ID || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected stop request: %+v", request)
	}
	if request.Validation.Target.Name != "alpha-2" || request.Validation.CurrentPrimary.Name != "alpha-1" {
		t.Fatalf("unexpected stop validation: %+v", request.Validation)
	}

	target, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected reinit target node status")
	}
	if target.State != cluster.MemberStateStopping || target.Postgres.Up {
		t.Fatalf("expected stopped reinit target, got %+v", target)
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected stopped reinit to keep operation active without history, got %+v", history)
	}
}

func TestMemoryStateStoreExecuteReinitStopPostgresRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 9, 30, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		prepare func(t *testing.T) *MemoryStateStore
		member  string
		stopper ReinitPostgresStopExecutor
		wantErr error
	}{
		{
			name: "stopper is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededReinitIntentStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitPostgresStopExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededReinitIntentStore(t, now)
			},
			stopper: &recordingReinitPostgresStopper{},
			wantErr: ErrReinitTargetRequired,
		},
		{
			name: "active reinit is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededReinitStore(t, now, []agentmodel.NodeStatus{
					failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
					failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now.Add(time.Second), true, 21, 0),
				})
			},
			member:  "alpha-2",
			stopper: &recordingReinitPostgresStopper{},
			wantErr: ErrReinitExecutionRequired,
		},
		{
			name: "local member must match target",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededReinitIntentStore(t, now)
			},
			member:  "alpha-3",
			stopper: &recordingReinitPostgresStopper{},
			wantErr: ErrReinitExecutionChanged,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitStopPostgres(context.Background(), testCase.member, testCase.stopper)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit stop error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitStopPostgresRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 10, 0, 0, 0, time.UTC)
	store := seededReinitIntentStore(t, now)

	stopErr := errors.New("pg_ctl stop failed")
	_, err := store.ExecuteReinitStopPostgres(context.Background(), "alpha-2", &recordingReinitPostgresStopper{err: stopErr})
	if !errors.Is(err, stopErr) {
		t.Fatalf("execute reinit stop error: got %v want %v", err, stopErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed reinit stop to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}

func TestMemoryStateStoreExecuteReinitArchiveDataDirArchivesAfterStop(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 10, 30, 0, 0, time.UTC)
	store := seededStoppedReinitStore(t, now)

	archiver := &recordingReinitDataDirArchiver{
		result: ReinitDataDirArchiveResult{
			DataDir:     "/var/lib/postgresql/data",
			ArchivePath: "/var/lib/postgresql/.pacman-reinit-archive/data-op-1",
			Archived:    true,
		},
	}
	execution, err := store.ExecuteReinitArchiveDataDir(context.Background(), " alpha-2 ", archiver)
	if err != nil {
		t.Fatalf("execute reinit data dir archive: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || execution.ArchivePath != archiver.result.ArchivePath {
		t.Fatalf("unexpected reinit archive execution: %+v", execution)
	}
	if len(archiver.requests) != 1 {
		t.Fatalf("expected one data dir archive request, got %+v", archiver.requests)
	}
	request := archiver.requests[0]
	if request.Operation.Kind != cluster.OperationKindReinit || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected archive request: %+v", request)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after archive")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active operation after archive: %+v", active)
	}
	if active.Message != reinitDataDirArchiveCompletedMessage("alpha-2") {
		t.Fatalf("unexpected archive operation message: %+v", active)
	}
}

func TestMemoryStateStoreExecuteReinitArchiveDataDirRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 11, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		prepare  func(t *testing.T) *MemoryStateStore
		member   string
		archiver ReinitDataDirArchiveExecutor
		wantErr  error
	}{
		{
			name: "archiver is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededStoppedReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitDataDirArchiveExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededStoppedReinitStore(t, now)
			},
			archiver: &recordingReinitDataDirArchiver{},
			wantErr:  ErrReinitTargetRequired,
		},
		{
			name: "postgres must be stopped",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededReinitIntentStore(t, now)
			},
			member:   "alpha-2",
			archiver: &recordingReinitDataDirArchiver{},
			wantErr:  ErrReinitExecutionChanged,
		},
		{
			name: "already archived blocks repeat",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededStoppedReinitStore(t, now)
				if _, err := store.ExecuteReinitArchiveDataDir(context.Background(), "alpha-2", &recordingReinitDataDirArchiver{}); err != nil {
					t.Fatalf("archive data dir: %v", err)
				}
				return store
			},
			member:   "alpha-2",
			archiver: &recordingReinitDataDirArchiver{},
			wantErr:  ErrReinitExecutionChanged,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitArchiveDataDir(context.Background(), testCase.member, testCase.archiver)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit archive error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitArchiveDataDirRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 11, 30, 0, 0, time.UTC)
	store := seededStoppedReinitStore(t, now)

	archiveErr := errors.New("rename failed")
	_, err := store.ExecuteReinitArchiveDataDir(context.Background(), "alpha-2", &recordingReinitDataDirArchiver{err: archiveErr})
	if !errors.Is(err, archiveErr) {
		t.Fatalf("execute reinit archive error: got %v want %v", err, archiveErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed archive to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}

func TestMemoryStateStoreExecuteReinitWALGRestoreRestoresAfterArchive(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 12, 0, 0, 0, time.UTC)
	store := seededArchivedReinitStore(t, now)

	restorer := &recordingReinitWALGRestorer{
		result: ReinitWALGRestoreResult{
			DataDir:    "/var/lib/postgresql/data",
			BackupName: "LATEST",
		},
	}
	execution, err := store.ExecuteReinitWALGRestore(context.Background(), " alpha-2 ", restorer)
	if err != nil {
		t.Fatalf("execute reinit WAL-G restore: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || !execution.WALGRestored || execution.WALGBackupName != "LATEST" {
		t.Fatalf("unexpected reinit WAL-G restore execution: %+v", execution)
	}
	if len(restorer.requests) != 1 {
		t.Fatalf("expected one WAL-G restore request, got %+v", restorer.requests)
	}
	request := restorer.requests[0]
	if request.Operation.Kind != cluster.OperationKindReinit || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected WAL-G restore request: %+v", request)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after WAL-G restore")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active operation after WAL-G restore: %+v", active)
	}
	if active.Message != reinitWALGRestoreCompletedMessage("alpha-2") {
		t.Fatalf("unexpected WAL-G restore operation message: %+v", active)
	}
}

func TestMemoryStateStoreExecuteReinitWALGRestoreRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 12, 30, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		prepare  func(t *testing.T) *MemoryStateStore
		member   string
		restorer ReinitWALGRestoreExecutor
		wantErr  error
	}{
		{
			name: "restorer is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededArchivedReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitWALGRestoreExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededArchivedReinitStore(t, now)
			},
			restorer: &recordingReinitWALGRestorer{},
			wantErr:  ErrReinitTargetRequired,
		},
		{
			name: "archive must be completed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededStoppedReinitStore(t, now)
			},
			member:   "alpha-2",
			restorer: &recordingReinitWALGRestorer{},
			wantErr:  ErrReinitExecutionChanged,
		},
		{
			name: "already restored blocks repeat",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededArchivedReinitStore(t, now)
				if _, err := store.ExecuteReinitWALGRestore(context.Background(), "alpha-2", &recordingReinitWALGRestorer{}); err != nil {
					t.Fatalf("restore from WAL-G: %v", err)
				}
				return store
			},
			member:   "alpha-2",
			restorer: &recordingReinitWALGRestorer{},
			wantErr:  ErrReinitExecutionChanged,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitWALGRestore(context.Background(), testCase.member, testCase.restorer)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit WAL-G restore error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitWALGRestoreRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 13, 0, 0, 0, time.UTC)
	store := seededArchivedReinitStore(t, now)

	restoreErr := errors.New("wal-g backup-fetch failed")
	_, err := store.ExecuteReinitWALGRestore(context.Background(), "alpha-2", &recordingReinitWALGRestorer{err: restoreErr})
	if !errors.Is(err, restoreErr) {
		t.Fatalf("execute reinit WAL-G restore error: got %v want %v", err, restoreErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed WAL-G restore to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}

func TestMemoryStateStoreExecuteReinitRecoveryConfigRendersAfterWALGRestore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 13, 30, 0, 0, time.UTC)
	store := seededWALGRestoredReinitStore(t, now)

	configurator := &recordingReinitRecoveryConfigurator{
		result: ReinitRecoveryConfigResult{
			DataDir:        "/var/lib/postgresql/data",
			RestoreCommand: "env WALG_FILE_PREFIX=/backup wal-g wal-fetch %f %p",
		},
	}
	execution, err := store.ExecuteReinitRecoveryConfig(context.Background(), " alpha-2 ", configurator)
	if err != nil {
		t.Fatalf("execute reinit recovery config: %v", err)
	}

	if !execution.PostgresStopped || !execution.DataDirArchived || !execution.WALGRestored || !execution.RecoveryConfig {
		t.Fatalf("unexpected reinit recovery config execution: %+v", execution)
	}
	if execution.RestoreCommand != configurator.result.RestoreCommand {
		t.Fatalf("unexpected restore command: got %q want %q", execution.RestoreCommand, configurator.result.RestoreCommand)
	}
	if len(configurator.requests) != 1 {
		t.Fatalf("expected one recovery config request, got %+v", configurator.requests)
	}
	request := configurator.requests[0]
	if request.Operation.Kind != cluster.OperationKindReinit || request.TargetNode.NodeName != "alpha-2" || request.CurrentPrimaryNode.NodeName != "alpha-1" {
		t.Fatalf("unexpected recovery config request: %+v", request)
	}
	if request.Standby.PrimaryConnInfo != "host=10.0.0.1 port=5432 application_name=alpha-2" {
		t.Fatalf("unexpected recovery primary_conninfo: %+v", request.Standby)
	}
	if request.Standby.RecoveryTargetTimeline != postgres.DefaultRecoveryTargetTimeline {
		t.Fatalf("unexpected recovery target timeline: %+v", request.Standby)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active reinit operation after recovery config")
	}
	if active.Kind != cluster.OperationKindReinit || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active operation after recovery config: %+v", active)
	}
	if active.Message != reinitRecoveryConfigCompletedMessage("alpha-2") {
		t.Fatalf("unexpected recovery config operation message: %+v", active)
	}

	target, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected reinit target node status")
	}
	if !target.PendingRestart || !target.Postgres.Details.PendingRestart || target.Postgres.Up {
		t.Fatalf("expected recovery-configured target to be pending restart and down, got %+v", target)
	}
}

func TestMemoryStateStoreExecuteReinitRecoveryConfigRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 14, 0, 0, 0, time.UTC)

	testCases := []struct {
		name         string
		prepare      func(t *testing.T) *MemoryStateStore
		member       string
		configurator ReinitRecoveryConfigExecutor
		wantErr      error
	}{
		{
			name: "configurator is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededWALGRestoredReinitStore(t, now)
			},
			member:  "alpha-2",
			wantErr: ErrReinitRecoveryConfigExecutorRequired,
		},
		{
			name: "member is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededWALGRestoredReinitStore(t, now)
			},
			configurator: &recordingReinitRecoveryConfigurator{},
			wantErr:      ErrReinitTargetRequired,
		},
		{
			name: "WAL-G restore must be completed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				return seededArchivedReinitStore(t, now)
			},
			member:       "alpha-2",
			configurator: &recordingReinitRecoveryConfigurator{},
			wantErr:      ErrReinitExecutionChanged,
		},
		{
			name: "already configured blocks repeat",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()
				store := seededWALGRestoredReinitStore(t, now)
				if _, err := store.ExecuteReinitRecoveryConfig(context.Background(), "alpha-2", &recordingReinitRecoveryConfigurator{}); err != nil {
					t.Fatalf("configure recovery: %v", err)
				}
				return store
			},
			member:       "alpha-2",
			configurator: &recordingReinitRecoveryConfigurator{},
			wantErr:      ErrReinitExecutionChanged,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteReinitRecoveryConfig(context.Background(), testCase.member, testCase.configurator)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected reinit recovery config error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteReinitRecoveryConfigRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 14, 30, 0, 0, time.UTC)
	store := seededWALGRestoredReinitStore(t, now)

	configErr := errors.New("write recovery config failed")
	_, err := store.ExecuteReinitRecoveryConfig(context.Background(), "alpha-2", &recordingReinitRecoveryConfigurator{err: configErr})
	if !errors.Is(err, configErr) {
		t.Fatalf("execute reinit recovery config error: got %v want %v", err, configErr)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed recovery config to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed reinit history entry, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindReinit || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed reinit history entry: %+v", history[0])
	}
}

func TestMemoryStateStoreValidateReinitRejectsIneligibleTargets(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 16, 9, 30, 0, 0, time.UTC)

	cases := []struct {
		name    string
		member  string
		wantErr error
	}{
		{name: "missing member", member: "", wantErr: ErrReinitTargetRequired},
		{name: "unknown member", member: "missing", wantErr: ErrReinitTargetUnknown},
		{name: "current primary", member: "alpha-1", wantErr: ErrReinitTargetIsCurrentPrimary},
		{name: "witness", member: "witness-1", wantErr: ErrReinitTargetIsWitness},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := seededReinitStore(t, now, []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
				failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
				failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, true, 0, 0),
			})

			_, err := store.ValidateReinit(context.Background(), ReinitRequest{Member: tc.member})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("validate reinit error: got %v want %v", err, tc.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreValidateReinitRequiresHealthyPrimaryAndNoActiveOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 16, 10, 0, 0, 0, time.UTC)

	t.Run("source primary unhealthy", func(t *testing.T) {
		t.Parallel()

		store := seededReinitStore(t, now, []agentmodel.NodeStatus{
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 21, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
		})

		_, err := store.ValidateReinit(context.Background(), ReinitRequest{Member: "alpha-2"})
		if !errors.Is(err, ErrReinitSourcePrimaryUnhealthy) {
			t.Fatalf("validate reinit error: got %v want %v", err, ErrReinitSourcePrimaryUnhealthy)
		}
	})

	t.Run("operation in progress", func(t *testing.T) {
		t.Parallel()

		store := seededReinitStore(t, now, []agentmodel.NodeStatus{
			failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, now, true, 21, 0),
			failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(time.Second), false, 21, 0),
		})

		_, err := store.JournalOperation(context.Background(), cluster.Operation{
			ID:          "switchover-active",
			Kind:        cluster.OperationKindSwitchover,
			State:       cluster.OperationStateRunning,
			RequestedAt: now,
			Result:      cluster.OperationResultPending,
		})
		if err != nil {
			t.Fatalf("journal active operation: %v", err)
		}

		_, err = store.ValidateReinit(context.Background(), ReinitRequest{Member: "alpha-2"})
		if !errors.Is(err, ErrReinitOperationInProgress) {
			t.Fatalf("validate reinit error: got %v want %v", err, ErrReinitOperationInProgress)
		}
	})
}

func seededReinitStore(t *testing.T, now time.Time, statuses []agentmodel.NodeStatus) *MemoryStateStore {
	t.Helper()

	return seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
			{Name: "witness-1"},
		},
	}, statuses)
}

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

func reinitNodeStatus(nodeName string, role cluster.MemberRole, state cluster.MemberState, observedAt time.Time, postgresUp bool, timeline int64, lagBytes int64, address string) agentmodel.NodeStatus {
	status := failoverNodeStatus(nodeName, role, state, observedAt, postgresUp, timeline, lagBytes)
	status.Postgres.Address = address

	return status
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
