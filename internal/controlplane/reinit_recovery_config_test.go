package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

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
	if request.PrimaryAddress != "10.0.0.1:5432" {
		t.Fatalf("unexpected recovery primary address: %q", request.PrimaryAddress)
	}
	if request.Standby.PrimarySlotName != "pacman_alpha_2" {
		t.Fatalf("unexpected recovery primary_slot_name: %+v", request.Standby)
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

func TestMemoryStateStorePublishNodeStatusPreservesReinitPendingRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 13, 45, 0, 0, time.UTC)
	store := seededWALGRestoredReinitStore(t, now)

	if _, err := store.ExecuteReinitRecoveryConfig(context.Background(), "alpha-2", &recordingReinitRecoveryConfigurator{}); err != nil {
		t.Fatalf("execute reinit recovery config: %v", err)
	}

	heartbeat := reinitNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, now.Add(10*time.Second), false, 21, 0, "10.0.0.2:5432")
	heartbeat.PendingRestart = false
	heartbeat.Postgres.Details.PendingRestart = false

	if _, err := store.PublishNodeStatus(context.Background(), heartbeat); err != nil {
		t.Fatalf("publish reinit target heartbeat: %v", err)
	}

	target, ok := store.NodeStatus("alpha-2")
	if !ok {
		t.Fatal("expected reinit target node status after heartbeat")
	}
	if !target.PendingRestart || !target.Postgres.Details.PendingRestart {
		t.Fatalf("expected reinit pending restart to survive heartbeat merge, got %+v", target)
	}
}

func TestMemoryStateStoreExecuteReinitRecoveryConfigUsesRegisteredPrimaryHost(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 17, 13, 50, 0, 0, time.UTC)
	store := seededWALGRestoredReinitStore(t, now)

	if err := store.RegisterMember(context.Background(), MemberRegistration{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "pacman-primary:8080",
		ControlAddress: "pacman-primary:9090",
		RegisteredAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("register primary member: %v", err)
	}

	configurator := &recordingReinitRecoveryConfigurator{}
	if _, err := store.ExecuteReinitRecoveryConfig(context.Background(), "alpha-2", configurator); err != nil {
		t.Fatalf("execute reinit recovery config: %v", err)
	}

	if len(configurator.requests) != 1 {
		t.Fatalf("expected one recovery config request, got %+v", configurator.requests)
	}
	if got, want := configurator.requests[0].Standby.PrimaryConnInfo, "host=pacman-primary port=5432 application_name=alpha-2"; got != want {
		t.Fatalf("unexpected recovery primary_conninfo: got %q want %q", got, want)
	}
	if got, want := configurator.requests[0].PrimaryAddress, "pacman-primary:5432"; got != want {
		t.Fatalf("unexpected recovery primary address: got %q want %q", got, want)
	}
}

func TestReinitPrimarySlotNameUsesLabReplicationSlotConvention(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		member string
		want   string
	}{
		{member: "alpha-2", want: "pacman_alpha_2"},
		{member: "alpha.3", want: "pacman_alpha_3"},
		{member: "alpha--4", want: "pacman_alpha__4"},
		{member: "", want: "pacman_rejoin"},
		{member: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnop", want: "pacman_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.member, func(t *testing.T) {
			t.Parallel()

			if got := reinitPrimarySlotName(testCase.member); got != testCase.want {
				t.Fatalf("unexpected reinit slot name: got %q want %q", got, testCase.want)
			}
		})
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
