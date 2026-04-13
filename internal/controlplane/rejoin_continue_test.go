package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreExecuteRejoinStandbyConfigMarksPendingRestart(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 0, 0, 0, time.UTC)
	store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Postgres: cluster.PostgresPolicy{
			Parameters: map[string]any{
				"restore_command":          "cp /archive/%f %p",
				"recovery_target_timeline": "current",
			},
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, now, now.Add(10*time.Second))

	configurator := &recordingStandbyConfigurer{}
	execution, err := store.ExecuteRejoinStandbyConfig(context.Background(), configurator)
	if err != nil {
		t.Fatalf("execute rejoin standby config: %v", err)
	}

	if execution.State != cluster.RejoinStateConfiguringStandby || !execution.StandbyConfigured || execution.RestartedAsStandby || execution.CurrentEpoch != 7 {
		t.Fatalf("unexpected rejoin standby config execution: %+v", execution)
	}

	if len(configurator.requests) != 1 {
		t.Fatalf("expected one standby config request, got %+v", configurator.requests)
	}

	request := configurator.requests[0]
	if request.Operation.Kind != cluster.OperationKindRejoin || request.Operation.FromMember != "alpha-1" || request.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected standby config operation payload: %+v", request.Operation)
	}

	if request.Standby.PrimaryConnInfo != "host=alpha-2-postgres port=5432 application_name=alpha-1" {
		t.Fatalf("unexpected standby primary_conninfo: %+v", request.Standby)
	}

	if request.Standby.PrimarySlotName != "alpha_1" || request.Standby.RestoreCommand != "cp /archive/%f %p" || request.Standby.RecoveryTargetTimeline != "current" {
		t.Fatalf("unexpected standby config payload: %+v", request.Standby)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active rejoin operation after standby config")
	}

	if active.Kind != cluster.OperationKindRejoin || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active rejoin operation: %+v", active)
	}

	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status after standby config")
	}

	if member.State != cluster.MemberStateNeedsRejoin || !member.NeedsRejoin || member.Postgres.Up {
		t.Fatalf("expected former primary to remain offline after standby config, got %+v", member)
	}

	if !member.PendingRestart || !member.Postgres.Details.PendingRestart {
		t.Fatalf("expected standby config to mark pending restart, got %+v", member)
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected standby config success to keep rejoin active without history, got %+v", history)
	}
}

func TestMemoryStateStoreExecuteRejoinRestartAsStandbyTransitionsToStarting(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 30, 0, 0, time.UTC)
	store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second), now.Add(20*time.Second))

	if _, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{}); err != nil {
		t.Fatalf("execute rejoin standby config: %v", err)
	}

	restarter := &recordingStandbyRestarter{}
	execution, err := store.ExecuteRejoinRestartAsStandby(context.Background(), restarter)
	if err != nil {
		t.Fatalf("execute rejoin restart as standby: %v", err)
	}

	if execution.State != cluster.RejoinStateStartingReplica || !execution.RestartedAsStandby || execution.StandbyConfigured || execution.CurrentEpoch != 7 {
		t.Fatalf("unexpected restart-as-standby execution: %+v", execution)
	}

	if len(restarter.requests) != 1 {
		t.Fatalf("expected one standby restart request, got %+v", restarter.requests)
	}

	request := restarter.requests[0]
	if request.Operation.Kind != cluster.OperationKindRejoin || request.Operation.FromMember != "alpha-1" || request.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected standby restart operation payload: %+v", request.Operation)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after standby restart")
	}

	if status.Phase != cluster.ClusterPhaseRecovering {
		t.Fatalf("expected recovering cluster phase during rejoin restart, got %+v", status)
	}

	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status after standby restart")
	}

	if member.State != cluster.MemberStateStarting || member.PendingRestart || !member.NeedsRejoin {
		t.Fatalf("expected restart to move member into starting state, got %+v", member)
	}

	if !member.Postgres.Up || !member.Postgres.RecoveryKnown || !member.Postgres.InRecovery || member.Postgres.Details.PendingRestart {
		t.Fatalf("expected restart to bring postgres up in recovery mode, got %+v", member)
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected restart success to keep rejoin active without history, got %+v", history)
	}
}

func TestMemoryStateStoreRejoinStandbyContinuationRejectsBlockedExecution(t *testing.T) {
	now := time.Date(2026, time.March, 30, 11, 0, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		call    func(t *testing.T) error
		wantErr error
	}{
		{
			name: "standby configurator is required",
			call: func(t *testing.T) error {
				t.Helper()
				store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, now)
				_, err := store.ExecuteRejoinStandbyConfig(context.Background(), nil)
				return err
			},
			wantErr: ErrRejoinStandbyConfigExecutorRequired,
		},
		{
			name: "standby config requires active rejoin operation",
			call: func(t *testing.T) error {
				t.Helper()
				store := seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, nil)
				_, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{})
				return err
			},
			wantErr: ErrRejoinExecutionRequired,
		},
		{
			name: "standby config requires current primary address",
			call: func(t *testing.T) error {
				t.Helper()

				formerPrimary := rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha")
				currentPrimary := rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha")
				currentPrimary.Postgres.Address = ""

				store := seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, []agentmodel.NodeStatus{formerPrimary, currentPrimary})
				setTestNow(store, sequencedNow(now))
				store.mu.Lock()
				store.clusterStatus.CurrentEpoch = 7
				store.mu.Unlock()
				if _, err := store.ExecuteRejoinRewind(context.Background(), RejoinRequest{Member: "alpha-1"}, &recordingRewinder{}); err != nil {
					t.Fatalf("execute rejoin rewind: %v", err)
				}

				_, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{})
				return err
			},
			wantErr: ErrRejoinCurrentPrimaryAddressRequired,
		},
		{
			name: "restart requires rendered standby configuration",
			call: func(t *testing.T) error {
				t.Helper()
				store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, now)
				_, err := store.ExecuteRejoinRestartAsStandby(context.Background(), &recordingStandbyRestarter{})
				return err
			},
			wantErr: ErrRejoinStandbyConfigurationRequired,
		},
		{
			name: "standby restarter is required",
			call: func(t *testing.T) error {
				t.Helper()
				store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, now, now.Add(10*time.Second))
				if _, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{}); err != nil {
					t.Fatalf("execute rejoin standby config: %v", err)
				}
				_, err := store.ExecuteRejoinRestartAsStandby(context.Background(), nil)
				return err
			},
			wantErr: ErrRejoinStandbyRestartExecutorRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call(t)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected rejoin continuation error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestRejoinStandbyFailureMessage(t *testing.T) {
	t.Parallel()

	if got := rejoinStandbyConfigFailedMessage("alpha-1", "alpha-2"); got != "standby configuration failed for alpha-1 against alpha-2" {
		t.Fatalf("unexpected standby config failure message: %q", got)
	}
}

func TestMemoryStateStoreExecuteRejoinRestartAsStandbyRecordsFailure(t *testing.T) {
	now := time.Date(2026, time.March, 30, 11, 30, 0, 0, time.UTC)
	store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second), now.Add(20*time.Second))

	if _, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{}); err != nil {
		t.Fatalf("execute rejoin standby config: %v", err)
	}

	restarter := &recordingStandbyRestarter{err: errors.New("restart failed")}
	_, err := store.ExecuteRejoinRestartAsStandby(context.Background(), restarter)
	if err == nil {
		t.Fatal("expected restart-as-standby failure")
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed standby restart to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed rejoin history entry after restart failure, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindRejoin || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed rejoin history entry: %+v", history[0])
	}
}

func seededPreparedRejoinStore(t *testing.T, spec cluster.ClusterSpec, times ...time.Time) *MemoryStateStore {
	t.Helper()

	if len(times) == 0 {
		t.Fatal("at least one time value is required")
	}

	store := seededFailoverStore(t, spec, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", times[0].Add(-time.Minute), 10, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", times[0].Add(-time.Minute+time.Second), 11, "sys-alpha"),
	})
	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 7
	store.mu.Unlock()
	setTestLeaseDuration(store, time.Hour)
	setTestNow(store, sequencedNow(times...))

	if _, err := store.ExecuteRejoinRewind(context.Background(), RejoinRequest{Member: "alpha-1"}, &recordingRewinder{}); err != nil {
		t.Fatalf("execute rejoin rewind: %v", err)
	}

	return store
}

func sequencedNow(times ...time.Time) func() time.Time {
	var mu sync.Mutex
	callCount := 0

	const callsPerStep = 64

	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		index := callCount / callsPerStep
		if index >= len(times) {
			return times[len(times)-1]
		}

		current := times[index]
		callCount++
		return current
	}
}

type recordingStandbyConfigurer struct {
	requests []StandbyConfigRequest
	err      error
}

func (configurer *recordingStandbyConfigurer) ConfigureStandby(_ context.Context, request StandbyConfigRequest) error {
	configurer.requests = append(configurer.requests, request)
	return configurer.err
}

type recordingStandbyRestarter struct {
	requests []StandbyRestartRequest
	err      error
}

func (restarter *recordingStandbyRestarter) RestartAsStandby(_ context.Context, request StandbyRestartRequest) error {
	restarter.requests = append(restarter.requests, request)
	return restarter.err
}
