package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

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
