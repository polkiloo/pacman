package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStorePublishNodeStatusPreservesControlPlaneRejoinFlags(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 30, 12, 0, 0, 0, time.UTC)
	store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second))

	if _, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{}); err != nil {
		t.Fatalf("execute rejoin standby config: %v", err)
	}

	heartbeat := readyStandbyStatus("alpha-1", now.Add(20*time.Second), 11, 0)
	heartbeat.Postgres.Address = "alpha-1-postgres:5432"
	heartbeat.Postgres.Details.SystemIdentifier = "sys-alpha"

	if _, err := store.PublishNodeStatus(context.Background(), heartbeat); err != nil {
		t.Fatalf("publish healthy standby heartbeat: %v", err)
	}

	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status after heartbeat")
	}

	if !member.NeedsRejoin || !member.PendingRestart || !member.Postgres.Details.PendingRestart {
		t.Fatalf("expected control-plane-managed rejoin flags to survive heartbeat merge, got %+v", member)
	}
}

func TestMemoryStateStoreVerifyRejoinReplicationKeepsOperationRunning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 30, 12, 30, 0, 0, time.UTC)
	store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second), now.Add(20*time.Second))

	publishVerifiedRejoinReplica(t, store, now.Add(30*time.Second))

	execution, err := store.VerifyRejoinReplication(context.Background())
	if err != nil {
		t.Fatalf("verify rejoin replication: %v", err)
	}

	if execution.State != cluster.RejoinStateVerifyingReplication || !execution.ReplicationVerified || execution.Completed || execution.CurrentEpoch != 7 {
		t.Fatalf("unexpected rejoin verification execution: %+v", execution)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active rejoin operation after verification")
	}

	if active.Kind != cluster.OperationKindRejoin || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active rejoin operation after verification: %+v", active)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status during verified rejoin")
	}

	if status.Phase != cluster.ClusterPhaseRecovering {
		t.Fatalf("expected verified rejoin to keep cluster recovering, got %+v", status)
	}

	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status during verified rejoin")
	}

	if member.State != cluster.MemberStateStreaming || !member.NeedsRejoin || member.PendingRestart {
		t.Fatalf("expected verified standby to remain under rejoin control until completion, got %+v", member)
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected verification to keep rejoin active without history, got %+v", history)
	}
}

func TestMemoryStateStoreCompleteRejoinMarksMemberHealthyAgain(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 30, 13, 0, 0, 0, time.UTC)
	store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second), now.Add(20*time.Second), now.Add(30*time.Second), now.Add(40*time.Second))

	publishVerifiedRejoinReplica(t, store, now.Add(25*time.Second))
	if _, err := store.VerifyRejoinReplication(context.Background()); err != nil {
		t.Fatalf("verify rejoin replication: %v", err)
	}

	execution, err := store.CompleteRejoin(context.Background())
	if err != nil {
		t.Fatalf("complete rejoin: %v", err)
	}

	if execution.State != cluster.RejoinStateCompleted || !execution.Completed || execution.ReplicationVerified || execution.CurrentEpoch != 7 {
		t.Fatalf("unexpected rejoin completion execution: %+v", execution)
	}

	if execution.Operation.State != cluster.OperationStateCompleted || execution.Operation.Result != cluster.OperationResultSucceeded {
		t.Fatalf("expected completed rejoin operation, got %+v", execution.Operation)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected completed rejoin to clear active operation")
	}

	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status after completion")
	}

	if member.State != cluster.MemberStateStreaming || member.NeedsRejoin || member.PendingRestart {
		t.Fatalf("expected completed rejoin to clear synthetic flags, got %+v", member)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after rejoin completion")
	}

	if status.Phase != cluster.ClusterPhaseHealthy {
		t.Fatalf("expected cluster to return to healthy phase after rejoin completion, got %+v", status)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one completed rejoin history entry, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindRejoin || history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected rejoin history entry: %+v", history[0])
	}
}

func TestMemoryStateStoreVerifyAndCompleteRejoinRejectBlockedState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 30, 13, 30, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		call    func(t *testing.T) error
		wantErr error
	}{
		{
			name: "verification requires active rejoin operation",
			call: func(t *testing.T) error {
				t.Helper()
				_, err := seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, nil).VerifyRejoinReplication(context.Background())
				return err
			},
			wantErr: ErrRejoinExecutionRequired,
		},
		{
			name: "verification requires healthy streaming replica state",
			call: func(t *testing.T) error {
				t.Helper()
				store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, now, now.Add(10*time.Second), now.Add(20*time.Second))
				_, err := store.VerifyRejoinReplication(context.Background())
				return err
			},
			wantErr: ErrRejoinReplicationNotHealthy,
		},
		{
			name: "completion requires verified replication state",
			call: func(t *testing.T) error {
				t.Helper()
				store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, now, now.Add(10*time.Second), now.Add(20*time.Second))
				_, err := store.CompleteRejoin(context.Background())
				return err
			},
			wantErr: ErrRejoinReplicationNotHealthy,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.call(t)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected rejoin verification/completion error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func seededRestartedRejoinStore(t *testing.T, spec cluster.ClusterSpec, times ...time.Time) *MemoryStateStore {
	t.Helper()

	if len(times) < 3 {
		t.Fatal("restart rejoin setup requires at least three time values")
	}

	store := seededPreparedRejoinStore(t, spec, times[0], times[1], times[2])
	if _, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{}); err != nil {
		t.Fatalf("execute rejoin standby config: %v", err)
	}
	if _, err := store.ExecuteRejoinRestartAsStandby(context.Background(), &recordingStandbyRestarter{}); err != nil {
		t.Fatalf("execute rejoin restart as standby: %v", err)
	}

	if len(times) > 3 {
		store.now = sequencedNow(times[3:]...)
	}

	return store
}

func publishVerifiedRejoinReplica(t *testing.T, store *MemoryStateStore, observedAt time.Time) {
	t.Helper()

	status := readyStandbyStatus("alpha-1", observedAt, 11, 0)
	status.NeedsRejoin = false
	status.Postgres.Address = "alpha-1-postgres:5432"
	status.Postgres.Details.SystemIdentifier = "sys-alpha"
	status.Postgres.Details.PendingRestart = false
	status.PendingRestart = false

	if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("publish verified standby state: %v", err)
	}
}
