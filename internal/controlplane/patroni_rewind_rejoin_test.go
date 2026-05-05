package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestPatroniInspiredExecuteRejoinRewindSkipsDirectRejoinCandidate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 3, 10, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now, 11, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
	})

	rewinder := &recordingRewinder{}
	_, err := store.ExecuteRejoinRewind(context.Background(), RejoinRequest{Member: "alpha-1"}, rewinder)
	if !errors.Is(err, ErrRejoinRewindNotRequired) {
		t.Fatalf("unexpected direct rejoin rewind error: got %v, want %v", err, ErrRejoinRewindNotRequired)
	}

	if len(rewinder.requests) != 0 {
		t.Fatalf("expected direct rejoin candidate to skip pg_rewind executor, got %+v", rewinder.requests)
	}
	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected skipped rewind not to create an active operation")
	}
	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected skipped rewind not to record history, got %+v", history)
	}
}

func TestPatroniInspiredExecuteRejoinDirectContinuesWithoutRewind(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 3, 10, 30, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now, 11, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
	})
	setTestNow(store, func() time.Time { return now })

	execution, err := store.ExecuteRejoinDirect(context.Background(), RejoinRequest{
		Member:      " alpha-1 ",
		RequestedBy: " operator ",
	})
	if err != nil {
		t.Fatalf("execute direct rejoin: %v", err)
	}

	if execution.State != cluster.RejoinStateConfiguringStandby || execution.Rewound || execution.Decision.Decided || !execution.Decision.DirectRejoinPossible {
		t.Fatalf("unexpected direct rejoin execution: %+v", execution)
	}
	if execution.Operation.Message != rejoinDirectReadyMessage("alpha-1", "alpha-2") {
		t.Fatalf("unexpected direct rejoin operation message: %+v", execution.Operation)
	}
	if execution.Operation.RequestedBy != "operator" || execution.Operation.Reason != "direct rejoin of former primary without pg_rewind" {
		t.Fatalf("unexpected direct rejoin operation metadata: %+v", execution.Operation)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected direct rejoin to create active operation")
	}
	if active.FromMember != "alpha-1" || active.ToMember != "alpha-2" || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active direct rejoin operation: %+v", active)
	}
}

func TestPatroniInspiredRejoinVerificationRejectsTimelineAndSystemIdentifierMismatches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 3, 11, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name   string
		mutate func(agentmodel.NodeStatus) agentmodel.NodeStatus
	}{
		{
			name: "timeline mismatch",
			mutate: func(status agentmodel.NodeStatus) agentmodel.NodeStatus {
				status.Postgres.Details.Timeline = 10
				return status
			},
		},
		{
			name: "system identifier mismatch",
			mutate: func(status agentmodel.NodeStatus) agentmodel.NodeStatus {
				status.Postgres.Details.SystemIdentifier = "sys-other"
				return status
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
				ClusterName: "alpha",
				Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
			}, now, now.Add(10*time.Second), now.Add(20*time.Second))

			standby := readyStandbyStatus("alpha-1", now.Add(30*time.Second), 11, 0)
			standby.NeedsRejoin = false
			standby.Postgres.Address = "alpha-1-postgres:5432"
			standby.Postgres.Details.SystemIdentifier = "sys-alpha"
			standby = testCase.mutate(standby)

			if _, err := store.PublishNodeStatus(context.Background(), standby); err != nil {
				t.Fatalf("publish rejoin standby state: %v", err)
			}

			_, err := store.VerifyRejoinReplication(context.Background())
			if !errors.Is(err, ErrRejoinReplicationNotHealthy) {
				t.Fatalf("unexpected verification error: got %v, want %v", err, ErrRejoinReplicationNotHealthy)
			}
		})
	}
}
