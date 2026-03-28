//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

func TestFailoverPromotesRealStandbyAndRecordsHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	primaryAddress := primary.Address(t)
	primaryObservedAt := time.Now().UTC()
	primaryObservation := publishObservedNodeStatus(t, store, "alpha-1", primary, primaryObservedAt)
	standbyObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, primaryObservedAt.Add(time.Second))

	if primaryObservation.Details.Timeline == 0 {
		t.Fatalf("expected primary timeline from real postgres, got %+v", primaryObservation)
	}

	primary.Stop(t)
	waitForAddressUnavailable(t, primary.Name(), primaryAddress)

	failedPrimaryStatus := nodeStatusFromObservation("alpha-1", primaryAddress, primaryObservedAt.Add(2*time.Second), primaryObservation)
	failedPrimaryStatus.State = cluster.MemberStateFailed
	failedPrimaryStatus.Postgres.Up = false
	failedPrimaryStatus.Postgres.CheckedAt = failedPrimaryStatus.ObservedAt
	failedPrimaryStatus.Postgres.Errors.Availability = "postgres is unavailable"

	if _, err := store.PublishNodeStatus(context.Background(), failedPrimaryStatus); err != nil {
		t.Fatalf("publish failed primary state: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), nodeStatusFromObservation("alpha-2", standby.Address(t), failedPrimaryStatus.ObservedAt.Add(time.Second), standbyObservation)); err != nil {
		t.Fatalf("refresh standby state before failover: %v", err)
	}

	intent, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
		RequestedBy: "integration-test",
		Reason:      "primary container stopped",
	})
	if err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	if intent.CurrentPrimary != "alpha-1" || intent.Candidate != "alpha-2" {
		t.Fatalf("unexpected failover intent: %+v", intent)
	}

	execution, err := store.ExecuteFailover(context.Background(), newPostgresPromotionExecutor(t, standby), nil)
	if err != nil {
		t.Fatalf("execute failover: %v", err)
	}

	waitForPostgresRole(t, standby, cluster.MemberRolePrimary)

	promotedObservedAt := time.Now().UTC()
	promotedObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, promotedObservedAt)
	if promotedObservation.Role != cluster.MemberRolePrimary || promotedObservation.InRecovery {
		t.Fatalf("expected promoted standby to become primary, got %+v", promotedObservation)
	}

	execSQL(t, standby, `
CREATE TABLE IF NOT EXISTS failover_writable_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, standby, `
INSERT INTO failover_writable_marker (id, payload)
VALUES (1, 'promoted')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after failover")
	}

	if status.CurrentPrimary != "alpha-2" || status.CurrentEpoch != execution.CurrentEpoch {
		t.Fatalf("unexpected cluster status after failover: %+v", status)
	}

	formerPrimary, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary state after failover")
	}

	if formerPrimary.State != cluster.MemberStateNeedsRejoin || !formerPrimary.NeedsRejoin {
		t.Fatalf("expected former primary to require rejoin, got %+v", formerPrimary)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failover history entry, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindFailover || history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected failover history entry: %+v", history[0])
	}
}
