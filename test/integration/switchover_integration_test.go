//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

func TestSwitchoverValidationUsesRealStreamingStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	primaryObservation := publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	standbyObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	if primaryObservation.Role != cluster.MemberRolePrimary || primaryObservation.InRecovery {
		t.Fatalf("expected real primary observation, got %+v", primaryObservation)
	}

	if standbyObservation.Role != cluster.MemberRoleReplica || !standbyObservation.InRecovery {
		t.Fatalf("expected real standby observation, got %+v", standbyObservation)
	}

	readiness, err := store.SwitchoverTargetReadiness("alpha-2")
	if err != nil {
		t.Fatalf("switchover target readiness: %v", err)
	}

	if !readiness.Ready || readiness.CurrentPrimary != "alpha-1" || readiness.Member.Name != "alpha-2" {
		t.Fatalf("unexpected switchover readiness: %+v", readiness)
	}

	if len(readiness.Reasons) != 0 {
		t.Fatalf("expected ready standby with no rejection reasons, got %+v", readiness.Reasons)
	}

	validation, err := store.ValidateSwitchover(context.Background(), controlplane.SwitchoverRequest{
		RequestedBy: "operator",
		Reason:      "planned switchover integration test",
		Candidate:   "alpha-2",
		ScheduledAt: time.Now().Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("validate switchover: %v", err)
	}

	if validation.CurrentPrimary.Name != "alpha-1" || validation.Target.Member.Name != "alpha-2" || !validation.Target.Ready {
		t.Fatalf("unexpected switchover validation: %+v", validation)
	}
}

func TestSwitchoverIntentSchedulesRealStreamingStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	scheduledAt := time.Now().UTC().Add(20 * time.Minute)
	intent, err := store.CreateSwitchoverIntent(context.Background(), controlplane.SwitchoverRequest{
		RequestedBy: "operator",
		Reason:      "scheduled switchover integration test",
		Candidate:   "alpha-2",
		ScheduledAt: scheduledAt,
	})
	if err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	if intent.Operation.Kind != cluster.OperationKindSwitchover || intent.Operation.State != cluster.OperationStateScheduled {
		t.Fatalf("expected scheduled switchover intent, got %+v", intent.Operation)
	}

	if !intent.Operation.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("unexpected switchover schedule time: got %v, want %v", intent.Operation.ScheduledAt, scheduledAt)
	}

	if intent.Validation.CurrentPrimary.Name != "alpha-1" || intent.Validation.Target.Member.Name != "alpha-2" || !intent.Validation.Target.Ready {
		t.Fatalf("unexpected validated switchover intent: %+v", intent)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after scheduling switchover")
	}

	if status.Phase != cluster.ClusterPhaseHealthy {
		t.Fatalf("expected scheduled switchover to keep cluster healthy, got %+v", status)
	}

	if status.ScheduledSwitchover == nil {
		t.Fatal("expected scheduled switchover projection in cluster status")
	}

	if !status.ScheduledSwitchover.At.Equal(scheduledAt) || status.ScheduledSwitchover.From != "alpha-1" || status.ScheduledSwitchover.To != "alpha-2" {
		t.Fatalf("unexpected scheduled switchover projection: %+v", status.ScheduledSwitchover)
	}
}
