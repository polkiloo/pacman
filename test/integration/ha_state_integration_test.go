//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

func TestRejoinOperationProjectsRecoveringPhaseWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	operation, err := store.JournalOperation(context.Background(), cluster.Operation{
		ID:          "rejoin-integration",
		Kind:        cluster.OperationKindRejoin,
		State:       cluster.OperationStateRunning,
		RequestedBy: "integration-test",
		RequestedAt: observedAt.Add(2 * time.Second),
		FromMember:  "alpha-1",
	})
	if err != nil {
		t.Fatalf("journal rejoin operation: %v", err)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status during rejoin")
	}

	if status.Phase != cluster.ClusterPhaseRecovering {
		t.Fatalf("expected recovering phase, got %+v", status)
	}

	if status.ActiveOperation == nil || status.ActiveOperation.ID != operation.ID || status.ActiveOperation.Kind != cluster.OperationKindRejoin {
		t.Fatalf("expected active rejoin projection, got %+v", status.ActiveOperation)
	}

	truth, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile cluster state: %v", err)
	}

	if truth.Observed == nil || truth.Observed.Phase != cluster.ClusterPhaseRecovering {
		t.Fatalf("expected reconciled recovering phase, got %+v", truth)
	}

	if truth.Observed.ActiveOperation == nil || truth.Observed.ActiveOperation.Kind != cluster.OperationKindRejoin {
		t.Fatalf("expected reconciled active rejoin operation, got %+v", truth.Observed.ActiveOperation)
	}
}

func TestMaintenanceOverridesActiveFailoverPhaseWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	primaryAddress := primary.Address(t)
	observedAt := time.Now().UTC()
	primaryObservation := publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	primary.Stop(t)
	waitForAddressUnavailable(t, primary.Name(), primaryAddress)
	publishUnavailableNodeStatus(t, store, "alpha-1", primaryAddress, observedAt.Add(2*time.Second), primaryObservation)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(3*time.Second))

	if _, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
		RequestedBy: "integration-test",
		Reason:      "maintenance override should keep failover visible",
	}); err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	maintenance, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     true,
		Reason:      "operator freeze",
		RequestedBy: "integration-test",
	})
	if err != nil {
		t.Fatalf("enable maintenance: %v", err)
	}

	if !maintenance.Enabled || maintenance.Reason != "operator freeze" {
		t.Fatalf("unexpected maintenance status: %+v", maintenance)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after maintenance override")
	}

	if status.Phase != cluster.ClusterPhaseMaintenance {
		t.Fatalf("expected maintenance phase to override failover phase, got %+v", status)
	}

	if status.ActiveOperation == nil || status.ActiveOperation.Kind != cluster.OperationKindFailover {
		t.Fatalf("expected active failover to remain visible during maintenance, got %+v", status.ActiveOperation)
	}

	if !status.Maintenance.Enabled || status.Maintenance.Reason != "operator freeze" {
		t.Fatalf("expected maintenance status in cluster projection, got %+v", status.Maintenance)
	}

	truth, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile cluster state: %v", err)
	}

	if truth.Observed == nil || truth.Observed.Phase != cluster.ClusterPhaseMaintenance {
		t.Fatalf("expected maintenance phase in reconciled truth, got %+v", truth)
	}
}
