package controlplane

import (
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestOperationTraceCountsRecordObservedOperationTransitions(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	now := time.Date(2026, time.April, 15, 12, 30, 0, 0, time.UTC)

	switchoverAccepted := cluster.Operation{
		ID:          "sw-1",
		Kind:        cluster.OperationKindSwitchover,
		State:       cluster.OperationStateAccepted,
		RequestedAt: now,
		Result:      cluster.OperationResultPending,
	}
	switchoverRunning := switchoverAccepted.Clone()
	switchoverRunning.State = cluster.OperationStateRunning
	switchoverRunning.StartedAt = now
	switchoverRunning.Result = cluster.OperationResultPending

	switchoverCompleted := switchoverAccepted.Clone()
	switchoverCompleted.State = cluster.OperationStateCompleted
	switchoverCompleted.CompletedAt = now
	switchoverCompleted.Result = cluster.OperationResultSucceeded

	maintenanceCompleted := cluster.Operation{
		ID:          "maint-1",
		Kind:        cluster.OperationKindMaintenanceChange,
		State:       cluster.OperationStateCompleted,
		RequestedAt: now,
		CompletedAt: now,
		Result:      cluster.OperationResultSucceeded,
	}

	store.mu.Lock()
	store.journalOperationLocked(switchoverAccepted, now)
	store.journalOperationLocked(switchoverAccepted, now)
	store.journalOperationLocked(switchoverRunning, now)
	store.journalOperationLocked(switchoverCompleted, now)
	store.journalOperationLocked(maintenanceCompleted, now)
	store.mu.Unlock()

	counts := store.OperationTraceCounts()
	got := make(map[string]uint64, len(counts))
	for _, count := range counts {
		got[string(count.Kind)+":"+string(count.State)] = count.Count
	}

	if got["switchover:accepted"] != 1 {
		t.Fatalf("expected one switchover accepted trace, got %+v", got)
	}

	if got["switchover:running"] != 1 {
		t.Fatalf("expected one switchover running trace, got %+v", got)
	}

	if got["switchover:completed"] != 1 {
		t.Fatalf("expected one switchover completed trace, got %+v", got)
	}

	if _, ok := got["maintenance_change:completed"]; ok {
		t.Fatalf("expected maintenance transitions to be excluded, got %+v", got)
	}
}
