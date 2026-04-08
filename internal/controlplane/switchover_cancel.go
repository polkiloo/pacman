package controlplane

import (
	"context"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// CancelSwitchover cancels a pending switchover intent before execution
// begins and records the cancelled operation in history.
func (store *MemoryStateStore) CancelSwitchover(ctx context.Context) (cluster.Operation, error) {
	if err := ctx.Err(); err != nil {
		return cluster.Operation{}, err
	}

	if err := store.ensureCacheFresh(ctx); err != nil {
		return cluster.Operation{}, err
	}

	cancelledAt := store.now().UTC()

	store.mu.Lock()
	if store.activeOperation == nil || store.activeOperation.Kind != cluster.OperationKindSwitchover || store.activeOperation.State.IsTerminal() {
		store.mu.Unlock()
		return cluster.Operation{}, ErrScheduledSwitchoverNotFound
	}

	if store.activeOperation.State == cluster.OperationStateRunning {
		store.mu.Unlock()
		return cluster.Operation{}, ErrSwitchoverAlreadyRunning
	}

	cancelled := cancelSwitchoverOperation(store.activeOperation.Clone(), cancelledAt)
	store.journalOperationLocked(cancelled, cancelledAt)
	store.refreshSourceOfTruthLocked(cancelledAt)
	store.mu.Unlock()

	if err := store.persistJournaledOperation(ctx, cancelled); err != nil {
		return cluster.Operation{}, err
	}

	if err := store.refreshCache(ctx); err != nil {
		return cluster.Operation{}, err
	}

	return cancelled.Clone(), nil
}

func cancelSwitchoverOperation(operation cluster.Operation, cancelledAt time.Time) cluster.Operation {
	cancelled := operation.Clone()
	cancelled.State = cluster.OperationStateCancelled
	cancelled.CompletedAt = cancelledAt
	cancelled.Result = cluster.OperationResultCancelled
	cancelled.Message = cancelledSwitchoverMessage(operation)

	return cancelled
}

func cancelledSwitchoverMessage(operation cluster.Operation) string {
	if operation.State == cluster.OperationStateScheduled || operation.ScheduledAt.After(operation.RequestedAt) {
		return "scheduled switchover cancelled"
	}

	return "switchover cancelled"
}
