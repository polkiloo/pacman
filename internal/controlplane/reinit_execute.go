package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

type preparedReinitExecution struct {
	validation         ReinitValidation
	targetNode         agentmodel.NodeStatus
	currentPrimaryNode agentmodel.NodeStatus
	operation          cluster.Operation
	currentEpoch       cluster.Epoch
	archivePath        string
	walgBackupName     string
	restoreCommand     string
	standby            postgres.StandbyConfig
	verification       ReinitReplicationVerificationResult
	executedAt         time.Time
}

// ExecuteReinitStopPostgres stops PostgreSQL on the reinit target and leaves
// the active operation running for the later destructive restore phases.

func (store *MemoryStateStore) activeReinitOperationLocked() (cluster.Operation, error) {
	if store.activeOperation == nil {
		return cluster.Operation{}, ErrReinitExecutionRequired
	}

	operation := store.activeOperation.Clone()
	if operation.Kind != cluster.OperationKindReinit || operation.State.IsTerminal() {
		return cluster.Operation{}, ErrReinitExecutionRequired
	}

	if strings.TrimSpace(operation.FromMember) == "" || strings.TrimSpace(operation.ToMember) == "" {
		return cluster.Operation{}, ErrReinitExecutionRequired
	}

	return operation, nil
}

func (store *MemoryStateStore) reinitOperationForPublicationLocked(expected cluster.Operation) (cluster.Operation, error) {
	if store.activeOperation == nil || store.activeOperation.ID != expected.ID || store.activeOperation.Kind != cluster.OperationKindReinit {
		return cluster.Operation{}, ErrReinitExecutionChanged
	}

	return store.activeOperation.Clone(), nil
}

func (store *MemoryStateStore) failReinitExecution(prepared preparedReinitExecution, message string) {
	store.mu.Lock()
	failed := prepared.operation.Clone()
	failed.State = cluster.OperationStateFailed
	failed.Result = cluster.OperationResultFailed
	failed.CompletedAt = prepared.executedAt
	failed.Message = message
	store.journalOperationLocked(failed, prepared.executedAt)
	store.refreshSourceOfTruthLocked(prepared.executedAt)
	store.mu.Unlock()

	if err := store.persistJournaledOperation(context.Background(), failed); err != nil {
		store.logger.Error("failed to persist failed reinit operation", "operation_id", failed.ID, "error", err)
	}

	if err := store.refreshCache(context.Background()); err != nil {
		store.logger.Error("failed to refresh cache after reinit failure", "operation_id", failed.ID, "error", err)
	}
}
