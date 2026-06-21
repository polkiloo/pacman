package controlplane

import (
	"strings"

	"github.com/polkiloo/pacman/internal/cluster"
)

func (store *MemoryStateStore) latestReinitStatusLocked() *cluster.ReinitStatus {
	if status := reinitStatusFromActiveOperation(store.activeOperation); status != nil {
		return status
	}

	for index := len(store.history) - 1; index >= 0; index-- {
		if store.history[index].Kind != cluster.OperationKindReinit {
			continue
		}

		status := reinitStatusFromHistoryEntry(store.history[index])
		return &status
	}

	return nil
}

func (store *MemoryStateStore) reinitStatusForMemberLocked(memberName string) *cluster.ReinitStatus {
	target := strings.TrimSpace(memberName)
	if target == "" {
		return nil
	}

	if status := reinitStatusFromActiveOperation(store.activeOperation); status != nil && status.ToMember == target {
		return status
	}

	for index := len(store.history) - 1; index >= 0; index-- {
		entry := store.history[index]
		if entry.Kind != cluster.OperationKindReinit || strings.TrimSpace(entry.ToMember) != target {
			continue
		}

		status := reinitStatusFromHistoryEntry(entry)
		return &status
	}

	return nil
}

func reinitStatusFromActiveOperation(operation *cluster.Operation) *cluster.ReinitStatus {
	if operation == nil || operation.Kind != cluster.OperationKindReinit {
		return nil
	}

	status := cluster.ReinitStatus{
		OperationID: operation.ID,
		State:       reinitStateFromOperation(*operation),
		LastResult:  operation.Result,
		FromMember:  operation.FromMember,
		ToMember:    operation.ToMember,
		Message:     operation.Message,
		UpdatedAt:   operation.RequestedAt,
	}

	if !operation.CompletedAt.IsZero() {
		status.UpdatedAt = operation.CompletedAt
	} else if !operation.StartedAt.IsZero() {
		status.UpdatedAt = operation.StartedAt
	}
	if status.LastResult.IsZero() {
		status.LastResult = cluster.OperationResultPending
	}

	return &status
}

func reinitStatusFromHistoryEntry(entry cluster.HistoryEntry) cluster.ReinitStatus {
	return cluster.ReinitStatus{
		OperationID: entry.OperationID,
		State:       reinitStateFromHistoryEntry(entry),
		LastResult:  entry.Result,
		FromMember:  entry.FromMember,
		ToMember:    entry.ToMember,
		UpdatedAt:   entry.FinishedAt,
	}
}

func reinitStateFromOperation(operation cluster.Operation) cluster.ReinitState {
	switch {
	case operation.State == cluster.OperationStateAccepted:
		return cluster.ReinitStateAccepted
	case operation.State == cluster.OperationStateCompleted:
		return cluster.ReinitStateCompleted
	case operation.State == cluster.OperationStateFailed:
		return cluster.ReinitStateFailed
	case operation.State == cluster.OperationStateCancelled:
		return cluster.ReinitStateCancelled
	case operation.Message == reinitPostgresStopRunningMessage(operation.ToMember) ||
		operation.Message == reinitPostgresStopCompletedMessage(operation.ToMember):
		return cluster.ReinitStateStoppingPostgres
	case operation.Message == reinitDataDirArchiveRunningMessage(operation.ToMember) ||
		operation.Message == reinitDataDirArchiveCompletedMessage(operation.ToMember):
		return cluster.ReinitStateArchivingDataDir
	case operation.Message == reinitWALGRestoreRunningMessage(operation.ToMember) ||
		operation.Message == reinitWALGRestoreCompletedMessage(operation.ToMember):
		return cluster.ReinitStateRestoringBackup
	case operation.Message == reinitRecoveryConfigRunningMessage(operation.ToMember) ||
		operation.Message == reinitRecoveryConfigCompletedMessage(operation.ToMember):
		return cluster.ReinitStateRenderingRecoveryConfig
	case operation.Message == reinitStandbyRestartRunningMessage(operation.ToMember, operation.FromMember) ||
		operation.Message == reinitStandbyRestartCompletedMessage(operation.ToMember, operation.FromMember):
		return cluster.ReinitStateRestartingStandby
	case operation.Message == reinitReplicationVerificationRunningMessage(operation.ToMember, operation.FromMember):
		return cluster.ReinitStateVerifyingReplication
	default:
		return cluster.ReinitStateAccepted
	}
}

func reinitStateFromHistoryEntry(entry cluster.HistoryEntry) cluster.ReinitState {
	switch entry.Result {
	case cluster.OperationResultSucceeded:
		return cluster.ReinitStateCompleted
	case cluster.OperationResultFailed:
		return cluster.ReinitStateFailed
	case cluster.OperationResultCancelled:
		return cluster.ReinitStateCancelled
	default:
		return cluster.ReinitStateFailed
	}
}
