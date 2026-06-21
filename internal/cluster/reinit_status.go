package cluster

import (
	"strings"
	"time"
)

// ReinitState describes the replica reinitialization phase exposed in member
// and cluster status.
type ReinitState string

const (
	ReinitStateAccepted                ReinitState = "accepted"
	ReinitStateStoppingPostgres        ReinitState = "stopping_postgres"
	ReinitStateArchivingDataDir        ReinitState = "archiving_data_dir"
	ReinitStateRestoringBackup         ReinitState = "restoring_backup"
	ReinitStateRenderingRecoveryConfig ReinitState = "rendering_recovery_config"
	ReinitStateRestartingStandby       ReinitState = "restarting_standby"
	ReinitStateVerifyingReplication    ReinitState = "verifying_replication"
	ReinitStateCompleted               ReinitState = "completed"
	ReinitStateFailed                  ReinitState = "failed"
	ReinitStateCancelled               ReinitState = "cancelled"
)

var reinitStates = []ReinitState{
	ReinitStateAccepted,
	ReinitStateStoppingPostgres,
	ReinitStateArchivingDataDir,
	ReinitStateRestoringBackup,
	ReinitStateRenderingRecoveryConfig,
	ReinitStateRestartingStandby,
	ReinitStateVerifyingReplication,
	ReinitStateCompleted,
	ReinitStateFailed,
	ReinitStateCancelled,
}

// ReinitStates returns the full set of reinit states known to PACMAN.
func ReinitStates() []ReinitState {
	return append([]ReinitState(nil), reinitStates...)
}

func (state ReinitState) String() string {
	return string(state)
}

// IsValid reports whether the value is a supported reinit state.
func (state ReinitState) IsValid() bool {
	switch state {
	case ReinitStateAccepted,
		ReinitStateStoppingPostgres,
		ReinitStateArchivingDataDir,
		ReinitStateRestoringBackup,
		ReinitStateRenderingRecoveryConfig,
		ReinitStateRestartingStandby,
		ReinitStateVerifyingReplication,
		ReinitStateCompleted,
		ReinitStateFailed,
		ReinitStateCancelled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the state was left unspecified.
func (state ReinitState) IsZero() bool {
	return state == ""
}

// ReinitStatus exposes the active or latest completed replica reinitialization
// state for the cluster or a single target member.
type ReinitStatus struct {
	OperationID string
	State       ReinitState
	LastResult  OperationResult
	FromMember  string
	ToMember    string
	Message     string
	UpdatedAt   time.Time
}

// Validate reports whether the reinit status is coherent enough to publish.
func (status ReinitStatus) Validate() error {
	if strings.TrimSpace(status.OperationID) == "" {
		return ErrOperationIDRequired
	}

	if status.State.IsZero() {
		return ErrReinitStateRequired
	}

	if !status.State.IsValid() {
		return ErrInvalidReinitState
	}

	if status.LastResult.IsZero() {
		return ErrOperationResultRequired
	}

	if !status.LastResult.IsValid() {
		return ErrInvalidOperationResult
	}

	if status.UpdatedAt.IsZero() {
		return ErrReinitUpdatedAtRequired
	}

	return nil
}

// Clone returns a detached copy of the status.
func (status ReinitStatus) Clone() ReinitStatus {
	return status
}
