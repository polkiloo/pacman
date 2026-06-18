package controlplane

import (
	"context"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

// ReinitEngine exposes operator-triggered replica reinitialization planning
// against the replicated control-plane state. Unlike former-primary rejoin,
// reinit is an explicit destructive reclone operation for an existing
// non-primary data member.
type ReinitEngine interface {
	ValidateReinit(context.Context, ReinitRequest) (ReinitValidation, error)
	CreateReinitIntent(context.Context, ReinitRequest) (ReinitIntent, error)
	ExecuteReinitStopPostgres(context.Context, string, ReinitPostgresStopExecutor) (ReinitExecution, error)
	ExecuteReinitArchiveDataDir(context.Context, string, ReinitDataDirArchiveExecutor) (ReinitExecution, error)
	ExecuteReinitWALGRestore(context.Context, string, ReinitWALGRestoreExecutor) (ReinitExecution, error)
	ExecuteReinitRecoveryConfig(context.Context, string, ReinitRecoveryConfigExecutor) (ReinitExecution, error)
	ExecuteReinitRestartAsStandby(context.Context, string, ReinitStandbyRestartExecutor) (ReinitExecution, error)
}

// ReinitRequest captures operator metadata attached to a destructive replica
// reinitialization request.
type ReinitRequest struct {
	Member      string
	RequestedBy string
	Reason      string
}

// Clone returns a detached copy of the reinit request.
func (request ReinitRequest) Clone() ReinitRequest {
	return request
}

// ReinitValidation captures the accepted preflight result for a replica
// reclone request.
type ReinitValidation struct {
	Request        ReinitRequest
	CurrentPrimary cluster.MemberStatus
	Target         cluster.MemberStatus
	CurrentEpoch   cluster.Epoch
	ValidatedAt    time.Time
}

// Clone returns a detached copy of the reinit validation result.
func (validation ReinitValidation) Clone() ReinitValidation {
	clone := validation
	clone.Request = validation.Request.Clone()
	clone.CurrentPrimary = validation.CurrentPrimary.Clone()
	clone.Target = validation.Target.Clone()

	return clone
}

// ReinitIntent captures the accepted replica reinitialization operation
// recorded in the operation journal.
type ReinitIntent struct {
	Operation  cluster.Operation
	Validation ReinitValidation
	CreatedAt  time.Time
}

// Clone returns a detached copy of the reinit intent.
func (intent ReinitIntent) Clone() ReinitIntent {
	clone := intent
	clone.Operation = intent.Operation.Clone()
	clone.Validation = intent.Validation.Clone()

	return clone
}

// ReinitPostgresStopExecutor stops PostgreSQL on the local reinit target before
// destructive data-directory operations are allowed to proceed.
type ReinitPostgresStopExecutor interface {
	StopPostgres(context.Context, ReinitPostgresStopRequest) error
}

// ReinitPostgresStopRequest describes the local target that should stop
// PostgreSQL before later reinit phases wipe or restore the data directory.
type ReinitPostgresStopRequest struct {
	Operation          cluster.Operation
	Validation         ReinitValidation
	TargetNode         agentmodel.NodeStatus
	CurrentPrimaryNode agentmodel.NodeStatus
	CurrentEpoch       cluster.Epoch
}

// ReinitDataDirArchiveExecutor archives the local data directory after
// PostgreSQL is stopped and before WAL-G restores into a clean directory.
type ReinitDataDirArchiveExecutor interface {
	ArchiveDataDir(context.Context, ReinitDataDirArchiveRequest) (ReinitDataDirArchiveResult, error)
}

// ReinitDataDirArchiveRequest describes the local target whose data directory
// should be archived before reclone.
type ReinitDataDirArchiveRequest struct {
	Operation          cluster.Operation
	Validation         ReinitValidation
	TargetNode         agentmodel.NodeStatus
	CurrentPrimaryNode agentmodel.NodeStatus
	CurrentEpoch       cluster.Epoch
}

// ReinitDataDirArchiveResult reports the archived data directory selected by
// the local executor.
type ReinitDataDirArchiveResult struct {
	DataDir     string
	ArchivePath string
	Archived    bool
}

// ReinitWALGRestoreExecutor restores the selected WAL-G base backup into the
// archived target data directory.
type ReinitWALGRestoreExecutor interface {
	RestoreFromWALG(context.Context, ReinitWALGRestoreRequest) (ReinitWALGRestoreResult, error)
}

// ReinitWALGRestoreRequest describes the local target whose data directory
// should receive the WAL-G base backup.
type ReinitWALGRestoreRequest struct {
	Operation          cluster.Operation
	Validation         ReinitValidation
	TargetNode         agentmodel.NodeStatus
	CurrentPrimaryNode agentmodel.NodeStatus
	CurrentEpoch       cluster.Epoch
}

// ReinitWALGRestoreResult reports the restored WAL-G backup source selected by
// the local executor.
type ReinitWALGRestoreResult struct {
	DataDir    string
	BackupName string
}

// ReinitRecoveryConfigExecutor renders local PostgreSQL recovery settings into
// the restored data directory before PostgreSQL starts.
type ReinitRecoveryConfigExecutor interface {
	ConfigureReinitRecovery(context.Context, ReinitRecoveryConfigRequest) (ReinitRecoveryConfigResult, error)
}

// ReinitRecoveryConfigRequest describes the local target whose restored data
// directory should receive PostgreSQL recovery configuration.
type ReinitRecoveryConfigRequest struct {
	Operation          cluster.Operation
	Validation         ReinitValidation
	TargetNode         agentmodel.NodeStatus
	CurrentPrimaryNode agentmodel.NodeStatus
	CurrentEpoch       cluster.Epoch
	Standby            postgres.StandbyConfig
}

// ReinitRecoveryConfigResult reports the recovery settings rendered by the
// local executor.
type ReinitRecoveryConfigResult struct {
	DataDir        string
	RestoreCommand string
}

// ReinitStandbyRestartExecutor starts the restored target with the rendered
// recovery configuration so it can rejoin the current primary as a standby.
type ReinitStandbyRestartExecutor interface {
	RestartReinitStandby(context.Context, ReinitStandbyRestartRequest) error
}

// ReinitStandbyRestartRequest describes the local reinit target that should be
// started in standby mode after recovery configuration has been rendered.
type ReinitStandbyRestartRequest struct {
	Operation          cluster.Operation
	Validation         ReinitValidation
	TargetNode         agentmodel.NodeStatus
	CurrentPrimaryNode agentmodel.NodeStatus
	CurrentEpoch       cluster.Epoch
}

// ReinitExecution captures the outcome of executing a reinit phase.
type ReinitExecution struct {
	Operation          cluster.Operation
	Validation         ReinitValidation
	CurrentEpoch       cluster.Epoch
	PostgresStopped    bool
	DataDirArchived    bool
	ArchivePath        string
	WALGRestored       bool
	WALGBackupName     string
	RecoveryConfig     bool
	RestoreCommand     string
	RestartedAsStandby bool
	ExecutedAt         time.Time
}

// Clone returns a detached copy of the reinit execution result.
func (execution ReinitExecution) Clone() ReinitExecution {
	clone := execution
	clone.Operation = execution.Operation.Clone()
	clone.Validation = execution.Validation.Clone()

	return clone
}
