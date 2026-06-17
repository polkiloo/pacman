package controlplane

import (
	"context"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// ReinitEngine exposes operator-triggered replica reinitialization planning
// against the replicated control-plane state. Unlike former-primary rejoin,
// reinit is an explicit destructive reclone operation for an existing
// non-primary data member.
type ReinitEngine interface {
	ValidateReinit(context.Context, ReinitRequest) (ReinitValidation, error)
	CreateReinitIntent(context.Context, ReinitRequest) (ReinitIntent, error)
	ExecuteReinitStopPostgres(context.Context, string, ReinitPostgresStopExecutor) (ReinitExecution, error)
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

// ReinitExecution captures the outcome of executing a reinit phase.
type ReinitExecution struct {
	Operation       cluster.Operation
	Validation      ReinitValidation
	CurrentEpoch    cluster.Epoch
	PostgresStopped bool
	ExecutedAt      time.Time
}

// Clone returns a detached copy of the reinit execution result.
func (execution ReinitExecution) Clone() ReinitExecution {
	clone := execution
	clone.Operation = execution.Operation.Clone()
	clone.Validation = execution.Validation.Clone()

	return clone
}
