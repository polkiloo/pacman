package controlplane

import (
	"context"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// SwitchoverEngine exposes planned switchover validation against the
// replicated control-plane state.
type SwitchoverEngine interface {
	SwitchoverTargetReadiness(string) (SwitchoverTargetReadiness, error)
	ValidateSwitchover(context.Context, SwitchoverRequest) (SwitchoverValidation, error)
	CreateSwitchoverIntent(context.Context, SwitchoverRequest) (SwitchoverIntent, error)
	CancelSwitchover(context.Context) (cluster.Operation, error)
	ExecuteSwitchover(context.Context, DemotionExecutor, PromotionExecutor) (SwitchoverExecution, error)
}

// SwitchoverRequest captures the operator intent for a planned topology
// transition.
type SwitchoverRequest struct {
	RequestedBy string
	Reason      string
	Candidate   string
	ScheduledAt time.Time
}

// Clone returns a detached copy of the switchover request.
func (request SwitchoverRequest) Clone() SwitchoverRequest {
	return request
}

// SwitchoverTargetReadiness captures whether a requested standby is safe to
// promote during a planned switchover.
type SwitchoverTargetReadiness struct {
	CurrentPrimary string
	Member         cluster.MemberStatus
	Ready          bool
	Reasons        []string
	CheckedAt      time.Time
}

// Clone returns a detached copy of the readiness result.
func (readiness SwitchoverTargetReadiness) Clone() SwitchoverTargetReadiness {
	clone := readiness
	clone.Member = readiness.Member.Clone()
	if readiness.Reasons != nil {
		clone.Reasons = append([]string(nil), readiness.Reasons...)
	}

	return clone
}

// SwitchoverValidation captures the accepted validation outcome for a planned
// switchover request.
type SwitchoverValidation struct {
	Request        SwitchoverRequest
	CurrentPrimary cluster.MemberStatus
	Target         SwitchoverTargetReadiness
	CurrentEpoch   cluster.Epoch
	ValidatedAt    time.Time
}

// Clone returns a detached copy of the switchover validation result.
func (validation SwitchoverValidation) Clone() SwitchoverValidation {
	clone := validation
	clone.Request = validation.Request.Clone()
	clone.CurrentPrimary = validation.CurrentPrimary.Clone()
	clone.Target = validation.Target.Clone()

	return clone
}

// SwitchoverIntent captures the accepted planned topology transition recorded
// in the operation journal.
type SwitchoverIntent struct {
	Operation  cluster.Operation
	Validation SwitchoverValidation
	CreatedAt  time.Time
}

// Clone returns a detached copy of the switchover intent.
func (intent SwitchoverIntent) Clone() SwitchoverIntent {
	clone := intent
	clone.Operation = intent.Operation.Clone()
	clone.Validation = intent.Validation.Clone()

	return clone
}

// DemotionExecutor performs the node-local demotion or drain of the current
// primary during a planned switchover.
type DemotionExecutor interface {
	Demote(context.Context, DemotionRequest) error
}

// DemotionRequest describes the current primary that should stop accepting
// writes before the target standby is promoted.
type DemotionRequest struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	CurrentEpoch   cluster.Epoch
}

// SwitchoverExecution captures the outcome of executing the accepted
// switchover intent up to the point where the new epoch is published.
type SwitchoverExecution struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	PreviousEpoch  cluster.Epoch
	CurrentEpoch   cluster.Epoch
	Demoted        bool
	Promoted       bool
	ExecutedAt     time.Time
}

// Clone returns a detached copy of the switchover execution result.
func (execution SwitchoverExecution) Clone() SwitchoverExecution {
	clone := execution
	clone.Operation = execution.Operation.Clone()

	return clone
}
