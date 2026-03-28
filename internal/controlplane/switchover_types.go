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
