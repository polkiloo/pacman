package cluster

// SwitchoverState describes the control-plane phase of an in-flight planned
// switchover. These states intentionally model the topology transition itself
// rather than the generic API operation lifecycle.
type SwitchoverState string

const (
	// SwitchoverStateScheduled records an accepted future switchover that has
	// not reached its execution window yet.
	SwitchoverStateScheduled SwitchoverState = "scheduled"
	// SwitchoverStateValidatingTarget verifies that the requested standby is
	// healthy, sufficiently caught up, and still eligible for promotion.
	SwitchoverStateValidatingTarget SwitchoverState = "validating_target"
	// SwitchoverStateDrainingPrimary quiesces writes on the current primary and
	// prepares it to hand off authority safely.
	SwitchoverStateDrainingPrimary SwitchoverState = "draining_primary"
	// SwitchoverStatePromotingTarget drives the selected standby through local
	// promotion and waits for PostgreSQL confirmation.
	SwitchoverStatePromotingTarget SwitchoverState = "promoting_target"
	// SwitchoverStateFinalizing publishes the new epoch, updates authoritative
	// roles, and records the completed topology change.
	SwitchoverStateFinalizing SwitchoverState = "finalizing"
	SwitchoverStateCompleted  SwitchoverState = "completed"
	SwitchoverStateFailed     SwitchoverState = "failed"
	SwitchoverStateCancelled  SwitchoverState = "cancelled"
)

var switchoverStates = []SwitchoverState{
	SwitchoverStateScheduled,
	SwitchoverStateValidatingTarget,
	SwitchoverStateDrainingPrimary,
	SwitchoverStatePromotingTarget,
	SwitchoverStateFinalizing,
	SwitchoverStateCompleted,
	SwitchoverStateFailed,
	SwitchoverStateCancelled,
}

var switchoverStateTransitions = map[SwitchoverState][]SwitchoverState{
	SwitchoverStateScheduled: {
		SwitchoverStateValidatingTarget,
		SwitchoverStateCancelled,
	},
	SwitchoverStateValidatingTarget: {
		SwitchoverStateDrainingPrimary,
		SwitchoverStateFailed,
		SwitchoverStateCancelled,
	},
	SwitchoverStateDrainingPrimary: {
		SwitchoverStatePromotingTarget,
		SwitchoverStateFailed,
		SwitchoverStateCancelled,
	},
	SwitchoverStatePromotingTarget: {
		SwitchoverStateFinalizing,
		SwitchoverStateFailed,
	},
	SwitchoverStateFinalizing: {
		SwitchoverStateCompleted,
		SwitchoverStateFailed,
	},
}

// SwitchoverStates returns the full set of switchover states known to PACMAN.
func SwitchoverStates() []SwitchoverState {
	return append([]SwitchoverState(nil), switchoverStates...)
}

func (state SwitchoverState) String() string {
	return string(state)
}

// IsValid reports whether the value is a supported switchover state.
func (state SwitchoverState) IsValid() bool {
	switch state {
	case SwitchoverStateScheduled, SwitchoverStateValidatingTarget, SwitchoverStateDrainingPrimary, SwitchoverStatePromotingTarget, SwitchoverStateFinalizing, SwitchoverStateCompleted, SwitchoverStateFailed, SwitchoverStateCancelled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the switchover state was left unspecified.
func (state SwitchoverState) IsZero() bool {
	return state == ""
}

// IsTerminal reports whether the switchover has reached an immutable outcome.
func (state SwitchoverState) IsTerminal() bool {
	switch state {
	case SwitchoverStateCompleted, SwitchoverStateFailed, SwitchoverStateCancelled:
		return true
	default:
		return false
	}
}

// NextStates returns the valid next states from the current switchover state.
func (state SwitchoverState) NextStates() []SwitchoverState {
	return append([]SwitchoverState(nil), switchoverStateTransitions[state]...)
}

// CanTransitionTo reports whether the switchover state machine allows a
// transition from the current state to the provided next state.
func (state SwitchoverState) CanTransitionTo(next SwitchoverState) bool {
	if !state.IsValid() || !next.IsValid() {
		return false
	}

	for _, candidate := range switchoverStateTransitions[state] {
		if candidate == next {
			return true
		}
	}

	return false
}
