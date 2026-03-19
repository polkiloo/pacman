package cluster

// FailoverState describes the control-plane phase of an in-flight failover.
// These states intentionally model the failover-specific execution flow rather
// than the generic API operation lifecycle.
type FailoverState string

const (
	// FailoverStateConfirmingFailure waits for policy-based confirmation that
	// the current primary is truly unavailable.
	FailoverStateConfirmingFailure FailoverState = "confirming_failure"
	// FailoverStateSelectingCandidate ranks healthy standbys and chooses the
	// safest promotion target.
	FailoverStateSelectingCandidate FailoverState = "selecting_candidate"
	// FailoverStateAwaitingFence waits for an external fencing confirmation when
	// the active failover policy requires it.
	FailoverStateAwaitingFence FailoverState = "awaiting_fence"
	// FailoverStatePromotingCandidate drives the chosen standby through local
	// promotion and waits for PostgreSQL confirmation.
	FailoverStatePromotingCandidate FailoverState = "promoting_candidate"
	// FailoverStateFinalizing publishes the new epoch, updates authoritative
	// roles, and marks the former primary as needing rejoin.
	FailoverStateFinalizing FailoverState = "finalizing"
	FailoverStateCompleted  FailoverState = "completed"
	FailoverStateFailed     FailoverState = "failed"
	FailoverStateCancelled  FailoverState = "cancelled"
)

var failoverStates = []FailoverState{
	FailoverStateConfirmingFailure,
	FailoverStateSelectingCandidate,
	FailoverStateAwaitingFence,
	FailoverStatePromotingCandidate,
	FailoverStateFinalizing,
	FailoverStateCompleted,
	FailoverStateFailed,
	FailoverStateCancelled,
}

var failoverStateTransitions = map[FailoverState][]FailoverState{
	FailoverStateConfirmingFailure: {
		FailoverStateSelectingCandidate,
		FailoverStateFailed,
		FailoverStateCancelled,
	},
	FailoverStateSelectingCandidate: {
		FailoverStateAwaitingFence,
		FailoverStatePromotingCandidate,
		FailoverStateFailed,
		FailoverStateCancelled,
	},
	FailoverStateAwaitingFence: {
		FailoverStatePromotingCandidate,
		FailoverStateFailed,
		FailoverStateCancelled,
	},
	FailoverStatePromotingCandidate: {
		FailoverStateFinalizing,
		FailoverStateFailed,
	},
	FailoverStateFinalizing: {
		FailoverStateCompleted,
		FailoverStateFailed,
	},
}

// FailoverStates returns the full set of failover states known to PACMAN.
func FailoverStates() []FailoverState {
	return append([]FailoverState(nil), failoverStates...)
}

func (state FailoverState) String() string {
	return string(state)
}

// IsValid reports whether the value is a supported failover state.
func (state FailoverState) IsValid() bool {
	switch state {
	case FailoverStateConfirmingFailure, FailoverStateSelectingCandidate, FailoverStateAwaitingFence, FailoverStatePromotingCandidate, FailoverStateFinalizing, FailoverStateCompleted, FailoverStateFailed, FailoverStateCancelled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the failover state was left unspecified.
func (state FailoverState) IsZero() bool {
	return state == ""
}

// IsTerminal reports whether the failover has reached an immutable outcome.
func (state FailoverState) IsTerminal() bool {
	switch state {
	case FailoverStateCompleted, FailoverStateFailed, FailoverStateCancelled:
		return true
	default:
		return false
	}
}

// NextStates returns the valid next states from the current failover state.
func (state FailoverState) NextStates() []FailoverState {
	return append([]FailoverState(nil), failoverStateTransitions[state]...)
}

// CanTransitionTo reports whether the failover state machine allows a
// transition from the current state to the provided next state.
func (state FailoverState) CanTransitionTo(next FailoverState) bool {
	if !state.IsValid() || !next.IsValid() {
		return false
	}

	for _, candidate := range failoverStateTransitions[state] {
		if candidate == next {
			return true
		}
	}

	return false
}
