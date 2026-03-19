package cluster

// RejoinStrategy describes how PACMAN repairs a member before reintroducing it
// as a healthy replica.
type RejoinStrategy string

const (
	RejoinStrategyRewind  RejoinStrategy = "rewind"
	RejoinStrategyReclone RejoinStrategy = "reclone"
)

var rejoinStrategies = []RejoinStrategy{
	RejoinStrategyRewind,
	RejoinStrategyReclone,
}

// RejoinStrategies returns the full set of rejoin strategies known to PACMAN.
func RejoinStrategies() []RejoinStrategy {
	return append([]RejoinStrategy(nil), rejoinStrategies...)
}

func (strategy RejoinStrategy) String() string {
	return string(strategy)
}

// IsValid reports whether the value is a supported rejoin strategy.
func (strategy RejoinStrategy) IsValid() bool {
	switch strategy {
	case RejoinStrategyRewind, RejoinStrategyReclone:
		return true
	default:
		return false
	}
}

// IsZero reports whether the strategy was left unspecified.
func (strategy RejoinStrategy) IsZero() bool {
	return strategy == ""
}

// RejoinState describes the control-plane phase of an in-flight rejoin.
type RejoinState string

const (
	// RejoinStateAssessingMember verifies that the returning member is reachable
	// enough to inspect and repair safely.
	RejoinStateAssessingMember RejoinState = "assessing_member"
	// RejoinStateDetectingDivergence determines whether the member can be
	// rejoined directly, rewound, or requires a full reclone.
	RejoinStateDetectingDivergence RejoinState = "detecting_divergence"
	// RejoinStateSelectingStrategy chooses between pg_rewind and reclone.
	RejoinStateSelectingStrategy RejoinState = "selecting_strategy"
	// RejoinStateRewinding executes the pg_rewind path.
	RejoinStateRewinding RejoinState = "rewinding"
	// RejoinStateRecloning executes the full rebuild path.
	RejoinStateRecloning RejoinState = "recloning"
	// RejoinStateConfiguringStandby renders the local standby configuration for
	// the repaired member.
	RejoinStateConfiguringStandby RejoinState = "configuring_standby"
	// RejoinStateStartingReplica starts PostgreSQL in replica mode.
	RejoinStateStartingReplica RejoinState = "starting_replica"
	// RejoinStateVerifyingReplication confirms that the rejoined member is
	// streaming from the current primary and is healthy again.
	RejoinStateVerifyingReplication RejoinState = "verifying_replication"
	RejoinStateCompleted            RejoinState = "completed"
	RejoinStateFailed               RejoinState = "failed"
	RejoinStateCancelled            RejoinState = "cancelled"
)

var rejoinStates = []RejoinState{
	RejoinStateAssessingMember,
	RejoinStateDetectingDivergence,
	RejoinStateSelectingStrategy,
	RejoinStateRewinding,
	RejoinStateRecloning,
	RejoinStateConfiguringStandby,
	RejoinStateStartingReplica,
	RejoinStateVerifyingReplication,
	RejoinStateCompleted,
	RejoinStateFailed,
	RejoinStateCancelled,
}

var rejoinStateTransitions = map[RejoinState][]RejoinState{
	RejoinStateAssessingMember: {
		RejoinStateDetectingDivergence,
		RejoinStateFailed,
		RejoinStateCancelled,
	},
	RejoinStateDetectingDivergence: {
		RejoinStateSelectingStrategy,
		RejoinStateFailed,
		RejoinStateCancelled,
	},
	RejoinStateSelectingStrategy: {
		RejoinStateRewinding,
		RejoinStateRecloning,
		RejoinStateFailed,
		RejoinStateCancelled,
	},
	RejoinStateRewinding: {
		RejoinStateConfiguringStandby,
		RejoinStateFailed,
	},
	RejoinStateRecloning: {
		RejoinStateConfiguringStandby,
		RejoinStateFailed,
	},
	RejoinStateConfiguringStandby: {
		RejoinStateStartingReplica,
		RejoinStateFailed,
	},
	RejoinStateStartingReplica: {
		RejoinStateVerifyingReplication,
		RejoinStateFailed,
	},
	RejoinStateVerifyingReplication: {
		RejoinStateCompleted,
		RejoinStateFailed,
	},
}

// RejoinStates returns the full set of rejoin states known to PACMAN.
func RejoinStates() []RejoinState {
	return append([]RejoinState(nil), rejoinStates...)
}

func (state RejoinState) String() string {
	return string(state)
}

// IsValid reports whether the value is a supported rejoin state.
func (state RejoinState) IsValid() bool {
	switch state {
	case RejoinStateAssessingMember, RejoinStateDetectingDivergence, RejoinStateSelectingStrategy, RejoinStateRewinding, RejoinStateRecloning, RejoinStateConfiguringStandby, RejoinStateStartingReplica, RejoinStateVerifyingReplication, RejoinStateCompleted, RejoinStateFailed, RejoinStateCancelled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the rejoin state was left unspecified.
func (state RejoinState) IsZero() bool {
	return state == ""
}

// IsTerminal reports whether the rejoin has reached an immutable outcome.
func (state RejoinState) IsTerminal() bool {
	switch state {
	case RejoinStateCompleted, RejoinStateFailed, RejoinStateCancelled:
		return true
	default:
		return false
	}
}

// NextStates returns the valid next states from the current rejoin state.
func (state RejoinState) NextStates() []RejoinState {
	return append([]RejoinState(nil), rejoinStateTransitions[state]...)
}

// CanTransitionTo reports whether the rejoin state machine allows a
// transition from the current state to the provided next state.
func (state RejoinState) CanTransitionTo(next RejoinState) bool {
	if !state.IsValid() || !next.IsValid() {
		return false
	}

	for _, candidate := range rejoinStateTransitions[state] {
		if candidate == next {
			return true
		}
	}

	return false
}
