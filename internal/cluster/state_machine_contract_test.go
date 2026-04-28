package cluster

import (
	"reflect"
	"testing"
)

func TestStateMachineContracts(t *testing.T) {
	t.Parallel()

	t.Run("failover", func(t *testing.T) {
		t.Parallel()

		assertStateMachineContract(t, stateMachineContract[FailoverState]{
			states:     FailoverStates(),
			isValid:    FailoverState.IsValid,
			isTerminal: FailoverState.IsTerminal,
			nextStates: FailoverState.NextStates,
			canMove:    FailoverState.CanTransitionTo,
		})
	})

	t.Run("switchover", func(t *testing.T) {
		t.Parallel()

		assertStateMachineContract(t, stateMachineContract[SwitchoverState]{
			states:     SwitchoverStates(),
			isValid:    SwitchoverState.IsValid,
			isTerminal: SwitchoverState.IsTerminal,
			nextStates: SwitchoverState.NextStates,
			canMove:    SwitchoverState.CanTransitionTo,
		})
	})

	t.Run("rejoin", func(t *testing.T) {
		t.Parallel()

		assertStateMachineContract(t, stateMachineContract[RejoinState]{
			states:     RejoinStates(),
			isValid:    RejoinState.IsValid,
			isTerminal: RejoinState.IsTerminal,
			nextStates: RejoinState.NextStates,
			canMove:    RejoinState.CanTransitionTo,
		})
	})
}

type stateMachineContract[T comparable] struct {
	states     []T
	isValid    func(T) bool
	isTerminal func(T) bool
	nextStates func(T) []T
	canMove    func(T, T) bool
}

func assertStateMachineContract[T comparable](t *testing.T, contract stateMachineContract[T]) {
	t.Helper()

	if len(contract.states) == 0 {
		t.Fatal("state machine must declare at least one state")
	}

	seen := make(map[T]struct{}, len(contract.states))
	for _, state := range contract.states {
		if _, ok := seen[state]; ok {
			t.Fatalf("duplicate state declared: %v", state)
		}
		seen[state] = struct{}{}

		if !contract.isValid(state) {
			t.Fatalf("declared state is invalid: %v", state)
		}

		nextStates := contract.nextStates(state)
		if contract.isTerminal(state) && len(nextStates) != 0 {
			t.Fatalf("terminal state %v declared next states: %v", state, nextStates)
		}

		if !contract.isTerminal(state) && len(nextStates) == 0 {
			t.Fatalf("non-terminal state %v has no forward transitions", state)
		}

		assertTransitionSetMatchesNextStates(t, contract, state, nextStates)
		assertNextStatesReturnsCopy(t, contract, state, nextStates)
	}
}

func assertTransitionSetMatchesNextStates[T comparable](t *testing.T, contract stateMachineContract[T], state T, nextStates []T) {
	t.Helper()

	allowed := make(map[T]struct{}, len(nextStates))
	for _, next := range nextStates {
		if !contract.isValid(next) {
			t.Fatalf("state %v declared invalid next state: %v", state, next)
		}

		if _, ok := allowed[next]; ok {
			t.Fatalf("state %v declared duplicate next state: %v", state, next)
		}
		allowed[next] = struct{}{}

		if !contract.canMove(state, next) {
			t.Fatalf("state %v cannot transition to declared next state %v", state, next)
		}
	}

	for _, candidate := range contract.states {
		_, shouldMove := allowed[candidate]
		if got := contract.canMove(state, candidate); got != shouldMove {
			t.Fatalf("transition mismatch from %v to %v: got %v, want %v", state, candidate, got, shouldMove)
		}
	}
}

func assertNextStatesReturnsCopy[T comparable](t *testing.T, contract stateMachineContract[T], state T, nextStates []T) {
	t.Helper()

	if len(nextStates) == 0 {
		return
	}

	snapshot := append([]T(nil), nextStates...)
	nextStates[0] = contract.states[len(contract.states)-1]

	if got := contract.nextStates(state); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("expected next states copy for %v: got %v, want %v", state, got, snapshot)
	}
}
