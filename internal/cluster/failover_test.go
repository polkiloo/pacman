package cluster

import (
	"reflect"
	"testing"
)

func TestFailoverStates(t *testing.T) {
	t.Parallel()

	want := []FailoverState{
		FailoverStateConfirmingFailure,
		FailoverStateSelectingCandidate,
		FailoverStateAwaitingFence,
		FailoverStatePromotingCandidate,
		FailoverStateFinalizing,
		FailoverStateCompleted,
		FailoverStateFailed,
		FailoverStateCancelled,
	}

	got := FailoverStates()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected failover states: got %v, want %v", got, want)
	}

	got[0] = FailoverStateCancelled

	if second := FailoverStates(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected failover states copy, got %v, want %v", second, want)
	}
}

func TestFailoverStateValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		state       FailoverState
		valid       bool
		zero        bool
		terminal    bool
		stringValue string
	}{
		{name: "confirming failure", state: FailoverStateConfirmingFailure, valid: true, zero: false, terminal: false, stringValue: "confirming_failure"},
		{name: "selecting candidate", state: FailoverStateSelectingCandidate, valid: true, zero: false, terminal: false, stringValue: "selecting_candidate"},
		{name: "awaiting fence", state: FailoverStateAwaitingFence, valid: true, zero: false, terminal: false, stringValue: "awaiting_fence"},
		{name: "promoting candidate", state: FailoverStatePromotingCandidate, valid: true, zero: false, terminal: false, stringValue: "promoting_candidate"},
		{name: "finalizing", state: FailoverStateFinalizing, valid: true, zero: false, terminal: false, stringValue: "finalizing"},
		{name: "completed", state: FailoverStateCompleted, valid: true, zero: false, terminal: true, stringValue: "completed"},
		{name: "failed", state: FailoverStateFailed, valid: true, zero: false, terminal: true, stringValue: "failed"},
		{name: "cancelled", state: FailoverStateCancelled, valid: true, zero: false, terminal: true, stringValue: "cancelled"},
		{name: "zero", state: "", valid: false, zero: true, terminal: false, stringValue: ""},
		{name: "invalid", state: FailoverState("paused"), valid: false, zero: false, terminal: false, stringValue: "paused"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.state.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.state.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.state.IsTerminal(); got != testCase.terminal {
				t.Fatalf("unexpected terminal flag: got %v, want %v", got, testCase.terminal)
			}

			if got := testCase.state.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestFailoverStateNextStates(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		state FailoverState
		want  []FailoverState
	}{
		{
			name:  "confirming failure",
			state: FailoverStateConfirmingFailure,
			want: []FailoverState{
				FailoverStateSelectingCandidate,
				FailoverStateFailed,
				FailoverStateCancelled,
			},
		},
		{
			name:  "selecting candidate",
			state: FailoverStateSelectingCandidate,
			want: []FailoverState{
				FailoverStateAwaitingFence,
				FailoverStatePromotingCandidate,
				FailoverStateFailed,
				FailoverStateCancelled,
			},
		},
		{
			name:  "awaiting fence",
			state: FailoverStateAwaitingFence,
			want: []FailoverState{
				FailoverStatePromotingCandidate,
				FailoverStateFailed,
				FailoverStateCancelled,
			},
		},
		{
			name:  "promoting candidate",
			state: FailoverStatePromotingCandidate,
			want: []FailoverState{
				FailoverStateFinalizing,
				FailoverStateFailed,
			},
		},
		{
			name:  "finalizing",
			state: FailoverStateFinalizing,
			want: []FailoverState{
				FailoverStateCompleted,
				FailoverStateFailed,
			},
		},
		{name: "completed", state: FailoverStateCompleted, want: nil},
		{name: "failed", state: FailoverStateFailed, want: nil},
		{name: "cancelled", state: FailoverStateCancelled, want: nil},
		{name: "invalid", state: FailoverState("paused"), want: nil},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := testCase.state.NextStates()
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("unexpected next states: got %v, want %v", got, testCase.want)
			}

			if len(got) == 0 {
				return
			}

			got[0] = FailoverStateCancelled

			second := testCase.state.NextStates()
			if !reflect.DeepEqual(second, testCase.want) {
				t.Fatalf("expected next states copy, got %v, want %v", second, testCase.want)
			}
		})
	}
}

func TestFailoverStateCanTransitionTo(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		from FailoverState
		to   FailoverState
		want bool
	}{
		{
			name: "confirming failure to selecting candidate",
			from: FailoverStateConfirmingFailure,
			to:   FailoverStateSelectingCandidate,
			want: true,
		},
		{
			name: "selecting candidate to awaiting fence",
			from: FailoverStateSelectingCandidate,
			to:   FailoverStateAwaitingFence,
			want: true,
		},
		{
			name: "selecting candidate to promoting candidate",
			from: FailoverStateSelectingCandidate,
			to:   FailoverStatePromotingCandidate,
			want: true,
		},
		{
			name: "awaiting fence to promoting candidate",
			from: FailoverStateAwaitingFence,
			to:   FailoverStatePromotingCandidate,
			want: true,
		},
		{
			name: "promoting candidate to finalizing",
			from: FailoverStatePromotingCandidate,
			to:   FailoverStateFinalizing,
			want: true,
		},
		{
			name: "finalizing to completed",
			from: FailoverStateFinalizing,
			to:   FailoverStateCompleted,
			want: true,
		},
		{
			name: "confirming failure cannot skip to promoting candidate",
			from: FailoverStateConfirmingFailure,
			to:   FailoverStatePromotingCandidate,
			want: false,
		},
		{
			name: "awaiting fence cannot go back to selecting candidate",
			from: FailoverStateAwaitingFence,
			to:   FailoverStateSelectingCandidate,
			want: false,
		},
		{
			name: "terminal state cannot transition",
			from: FailoverStateCompleted,
			to:   FailoverStateFailed,
			want: false,
		},
		{
			name: "zero state cannot transition",
			from: "",
			to:   FailoverStateConfirmingFailure,
			want: false,
		},
		{
			name: "invalid next state rejected",
			from: FailoverStateSelectingCandidate,
			to:   FailoverState("paused"),
			want: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.from.CanTransitionTo(testCase.to); got != testCase.want {
				t.Fatalf("unexpected transition result: got %v, want %v", got, testCase.want)
			}
		})
	}
}
