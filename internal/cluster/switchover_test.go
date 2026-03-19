package cluster

import (
	"reflect"
	"testing"
)

func TestSwitchoverStates(t *testing.T) {
	t.Parallel()

	want := []SwitchoverState{
		SwitchoverStateScheduled,
		SwitchoverStateValidatingTarget,
		SwitchoverStateDrainingPrimary,
		SwitchoverStatePromotingTarget,
		SwitchoverStateFinalizing,
		SwitchoverStateCompleted,
		SwitchoverStateFailed,
		SwitchoverStateCancelled,
	}

	got := SwitchoverStates()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected switchover states: got %v, want %v", got, want)
	}

	got[0] = SwitchoverStateCancelled

	if second := SwitchoverStates(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected switchover states copy, got %v, want %v", second, want)
	}
}

func TestSwitchoverStateValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		state       SwitchoverState
		valid       bool
		zero        bool
		terminal    bool
		stringValue string
	}{
		{name: "scheduled", state: SwitchoverStateScheduled, valid: true, zero: false, terminal: false, stringValue: "scheduled"},
		{name: "validating target", state: SwitchoverStateValidatingTarget, valid: true, zero: false, terminal: false, stringValue: "validating_target"},
		{name: "draining primary", state: SwitchoverStateDrainingPrimary, valid: true, zero: false, terminal: false, stringValue: "draining_primary"},
		{name: "promoting target", state: SwitchoverStatePromotingTarget, valid: true, zero: false, terminal: false, stringValue: "promoting_target"},
		{name: "finalizing", state: SwitchoverStateFinalizing, valid: true, zero: false, terminal: false, stringValue: "finalizing"},
		{name: "completed", state: SwitchoverStateCompleted, valid: true, zero: false, terminal: true, stringValue: "completed"},
		{name: "failed", state: SwitchoverStateFailed, valid: true, zero: false, terminal: true, stringValue: "failed"},
		{name: "cancelled", state: SwitchoverStateCancelled, valid: true, zero: false, terminal: true, stringValue: "cancelled"},
		{name: "zero", state: "", valid: false, zero: true, terminal: false, stringValue: ""},
		{name: "invalid", state: SwitchoverState("paused"), valid: false, zero: false, terminal: false, stringValue: "paused"},
	}

	for _, testCase := range testCases {
		testCase := testCase

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

func TestSwitchoverStateNextStates(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		state SwitchoverState
		want  []SwitchoverState
	}{
		{
			name:  "scheduled",
			state: SwitchoverStateScheduled,
			want: []SwitchoverState{
				SwitchoverStateValidatingTarget,
				SwitchoverStateCancelled,
			},
		},
		{
			name:  "validating target",
			state: SwitchoverStateValidatingTarget,
			want: []SwitchoverState{
				SwitchoverStateDrainingPrimary,
				SwitchoverStateFailed,
				SwitchoverStateCancelled,
			},
		},
		{
			name:  "draining primary",
			state: SwitchoverStateDrainingPrimary,
			want: []SwitchoverState{
				SwitchoverStatePromotingTarget,
				SwitchoverStateFailed,
				SwitchoverStateCancelled,
			},
		},
		{
			name:  "promoting target",
			state: SwitchoverStatePromotingTarget,
			want: []SwitchoverState{
				SwitchoverStateFinalizing,
				SwitchoverStateFailed,
			},
		},
		{
			name:  "finalizing",
			state: SwitchoverStateFinalizing,
			want: []SwitchoverState{
				SwitchoverStateCompleted,
				SwitchoverStateFailed,
			},
		},
		{name: "completed", state: SwitchoverStateCompleted, want: nil},
		{name: "failed", state: SwitchoverStateFailed, want: nil},
		{name: "cancelled", state: SwitchoverStateCancelled, want: nil},
		{name: "invalid", state: SwitchoverState("paused"), want: nil},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := testCase.state.NextStates()
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("unexpected next states: got %v, want %v", got, testCase.want)
			}

			if len(got) == 0 {
				return
			}

			got[0] = SwitchoverStateCancelled

			second := testCase.state.NextStates()
			if !reflect.DeepEqual(second, testCase.want) {
				t.Fatalf("expected next states copy, got %v, want %v", second, testCase.want)
			}
		})
	}
}

func TestSwitchoverStateCanTransitionTo(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		from SwitchoverState
		to   SwitchoverState
		want bool
	}{
		{
			name: "scheduled to validating target",
			from: SwitchoverStateScheduled,
			to:   SwitchoverStateValidatingTarget,
			want: true,
		},
		{
			name: "validating target to draining primary",
			from: SwitchoverStateValidatingTarget,
			to:   SwitchoverStateDrainingPrimary,
			want: true,
		},
		{
			name: "draining primary to promoting target",
			from: SwitchoverStateDrainingPrimary,
			to:   SwitchoverStatePromotingTarget,
			want: true,
		},
		{
			name: "promoting target to finalizing",
			from: SwitchoverStatePromotingTarget,
			to:   SwitchoverStateFinalizing,
			want: true,
		},
		{
			name: "finalizing to completed",
			from: SwitchoverStateFinalizing,
			to:   SwitchoverStateCompleted,
			want: true,
		},
		{
			name: "scheduled can be cancelled",
			from: SwitchoverStateScheduled,
			to:   SwitchoverStateCancelled,
			want: true,
		},
		{
			name: "validating target can be cancelled",
			from: SwitchoverStateValidatingTarget,
			to:   SwitchoverStateCancelled,
			want: true,
		},
		{
			name: "draining primary cannot skip to finalizing",
			from: SwitchoverStateDrainingPrimary,
			to:   SwitchoverStateFinalizing,
			want: false,
		},
		{
			name: "promoting target cannot go back to validating target",
			from: SwitchoverStatePromotingTarget,
			to:   SwitchoverStateValidatingTarget,
			want: false,
		},
		{
			name: "terminal state cannot transition",
			from: SwitchoverStateCompleted,
			to:   SwitchoverStateFailed,
			want: false,
		},
		{
			name: "zero state cannot transition",
			from: "",
			to:   SwitchoverStateScheduled,
			want: false,
		},
		{
			name: "invalid next state rejected",
			from: SwitchoverStateValidatingTarget,
			to:   SwitchoverState("paused"),
			want: false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.from.CanTransitionTo(testCase.to); got != testCase.want {
				t.Fatalf("unexpected transition result: got %v, want %v", got, testCase.want)
			}
		})
	}
}
