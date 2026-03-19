package cluster

import (
	"reflect"
	"testing"
)

func TestRejoinStrategies(t *testing.T) {
	t.Parallel()

	want := []RejoinStrategy{
		RejoinStrategyRewind,
		RejoinStrategyReclone,
	}

	got := RejoinStrategies()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected rejoin strategies: got %v, want %v", got, want)
	}

	got[0] = RejoinStrategyReclone

	if second := RejoinStrategies(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected rejoin strategies copy, got %v, want %v", second, want)
	}
}

func TestRejoinStrategyValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		strategy    RejoinStrategy
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "rewind", strategy: RejoinStrategyRewind, valid: true, zero: false, stringValue: "rewind"},
		{name: "reclone", strategy: RejoinStrategyReclone, valid: true, zero: false, stringValue: "reclone"},
		{name: "zero", strategy: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", strategy: RejoinStrategy("basebackup"), valid: false, zero: false, stringValue: "basebackup"},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.strategy.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.strategy.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.strategy.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestRejoinStates(t *testing.T) {
	t.Parallel()

	want := []RejoinState{
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

	got := RejoinStates()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected rejoin states: got %v, want %v", got, want)
	}

	got[0] = RejoinStateCancelled

	if second := RejoinStates(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected rejoin states copy, got %v, want %v", second, want)
	}
}

func TestRejoinStateValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		state       RejoinState
		valid       bool
		zero        bool
		terminal    bool
		stringValue string
	}{
		{name: "assessing member", state: RejoinStateAssessingMember, valid: true, zero: false, terminal: false, stringValue: "assessing_member"},
		{name: "detecting divergence", state: RejoinStateDetectingDivergence, valid: true, zero: false, terminal: false, stringValue: "detecting_divergence"},
		{name: "selecting strategy", state: RejoinStateSelectingStrategy, valid: true, zero: false, terminal: false, stringValue: "selecting_strategy"},
		{name: "rewinding", state: RejoinStateRewinding, valid: true, zero: false, terminal: false, stringValue: "rewinding"},
		{name: "recloning", state: RejoinStateRecloning, valid: true, zero: false, terminal: false, stringValue: "recloning"},
		{name: "configuring standby", state: RejoinStateConfiguringStandby, valid: true, zero: false, terminal: false, stringValue: "configuring_standby"},
		{name: "starting replica", state: RejoinStateStartingReplica, valid: true, zero: false, terminal: false, stringValue: "starting_replica"},
		{name: "verifying replication", state: RejoinStateVerifyingReplication, valid: true, zero: false, terminal: false, stringValue: "verifying_replication"},
		{name: "completed", state: RejoinStateCompleted, valid: true, zero: false, terminal: true, stringValue: "completed"},
		{name: "failed", state: RejoinStateFailed, valid: true, zero: false, terminal: true, stringValue: "failed"},
		{name: "cancelled", state: RejoinStateCancelled, valid: true, zero: false, terminal: true, stringValue: "cancelled"},
		{name: "zero", state: "", valid: false, zero: true, terminal: false, stringValue: ""},
		{name: "invalid", state: RejoinState("paused"), valid: false, zero: false, terminal: false, stringValue: "paused"},
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

func TestRejoinStateNextStates(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		state RejoinState
		want  []RejoinState
	}{
		{
			name:  "assessing member",
			state: RejoinStateAssessingMember,
			want: []RejoinState{
				RejoinStateDetectingDivergence,
				RejoinStateFailed,
				RejoinStateCancelled,
			},
		},
		{
			name:  "detecting divergence",
			state: RejoinStateDetectingDivergence,
			want: []RejoinState{
				RejoinStateSelectingStrategy,
				RejoinStateFailed,
				RejoinStateCancelled,
			},
		},
		{
			name:  "selecting strategy",
			state: RejoinStateSelectingStrategy,
			want: []RejoinState{
				RejoinStateRewinding,
				RejoinStateRecloning,
				RejoinStateFailed,
				RejoinStateCancelled,
			},
		},
		{
			name:  "rewinding",
			state: RejoinStateRewinding,
			want: []RejoinState{
				RejoinStateConfiguringStandby,
				RejoinStateFailed,
			},
		},
		{
			name:  "recloning",
			state: RejoinStateRecloning,
			want: []RejoinState{
				RejoinStateConfiguringStandby,
				RejoinStateFailed,
			},
		},
		{
			name:  "configuring standby",
			state: RejoinStateConfiguringStandby,
			want: []RejoinState{
				RejoinStateStartingReplica,
				RejoinStateFailed,
			},
		},
		{
			name:  "starting replica",
			state: RejoinStateStartingReplica,
			want: []RejoinState{
				RejoinStateVerifyingReplication,
				RejoinStateFailed,
			},
		},
		{
			name:  "verifying replication",
			state: RejoinStateVerifyingReplication,
			want: []RejoinState{
				RejoinStateCompleted,
				RejoinStateFailed,
			},
		},
		{name: "completed", state: RejoinStateCompleted, want: nil},
		{name: "failed", state: RejoinStateFailed, want: nil},
		{name: "cancelled", state: RejoinStateCancelled, want: nil},
		{name: "invalid", state: RejoinState("paused"), want: nil},
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

			got[0] = RejoinStateCancelled

			second := testCase.state.NextStates()
			if !reflect.DeepEqual(second, testCase.want) {
				t.Fatalf("expected next states copy, got %v, want %v", second, testCase.want)
			}
		})
	}
}

func TestRejoinStateCanTransitionTo(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		from RejoinState
		to   RejoinState
		want bool
	}{
		{name: "assessing member to detecting divergence", from: RejoinStateAssessingMember, to: RejoinStateDetectingDivergence, want: true},
		{name: "detecting divergence to selecting strategy", from: RejoinStateDetectingDivergence, to: RejoinStateSelectingStrategy, want: true},
		{name: "selecting strategy to rewinding", from: RejoinStateSelectingStrategy, to: RejoinStateRewinding, want: true},
		{name: "selecting strategy to recloning", from: RejoinStateSelectingStrategy, to: RejoinStateRecloning, want: true},
		{name: "rewinding to configuring standby", from: RejoinStateRewinding, to: RejoinStateConfiguringStandby, want: true},
		{name: "recloning to configuring standby", from: RejoinStateRecloning, to: RejoinStateConfiguringStandby, want: true},
		{name: "configuring standby to starting replica", from: RejoinStateConfiguringStandby, to: RejoinStateStartingReplica, want: true},
		{name: "starting replica to verifying replication", from: RejoinStateStartingReplica, to: RejoinStateVerifyingReplication, want: true},
		{name: "verifying replication to completed", from: RejoinStateVerifyingReplication, to: RejoinStateCompleted, want: true},
		{name: "cannot skip from detecting divergence to rewinding", from: RejoinStateDetectingDivergence, to: RejoinStateRewinding, want: false},
		{name: "cannot return from recloning to selecting strategy", from: RejoinStateRecloning, to: RejoinStateSelectingStrategy, want: false},
		{name: "terminal state cannot transition", from: RejoinStateCompleted, to: RejoinStateFailed, want: false},
		{name: "zero state cannot transition", from: "", to: RejoinStateAssessingMember, want: false},
		{name: "invalid next state rejected", from: RejoinStateSelectingStrategy, to: RejoinState("paused"), want: false},
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
