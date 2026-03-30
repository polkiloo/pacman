package cluster

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestOperationKinds(t *testing.T) {
	t.Parallel()

	want := []OperationKind{
		OperationKindSwitchover,
		OperationKindFailover,
		OperationKindRejoin,
		OperationKindMaintenanceChange,
	}

	got := OperationKinds()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected operation kinds: got %v, want %v", got, want)
	}

	got[0] = OperationKindMaintenanceChange

	if second := OperationKinds(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected operation kinds copy, got %v, want %v", second, want)
	}
}

func TestOperationKindValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		kind        OperationKind
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "switchover", kind: OperationKindSwitchover, valid: true, zero: false, stringValue: "switchover"},
		{name: "failover", kind: OperationKindFailover, valid: true, zero: false, stringValue: "failover"},
		{name: "rejoin", kind: OperationKindRejoin, valid: true, zero: false, stringValue: "rejoin"},
		{name: "maintenance change", kind: OperationKindMaintenanceChange, valid: true, zero: false, stringValue: "maintenance_change"},
		{name: "zero", kind: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", kind: OperationKind("rebalance"), valid: false, zero: false, stringValue: "rebalance"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.kind.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.kind.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.kind.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestOperationStates(t *testing.T) {
	t.Parallel()

	want := []OperationState{
		OperationStateAccepted,
		OperationStateScheduled,
		OperationStateRunning,
		OperationStateCompleted,
		OperationStateFailed,
		OperationStateCancelled,
	}

	got := OperationStates()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected operation states: got %v, want %v", got, want)
	}

	got[0] = OperationStateCancelled

	if second := OperationStates(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected operation states copy, got %v, want %v", second, want)
	}
}

func TestOperationStateValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		state       OperationState
		valid       bool
		zero        bool
		terminal    bool
		stringValue string
	}{
		{name: "accepted", state: OperationStateAccepted, valid: true, zero: false, terminal: false, stringValue: "accepted"},
		{name: "scheduled", state: OperationStateScheduled, valid: true, zero: false, terminal: false, stringValue: "scheduled"},
		{name: "running", state: OperationStateRunning, valid: true, zero: false, terminal: false, stringValue: "running"},
		{name: "completed", state: OperationStateCompleted, valid: true, zero: false, terminal: true, stringValue: "completed"},
		{name: "failed", state: OperationStateFailed, valid: true, zero: false, terminal: true, stringValue: "failed"},
		{name: "cancelled", state: OperationStateCancelled, valid: true, zero: false, terminal: true, stringValue: "cancelled"},
		{name: "zero", state: "", valid: false, zero: true, terminal: false, stringValue: ""},
		{name: "invalid", state: OperationState("paused"), valid: false, zero: false, terminal: false, stringValue: "paused"},
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

func TestOperationResults(t *testing.T) {
	t.Parallel()

	want := []OperationResult{
		OperationResultPending,
		OperationResultSucceeded,
		OperationResultFailed,
		OperationResultCancelled,
	}

	got := OperationResults()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected operation results: got %v, want %v", got, want)
	}

	got[0] = OperationResultCancelled

	if second := OperationResults(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected operation results copy, got %v, want %v", second, want)
	}
}

func TestOperationResultValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		result      OperationResult
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "pending", result: OperationResultPending, valid: true, zero: false, stringValue: "pending"},
		{name: "succeeded", result: OperationResultSucceeded, valid: true, zero: false, stringValue: "succeeded"},
		{name: "failed", result: OperationResultFailed, valid: true, zero: false, stringValue: "failed"},
		{name: "cancelled", result: OperationResultCancelled, valid: true, zero: false, stringValue: "cancelled"},
		{name: "zero", result: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", result: OperationResult("unknown"), valid: false, zero: false, stringValue: "unknown"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.result.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.result.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.result.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestOperationValidate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name      string
		operation Operation
		wantErr   error
	}{
		{
			name: "valid operation",
			operation: Operation{
				ID:          "op-1",
				Kind:        OperationKindSwitchover,
				State:       OperationStateRunning,
				RequestedBy: "operator",
				RequestedAt: now,
				Reason:      "move primary",
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				ScheduledAt: now.Add(10 * time.Minute),
				StartedAt:   now.Add(10 * time.Minute),
				Result:      OperationResultPending,
				Message:     "running",
			},
		},
		{
			name: "operation id required",
			operation: Operation{
				ID:          "   ",
				Kind:        OperationKindSwitchover,
				State:       OperationStateAccepted,
				RequestedAt: now,
			},
			wantErr: ErrOperationIDRequired,
		},
		{
			name: "operation kind required",
			operation: Operation{
				ID:          "op-1",
				State:       OperationStateAccepted,
				RequestedAt: now,
			},
			wantErr: ErrOperationKindRequired,
		},
		{
			name: "operation kind must be valid",
			operation: Operation{
				ID:          "op-1",
				Kind:        OperationKind("rebalance"),
				State:       OperationStateAccepted,
				RequestedAt: now,
			},
			wantErr: ErrInvalidOperationKind,
		},
		{
			name: "operation state required",
			operation: Operation{
				ID:          "op-1",
				Kind:        OperationKindSwitchover,
				RequestedAt: now,
			},
			wantErr: ErrOperationStateRequired,
		},
		{
			name: "operation state must be valid",
			operation: Operation{
				ID:          "op-1",
				Kind:        OperationKindSwitchover,
				State:       OperationState("paused"),
				RequestedAt: now,
			},
			wantErr: ErrInvalidOperationState,
		},
		{
			name: "operation requested time required",
			operation: Operation{
				ID:    "op-1",
				Kind:  OperationKindSwitchover,
				State: OperationStateAccepted,
			},
			wantErr: ErrOperationRequestedAtRequired,
		},
		{
			name: "operation result must be valid when set",
			operation: Operation{
				ID:          "op-1",
				Kind:        OperationKindSwitchover,
				State:       OperationStateAccepted,
				RequestedAt: now,
				Result:      OperationResult("broken"),
			},
			wantErr: ErrInvalidOperationResult,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.operation.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestOperationClone(t *testing.T) {
	t.Parallel()

	original := Operation{
		ID:          "op-1",
		Kind:        OperationKindFailover,
		State:       OperationStateCompleted,
		RequestedBy: "controller",
		RequestedAt: time.Now().UTC(),
		Result:      OperationResultSucceeded,
		Message:     "done",
	}

	clone := original.Clone()
	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("unexpected clone: got %+v, want %+v", clone, original)
	}
}

func TestHistoryEntryValidate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name    string
		entry   HistoryEntry
		wantErr error
	}{
		{
			name: "valid history entry",
			entry: HistoryEntry{
				OperationID: "op-1",
				Kind:        OperationKindRejoin,
				Timeline:    7,
				WALLSN:      "0/40001A0",
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				Reason:      "repair diverged primary",
				Result:      OperationResultSucceeded,
				FinishedAt:  now,
			},
		},
		{
			name: "history operation id required",
			entry: HistoryEntry{
				Kind:       OperationKindRejoin,
				Result:     OperationResultSucceeded,
				FinishedAt: now,
			},
			wantErr: ErrHistoryOperationIDRequired,
		},
		{
			name: "history kind required",
			entry: HistoryEntry{
				OperationID: "op-1",
				Result:      OperationResultSucceeded,
				FinishedAt:  now,
			},
			wantErr: ErrOperationKindRequired,
		},
		{
			name: "history kind must be valid",
			entry: HistoryEntry{
				OperationID: "op-1",
				Kind:        OperationKind("rebalance"),
				Result:      OperationResultSucceeded,
				FinishedAt:  now,
			},
			wantErr: ErrInvalidOperationKind,
		},
		{
			name: "history timeline must be non-negative",
			entry: HistoryEntry{
				OperationID: "op-1",
				Kind:        OperationKindFailover,
				Timeline:    -1,
				Result:      OperationResultSucceeded,
				FinishedAt:  now,
			},
			wantErr: ErrHistoryTimelineNegative,
		},
		{
			name: "history result required",
			entry: HistoryEntry{
				OperationID: "op-1",
				Kind:        OperationKindFailover,
				FinishedAt:  now,
			},
			wantErr: ErrOperationResultRequired,
		},
		{
			name: "history result must be final",
			entry: HistoryEntry{
				OperationID: "op-1",
				Kind:        OperationKindFailover,
				Result:      OperationResultPending,
				FinishedAt:  now,
			},
			wantErr: ErrInvalidOperationResult,
		},
		{
			name: "history finished time required",
			entry: HistoryEntry{
				OperationID: "op-1",
				Kind:        OperationKindFailover,
				Result:      OperationResultSucceeded,
			},
			wantErr: ErrHistoryFinishedAtRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.entry.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestScheduledSwitchoverValidate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name      string
		scheduled ScheduledSwitchover
		wantErr   error
	}{
		{
			name: "valid scheduled switchover",
			scheduled: ScheduledSwitchover{
				At:   now,
				From: "alpha-1",
				To:   "alpha-2",
			},
		},
		{
			name: "scheduled switchover time required",
			scheduled: ScheduledSwitchover{
				From: "alpha-1",
			},
			wantErr: ErrScheduledSwitchoverAtRequired,
		},
		{
			name: "scheduled switchover source required",
			scheduled: ScheduledSwitchover{
				At: now,
			},
			wantErr: ErrScheduledSwitchoverFromRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.scheduled.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
