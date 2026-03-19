package cluster

import (
	"strings"
	"time"
)

// OperationKind identifies a topology or maintenance action tracked by
// PACMAN.
type OperationKind string

const (
	OperationKindSwitchover        OperationKind = "switchover"
	OperationKindFailover          OperationKind = "failover"
	OperationKindRejoin            OperationKind = "rejoin"
	OperationKindMaintenanceChange OperationKind = "maintenance_change"
)

var operationKinds = []OperationKind{
	OperationKindSwitchover,
	OperationKindFailover,
	OperationKindRejoin,
	OperationKindMaintenanceChange,
}

// OperationKinds returns the full set of operation kinds known to PACMAN.
func OperationKinds() []OperationKind {
	return append([]OperationKind(nil), operationKinds...)
}

func (kind OperationKind) String() string {
	return string(kind)
}

// IsValid reports whether the value is a supported operation kind.
func (kind OperationKind) IsValid() bool {
	switch kind {
	case OperationKindSwitchover, OperationKindFailover, OperationKindRejoin, OperationKindMaintenanceChange:
		return true
	default:
		return false
	}
}

// IsZero reports whether the kind was left unspecified.
func (kind OperationKind) IsZero() bool {
	return kind == ""
}

// OperationState describes the lifecycle state of a tracked operation.
type OperationState string

const (
	OperationStateAccepted  OperationState = "accepted"
	OperationStateScheduled OperationState = "scheduled"
	OperationStateRunning   OperationState = "running"
	OperationStateCompleted OperationState = "completed"
	OperationStateFailed    OperationState = "failed"
	OperationStateCancelled OperationState = "cancelled"
)

var operationStates = []OperationState{
	OperationStateAccepted,
	OperationStateScheduled,
	OperationStateRunning,
	OperationStateCompleted,
	OperationStateFailed,
	OperationStateCancelled,
}

// OperationStates returns the full set of operation states known to PACMAN.
func OperationStates() []OperationState {
	return append([]OperationState(nil), operationStates...)
}

func (state OperationState) String() string {
	return string(state)
}

// IsValid reports whether the value is a supported operation state.
func (state OperationState) IsValid() bool {
	switch state {
	case OperationStateAccepted, OperationStateScheduled, OperationStateRunning, OperationStateCompleted, OperationStateFailed, OperationStateCancelled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the state was left unspecified.
func (state OperationState) IsZero() bool {
	return state == ""
}

// IsTerminal reports whether the operation has reached a final lifecycle
// state.
func (state OperationState) IsTerminal() bool {
	switch state {
	case OperationStateCompleted, OperationStateFailed, OperationStateCancelled:
		return true
	default:
		return false
	}
}

// OperationResult captures the outcome of an operation.
type OperationResult string

const (
	OperationResultPending   OperationResult = "pending"
	OperationResultSucceeded OperationResult = "succeeded"
	OperationResultFailed    OperationResult = "failed"
	OperationResultCancelled OperationResult = "cancelled"
)

var operationResults = []OperationResult{
	OperationResultPending,
	OperationResultSucceeded,
	OperationResultFailed,
	OperationResultCancelled,
}

// OperationResults returns the full set of operation results known to PACMAN.
func OperationResults() []OperationResult {
	return append([]OperationResult(nil), operationResults...)
}

func (result OperationResult) String() string {
	return string(result)
}

// IsValid reports whether the value is a supported operation result.
func (result OperationResult) IsValid() bool {
	switch result {
	case OperationResultPending, OperationResultSucceeded, OperationResultFailed, OperationResultCancelled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the result was left unspecified.
func (result OperationResult) IsZero() bool {
	return result == ""
}

// Operation describes an accepted or running cluster action.
type Operation struct {
	ID          string
	Kind        OperationKind
	State       OperationState
	RequestedBy string
	RequestedAt time.Time
	Reason      string
	FromMember  string
	ToMember    string
	ScheduledAt time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	Result      OperationResult
	Message     string
}

// Validate reports whether the operation is coherent enough to be published.
func (operation Operation) Validate() error {
	if strings.TrimSpace(operation.ID) == "" {
		return ErrOperationIDRequired
	}

	if operation.Kind.IsZero() {
		return ErrOperationKindRequired
	}

	if !operation.Kind.IsValid() {
		return ErrInvalidOperationKind
	}

	if operation.State.IsZero() {
		return ErrOperationStateRequired
	}

	if !operation.State.IsValid() {
		return ErrInvalidOperationState
	}

	if operation.RequestedAt.IsZero() {
		return ErrOperationRequestedAtRequired
	}

	if !operation.Result.IsZero() && !operation.Result.IsValid() {
		return ErrInvalidOperationResult
	}

	return nil
}

// Clone returns a detached copy of the operation.
func (operation Operation) Clone() Operation {
	return operation
}

// HistoryEntry records the finished outcome of a topology or maintenance
// operation.
type HistoryEntry struct {
	OperationID string
	Kind        OperationKind
	Timeline    int64
	WALLSN      string
	FromMember  string
	ToMember    string
	Reason      string
	Result      OperationResult
	FinishedAt  time.Time
}

// Validate reports whether the history entry is coherent enough to publish.
func (entry HistoryEntry) Validate() error {
	if strings.TrimSpace(entry.OperationID) == "" {
		return ErrHistoryOperationIDRequired
	}

	if entry.Kind.IsZero() {
		return ErrOperationKindRequired
	}

	if !entry.Kind.IsValid() {
		return ErrInvalidOperationKind
	}

	if entry.Timeline < 0 {
		return ErrHistoryTimelineNegative
	}

	if entry.Result.IsZero() {
		return ErrOperationResultRequired
	}

	if !entry.Result.IsValid() || entry.Result == OperationResultPending {
		return ErrInvalidOperationResult
	}

	if entry.FinishedAt.IsZero() {
		return ErrHistoryFinishedAtRequired
	}

	return nil
}

// ScheduledSwitchover captures a future planned topology handoff.
type ScheduledSwitchover struct {
	At   time.Time
	From string
	To   string
}

// Validate reports whether the scheduled switchover is coherent enough to be
// published.
func (scheduled ScheduledSwitchover) Validate() error {
	if scheduled.At.IsZero() {
		return ErrScheduledSwitchoverAtRequired
	}

	if strings.TrimSpace(scheduled.From) == "" {
		return ErrScheduledSwitchoverFromRequired
	}

	return nil
}

// Clone returns a detached copy of the scheduled switchover.
func (scheduled ScheduledSwitchover) Clone() ScheduledSwitchover {
	return scheduled
}
