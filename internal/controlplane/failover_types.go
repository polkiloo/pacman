package controlplane

import (
	"context"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// FailoverEngine exposes automatic failover planning against the replicated
// control-plane state.
type FailoverEngine interface {
	FailoverCandidates() ([]FailoverCandidate, error)
	ConfirmPrimaryFailure() (PrimaryFailureConfirmation, error)
	CreateFailoverIntent(context.Context, FailoverIntentRequest) (FailoverIntent, error)
	ExecuteFailover(context.Context, PromotionExecutor, FencingHook) (FailoverExecution, error)
}

// FailoverCandidate describes a member's promotion eligibility and ranking
// under the current failover policy.
type FailoverCandidate struct {
	Member   cluster.MemberStatus
	Eligible bool
	Rank     int
	Reasons  []string
}

// Clone returns a detached copy of the failover candidate.
func (candidate FailoverCandidate) Clone() FailoverCandidate {
	clone := candidate
	clone.Member = candidate.Member.Clone()
	if candidate.Reasons != nil {
		clone.Reasons = append([]string(nil), candidate.Reasons...)
	}

	return clone
}

// PrimaryFailureConfirmation captures whether the current primary has been
// confirmed failed strongly enough to start automatic failover.
type PrimaryFailureConfirmation struct {
	CurrentPrimary  string
	Confirmed       bool
	PrimaryHealthy  bool
	QuorumRequired  bool
	QuorumReachable bool
	ReachableVoters int
	RequiredVoters  int
	TotalVoters     int
}

// Clone returns a detached copy of the confirmation result.
func (confirmation PrimaryFailureConfirmation) Clone() PrimaryFailureConfirmation {
	return confirmation
}

// FailoverIntentRequest captures operator metadata attached to a newly created
// automatic failover intent.
type FailoverIntentRequest struct {
	Candidate   string
	RequestedBy string
	Reason      string
}

// FailoverIntent captures the accepted operation and the evaluated state that
// justified it.
type FailoverIntent struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	Confirmation   PrimaryFailureConfirmation
	Candidates     []FailoverCandidate
	CreatedAt      time.Time
}

// Clone returns a detached copy of the failover intent.
func (intent FailoverIntent) Clone() FailoverIntent {
	clone := intent
	clone.Operation = intent.Operation.Clone()
	clone.Confirmation = intent.Confirmation.Clone()
	clone.Candidates = cloneFailoverCandidates(intent.Candidates)

	return clone
}

// FencingHook authorizes or performs split-brain prevention steps before a
// candidate is promoted.
type FencingHook interface {
	Fence(context.Context, FencingRequest) error
}

// FencingRequest describes the active failover intent that requires fencing.
type FencingRequest struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	CurrentEpoch   cluster.Epoch
}

// PromotionExecutor performs the node-local promotion for the chosen failover
// candidate.
type PromotionExecutor interface {
	Promote(context.Context, PromotionRequest) error
}

// PromotionRequest describes the candidate selected for promotion during an
// active failover.
type PromotionRequest struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	CurrentEpoch   cluster.Epoch
}

// FailoverExecution captures the outcome of executing the accepted failover
// intent up to the point where the new epoch is published.
type FailoverExecution struct {
	Operation      cluster.Operation
	CurrentPrimary string
	Candidate      string
	PreviousEpoch  cluster.Epoch
	CurrentEpoch   cluster.Epoch
	Fenced         bool
	Promoted       bool
	ExecutedAt     time.Time
}

// Clone returns a detached copy of the failover execution result.
func (execution FailoverExecution) Clone() FailoverExecution {
	clone := execution
	clone.Operation = execution.Operation.Clone()

	return clone
}
