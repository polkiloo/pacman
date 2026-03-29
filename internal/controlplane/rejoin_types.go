package controlplane

import (
	"context"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// RejoinEngine exposes rejoin-planning and early execution against the
// replicated control-plane state.
type RejoinEngine interface {
	AssessRejoinMember(string) (RejoinMemberAssessment, error)
	DetectRejoinDivergence(string) (RejoinDivergenceAssessment, error)
	DecideRejoinStrategy(string) (RejoinStrategyDecision, error)
	ExecuteRejoinRewind(context.Context, RejoinRequest, RewindExecutor) (RejoinExecution, error)
}

// RejoinMemberAssessment captures whether an observed member looks like a
// former primary that is ready to enter the rejoin workflow.
type RejoinMemberAssessment struct {
	State           cluster.RejoinState
	CurrentPrimary  cluster.MemberStatus
	Member          cluster.MemberStatus
	FormerPrimary   bool
	ManagedPostgres bool
	PostgresUp      bool
	Ready           bool
	Reasons         []string
	CheckedAt       time.Time
}

// Clone returns a detached copy of the rejoin member assessment.
func (assessment RejoinMemberAssessment) Clone() RejoinMemberAssessment {
	clone := assessment
	clone.CurrentPrimary = assessment.CurrentPrimary.Clone()
	clone.Member = assessment.Member.Clone()
	if assessment.Reasons != nil {
		clone.Reasons = append([]string(nil), assessment.Reasons...)
	}

	return clone
}

// RejoinDivergenceAssessment captures whether the rejoining member appears to
// have diverged from the current primary strongly enough to require rewind or
// a full reclone.
type RejoinDivergenceAssessment struct {
	State                          cluster.RejoinState
	CurrentPrimary                 cluster.MemberStatus
	Member                         cluster.MemberStatus
	MemberSystemIdentifier         string
	CurrentPrimarySystemIdentifier string
	Compared                       bool
	Diverged                       bool
	RequiresRewind                 bool
	RequiresReclone                bool
	Reasons                        []string
	CheckedAt                      time.Time
}

// Clone returns a detached copy of the rejoin divergence assessment.
func (assessment RejoinDivergenceAssessment) Clone() RejoinDivergenceAssessment {
	clone := assessment
	clone.CurrentPrimary = assessment.CurrentPrimary.Clone()
	clone.Member = assessment.Member.Clone()
	if assessment.Reasons != nil {
		clone.Reasons = append([]string(nil), assessment.Reasons...)
	}

	return clone
}

// RejoinStrategyDecision captures whether the current rejoin assessment can
// choose between pg_rewind and a full reclone.
type RejoinStrategyDecision struct {
	State                cluster.RejoinState
	CurrentPrimary       cluster.MemberStatus
	Member               cluster.MemberStatus
	Divergence           RejoinDivergenceAssessment
	Strategy             cluster.RejoinStrategy
	Decided              bool
	DirectRejoinPossible bool
	Reasons              []string
	DecidedAt            time.Time
}

// Clone returns a detached copy of the strategy decision.
func (decision RejoinStrategyDecision) Clone() RejoinStrategyDecision {
	clone := decision
	clone.CurrentPrimary = decision.CurrentPrimary.Clone()
	clone.Member = decision.Member.Clone()
	clone.Divergence = decision.Divergence.Clone()
	if decision.Reasons != nil {
		clone.Reasons = append([]string(nil), decision.Reasons...)
	}

	return clone
}

// RejoinRequest captures operator metadata attached to an explicit rejoin
// execution request.
type RejoinRequest struct {
	Member      string
	RequestedBy string
	Reason      string
}

// RewindExecutor performs the local pg_rewind workflow for a rejoining member.
type RewindExecutor interface {
	Rewind(context.Context, RewindRequest) error
}

// RewindRequest describes the selected rejoin repair path and the source
// topology information the local executor needs for pg_rewind.
type RewindRequest struct {
	Operation          cluster.Operation
	Decision           RejoinStrategyDecision
	MemberNode         agentmodel.NodeStatus
	CurrentPrimaryNode agentmodel.NodeStatus
	CurrentEpoch       cluster.Epoch
}

// RejoinExecution captures the outcome of executing the pg_rewind phase of an
// in-flight rejoin.
type RejoinExecution struct {
	Operation    cluster.Operation
	Decision     RejoinStrategyDecision
	CurrentEpoch cluster.Epoch
	State        cluster.RejoinState
	Rewound      bool
	ExecutedAt   time.Time
}

// Clone returns a detached copy of the rejoin execution result.
func (execution RejoinExecution) Clone() RejoinExecution {
	clone := execution
	clone.Operation = execution.Operation.Clone()
	clone.Decision = execution.Decision.Clone()

	return clone
}
