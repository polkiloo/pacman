package controlplane

import (
	"context"
	"strings"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

type rejoinInputs struct {
	checkedAt             time.Time
	currentEpoch          cluster.Epoch
	member                cluster.MemberStatus
	memberNode            agentmodel.NodeStatus
	hasMemberNode         bool
	currentPrimary        cluster.MemberStatus
	hasCurrentPrimary     bool
	currentPrimaryNode    agentmodel.NodeStatus
	hasCurrentPrimaryNode bool
}

// AssessRejoinMember reports whether the requested node currently looks like a
// former primary that should enter the rejoin workflow.
func (store *MemoryStateStore) AssessRejoinMember(nodeName string) (RejoinMemberAssessment, error) {
	target := strings.TrimSpace(nodeName)
	if target == "" {
		return RejoinMemberAssessment{}, ErrRejoinTargetRequired
	}

	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return RejoinMemberAssessment{}, err
	}

	store.mu.RLock()
	inputs, err := store.rejoinInputsLocked(target)
	store.mu.RUnlock()
	if err != nil {
		return RejoinMemberAssessment{}, err
	}

	return buildRejoinMemberAssessment(inputs), nil
}

// DetectRejoinDivergence compares the observed former primary state with the
// current primary and reports whether rewind or reclone will likely be
// required later in the rejoin workflow.
func (store *MemoryStateStore) DetectRejoinDivergence(nodeName string) (RejoinDivergenceAssessment, error) {
	target := strings.TrimSpace(nodeName)
	if target == "" {
		return RejoinDivergenceAssessment{}, ErrRejoinTargetRequired
	}

	if err := store.ensureCacheFresh(context.Background()); err != nil {
		return RejoinDivergenceAssessment{}, err
	}

	store.mu.RLock()
	inputs, err := store.rejoinInputsLocked(target)
	store.mu.RUnlock()
	if err != nil {
		return RejoinDivergenceAssessment{}, err
	}

	return buildRejoinDivergenceAssessment(inputs), nil
}

func buildRejoinMemberAssessment(inputs rejoinInputs) RejoinMemberAssessment {
	assessment := RejoinMemberAssessment{
		State:         cluster.RejoinStateAssessingMember,
		Member:        inputs.member.Clone(),
		FormerPrimary: inputs.member.NeedsRejoin,
		CheckedAt:     inputs.checkedAt,
	}

	if inputs.hasCurrentPrimary {
		assessment.CurrentPrimary = inputs.currentPrimary.Clone()
	}

	if inputs.hasMemberNode {
		assessment.ManagedPostgres = inputs.memberNode.Postgres.Managed
		assessment.PostgresUp = inputs.memberNode.Postgres.Up
	}

	assessment.Reasons = assessRejoinMemberReasons(inputs)
	assessment.Ready = len(assessment.Reasons) == 0

	return assessment.Clone()
}

func buildRejoinDivergenceAssessment(inputs rejoinInputs) RejoinDivergenceAssessment {
	assessment := RejoinDivergenceAssessment{
		State:     cluster.RejoinStateDetectingDivergence,
		Member:    inputs.member.Clone(),
		CheckedAt: inputs.checkedAt,
	}

	if inputs.hasCurrentPrimary {
		assessment.CurrentPrimary = inputs.currentPrimary.Clone()
	}

	if inputs.hasMemberNode {
		assessment.MemberSystemIdentifier = strings.TrimSpace(inputs.memberNode.Postgres.Details.SystemIdentifier)
	}

	if inputs.hasCurrentPrimaryNode {
		assessment.CurrentPrimarySystemIdentifier = strings.TrimSpace(inputs.currentPrimaryNode.Postgres.Details.SystemIdentifier)
	}

	assessment.Reasons = assessRejoinMemberReasons(inputs)
	if len(assessment.Reasons) > 0 {
		return assessment.Clone()
	}

	if !inputs.hasCurrentPrimaryNode {
		assessment.Reasons = append(assessment.Reasons, reasonCurrentPrimaryStateNotObserved)
		return assessment.Clone()
	}

	switch {
	case assessment.MemberSystemIdentifier == "":
		assessment.Reasons = append(assessment.Reasons, reasonMemberSystemIdentifierUnknown)
		return assessment.Clone()
	case assessment.CurrentPrimarySystemIdentifier == "":
		assessment.Reasons = append(assessment.Reasons, reasonCurrentPrimarySystemIdentifierUnknown)
		return assessment.Clone()
	}

	if assessment.MemberSystemIdentifier != assessment.CurrentPrimarySystemIdentifier {
		assessment.Compared = true
		assessment.Diverged = true
		assessment.RequiresReclone = true
		assessment.Reasons = append(assessment.Reasons, reasonSystemIdentifierMismatch)
		return assessment.Clone()
	}

	switch {
	case inputs.member.Timeline == 0:
		assessment.Reasons = append(assessment.Reasons, reasonMemberTimelineUnknown)
		return assessment.Clone()
	case inputs.currentPrimary.Timeline == 0:
		assessment.Reasons = append(assessment.Reasons, reasonCurrentPrimaryTimelineUnknown)
		return assessment.Clone()
	}

	assessment.Compared = true
	switch {
	case inputs.member.Timeline > inputs.currentPrimary.Timeline:
		assessment.Diverged = true
		assessment.RequiresReclone = true
		assessment.Reasons = append(assessment.Reasons, reasonTimelineAheadOfCurrentPrimary)
	}

	return assessment.Clone()
}

func (store *MemoryStateStore) rejoinInputsLocked(nodeName string) (rejoinInputs, error) {
	if store.clusterStatus == nil {
		return rejoinInputs{}, ErrRejoinObservedStateRequired
	}

	member, ok := store.memberLocked(nodeName)
	if !ok {
		return rejoinInputs{}, ErrRejoinTargetUnknown
	}

	inputs := rejoinInputs{
		checkedAt:    store.now().UTC(),
		currentEpoch: store.clusterStatus.CurrentEpoch,
		member:       member.Clone(),
	}

	if status, ok := store.nodeStatuses[nodeName]; ok {
		inputs.memberNode = status.Clone()
		inputs.hasMemberNode = true
	}

	currentPrimary, ok := currentPrimaryMember(store.clusterStatus.Members)
	if !ok {
		return inputs, nil
	}

	inputs.currentPrimary = currentPrimary.Clone()
	inputs.hasCurrentPrimary = true

	if status, ok := store.nodeStatuses[currentPrimary.Name]; ok {
		inputs.currentPrimaryNode = status.Clone()
		inputs.hasCurrentPrimaryNode = true
	}

	return inputs, nil
}

func assessRejoinMemberReasons(inputs rejoinInputs) []string {
	reasons := make([]string, 0, 4)

	if !inputs.member.NeedsRejoin {
		reasons = append(reasons, reasonMemberDoesNotRequireRejoin)
	}

	if inputs.hasCurrentPrimary {
		if inputs.currentPrimary.Name == inputs.member.Name {
			reasons = append(reasons, reasonCurrentPrimary)
		}

		if !inputs.currentPrimary.Healthy {
			reasons = append(reasons, reasonCurrentPrimaryUnhealthy)
		}
	} else {
		reasons = append(reasons, reasonCurrentPrimaryUnknown)
	}

	if !inputs.hasMemberNode {
		reasons = append(reasons, reasonNodeStateNotObserved)
		return reasons
	}

	if !inputs.memberNode.Postgres.Managed {
		reasons = append(reasons, reasonPostgresNotManaged)
	}

	return reasons
}
