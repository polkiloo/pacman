package controlplane

import "github.com/polkiloo/pacman/internal/cluster"

// DecideRejoinStrategy chooses between pg_rewind and reclone when the current
// divergence assessment has enough information to select a repair path.
func (store *MemoryStateStore) DecideRejoinStrategy(nodeName string) (RejoinStrategyDecision, error) {
	divergence, err := store.DetectRejoinDivergence(nodeName)
	if err != nil {
		return RejoinStrategyDecision{}, err
	}

	return buildRejoinStrategyDecision(divergence), nil
}

func buildRejoinStrategyDecision(divergence RejoinDivergenceAssessment) RejoinStrategyDecision {
	decision := RejoinStrategyDecision{
		State:          cluster.RejoinStateSelectingStrategy,
		CurrentPrimary: divergence.CurrentPrimary.Clone(),
		Member:         divergence.Member.Clone(),
		Divergence:     divergence.Clone(),
		Reasons:        append([]string(nil), divergence.Reasons...),
		DecidedAt:      divergence.CheckedAt,
	}

	switch {
	case divergence.RequiresReclone:
		decision.Strategy = cluster.RejoinStrategyReclone
		decision.Decided = true
	case divergence.RequiresRewind:
		decision.Strategy = cluster.RejoinStrategyRewind
		decision.Decided = true
	case divergence.Compared && !divergence.Diverged:
		decision.DirectRejoinPossible = true
	case len(decision.Reasons) > 0:
	}

	return decision.Clone()
}
