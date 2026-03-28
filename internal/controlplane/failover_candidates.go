package controlplane

import (
	"sort"
	"strings"

	"github.com/polkiloo/pacman/internal/cluster"
)

func evaluateFailoverCandidates(spec cluster.ClusterSpec, status cluster.ClusterStatus) []FailoverCandidate {
	primary, primaryTimeline := failoverPrimaryReference(status)
	candidates := buildFailoverCandidates(spec, status.Members, primary, primaryTimeline)
	sortFailoverCandidates(candidates)
	rankFailoverCandidates(candidates)

	return cloneFailoverCandidates(candidates)
}

func failoverPrimaryReference(status cluster.ClusterStatus) (cluster.MemberStatus, int64) {
	primary, ok := failoverPrimaryMember(status)
	if !ok {
		return cluster.MemberStatus{}, 0
	}

	return primary, primary.Timeline
}

func buildFailoverCandidates(spec cluster.ClusterSpec, members []cluster.MemberStatus, primary cluster.MemberStatus, primaryTimeline int64) []FailoverCandidate {
	candidates := make([]FailoverCandidate, 0, len(members))
	for _, member := range members {
		candidates = append(candidates, buildFailoverCandidate(spec, member, primary, primaryTimeline))
	}

	return candidates
}

func buildFailoverCandidate(spec cluster.ClusterSpec, member, primary cluster.MemberStatus, primaryTimeline int64) FailoverCandidate {
	candidate := FailoverCandidate{
		Member: member.Clone(),
	}
	candidate.Reasons = failoverCandidateReasons(spec, member, primary, primaryTimeline)
	candidate.Eligible = len(candidate.Reasons) == 0

	return candidate
}

func failoverCandidateReasons(spec cluster.ClusterSpec, member, primary cluster.MemberStatus, primaryTimeline int64) []string {
	reasons := make([]string, 0, 6)

	if member.Name == primary.Name && primary.Name != "" {
		reasons = append(reasons, reasonCurrentPrimary)
	}

	reasons = appendFailoverPromotableRoleReason(reasons, member)
	reasons = appendFailoverHealthReason(reasons, member)
	reasons = appendFailoverPolicyReasons(reasons, spec, member, primaryTimeline)

	return reasons
}

func appendFailoverPromotableRoleReason(reasons []string, member cluster.MemberStatus) []string {
	switch member.Role {
	case cluster.MemberRoleReplica, cluster.MemberRoleStandbyLeader:
		return reasons
	default:
		return append(reasons, reasonRoleNotPromotable)
	}
}

func appendFailoverHealthReason(reasons []string, member cluster.MemberStatus) []string {
	if !member.Healthy {
		reasons = append(reasons, reasonMemberUnhealthy)
	}

	if member.NeedsRejoin {
		reasons = append(reasons, reasonMemberRequiresRejoin)
	}

	return reasons
}

func appendFailoverPolicyReasons(reasons []string, spec cluster.ClusterSpec, member cluster.MemberStatus, primaryTimeline int64) []string {
	if member.NoFailover {
		reasons = append(reasons, reasonNoFailoverTagged)
	}

	if spec.Failover.MaximumLagBytes > 0 && member.LagBytes > spec.Failover.MaximumLagBytes {
		reasons = append(reasons, reasonLagExceedsFailoverPolicy)
	}

	if spec.Failover.CheckTimeline && primaryTimeline > 0 && member.Timeline != primaryTimeline {
		reasons = append(reasons, reasonTimelineMismatch)
	}

	return reasons
}

func sortFailoverCandidates(candidates []FailoverCandidate) {
	sort.Slice(candidates, func(left, right int) bool {
		return failoverCandidateLess(candidates[left], candidates[right])
	})
}

func failoverCandidateLess(left, right FailoverCandidate) bool {
	if left.Eligible != right.Eligible {
		return left.Eligible
	}

	if !left.Eligible {
		return left.Member.Name < right.Member.Name
	}

	if left.Member.Priority != right.Member.Priority {
		return left.Member.Priority > right.Member.Priority
	}

	if left.Member.LagBytes != right.Member.LagBytes {
		return left.Member.LagBytes < right.Member.LagBytes
	}

	if left.Member.Timeline != right.Member.Timeline {
		return left.Member.Timeline > right.Member.Timeline
	}

	if !left.Member.LastSeenAt.Equal(right.Member.LastSeenAt) {
		return left.Member.LastSeenAt.After(right.Member.LastSeenAt)
	}

	return left.Member.Name < right.Member.Name
}

func rankFailoverCandidates(candidates []FailoverCandidate) {
	rank := 1
	for index := range candidates {
		if !candidates[index].Eligible {
			continue
		}

		candidates[index].Rank = rank
		rank++
	}
}

func failoverPrimaryMember(status cluster.ClusterStatus) (cluster.MemberStatus, bool) {
	if strings.TrimSpace(status.CurrentPrimary) != "" {
		for _, member := range status.Members {
			if member.Name == status.CurrentPrimary {
				return member.Clone(), true
			}
		}
	}

	return currentPrimaryMember(status.Members)
}

func firstEligibleFailoverCandidate(candidates []FailoverCandidate) (FailoverCandidate, bool) {
	for _, candidate := range candidates {
		if candidate.Eligible {
			return candidate.Clone(), true
		}
	}

	return FailoverCandidate{}, false
}

func cloneFailoverCandidates(candidates []FailoverCandidate) []FailoverCandidate {
	if candidates == nil {
		return nil
	}

	cloned := make([]FailoverCandidate, len(candidates))
	for index, candidate := range candidates {
		cloned[index] = candidate.Clone()
	}

	return cloned
}
