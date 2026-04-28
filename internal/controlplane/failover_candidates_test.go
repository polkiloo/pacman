package controlplane

import (
	"reflect"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestSortFailoverCandidatesAppliesDeterministicRankingTieBreakers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 23, 9, 0, 0, 0, time.UTC)
	candidates := []FailoverCandidate{
		rankingCandidate("ineligible-z", false, 500, 0, 9, now),
		rankingCandidate("alpha-z", true, 100, 10, 5, now),
		rankingCandidate("alpha-high-timeline", true, 100, 10, 6, now),
		rankingCandidate("alpha-priority", true, 200, 100, 1, now),
		rankingCandidate("alpha-a", true, 100, 10, 5, now),
		rankingCandidate("alpha-newer", true, 100, 10, 5, now.Add(time.Second)),
		rankingCandidate("ineligible-a", false, 500, 0, 9, now),
		rankingCandidate("alpha-low-lag", true, 100, 1, 5, now),
	}

	sortFailoverCandidates(candidates)
	rankFailoverCandidates(candidates)

	gotNames := make([]string, len(candidates))
	gotRanks := make([]int, len(candidates))
	for index, candidate := range candidates {
		gotNames[index] = candidate.Member.Name
		gotRanks[index] = candidate.Rank
	}

	wantNames := []string{
		"alpha-priority",
		"alpha-low-lag",
		"alpha-high-timeline",
		"alpha-newer",
		"alpha-a",
		"alpha-z",
		"ineligible-a",
		"ineligible-z",
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("unexpected candidate order: got %v, want %v", gotNames, wantNames)
	}

	wantRanks := []int{1, 2, 3, 4, 5, 6, 0, 0}
	if !reflect.DeepEqual(gotRanks, wantRanks) {
		t.Fatalf("unexpected candidate ranks: got %v, want %v", gotRanks, wantRanks)
	}
}

func rankingCandidate(name string, eligible bool, priority int, lagBytes, timeline int64, lastSeenAt time.Time) FailoverCandidate {
	candidate := FailoverCandidate{
		Member: cluster.MemberStatus{
			Name:       name,
			Role:       cluster.MemberRoleReplica,
			State:      cluster.MemberStateStreaming,
			Healthy:    true,
			Priority:   priority,
			LagBytes:   lagBytes,
			Timeline:   timeline,
			LastSeenAt: lastSeenAt,
		},
		Eligible: eligible,
	}

	if !eligible {
		candidate.Reasons = []string{reasonMemberUnhealthy}
	}

	return candidate
}
