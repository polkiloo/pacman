package controlplane

import (
	"errors"
	"reflect"
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestValidateFailoverIntentCreationPolicyModes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		mode    cluster.FailoverMode
		wantErr error
	}{
		{name: "default permits automatic failover"},
		{name: "automatic permits failover", mode: cluster.FailoverModeAutomatic},
		{name: "manual only blocks automatic failover", mode: cluster.FailoverModeManualOnly, wantErr: ErrAutomaticFailoverNotAllowed},
		{name: "disabled blocks automatic failover", mode: cluster.FailoverModeDisabled, wantErr: ErrAutomaticFailoverNotAllowed},
		{name: "unknown mode blocks automatic failover", mode: cluster.FailoverMode("operator_only"), wantErr: ErrAutomaticFailoverNotAllowed},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := validateFailoverIntentCreation(
				cluster.ClusterSpec{Failover: cluster.FailoverPolicy{Mode: testCase.mode}},
				cluster.ClusterStatus{},
				nil,
			)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestValidateFailoverConfirmationPolicyGuards(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		confirmation PrimaryFailureConfirmation
		wantErr      error
	}{
		{
			name:    "current primary required",
			wantErr: ErrFailoverPrimaryUnknown,
		},
		{
			name: "healthy primary blocks failover",
			confirmation: PrimaryFailureConfirmation{
				CurrentPrimary: "alpha-1",
				PrimaryHealthy: true,
			},
			wantErr: ErrFailoverPrimaryHealthy,
		},
		{
			name: "quorum required and unreachable blocks failover",
			confirmation: PrimaryFailureConfirmation{
				CurrentPrimary:  "alpha-1",
				QuorumRequired:  true,
				QuorumReachable: false,
			},
			wantErr: ErrFailoverQuorumUnavailable,
		},
		{
			name: "confirmed failed primary is accepted without quorum requirement",
			confirmation: PrimaryFailureConfirmation{
				CurrentPrimary: "alpha-1",
			},
		},
		{
			name: "confirmed failed primary is accepted with reachable quorum",
			confirmation: PrimaryFailureConfirmation{
				CurrentPrimary:  "alpha-1",
				QuorumRequired:  true,
				QuorumReachable: true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := validateFailoverConfirmation(testCase.confirmation)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestAppendFailoverPolicyReasons(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		policy          cluster.FailoverPolicy
		member          cluster.MemberStatus
		primaryTimeline int64
		want            []string
	}{
		{
			name: "no-failover tag rejects candidate",
			member: cluster.MemberStatus{
				NoFailover: true,
			},
			want: []string{reasonNoFailoverTagged},
		},
		{
			name: "lag above policy rejects candidate",
			policy: cluster.FailoverPolicy{
				MaximumLagBytes: 64,
			},
			member: cluster.MemberStatus{
				LagBytes: 65,
			},
			want: []string{reasonLagExceedsFailoverPolicy},
		},
		{
			name: "timeline mismatch rejects candidate when policy checks timelines",
			policy: cluster.FailoverPolicy{
				CheckTimeline: true,
			},
			member: cluster.MemberStatus{
				Timeline: 7,
			},
			primaryTimeline: 8,
			want:            []string{reasonTimelineMismatch},
		},
		{
			name: "timeline is ignored without primary timeline reference",
			policy: cluster.FailoverPolicy{
				CheckTimeline: true,
			},
			member: cluster.MemberStatus{
				Timeline: 7,
			},
			primaryTimeline: 0,
		},
		{
			name: "all policy reasons accumulate deterministically",
			policy: cluster.FailoverPolicy{
				MaximumLagBytes: 64,
				CheckTimeline:   true,
			},
			member: cluster.MemberStatus{
				NoFailover: true,
				LagBytes:   128,
				Timeline:   7,
			},
			primaryTimeline: 8,
			want: []string{
				reasonNoFailoverTagged,
				reasonLagExceedsFailoverPolicy,
				reasonTimelineMismatch,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := appendFailoverPolicyReasons(nil, cluster.ClusterSpec{Failover: testCase.policy}, testCase.member, testCase.primaryTimeline)
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("unexpected policy reasons: got %v, want %v", got, testCase.want)
			}
		})
	}
}
