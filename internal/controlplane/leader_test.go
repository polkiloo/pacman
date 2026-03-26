package controlplane

import (
	"testing"
	"time"
)

func TestLeaderLeaseIsActiveAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 27, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		lease    LeaderLease
		duration time.Duration
		want     bool
	}{
		{
			name:     "empty leader is inactive",
			lease:    LeaderLease{},
			duration: time.Second,
			want:     false,
		},
		{
			name: "missing timestamps is inactive",
			lease: LeaderLease{
				LeaderNode: "alpha-1",
			},
			duration: time.Second,
			want:     false,
		},
		{
			name: "renewed lease is active within duration",
			lease: LeaderLease{
				LeaderNode: "alpha-1",
				RenewedAt:  now.Add(-500 * time.Millisecond),
			},
			duration: time.Second,
			want:     true,
		},
		{
			name: "acquired lease expires after duration",
			lease: LeaderLease{
				LeaderNode: "alpha-1",
				AcquiredAt: now.Add(-2 * time.Second),
			},
			duration: time.Second,
			want:     false,
		},
		{
			name: "nonpositive duration treats populated lease as active",
			lease: LeaderLease{
				LeaderNode: "alpha-1",
				AcquiredAt: now.Add(-24 * time.Hour),
			},
			duration: 0,
			want:     true,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.lease.isActiveAt(now, testCase.duration); got != testCase.want {
				t.Fatalf("unexpected active result: got %v, want %v", got, testCase.want)
			}
		})
	}
}
