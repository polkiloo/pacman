package controlplane

import (
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestClusterSourceOfTruthValidate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 25, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		truth   ClusterSourceOfTruth
		wantErr error
	}{
		{
			name: "desired only",
			truth: ClusterSourceOfTruth{
				Desired: &cluster.ClusterSpec{
					ClusterName: "alpha",
				},
				UpdatedAt: now,
			},
		},
		{
			name: "observed only",
			truth: ClusterSourceOfTruth{
				Observed: &cluster.ClusterStatus{
					ClusterName: "alpha",
					Phase:       cluster.ClusterPhaseUnknown,
					Maintenance: cluster.MaintenanceModeStatus{},
					ObservedAt:  now,
				},
				UpdatedAt: now,
			},
		},
		{
			name: "empty source of truth rejected",
			truth: ClusterSourceOfTruth{
				UpdatedAt: now,
			},
			wantErr: ErrSourceOfTruthStateRequired,
		},
		{
			name: "updated time required",
			truth: ClusterSourceOfTruth{
				Desired: &cluster.ClusterSpec{
					ClusterName: "alpha",
				},
			},
			wantErr: ErrSourceOfTruthUpdatedAtRequired,
		},
		{
			name: "cluster names must match",
			truth: ClusterSourceOfTruth{
				Desired: &cluster.ClusterSpec{
					ClusterName: "alpha",
				},
				Observed: &cluster.ClusterStatus{
					ClusterName: "beta",
					Phase:       cluster.ClusterPhaseUnknown,
					Maintenance: cluster.MaintenanceModeStatus{},
					ObservedAt:  now,
				},
				UpdatedAt: now,
			},
			wantErr: ErrSourceOfTruthClusterNameMismatch,
		},
		{
			name: "invalid desired state is rejected",
			truth: ClusterSourceOfTruth{
				Desired:   &cluster.ClusterSpec{},
				UpdatedAt: now,
			},
			wantErr: cluster.ErrClusterNameRequired,
		},
		{
			name: "invalid observed state is rejected",
			truth: ClusterSourceOfTruth{
				Observed: &cluster.ClusterStatus{
					ClusterName: "alpha",
				},
				UpdatedAt: now,
			},
			wantErr: cluster.ErrClusterPhaseRequired,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.truth.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestClusterSourceOfTruthCloneCopiesMutableFields(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 25, 10, 30, 0, 0, time.UTC)
	original := ClusterSourceOfTruth{
		Desired: &cluster.ClusterSpec{
			ClusterName: "alpha",
			Generation:  2,
			Members: []cluster.MemberSpec{
				{
					Name: "alpha-1",
					Tags: map[string]any{
						"zone": "a",
					},
				},
			},
		},
		Observed: &cluster.ClusterStatus{
			ClusterName: "alpha",
			Phase:       cluster.ClusterPhaseHealthy,
			Maintenance: cluster.MaintenanceModeStatus{},
			Members: []cluster.MemberStatus{
				{
					Name:       "alpha-1",
					Role:       cluster.MemberRolePrimary,
					State:      cluster.MemberStateRunning,
					Healthy:    true,
					LastSeenAt: now,
					Tags: map[string]any{
						"zone": "a",
					},
				},
			},
			ObservedAt: now,
		},
		UpdatedAt: now,
	}

	clone := original.Clone()
	clone.Desired.Members[0].Tags["zone"] = "mutated"
	clone.Observed.Members[0].Tags["zone"] = "mutated"

	if got := original.Desired.Members[0].Tags["zone"]; got != "a" {
		t.Fatalf("expected desired state clone to be detached, got %v", got)
	}

	if got := original.Observed.Members[0].Tags["zone"]; got != "a" {
		t.Fatalf("expected observed state clone to be detached, got %v", got)
	}
}

func TestClusterSourceOfTruthClusterName(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		truth ClusterSourceOfTruth
		want  string
	}{
		{
			name: "desired name wins",
			truth: ClusterSourceOfTruth{
				Desired:  &cluster.ClusterSpec{ClusterName: "alpha"},
				Observed: &cluster.ClusterStatus{ClusterName: "beta"},
			},
			want: "alpha",
		},
		{
			name: "observed name used when desired missing",
			truth: ClusterSourceOfTruth{
				Observed: &cluster.ClusterStatus{ClusterName: "beta"},
			},
			want: "beta",
		},
		{
			name:  "empty when no state exists",
			truth: ClusterSourceOfTruth{},
			want:  "",
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.truth.ClusterName(); got != testCase.want {
				t.Fatalf("unexpected cluster name: got %q, want %q", got, testCase.want)
			}
		})
	}
}
