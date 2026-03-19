package cluster

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestClusterPhases(t *testing.T) {
	t.Parallel()

	want := []ClusterPhase{
		ClusterPhaseInitializing,
		ClusterPhaseHealthy,
		ClusterPhaseDegraded,
		ClusterPhaseFailingOver,
		ClusterPhaseSwitchingOver,
		ClusterPhaseMaintenance,
		ClusterPhaseRecovering,
		ClusterPhaseUnknown,
	}

	got := ClusterPhases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected cluster phases: got %v, want %v", got, want)
	}

	got[0] = ClusterPhaseUnknown

	if second := ClusterPhases(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected cluster phases copy, got %v, want %v", second, want)
	}
}

func TestClusterPhaseValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		phase       ClusterPhase
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "initializing", phase: ClusterPhaseInitializing, valid: true, zero: false, stringValue: "initializing"},
		{name: "healthy", phase: ClusterPhaseHealthy, valid: true, zero: false, stringValue: "healthy"},
		{name: "degraded", phase: ClusterPhaseDegraded, valid: true, zero: false, stringValue: "degraded"},
		{name: "failing over", phase: ClusterPhaseFailingOver, valid: true, zero: false, stringValue: "failing_over"},
		{name: "switching over", phase: ClusterPhaseSwitchingOver, valid: true, zero: false, stringValue: "switching_over"},
		{name: "maintenance", phase: ClusterPhaseMaintenance, valid: true, zero: false, stringValue: "maintenance"},
		{name: "recovering", phase: ClusterPhaseRecovering, valid: true, zero: false, stringValue: "recovering"},
		{name: "unknown", phase: ClusterPhaseUnknown, valid: true, zero: false, stringValue: "unknown"},
		{name: "zero", phase: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", phase: ClusterPhase("paused"), valid: false, zero: false, stringValue: "paused"},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.phase.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.phase.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.phase.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestClusterStatusValidate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name    string
		status  ClusterStatus
		wantErr error
	}{
		{
			name: "valid status",
			status: ClusterStatus{
				ClusterName:    "alpha",
				Phase:          ClusterPhaseHealthy,
				CurrentPrimary: "alpha-1",
				CurrentEpoch:   42,
				Maintenance: MaintenanceModeStatus{
					Enabled:     false,
					Reason:      "",
					RequestedBy: "",
					UpdatedAt:   now,
				},
				Members: []MemberStatus{
					{
						Name:       "alpha-1",
						Role:       MemberRolePrimary,
						State:      MemberStateRunning,
						Healthy:    true,
						LastSeenAt: now,
					},
				},
				ObservedAt: now,
			},
		},
		{
			name: "cluster name required",
			status: ClusterStatus{
				ClusterName: "   ",
				Phase:       ClusterPhaseHealthy,
				ObservedAt:  now,
			},
			wantErr: ErrClusterNameRequired,
		},
		{
			name: "phase required",
			status: ClusterStatus{
				ClusterName: "alpha",
				ObservedAt:  now,
			},
			wantErr: ErrClusterPhaseRequired,
		},
		{
			name: "phase must be valid",
			status: ClusterStatus{
				ClusterName: "alpha",
				Phase:       ClusterPhase("paused"),
				ObservedAt:  now,
			},
			wantErr: ErrInvalidClusterPhase,
		},
		{
			name: "epoch must be non-negative",
			status: ClusterStatus{
				ClusterName:  "alpha",
				Phase:        ClusterPhaseHealthy,
				CurrentEpoch: -1,
				ObservedAt:   now,
			},
			wantErr: ErrClusterEpochNegative,
		},
		{
			name: "observed time required",
			status: ClusterStatus{
				ClusterName: "alpha",
				Phase:       ClusterPhaseHealthy,
			},
			wantErr: ErrClusterObservedAtRequired,
		},
		{
			name: "member status must be valid when set",
			status: ClusterStatus{
				ClusterName: "alpha",
				Phase:       ClusterPhaseHealthy,
				ObservedAt:  now,
				Members: []MemberStatus{
					{
						Name:       "alpha-1",
						Role:       MemberRolePrimary,
						State:      MemberStateRunning,
						Priority:   -1,
						LastSeenAt: now,
					},
				},
			},
			wantErr: ErrMemberPriorityNegative,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.status.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestClusterStatusCloneCopiesMutableFields(t *testing.T) {
	t.Parallel()

	original := ClusterStatus{
		ClusterName:    "alpha",
		Phase:          ClusterPhaseHealthy,
		CurrentPrimary: "alpha-1",
		CurrentEpoch:   7,
		Maintenance: MaintenanceModeStatus{
			Enabled:     true,
			Reason:      "planned maintenance",
			RequestedBy: "operator",
			UpdatedAt:   time.Now().UTC(),
		},
		Members: []MemberStatus{
			{
				Name:       "alpha-1",
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				Healthy:    true,
				LastSeenAt: time.Now().UTC(),
				Tags: map[string]any{
					"zone": "a",
				},
			},
		},
		ObservedAt: time.Now().UTC(),
	}

	clone := original.Clone()

	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("unexpected clone: got %+v, want %+v", clone, original)
	}

	clone.Members[0].Tags["zone"] = "b"
	clone.Members[0].Tags["rack"] = "r1"

	if got, want := original.Members[0].Tags["zone"], "a"; got != want {
		t.Fatalf("original member tags were mutated: got %v, want %v", got, want)
	}

	if _, ok := original.Members[0].Tags["rack"]; ok {
		t.Fatal("expected clone member tag mutation to stay isolated from original")
	}
}
