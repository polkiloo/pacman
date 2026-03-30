package cluster

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMemberStates(t *testing.T) {
	t.Parallel()

	want := []MemberState{
		MemberStateRunning,
		MemberStateStreaming,
		MemberStateStarting,
		MemberStateStopping,
		MemberStateFailed,
		MemberStateUnreachable,
		MemberStateNeedsRejoin,
		MemberStateUnknown,
	}

	got := MemberStates()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected member states: got %v, want %v", got, want)
	}

	got[0] = MemberStateUnknown

	if second := MemberStates(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected member states copy, got %v, want %v", second, want)
	}
}

func TestMemberStateValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		state       MemberState
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "running", state: MemberStateRunning, valid: true, zero: false, stringValue: "running"},
		{name: "streaming", state: MemberStateStreaming, valid: true, zero: false, stringValue: "streaming"},
		{name: "starting", state: MemberStateStarting, valid: true, zero: false, stringValue: "starting"},
		{name: "stopping", state: MemberStateStopping, valid: true, zero: false, stringValue: "stopping"},
		{name: "failed", state: MemberStateFailed, valid: true, zero: false, stringValue: "failed"},
		{name: "unreachable", state: MemberStateUnreachable, valid: true, zero: false, stringValue: "unreachable"},
		{name: "needs rejoin", state: MemberStateNeedsRejoin, valid: true, zero: false, stringValue: "needs_rejoin"},
		{name: "unknown", state: MemberStateUnknown, valid: true, zero: false, stringValue: "unknown"},
		{name: "zero", state: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", state: MemberState("paused"), valid: false, zero: false, stringValue: "paused"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.state.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.state.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.state.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestMemberSpecValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		spec    MemberSpec
		wantErr error
	}{
		{
			name: "valid member spec",
			spec: MemberSpec{
				Name:       "alpha-1",
				Priority:   10,
				NoFailover: true,
				Tags: map[string]any{
					"zone": "a",
				},
			},
		},
		{
			name: "member name required",
			spec: MemberSpec{
				Name: "   ",
			},
			wantErr: ErrMemberNameRequired,
		},
		{
			name: "priority must be non-negative",
			spec: MemberSpec{
				Name:     "alpha-1",
				Priority: -1,
			},
			wantErr: ErrMemberPriorityNegative,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.spec.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemberSpecCloneCopiesMutableFields(t *testing.T) {
	t.Parallel()

	original := MemberSpec{
		Name:       "alpha-1",
		Priority:   10,
		NoFailover: true,
		Tags: map[string]any{
			"zone": "a",
		},
	}

	clone := original.Clone()

	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("unexpected clone: got %+v, want %+v", clone, original)
	}

	clone.Tags["zone"] = "b"
	clone.Tags["rack"] = "r1"

	if got, want := original.Tags["zone"], "a"; got != want {
		t.Fatalf("original tags were mutated: got %v, want %v", got, want)
	}

	if _, ok := original.Tags["rack"]; ok {
		t.Fatal("expected clone tag mutation to stay isolated from original")
	}
}

func TestMemberStatusValidate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name    string
		status  MemberStatus
		wantErr error
	}{
		{
			name: "valid member status",
			status: MemberStatus{
				Name:       "alpha-1",
				APIURL:     "http://alpha-1.internal:8080",
				Host:       "alpha-1.internal",
				Port:       8080,
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				Healthy:    true,
				Leader:     true,
				Timeline:   3,
				LagBytes:   0,
				Priority:   100,
				LastSeenAt: now,
				Tags: map[string]any{
					"zone": "a",
				},
			},
		},
		{
			name: "member name required",
			status: MemberStatus{
				Name:       "   ",
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				LastSeenAt: now,
			},
			wantErr: ErrMemberNameRequired,
		},
		{
			name: "api url must be valid",
			status: MemberStatus{
				Name:       "alpha-1",
				APIURL:     "://invalid",
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				LastSeenAt: now,
			},
			wantErr: ErrMemberAPIURLInvalid,
		},
		{
			name: "port must be in range when set",
			status: MemberStatus{
				Name:       "alpha-1",
				Port:       70000,
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				LastSeenAt: now,
			},
			wantErr: ErrMemberPortOutOfRange,
		},
		{
			name: "role required",
			status: MemberStatus{
				Name:       "alpha-1",
				State:      MemberStateRunning,
				LastSeenAt: now,
			},
			wantErr: ErrMemberRoleRequired,
		},
		{
			name: "role must be valid",
			status: MemberStatus{
				Name:       "alpha-1",
				Role:       MemberRole("archive"),
				State:      MemberStateRunning,
				LastSeenAt: now,
			},
			wantErr: ErrInvalidMemberRole,
		},
		{
			name: "state required",
			status: MemberStatus{
				Name:       "alpha-1",
				Role:       MemberRolePrimary,
				LastSeenAt: now,
			},
			wantErr: ErrMemberStateRequired,
		},
		{
			name: "state must be valid",
			status: MemberStatus{
				Name:       "alpha-1",
				Role:       MemberRolePrimary,
				State:      MemberState("paused"),
				LastSeenAt: now,
			},
			wantErr: ErrInvalidMemberState,
		},
		{
			name: "timeline must be non-negative",
			status: MemberStatus{
				Name:       "alpha-1",
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				Timeline:   -1,
				LastSeenAt: now,
			},
			wantErr: ErrMemberTimelineNegative,
		},
		{
			name: "lag must be non-negative",
			status: MemberStatus{
				Name:       "alpha-1",
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				LagBytes:   -1,
				LastSeenAt: now,
			},
			wantErr: ErrMemberLagNegative,
		},
		{
			name: "priority must be non-negative",
			status: MemberStatus{
				Name:       "alpha-1",
				Role:       MemberRolePrimary,
				State:      MemberStateRunning,
				Priority:   -1,
				LastSeenAt: now,
			},
			wantErr: ErrMemberPriorityNegative,
		},
		{
			name: "last seen time required",
			status: MemberStatus{
				Name:  "alpha-1",
				Role:  MemberRolePrimary,
				State: MemberStateRunning,
			},
			wantErr: ErrMemberLastSeenAtRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.status.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemberStatusCloneCopiesMutableFields(t *testing.T) {
	t.Parallel()

	original := MemberStatus{
		Name:       "alpha-1",
		Role:       MemberRoleReplica,
		State:      MemberStateStreaming,
		Healthy:    true,
		Timeline:   7,
		LagBytes:   128,
		Priority:   5,
		LastSeenAt: time.Now().UTC(),
		Tags: map[string]any{
			"zone": "a",
		},
	}

	clone := original.Clone()

	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("unexpected clone: got %+v, want %+v", clone, original)
	}

	clone.Tags["zone"] = "b"
	clone.Tags["rack"] = "r1"

	if got, want := original.Tags["zone"], "a"; got != want {
		t.Fatalf("original tags were mutated: got %v, want %v", got, want)
	}

	if _, ok := original.Tags["rack"]; ok {
		t.Fatal("expected clone tag mutation to stay isolated from original")
	}
}
