package cluster

import (
	"errors"
	"reflect"
	"testing"
)

func TestFailoverModes(t *testing.T) {
	t.Parallel()

	want := []FailoverMode{
		FailoverModeAutomatic,
		FailoverModeManualOnly,
		FailoverModeDisabled,
	}

	got := FailoverModes()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected failover modes: got %v, want %v", got, want)
	}

	got[0] = FailoverModeDisabled

	if second := FailoverModes(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected failover modes copy, got %v, want %v", second, want)
	}
}

func TestFailoverModeValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		mode        FailoverMode
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "automatic", mode: FailoverModeAutomatic, valid: true, zero: false, stringValue: "automatic"},
		{name: "manual only", mode: FailoverModeManualOnly, valid: true, zero: false, stringValue: "manual_only"},
		{name: "disabled", mode: FailoverModeDisabled, valid: true, zero: false, stringValue: "disabled"},
		{name: "zero", mode: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", mode: FailoverMode("degraded"), valid: false, zero: false, stringValue: "degraded"},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.mode.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.mode.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.mode.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestSynchronousModes(t *testing.T) {
	t.Parallel()

	want := []SynchronousMode{
		SynchronousModeDisabled,
		SynchronousModeQuorum,
		SynchronousModeStrict,
	}

	got := SynchronousModes()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected synchronous modes: got %v, want %v", got, want)
	}

	got[0] = SynchronousModeStrict

	if second := SynchronousModes(); !reflect.DeepEqual(second, want) {
		t.Fatalf("expected synchronous modes copy, got %v, want %v", second, want)
	}
}

func TestSynchronousModeValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		mode        SynchronousMode
		valid       bool
		zero        bool
		stringValue string
	}{
		{name: "disabled", mode: SynchronousModeDisabled, valid: true, zero: false, stringValue: "disabled"},
		{name: "quorum", mode: SynchronousModeQuorum, valid: true, zero: false, stringValue: "quorum"},
		{name: "strict", mode: SynchronousModeStrict, valid: true, zero: false, stringValue: "strict"},
		{name: "zero", mode: "", valid: false, zero: true, stringValue: ""},
		{name: "invalid", mode: SynchronousMode("async"), valid: false, zero: false, stringValue: "async"},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.mode.IsValid(); got != testCase.valid {
				t.Fatalf("unexpected validity: got %v, want %v", got, testCase.valid)
			}

			if got := testCase.mode.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.mode.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestClusterSpecValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		spec    ClusterSpec
		wantErr error
	}{
		{
			name: "minimal valid spec",
			spec: ClusterSpec{
				ClusterName: "alpha",
				Generation:  0,
			},
		},
		{
			name: "valid spec with explicit policies",
			spec: ClusterSpec{
				ClusterName: "alpha",
				Generation:  7,
				Maintenance: MaintenanceDesiredState{
					Enabled:       true,
					DefaultReason: "planned switchover",
				},
				Failover: FailoverPolicy{
					Mode:            FailoverModeManualOnly,
					MaximumLagBytes: 1024,
					CheckTimeline:   true,
					RequireQuorum:   true,
					FencingRequired: true,
				},
				Switchover: SwitchoverPolicy{
					AllowScheduled: true,
					RequireSpecificCandidateDuringMaintenance: true,
				},
				Postgres: PostgresPolicy{
					SynchronousMode: SynchronousModeQuorum,
					UsePgRewind:     true,
					Parameters: map[string]any{
						"max_wal_senders": 16,
					},
				},
			},
		},
		{
			name: "cluster name required",
			spec: ClusterSpec{
				ClusterName: "   ",
			},
			wantErr: ErrClusterNameRequired,
		},
		{
			name: "generation must be non-negative",
			spec: ClusterSpec{
				ClusterName: "alpha",
				Generation:  -1,
			},
			wantErr: ErrClusterGenerationNegative,
		},
		{
			name: "failover mode must be valid when set",
			spec: ClusterSpec{
				ClusterName: "alpha",
				Failover: FailoverPolicy{
					Mode: FailoverMode("unsafe"),
				},
			},
			wantErr: ErrInvalidFailoverMode,
		},
		{
			name: "synchronous mode must be valid when set",
			spec: ClusterSpec{
				ClusterName: "alpha",
				Postgres: PostgresPolicy{
					SynchronousMode: SynchronousMode("async"),
				},
			},
			wantErr: ErrInvalidSynchronousMode,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.spec.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestClusterSpecCloneCopiesMutableFields(t *testing.T) {
	t.Parallel()

	original := ClusterSpec{
		ClusterName: "alpha",
		Generation:  3,
		Postgres: PostgresPolicy{
			SynchronousMode: SynchronousModeStrict,
			UsePgRewind:     true,
			Parameters: map[string]any{
				"max_wal_senders": 10,
				"hot_standby":     "on",
			},
		},
	}

	clone := original.Clone()

	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("unexpected clone: got %+v, want %+v", clone, original)
	}

	clone.Postgres.Parameters["max_wal_senders"] = 20
	clone.Postgres.Parameters["shared_buffers"] = "1GB"

	if got, want := original.Postgres.Parameters["max_wal_senders"], 10; got != want {
		t.Fatalf("original parameters were mutated: got %v, want %v", got, want)
	}

	if _, ok := original.Postgres.Parameters["shared_buffers"]; ok {
		t.Fatal("expected clone parameter mutation to stay isolated from original")
	}
}
