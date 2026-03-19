package cluster

import (
	"errors"
	"testing"
	"time"
)

func TestMaintenanceDesiredStateEffectiveReason(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		state      MaintenanceDesiredState
		reason     string
		wantReason string
	}{
		{
			name: "explicit reason wins",
			state: MaintenanceDesiredState{
				Enabled:       true,
				DefaultReason: "planned work",
			},
			reason:     "operator request",
			wantReason: "operator request",
		},
		{
			name: "default reason used when explicit reason missing",
			state: MaintenanceDesiredState{
				Enabled:       true,
				DefaultReason: "planned work",
			},
			reason:     "   ",
			wantReason: "planned work",
		},
		{
			name: "empty when neither reason exists",
			state: MaintenanceDesiredState{
				Enabled: false,
			},
			wantReason: "",
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if err := testCase.state.Validate(); err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}

			if got := testCase.state.EffectiveReason(testCase.reason); got != testCase.wantReason {
				t.Fatalf("unexpected effective reason: got %q, want %q", got, testCase.wantReason)
			}
		})
	}
}

func TestMaintenanceModeUpdateRequestEffectiveReason(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		request       MaintenanceModeUpdateRequest
		defaultReason string
		wantReason    string
	}{
		{
			name: "request reason wins",
			request: MaintenanceModeUpdateRequest{
				Enabled: true,
				Reason:  "node migration",
			},
			defaultReason: "planned work",
			wantReason:    "node migration",
		},
		{
			name: "default reason used when request reason missing",
			request: MaintenanceModeUpdateRequest{
				Enabled: true,
			},
			defaultReason: "planned work",
			wantReason:    "planned work",
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if err := testCase.request.Validate(); err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}

			if got := testCase.request.EffectiveReason(testCase.defaultReason); got != testCase.wantReason {
				t.Fatalf("unexpected effective reason: got %q, want %q", got, testCase.wantReason)
			}
		})
	}
}

func TestMaintenanceModeStatusValidate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	testCases := []struct {
		name    string
		status  MaintenanceModeStatus
		wantErr error
	}{
		{
			name: "zero-value disabled maintenance is valid",
			status: MaintenanceModeStatus{
				Enabled: false,
			},
		},
		{
			name: "enabled maintenance requires update time",
			status: MaintenanceModeStatus{
				Enabled: true,
				Reason:  "planned work",
			},
			wantErr: ErrMaintenanceUpdatedAtRequired,
		},
		{
			name: "metadata requires update time",
			status: MaintenanceModeStatus{
				Enabled:     false,
				RequestedBy: "operator",
			},
			wantErr: ErrMaintenanceUpdatedAtRequired,
		},
		{
			name: "valid enabled maintenance",
			status: MaintenanceModeStatus{
				Enabled:     true,
				Reason:      "planned work",
				RequestedBy: "operator",
				UpdatedAt:   now,
			},
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
