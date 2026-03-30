package cluster

import (
	"errors"
	"testing"
)

func TestGenerationValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		generation  Generation
		wantErr     error
		zero        bool
		stringValue string
	}{
		{name: "zero generation", generation: 0, zero: true, stringValue: "0"},
		{name: "non-zero generation", generation: 7, zero: false, stringValue: "7"},
		{name: "negative generation", generation: -1, wantErr: ErrClusterGenerationNegative, zero: false, stringValue: "-1"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.generation.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}

			if got := testCase.generation.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.generation.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}

func TestEpochValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		epoch       Epoch
		wantErr     error
		zero        bool
		stringValue string
	}{
		{name: "zero epoch", epoch: 0, zero: true, stringValue: "0"},
		{name: "non-zero epoch", epoch: 42, zero: false, stringValue: "42"},
		{name: "negative epoch", epoch: -1, wantErr: ErrClusterEpochNegative, zero: false, stringValue: "-1"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.epoch.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}

			if got := testCase.epoch.IsZero(); got != testCase.zero {
				t.Fatalf("unexpected zero result: got %v, want %v", got, testCase.zero)
			}

			if got := testCase.epoch.String(); got != testCase.stringValue {
				t.Fatalf("unexpected string value: got %q, want %q", got, testCase.stringValue)
			}
		})
	}
}
