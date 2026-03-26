package controlplane

import (
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemberRegistrationValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		registration MemberRegistration
		wantErr      error
	}{
		{
			name: "valid registration",
			registration: MemberRegistration{
				NodeName:       "alpha-1",
				NodeRole:       cluster.NodeRoleData,
				APIAddress:     "10.0.0.10:8080",
				ControlAddress: "10.0.0.10:9090",
				RegisteredAt:   time.Date(2026, time.March, 27, 10, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "registered at required",
			registration: MemberRegistration{
				NodeName:       "alpha-1",
				NodeRole:       cluster.NodeRoleData,
				APIAddress:     "10.0.0.10:8080",
				ControlAddress: "10.0.0.10:9090",
			},
			wantErr: ErrMemberRegistrationTimeRequired,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.registration.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemberAddressHelpers(t *testing.T) {
	t.Parallel()

	if got := memberAPIURL(""); got != "" {
		t.Fatalf("expected empty api url, got %q", got)
	}

	if got := memberAPIURL("10.0.0.10:8080"); got != "http://10.0.0.10:8080" {
		t.Fatalf("unexpected api url: got %q", got)
	}

	host, port := memberEndpoint("10.0.0.10:8080")
	if host != "10.0.0.10" || port != 8080 {
		t.Fatalf("unexpected parsed endpoint: got %s:%d", host, port)
	}

	host, port = memberEndpoint("broken")
	if host != "" || port != 0 {
		t.Fatalf("expected invalid endpoint split to return zero values, got %s:%d", host, port)
	}

	host, port = memberEndpoint("10.0.0.10:http")
	if host != "" || port != 0 {
		t.Fatalf("expected invalid endpoint port to return zero values, got %s:%d", host, port)
	}
}
