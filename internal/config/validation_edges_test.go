package config

import (
	"errors"
	"strings"
	"testing"
)

func TestTLSConfigValidateRejectsHalfConfiguredKeyPairWhenDisabled(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		tls     TLSConfig
		wantErr error
	}{
		{
			name: "cert without key",
			tls: TLSConfig{
				CertFile: "tls/server.crt",
			},
			wantErr: ErrTLSKeyFileRequired,
		},
		{
			name: "key without cert",
			tls: TLSConfig{
				KeyFile: "tls/server.key",
			},
			wantErr: ErrTLSCertFileRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.tls.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestPostgresLocalConfigValidateRejectsUnsafeParameterOverridesAfterNormalization(t *testing.T) {
	t.Parallel()

	config := PostgresLocalConfig{
		DataDir:       "/var/lib/postgresql/data",
		ListenAddress: "127.0.0.1",
		Port:          5432,
		Parameters: map[string]string{
			"  PRIMARY_CONNINFO  ": "host=alpha-1 port=5432",
		},
	}

	err := config.Validate()
	if !errors.Is(err, ErrUnsafeClusterParameterOverride) {
		t.Fatalf("unexpected validation error: got %v, want %v", err, ErrUnsafeClusterParameterOverride)
	}

	if !strings.Contains(err.Error(), "primary_conninfo") {
		t.Fatalf("expected normalized unsafe parameter in error, got %v", err)
	}
}

func TestClusterBootstrapConfigValidateRequiredSections(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		bootstrap ClusterBootstrapConfig
		wantErr   error
	}{
		{
			name: "initial primary required",
			bootstrap: ClusterBootstrapConfig{
				ClusterName:     "alpha",
				SeedAddresses:   []string{"127.0.0.1:9090"},
				ExpectedMembers: []string{"alpha-1"},
			},
			wantErr: ErrBootstrapInitialPrimaryRequired,
		},
		{
			name: "seed address required",
			bootstrap: ClusterBootstrapConfig{
				ClusterName:     "alpha",
				InitialPrimary:  "alpha-1",
				ExpectedMembers: []string{"alpha-1"},
			},
			wantErr: ErrBootstrapSeedAddressRequired,
		},
		{
			name: "expected members required",
			bootstrap: ClusterBootstrapConfig{
				ClusterName:    "alpha",
				InitialPrimary: "alpha-1",
				SeedAddresses:  []string{"127.0.0.1:9090"},
			},
			wantErr: ErrBootstrapExpectedMembersRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.bootstrap.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
