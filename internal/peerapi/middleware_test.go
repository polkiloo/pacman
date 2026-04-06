package peerapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"testing"
)

func TestCurrentPrincipalMissing(t *testing.T) {
	t.Parallel()

	if principal, ok := CurrentPrincipal(context.Background()); ok || principal != nil {
		t.Fatalf("expected missing principal, got %#v ok=%t", principal, ok)
	}
}

func TestResolvePeerPrincipalRejectsNilTLSState(t *testing.T) {
	t.Parallel()

	server := New("alpha-1", nil, Config{})

	if _, err := server.resolvePeerPrincipal(nil); !errors.Is(err, errPeerTLSRequired) {
		t.Fatalf("unexpected error: got %v, want %v", err, errPeerTLSRequired)
	}
}

func TestResolvePeerPrincipalRejectsMissingVerifiedCertificate(t *testing.T) {
	t.Parallel()

	server := New("alpha-1", nil, Config{})

	if _, err := server.resolvePeerPrincipal(&tls.ConnectionState{}); !errors.Is(err, errPeerCertificateRequired) {
		t.Fatalf("unexpected error: got %v, want %v", err, errPeerCertificateRequired)
	}
}

func TestResolvePeerPrincipalRejectsEmptySubject(t *testing.T) {
	t.Parallel()

	server := New("alpha-1", nil, Config{})
	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{}},
		VerifiedChains:   [][]*x509.Certificate{{{}}},
	}

	if _, err := server.resolvePeerPrincipal(state); !errors.Is(err, errPeerSubjectRequired) {
		t.Fatalf("unexpected error: got %v, want %v", err, errPeerSubjectRequired)
	}
}

func TestNormalizeAllowedPeersDropsEmptyDuplicates(t *testing.T) {
	t.Parallel()

	allowed := normalizeAllowedPeers([]string{" beta-1 ", "", "beta-1", "gamma-1"})
	if len(allowed) != 2 {
		t.Fatalf("unexpected allowed peer count: got %d, want %d", len(allowed), 2)
	}

	if _, ok := allowed["beta-1"]; !ok {
		t.Fatal("expected beta-1 to be present")
	}

	if _, ok := allowed["gamma-1"]; !ok {
		t.Fatal("expected gamma-1 to be present")
	}
}

func TestPeerSubjectRejectsNilCertificate(t *testing.T) {
	t.Parallel()

	if got := peerSubject(nil); got != "" {
		t.Fatalf("unexpected peer subject: got %q", got)
	}
}
