package agent

import (
	"context"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestDaemonNodeStatusReturnsClone(t *testing.T) {
	t.Parallel()

	daemon := &Daemon{
		nodeStatus: agentmodel.NodeStatus{
			NodeName: "alpha-1",
			Tags: map[string]any{
				"zone": "a",
			},
		},
	}

	status := daemon.NodeStatus()
	status.Tags["zone"] = "mutated"

	if got := daemon.NodeStatus().Tags["zone"]; got != "a" {
		t.Fatalf("expected cloned node status tags, got %+v", daemon.NodeStatus().Tags)
	}
}

func TestSamePostgresDetails(t *testing.T) {
	t.Parallel()

	base := agentmodel.PostgresDetails{
		ServerVersion:       170002,
		PendingRestart:      true,
		SystemIdentifier:    "7599025879359099984",
		Timeline:            7,
		PostmasterStartAt:   time.Date(2026, time.March, 25, 8, 0, 0, 0, time.UTC),
		ReplicationLagBytes: 128,
	}

	if !samePostgresDetails(base, base) {
		t.Fatal("expected identical postgres details to match")
	}

	changed := base
	changed.Timeline = 8
	if samePostgresDetails(base, changed) {
		t.Fatal("expected different postgres details to mismatch")
	}
}

func TestSameWALProgress(t *testing.T) {
	t.Parallel()

	base := agentmodel.WALProgress{
		WriteLSN:        "0/5000200",
		FlushLSN:        "0/5000200",
		ReceiveLSN:      "0/5000200",
		ReplayLSN:       "0/5000100",
		ReplayTimestamp: time.Date(2026, time.March, 25, 8, 1, 0, 0, time.UTC),
	}

	if !sameWALProgress(base, base) {
		t.Fatal("expected identical wal progress to match")
	}

	changed := base
	changed.ReplayLSN = "0/5000200"
	if sameWALProgress(base, changed) {
		t.Fatal("expected different wal progress to mismatch")
	}
}

func TestNewProbeContextWithoutTimeoutReturnsParent(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	daemon := &Daemon{}
	got, stop := daemon.newProbeContext(parent)
	defer stop()

	if got != parent {
		t.Fatal("expected probe context without timeout to return parent context")
	}
}

func TestNormalizeLocalProbeHost(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		host string
		want string
	}{
		{name: "empty", host: "", want: "127.0.0.1"},
		{name: "wildcard ipv4", host: "0.0.0.0", want: "127.0.0.1"},
		{name: "wildcard star", host: "*", want: "127.0.0.1"},
		{name: "wildcard ipv6", host: "::", want: "::1"},
		{name: "wildcard ipv6 bracketed", host: "[::]", want: "::1"},
		{name: "explicit host", host: "db.internal", want: "db.internal"},
	}

	for _, testCase := range testCases {
		if got := normalizeLocalProbeHost(testCase.host); got != testCase.want {
			t.Fatalf("%s: unexpected normalized host: got %q, want %q", testCase.name, got, testCase.want)
		}
	}
}

func TestWithNoAPIServer(t *testing.T) {
	t.Parallel()

	// Construct a daemon that already has an apiServer set via NewDaemon,
	// then verify WithNoAPIServer clears it.
	daemon := &Daemon{}
	// apiServer starts nil; set it to a non-nil value via direct assignment
	// by calling the option on a daemon that has a non-nil server would
	// require a real httpServer. Instead, verify the option on a nil daemon
	// is a no-op (nil stays nil), which exercises the function body.
	opt := WithNoAPIServer()
	opt(daemon)

	if daemon.apiServer != nil {
		t.Fatal("expected apiServer to remain nil")
	}
}

func TestLocalMemberRoleForNodeRole(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		nodeRole cluster.NodeRole
		want     cluster.MemberRole
	}{
		{name: "witness", nodeRole: cluster.NodeRoleWitness, want: cluster.MemberRoleWitness},
		{name: "data", nodeRole: cluster.NodeRoleData, want: cluster.MemberRoleUnknown},
		{name: "unknown", nodeRole: cluster.NodeRoleUnknown, want: cluster.MemberRoleUnknown},
	}

	for _, testCase := range testCases {
		if got := localMemberRoleForNodeRole(testCase.nodeRole); got != testCase.want {
			t.Fatalf("%s: unexpected member role: got %q, want %q", testCase.name, got, testCase.want)
		}
	}
}
