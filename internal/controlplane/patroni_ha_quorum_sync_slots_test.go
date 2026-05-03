package controlplane

import (
	"context"
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestPatroniInspiredHALoopFailoverWithWitnessQuorumKeepsSyncPolicy(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 1, 13, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:          cluster.FailoverModeAutomatic,
			RequireQuorum: true,
		},
		Postgres: cluster.PostgresPolicy{
			SynchronousMode: cluster.SynchronousModeQuorum,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
			{Name: "witness-1"},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now.Add(-time.Second), false, 31, 0),
		readyStandbyStatus("alpha-2", now, 31, 0),
		failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })

	confirmation, err := store.ConfirmPrimaryFailure()
	if err != nil {
		t.Fatalf("confirm primary failure: %v", err)
	}
	if !confirmation.Confirmed || !confirmation.QuorumReachable {
		t.Fatalf("expected witness-assisted quorum confirmation, got %+v", confirmation)
	}
	if confirmation.ReachableVoters != 2 || confirmation.RequiredVoters != 2 || confirmation.TotalVoters != 3 {
		t.Fatalf("unexpected quorum counts: %+v", confirmation)
	}

	intent, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{
		RequestedBy: "pacmand",
		Reason:      "ha loop detected failed primary",
	})
	if err != nil {
		t.Fatalf("create failover intent: %v", err)
	}
	if intent.Candidate != "alpha-2" {
		t.Fatalf("unexpected failover candidate: %+v", intent)
	}

	promoter := &recordingPromoter{}
	execution, err := store.ExecuteFailover(context.Background(), promoter, nil)
	if err != nil {
		t.Fatalf("execute failover: %v", err)
	}
	if execution.Candidate != "alpha-2" || execution.CurrentPrimary != "alpha-1" {
		t.Fatalf("unexpected failover execution members: %+v", execution)
	}
	if len(promoter.requests) != 1 || promoter.requests[0].Candidate != "alpha-2" {
		t.Fatalf("unexpected promotion requests: %+v", promoter.requests)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after failover")
	}
	if status.CurrentPrimary != "alpha-2" || status.Phase != cluster.ClusterPhaseDegraded {
		t.Fatalf("unexpected post-failover status: %+v", status)
	}

	formerPrimary, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status")
	}
	if formerPrimary.Role != cluster.MemberRoleReplica || !formerPrimary.NeedsRejoin {
		t.Fatalf("expected former primary to be demoted into rejoin workflow, got %+v", formerPrimary)
	}

	storedSpec, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected stored cluster spec")
	}
	if storedSpec.Postgres.SynchronousMode != cluster.SynchronousModeQuorum {
		t.Fatalf("expected synchronous quorum policy to remain stored, got %+v", storedSpec.Postgres)
	}
}

func TestPatroniInspiredQuorumVotesUseDesiredMembershipBeforeObservedFallback(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.May, 1, 13, 30, 0, 0, time.UTC)
	tests := []struct {
		name           string
		members        []cluster.MemberSpec
		statuses       []agentmodel.NodeStatus
		wantConfirmed  bool
		wantReachable  bool
		wantReachableN int
		wantRequiredN  int
		wantTotalN     int
	}{
		{
			name: "unexpected observed voter is ignored when desired membership exists",
			members: []cluster.MemberSpec{
				{Name: "alpha-1"},
				{Name: "alpha-2"},
				{Name: "witness-1"},
			},
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 44, 0),
				readyStandbyStatus("alpha-2", now, 44, 0),
				readyStandbyStatus("rogue-1", now, 44, 0),
			},
			wantConfirmed:  false,
			wantReachable:  false,
			wantReachableN: 1,
			wantRequiredN:  2,
			wantTotalN:     3,
		},
		{
			name:    "observed membership is used only when desired members are absent",
			members: nil,
			statuses: []agentmodel.NodeStatus{
				failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 45, 0),
				readyStandbyStatus("alpha-2", now, 45, 0),
				failoverNodeStatus("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, now, false, 0, 0),
			},
			wantConfirmed:  true,
			wantReachable:  true,
			wantReachableN: 2,
			wantRequiredN:  2,
			wantTotalN:     3,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode:          cluster.FailoverModeAutomatic,
					RequireQuorum: true,
				},
				Members: testCase.members,
			}, testCase.statuses)

			confirmation, err := store.ConfirmPrimaryFailure()
			if err != nil {
				t.Fatalf("confirm primary failure: %v", err)
			}
			if confirmation.Confirmed != testCase.wantConfirmed || confirmation.QuorumReachable != testCase.wantReachable {
				t.Fatalf("unexpected quorum confirmation: got %+v", confirmation)
			}
			if confirmation.ReachableVoters != testCase.wantReachableN ||
				confirmation.RequiredVoters != testCase.wantRequiredN ||
				confirmation.TotalVoters != testCase.wantTotalN {
				t.Fatalf("unexpected quorum counts: got %+v", confirmation)
			}
		})
	}
}

func TestPatroniInspiredSynchronousReplicationPolicyPersistsAsDesiredPostgresState(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	stored, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Postgres: cluster.PostgresPolicy{
			SynchronousMode: cluster.SynchronousModeStrict,
			Parameters: map[string]any{
				"synchronous_standby_names": "FIRST 1 (alpha-2)",
			},
		},
	})
	if err != nil {
		t.Fatalf("store synchronous replication policy: %v", err)
	}
	if stored.Postgres.SynchronousMode != cluster.SynchronousModeStrict {
		t.Fatalf("unexpected stored synchronous mode: %+v", stored.Postgres)
	}

	cloned, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected cluster spec")
	}
	cloned.Postgres.Parameters["synchronous_standby_names"] = "mutated"

	again, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected cluster spec after clone mutation")
	}
	if got := again.Postgres.Parameters["synchronous_standby_names"]; got != "FIRST 1 (alpha-2)" {
		t.Fatalf("expected synchronous standby names to be clone-safe, got %q", got)
	}
}

func TestPatroniInspiredSlotManagementNormalizesAndRendersRejoinSlotNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		member string
		want   string
	}{
		{member: "Alpha 1", want: "alpha_1"},
		{member: "test-3", want: "test_3"},
		{member: "test.3", want: "test_3"},
		{member: "node__a", want: "node_a"},
		{member: strings.Repeat("a", 62) + "-b", want: strings.Repeat("a", 62)},
	}

	for _, testCase := range tests {
		t.Run(testCase.member, func(t *testing.T) {
			t.Parallel()

			if got := rejoinPrimarySlotName(testCase.member); got != testCase.want {
				t.Fatalf("unexpected slot name: got %q, want %q", got, testCase.want)
			}
		})
	}

	standby, err := buildRejoinStandbyConfig(cluster.ClusterSpec{}, "Alpha's standby", "primary.example.com:5432")
	if err != nil {
		t.Fatalf("build standby config: %v", err)
	}
	rendered, err := postgres.RenderStandbyFiles("/pgdata", standby)
	if err != nil {
		t.Fatalf("render standby files: %v", err)
	}

	for _, want := range []string{
		"primary_conninfo = 'host=primary.example.com port=5432 application_name=Alpha''s standby'",
		"primary_slot_name = 'alpha_s_standby'",
		"recovery_target_timeline = 'latest'",
	} {
		if !strings.Contains(rendered.PostgresAutoConf, want) {
			t.Fatalf("rendered standby config %q does not contain %q", rendered.PostgresAutoConf, want)
		}
	}
}
