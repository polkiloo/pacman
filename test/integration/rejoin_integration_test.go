//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/test/testenv"
)

type realRejoinScenario struct {
	store          *controlplane.MemoryStateStore
	currentPrimary *testenv.Postgres
	currentEpoch   cluster.Epoch
}

func TestRejoinStrategySelectsRewindAfterRealFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	scenario := prepareRealRejoinScenario(t)

	assessment, err := scenario.store.AssessRejoinMember("alpha-1")
	if err != nil {
		t.Fatalf("assess rejoin member: %v", err)
	}

	if !assessment.Ready || !assessment.FormerPrimary || !assessment.ManagedPostgres || assessment.PostgresUp {
		t.Fatalf("unexpected rejoin assessment flags: %+v", assessment)
	}

	if assessment.Member.Name != "alpha-1" || assessment.CurrentPrimary.Name != "alpha-2" {
		t.Fatalf("unexpected rejoin assessment topology: %+v", assessment)
	}

	if len(assessment.Reasons) != 0 {
		t.Fatalf("expected rejoin-ready former primary with no block reasons, got %+v", assessment.Reasons)
	}

	divergence, err := scenario.store.DetectRejoinDivergence("alpha-1")
	if err != nil {
		t.Fatalf("detect rejoin divergence: %v", err)
	}

	if !divergence.Compared || !divergence.Diverged || !divergence.RequiresRewind || divergence.RequiresReclone {
		t.Fatalf("unexpected rejoin divergence assessment: %+v", divergence)
	}

	if divergence.Member.Name != "alpha-1" || divergence.CurrentPrimary.Name != "alpha-2" {
		t.Fatalf("unexpected rejoin divergence topology: %+v", divergence)
	}

	if divergence.MemberSystemIdentifier == "" || divergence.CurrentPrimarySystemIdentifier == "" {
		t.Fatalf("expected real postgres system identifiers, got %+v", divergence)
	}

	if divergence.MemberSystemIdentifier != divergence.CurrentPrimarySystemIdentifier {
		t.Fatalf("expected rewind path to keep system identifiers aligned, got %+v", divergence)
	}

	if divergence.Member.Timeline >= divergence.CurrentPrimary.Timeline {
		t.Fatalf("expected former primary timeline to lag promoted primary, got %+v", divergence)
	}

	decision, err := scenario.store.DecideRejoinStrategy("alpha-1")
	if err != nil {
		t.Fatalf("decide rejoin strategy: %v", err)
	}

	if !decision.Decided || decision.Strategy != cluster.RejoinStrategyRewind || decision.DirectRejoinPossible {
		t.Fatalf("unexpected rejoin strategy decision: %+v", decision)
	}

	if decision.Member.Name != "alpha-1" || decision.CurrentPrimary.Name != "alpha-2" {
		t.Fatalf("unexpected rejoin strategy topology: %+v", decision)
	}
}

func TestExecuteRejoinRewindKeepsClusterRecoveringWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	scenario := prepareRealRejoinScenario(t)
	rewinder := newContainerPGRewindExecutor(t, scenario.currentPrimary)

	execution, err := scenario.store.ExecuteRejoinRewind(context.Background(), controlplane.RejoinRequest{
		Member:      "alpha-1",
		RequestedBy: "integration-test",
		Reason:      "repair former primary with pg_rewind",
	}, rewinder)
	if err != nil {
		t.Fatalf("execute rejoin rewind: %v", err)
	}

	if !execution.Rewound || execution.State != cluster.RejoinStateRewinding || execution.CurrentEpoch != scenario.currentEpoch {
		t.Fatalf("unexpected rejoin execution: %+v", execution)
	}

	if execution.Operation.Kind != cluster.OperationKindRejoin || execution.Operation.FromMember != "alpha-1" || execution.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected rejoin operation payload: %+v", execution.Operation)
	}

	if execution.Decision.Strategy != cluster.RejoinStrategyRewind {
		t.Fatalf("expected rewind strategy execution, got %+v", execution.Decision)
	}

	status, ok := scenario.store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after rejoin rewind")
	}

	if status.Phase != cluster.ClusterPhaseRecovering || status.CurrentPrimary != "alpha-2" {
		t.Fatalf("unexpected cluster status during rejoin: %+v", status)
	}

	if status.ActiveOperation == nil || status.ActiveOperation.Kind != cluster.OperationKindRejoin || status.ActiveOperation.State != cluster.OperationStateRunning {
		t.Fatalf("expected active rejoin operation in cluster status, got %+v", status.ActiveOperation)
	}

	formerPrimary, ok := scenario.store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status after rewind")
	}

	if formerPrimary.State != cluster.MemberStateNeedsRejoin || !formerPrimary.NeedsRejoin || formerPrimary.Postgres.Up {
		t.Fatalf("expected rewound former primary to remain offline and require rejoin, got %+v", formerPrimary)
	}

	history := scenario.store.History()
	if len(history) != 1 || history[0].Kind != cluster.OperationKindFailover {
		t.Fatalf("expected successful rewind stage to keep only failover history, got %+v", history)
	}

	truth := scenario.store.SourceOfTruth()
	if truth.Observed == nil || truth.Observed.Phase != cluster.ClusterPhaseRecovering {
		t.Fatalf("expected source of truth to reflect recovering phase, got %+v", truth.Observed)
	}

	marker := rewinder.RequireMarker(t)
	if !strings.Contains(marker, "member=alpha-1 primary=alpha-2") {
		t.Fatalf("unexpected pg_rewind marker output: %q", marker)
	}
}

func TestRejoinNegativeCasesWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	scenario := prepareRealRejoinScenario(t)
	rewinder := noOpRewindExecutor{}

	testCases := []struct {
		name    string
		call    func() error
		wantErr error
	}{
		{
			name: "negative assess rejects blank target",
			call: func() error {
				_, err := scenario.store.AssessRejoinMember(" ")
				return err
			},
			wantErr: controlplane.ErrRejoinTargetRequired,
		},
		{
			name: "negative divergence rejects blank target",
			call: func() error {
				_, err := scenario.store.DetectRejoinDivergence(" ")
				return err
			},
			wantErr: controlplane.ErrRejoinTargetRequired,
		},
		{
			name: "negative strategy rejects unknown target",
			call: func() error {
				_, err := scenario.store.DecideRejoinStrategy("alpha-missing")
				return err
			},
			wantErr: controlplane.ErrRejoinTargetUnknown,
		},
		{
			name: "negative rewind rejects blank target",
			call: func() error {
				_, err := scenario.store.ExecuteRejoinRewind(context.Background(), controlplane.RejoinRequest{Member: " "}, rewinder)
				return err
			},
			wantErr: controlplane.ErrRejoinTargetRequired,
		},
		{
			name: "negative rewind requires executor",
			call: func() error {
				_, err := scenario.store.ExecuteRejoinRewind(context.Background(), controlplane.RejoinRequest{Member: "alpha-1"}, nil)
				return err
			},
			wantErr: controlplane.ErrRejoinRewindExecutorRequired,
		},
		{
			name: "negative direct rejoin rejects rewind-required former primary",
			call: func() error {
				_, err := scenario.store.ExecuteRejoinDirect(context.Background(), controlplane.RejoinRequest{Member: "alpha-1"})
				return err
			},
			wantErr: controlplane.ErrRejoinStrategyUndetermined,
		},
		{
			name: "negative verification requires active rejoin operation",
			call: func() error {
				_, err := scenario.store.VerifyRejoinReplication(context.Background())
				return err
			},
			wantErr: controlplane.ErrRejoinExecutionRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected rejoin error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestRejoinAdditionalNegativeCasesWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	scenario := prepareRealRejoinScenario(t)
	rewinder := noOpRewindExecutor{}

	testCases := []struct {
		name    string
		call    func() error
		wantErr error
	}{
		{
			name: "negative assess rejects unknown target",
			call: func() error {
				_, err := scenario.store.AssessRejoinMember("alpha-missing")
				return err
			},
			wantErr: controlplane.ErrRejoinTargetUnknown,
		},
		{
			name: "negative divergence rejects unknown target",
			call: func() error {
				_, err := scenario.store.DetectRejoinDivergence("alpha-missing")
				return err
			},
			wantErr: controlplane.ErrRejoinTargetUnknown,
		},
		{
			name: "negative strategy rejects blank target",
			call: func() error {
				_, err := scenario.store.DecideRejoinStrategy(" ")
				return err
			},
			wantErr: controlplane.ErrRejoinTargetRequired,
		},
		{
			name: "negative rewind rejects unknown target",
			call: func() error {
				_, err := scenario.store.ExecuteRejoinRewind(context.Background(), controlplane.RejoinRequest{Member: "alpha-missing"}, rewinder)
				return err
			},
			wantErr: controlplane.ErrRejoinTargetUnknown,
		},
		{
			name: "negative direct rejoin rejects blank target",
			call: func() error {
				_, err := scenario.store.ExecuteRejoinDirect(context.Background(), controlplane.RejoinRequest{Member: " "})
				return err
			},
			wantErr: controlplane.ErrRejoinTargetRequired,
		},
		{
			name: "negative direct rejoin rejects unknown target",
			call: func() error {
				_, err := scenario.store.ExecuteRejoinDirect(context.Background(), controlplane.RejoinRequest{Member: "alpha-missing"})
				return err
			},
			wantErr: controlplane.ErrRejoinTargetUnknown,
		},
		{
			name: "negative completion requires active rejoin operation",
			call: func() error {
				_, err := scenario.store.CompleteRejoin(context.Background())
				return err
			},
			wantErr: controlplane.ErrRejoinExecutionRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected rejoin error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func prepareRealRejoinScenario(t *testing.T) realRejoinScenario {
	t.Helper()

	formerPrimary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	formerPrimaryObservation := publishObservedNodeStatus(t, store, "alpha-1", formerPrimary, observedAt)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	if formerPrimaryObservation.Role != cluster.MemberRolePrimary || formerPrimaryObservation.InRecovery {
		t.Fatalf("expected real primary observation, got %+v", formerPrimaryObservation)
	}

	if formerPrimaryObservation.Details.Timeline == 0 || formerPrimaryObservation.Details.SystemIdentifier == "" {
		t.Fatalf("expected former primary observation to include timeline and system identifier, got %+v", formerPrimaryObservation)
	}

	formerPrimaryAddress := formerPrimary.Address(t)
	formerPrimary.Stop(t)
	waitForAddressUnavailable(t, formerPrimary.Name(), formerPrimaryAddress)

	publishUnavailableNodeStatus(t, store, "alpha-1", formerPrimaryAddress, observedAt.Add(2*time.Second), formerPrimaryObservation)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(3*time.Second))

	intent, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
		RequestedBy: "integration-test",
		Reason:      "prepare former primary for rejoin integration test",
	})
	if err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	if intent.CurrentPrimary != "alpha-1" || intent.Candidate != "alpha-2" {
		t.Fatalf("unexpected failover intent for rejoin scenario: %+v", intent)
	}

	execution, err := store.ExecuteFailover(context.Background(), newPostgresPromotionExecutor(t, standby), nil)
	if err != nil {
		t.Fatalf("execute failover: %v", err)
	}

	promotedObservation := waitForPromotedPrimaryTimeline(t, standby, formerPrimaryObservation.Details.Timeline)
	if promotedObservation.Role != cluster.MemberRolePrimary || promotedObservation.InRecovery {
		t.Fatalf("expected promoted standby observation, got %+v", promotedObservation)
	}

	if promotedObservation.Details.SystemIdentifier == "" {
		t.Fatalf("expected promoted standby system identifier, got %+v", promotedObservation)
	}

	publishObservedNodeStatusFromObservation(t, store, "alpha-2", standby.Address(t), time.Now().UTC(), promotedObservation)

	execSQL(t, standby, `
CREATE TABLE IF NOT EXISTS rejoin_promoted_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, standby, `
INSERT INTO rejoin_promoted_marker (id, payload)
VALUES (1, 'promoted')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after failover in rejoin scenario")
	}

	if status.CurrentPrimary != "alpha-2" || status.CurrentEpoch != execution.CurrentEpoch {
		t.Fatalf("unexpected cluster status for rejoin scenario: %+v", status)
	}

	formerPrimaryStatus, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status in rejoin scenario")
	}

	if formerPrimaryStatus.State != cluster.MemberStateNeedsRejoin || !formerPrimaryStatus.NeedsRejoin || formerPrimaryStatus.Postgres.Up {
		t.Fatalf("expected former primary to require rejoin after real failover, got %+v", formerPrimaryStatus)
	}

	return realRejoinScenario{
		store:          store,
		currentPrimary: standby,
		currentEpoch:   execution.CurrentEpoch,
	}
}

type noOpRewindExecutor struct{}

func (noOpRewindExecutor) Rewind(context.Context, controlplane.RewindRequest) error {
	return nil
}
