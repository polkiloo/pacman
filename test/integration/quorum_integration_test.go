//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	pgobs "github.com/polkiloo/pacman/internal/postgres"
	"github.com/polkiloo/pacman/test/testenv"
)

type quorumNodeState string

const (
	quorumNodeMissing quorumNodeState = "missing"
	quorumNodeHealthy quorumNodeState = "healthy"
	quorumNodeFailed  quorumNodeState = "failed"
)

func TestConfirmPrimaryFailureConfiguredQuorumMatrixWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	primaryObservation := waitForObservation(t, primary, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRolePrimary && !observation.InRecovery
	})
	standbyObservation := waitForObservation(t, standby, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRoleReplica && observation.InRecovery
	})

	states := []quorumNodeState{quorumNodeMissing, quorumNodeHealthy, quorumNodeFailed}
	for _, primaryState := range states {
		for _, standbyState := range states {
			for _, witnessState := range states {
				testName := string(primaryState) + "-" + string(standbyState) + "-" + string(witnessState)
				t.Run(testName, func(t *testing.T) {
					store := seededRealStore(t, cluster.ClusterSpec{
						ClusterName: "alpha",
						Failover: cluster.FailoverPolicy{
							Mode:          cluster.FailoverModeAutomatic,
							RequireQuorum: true,
						},
						Members: []cluster.MemberSpec{
							{Name: "alpha-1"},
							{Name: "alpha-2"},
							{Name: "witness-1"},
						},
					})

					publishRealQuorumScenario(t, store, primary, standby, primaryObservation, standbyObservation, primaryState, standbyState, witnessState)

					confirmation, err := store.ConfirmPrimaryFailure()
					if err != nil {
						t.Fatalf("confirm primary failure: %v", err)
					}

					reachableVoters := countHealthyStates(primaryState, standbyState, witnessState)
					expectedCurrentPrimary := expectedPrimaryName(primaryState)
					expectedPrimaryHealthy := primaryState == quorumNodeHealthy
					expectedQuorumReachable := reachableVoters >= 2
					expectedConfirmed := expectedCurrentPrimary != "" && !expectedPrimaryHealthy && expectedQuorumReachable

					if confirmation.CurrentPrimary != expectedCurrentPrimary {
						t.Fatalf("unexpected current primary for %s: got %q want %q", testName, confirmation.CurrentPrimary, expectedCurrentPrimary)
					}

					if confirmation.PrimaryHealthy != expectedPrimaryHealthy {
						t.Fatalf("unexpected primary health for %s: got %v want %v", testName, confirmation.PrimaryHealthy, expectedPrimaryHealthy)
					}

					if !confirmation.QuorumRequired || confirmation.TotalVoters != 3 || confirmation.RequiredVoters != 2 || confirmation.ReachableVoters != reachableVoters {
						t.Fatalf("unexpected quorum counts for %s: %+v", testName, confirmation)
					}

					if confirmation.QuorumReachable != expectedQuorumReachable || confirmation.Confirmed != expectedConfirmed {
						t.Fatalf("unexpected confirmation result for %s: %+v", testName, confirmation)
					}
				})
			}
		}
	}
}

func TestConfirmPrimaryFailureObservedQuorumMatrixWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	primaryObservation := waitForObservation(t, primary, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRolePrimary && !observation.InRecovery
	})
	standbyObservation := waitForObservation(t, standby, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRoleReplica && observation.InRecovery
	})

	states := []quorumNodeState{quorumNodeMissing, quorumNodeHealthy, quorumNodeFailed}
	for _, primaryState := range states {
		for _, standbyState := range states {
			for _, witnessState := range states {
				testName := string(primaryState) + "-" + string(standbyState) + "-" + string(witnessState)
				t.Run(testName, func(t *testing.T) {
					store := seededRealStore(t, cluster.ClusterSpec{
						ClusterName: "alpha",
						Failover: cluster.FailoverPolicy{
							Mode:          cluster.FailoverModeAutomatic,
							RequireQuorum: true,
						},
					})

					publishRealQuorumScenario(t, store, primary, standby, primaryObservation, standbyObservation, primaryState, standbyState, witnessState)

					confirmation, err := store.ConfirmPrimaryFailure()
					if err != nil {
						t.Fatalf("confirm primary failure: %v", err)
					}

					totalVoters := countPresentStates(primaryState, standbyState, witnessState)
					reachableVoters := countHealthyStates(primaryState, standbyState, witnessState)
					requiredVoters := 0
					if totalVoters > 0 {
						requiredVoters = totalVoters/2 + 1
					}

					expectedCurrentPrimary := expectedPrimaryName(primaryState)
					expectedPrimaryHealthy := primaryState == quorumNodeHealthy
					expectedQuorumReachable := requiredVoters == 0 || reachableVoters >= requiredVoters
					expectedConfirmed := expectedCurrentPrimary != "" && !expectedPrimaryHealthy && expectedQuorumReachable

					if confirmation.CurrentPrimary != expectedCurrentPrimary {
						t.Fatalf("unexpected current primary for %s: got %q want %q", testName, confirmation.CurrentPrimary, expectedCurrentPrimary)
					}

					if confirmation.PrimaryHealthy != expectedPrimaryHealthy {
						t.Fatalf("unexpected primary health for %s: got %v want %v", testName, confirmation.PrimaryHealthy, expectedPrimaryHealthy)
					}

					if !confirmation.QuorumRequired || confirmation.TotalVoters != totalVoters || confirmation.RequiredVoters != requiredVoters || confirmation.ReachableVoters != reachableVoters {
						t.Fatalf("unexpected quorum counts for %s: %+v", testName, confirmation)
					}

					if confirmation.QuorumReachable != expectedQuorumReachable || confirmation.Confirmed != expectedConfirmed {
						t.Fatalf("unexpected confirmation result for %s: %+v", testName, confirmation)
					}
				})
			}
		}
	}
}

func TestConfiguredQuorumIgnoresObservedMembersOutsideSpecWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	primaryObservation := waitForObservation(t, primary, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRolePrimary && !observation.InRecovery
	})
	standbyObservation := waitForObservation(t, standby, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRoleReplica && observation.InRecovery
	})

	store := seededRealStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:          cluster.FailoverModeAutomatic,
			RequireQuorum: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	})

	observedAt := time.Now().UTC()
	publishUnavailableNodeStatus(t, store, "alpha-1", primary.Address(t), observedAt, primaryObservation)
	publishObservedNodeStatusFromObservation(t, store, "alpha-2", standby.Address(t), observedAt.Add(time.Second), standbyObservation)
	publishWitnessNodeStatus(t, store, "witness-1", observedAt.Add(2*time.Second), cluster.MemberStateRunning)

	confirmation, err := store.ConfirmPrimaryFailure()
	if err != nil {
		t.Fatalf("confirm primary failure: %v", err)
	}

	if confirmation.TotalVoters != 2 || confirmation.RequiredVoters != 2 || confirmation.ReachableVoters != 1 {
		t.Fatalf("expected configured quorum to ignore extra observed voter, got %+v", confirmation)
	}

	if confirmation.QuorumReachable || confirmation.Confirmed {
		t.Fatalf("expected extra observed voter not to satisfy quorum, got %+v", confirmation)
	}

	_, err = store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
		RequestedBy: "integration-test",
		Reason:      "extra observed members must not count toward quorum",
	})
	if !errors.Is(err, controlplane.ErrFailoverQuorumUnavailable) {
		t.Fatalf("unexpected failover intent error: got %v want %v", err, controlplane.ErrFailoverQuorumUnavailable)
	}
}

func TestCreateFailoverIntentObservedQuorumMatrixWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	primaryObservation := waitForObservation(t, primary, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRolePrimary && !observation.InRecovery
	})
	standbyObservation := waitForObservation(t, standby, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRoleReplica && observation.InRecovery
	})

	testCases := []struct {
		name          string
		primaryState  quorumNodeState
		standbyState  quorumNodeState
		witnessState  quorumNodeState
		wantErr       error
		wantCandidate string
	}{
		{
			name:          "accepted with healthy standby and witness",
			primaryState:  quorumNodeFailed,
			standbyState:  quorumNodeHealthy,
			witnessState:  quorumNodeHealthy,
			wantCandidate: "alpha-2",
		},
		{
			name:         "rejected when quorum is missing",
			primaryState: quorumNodeFailed,
			standbyState: quorumNodeHealthy,
			witnessState: quorumNodeMissing,
			wantErr:      controlplane.ErrFailoverQuorumUnavailable,
		},
		{
			name:         "rejected when primary is unknown",
			primaryState: quorumNodeMissing,
			standbyState: quorumNodeHealthy,
			witnessState: quorumNodeHealthy,
			wantErr:      controlplane.ErrFailoverPrimaryUnknown,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := seededRealStore(t, cluster.ClusterSpec{
				ClusterName: "alpha",
				Failover: cluster.FailoverPolicy{
					Mode:          cluster.FailoverModeAutomatic,
					RequireQuorum: true,
				},
			})

			publishRealQuorumScenario(t, store, primary, standby, primaryObservation, standbyObservation, testCase.primaryState, testCase.standbyState, testCase.witnessState)

			intent, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
				RequestedBy: "integration-test",
				Reason:      testCase.name,
			})
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("unexpected failover intent error: got %v want %v", err, testCase.wantErr)
				}

				if _, ok := store.ActiveOperation(); ok {
					t.Fatal("expected rejected failover intent not to create an active operation")
				}

				return
			}

			if err != nil {
				t.Fatalf("create failover intent: %v", err)
			}

			if intent.CurrentPrimary != "alpha-1" || intent.Candidate != testCase.wantCandidate {
				t.Fatalf("unexpected failover intent: %+v", intent)
			}

			if intent.Operation.Kind != cluster.OperationKindFailover || intent.Operation.State != cluster.OperationStateAccepted {
				t.Fatalf("unexpected failover operation: %+v", intent.Operation)
			}
		})
	}
}

func publishRealQuorumScenario(
	t *testing.T,
	store *controlplane.MemoryStateStore,
	primaryFixture *testenv.Postgres,
	standbyFixture *testenv.Postgres,
	primaryObservation pgobs.Observation,
	standbyObservation pgobs.Observation,
	primaryState quorumNodeState,
	standbyState quorumNodeState,
	witnessState quorumNodeState,
) {
	t.Helper()

	observedAt := time.Now().UTC()
	publishRealQuorumDataMember(t, store, "alpha-1", primaryFixture.Address(t), observedAt, primaryObservation, primaryState)
	publishRealQuorumDataMember(t, store, "alpha-2", standbyFixture.Address(t), observedAt.Add(time.Second), standbyObservation, standbyState)

	switch witnessState {
	case quorumNodeMissing:
	case quorumNodeHealthy:
		publishWitnessNodeStatus(t, store, "witness-1", observedAt.Add(2*time.Second), cluster.MemberStateRunning)
	case quorumNodeFailed:
		publishWitnessNodeStatus(t, store, "witness-1", observedAt.Add(2*time.Second), cluster.MemberStateFailed)
	default:
		t.Fatalf("unsupported witness state %q", witnessState)
	}
}

func publishRealQuorumDataMember(t *testing.T, store *controlplane.MemoryStateStore, nodeName, address string, observedAt time.Time, observation pgobs.Observation, state quorumNodeState) {
	t.Helper()

	switch state {
	case quorumNodeMissing:
	case quorumNodeHealthy:
		publishObservedNodeStatusFromObservation(t, store, nodeName, address, observedAt, observation)
	case quorumNodeFailed:
		publishUnavailableNodeStatus(t, store, nodeName, address, observedAt, observation)
	default:
		t.Fatalf("unsupported member state %q", state)
	}
}

func countHealthyStates(states ...quorumNodeState) int {
	count := 0
	for _, state := range states {
		if state == quorumNodeHealthy {
			count++
		}
	}

	return count
}

func countPresentStates(states ...quorumNodeState) int {
	count := 0
	for _, state := range states {
		if state != quorumNodeMissing {
			count++
		}
	}

	return count
}

func expectedPrimaryName(state quorumNodeState) string {
	if state == quorumNodeMissing {
		return ""
	}

	return "alpha-1"
}
