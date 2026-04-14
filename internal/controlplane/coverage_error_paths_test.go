package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

const (
	errFmtUnexpected = "unexpected error: got %v, want %v"

	testMissingNode = "missing-node"
	testSysAlpha    = "sys-alpha"
)

// ---------------------------------------------------------------------------
// switchover_plan.go — uncovered paths
// ---------------------------------------------------------------------------

func TestSwitchoverInputsLockedRejectsNilSpecAndNilStatus(t *testing.T) {
	t.Parallel()

	t.Run("nil spec returns ErrClusterSpecRequired", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStateStore()
		store.mu.Lock()
		_, _, err := store.switchoverInputsLocked()
		store.mu.Unlock()

		if !errors.Is(err, ErrClusterSpecRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
		}
	})

	t.Run("nil status returns ErrSwitchoverObservedStateRequired", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, time.April, 14, 9, 0, 0, 0, time.UTC)
		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, nil)
		store.mu.Lock()
		store.clusterStatus = nil
		_, _, err := store.switchoverInputsLocked()
		store.mu.Unlock()
		_ = now

		if !errors.Is(err, ErrSwitchoverObservedStateRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrSwitchoverObservedStateRequired)
		}
	})
}

func TestSwitchoverTargetReadinessRejectsNilInputs(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	_, err := store.SwitchoverTargetReadiness("alpha-2")
	if !errors.Is(err, ErrClusterSpecRequired) {
		t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
	}
}

func TestValidateSwitchoverRejectsCancelledContextAndNilInputs(t *testing.T) {
	t.Parallel()

	t.Run("cancelled context returns ctx error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		store := NewMemoryStateStore()
		_, err := store.ValidateSwitchover(ctx, SwitchoverRequest{Candidate: "alpha-2"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(errFmtUnexpected, err, context.Canceled)
		}
	})

	t.Run("nil spec returns ErrClusterSpecRequired", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStateStore()
		_, err := store.ValidateSwitchover(context.Background(), SwitchoverRequest{Candidate: "alpha-2"})
		if !errors.Is(err, ErrClusterSpecRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
		}
	})
}

func TestCreateSwitchoverIntentRejectsCancelledContextAndNilInputs(t *testing.T) {
	t.Parallel()

	t.Run("cancelled context returns ctx error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		store := NewMemoryStateStore()
		_, err := store.CreateSwitchoverIntent(ctx, SwitchoverRequest{Candidate: "alpha-2"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(errFmtUnexpected, err, context.Canceled)
		}
	})

	t.Run("nil spec returns ErrClusterSpecRequired", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStateStore()
		_, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"})
		if !errors.Is(err, ErrClusterSpecRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
		}
	})
}

// ---------------------------------------------------------------------------
// failover_plan.go — uncovered paths
// ---------------------------------------------------------------------------

func TestFailoverInputsLockedRejectsNilSpecAndNilStatus(t *testing.T) {
	t.Parallel()

	t.Run("nil spec returns ErrClusterSpecRequired", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStateStore()
		store.mu.Lock()
		_, _, err := store.failoverInputsLocked()
		store.mu.Unlock()

		if !errors.Is(err, ErrClusterSpecRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
		}
	})

	t.Run("nil status returns ErrFailoverObservedStateRequired", func(t *testing.T) {
		t.Parallel()

		store := seededFailoverStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, nil)
		store.mu.Lock()
		store.clusterStatus = nil
		_, _, err := store.failoverInputsLocked()
		store.mu.Unlock()

		if !errors.Is(err, ErrFailoverObservedStateRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrFailoverObservedStateRequired)
		}
	})
}

func TestFailoverCandidatesAndConfirmPrimaryFailureRejectNilInputs(t *testing.T) {
	t.Parallel()

	t.Run("FailoverCandidates returns ErrClusterSpecRequired", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStateStore()
		_, err := store.FailoverCandidates()
		if !errors.Is(err, ErrClusterSpecRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
		}
	})

	t.Run("ConfirmPrimaryFailure returns ErrClusterSpecRequired", func(t *testing.T) {
		t.Parallel()

		store := NewMemoryStateStore()
		_, err := store.ConfirmPrimaryFailure()
		if !errors.Is(err, ErrClusterSpecRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrClusterSpecRequired)
		}
	})
}

func TestCreateFailoverIntentRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.CreateFailoverIntent(ctx, FailoverIntentRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

// ---------------------------------------------------------------------------
// failover_execute.go — uncovered paths
// ---------------------------------------------------------------------------

func TestExecuteFailoverRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.ExecuteFailover(ctx, &recordingPromoter{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

func TestFormerPrimaryNodeStatusLockedReturnsSyntheticStatusForMissingNode(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 9, 30, 0, 0, time.UTC)
	store := &MemoryStateStore{
		nodeStatuses: make(map[string]agentmodel.NodeStatus),
	}

	status := store.formerPrimaryNodeStatusLocked(testMissingNode, now)
	if status.NodeName != testMissingNode || status.MemberName != testMissingNode || !status.ObservedAt.Equal(now) {
		t.Fatalf("unexpected synthetic former-primary status: %+v", status)
	}
}

func TestFailoverOperationForPublicationLockedRejectsNilAndMismatch(t *testing.T) {
	t.Parallel()

	t.Run("nil active operation", func(t *testing.T) {
		t.Parallel()

		store := &MemoryStateStore{}
		_, err := store.failoverOperationForPublicationLocked(cluster.Operation{ID: "fo-1", Kind: cluster.OperationKindFailover})
		if !errors.Is(err, ErrFailoverIntentChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrFailoverIntentChanged)
		}
	})

	t.Run("id mismatch", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{ID: "fo-2", Kind: cluster.OperationKindFailover}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.failoverOperationForPublicationLocked(cluster.Operation{ID: "fo-1", Kind: cluster.OperationKindFailover})
		if !errors.Is(err, ErrFailoverIntentChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrFailoverIntentChanged)
		}
	})

	t.Run("wrong kind", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{ID: "fo-1", Kind: cluster.OperationKindSwitchover}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.failoverOperationForPublicationLocked(cluster.Operation{ID: "fo-1", Kind: cluster.OperationKindFailover})
		if !errors.Is(err, ErrFailoverIntentChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrFailoverIntentChanged)
		}
	})
}

func TestFailoverCandidateStatusLockedRejectsMissingCandidate(t *testing.T) {
	t.Parallel()

	store := &MemoryStateStore{
		nodeStatuses: make(map[string]agentmodel.NodeStatus),
	}
	_, err := store.failoverCandidateStatusLocked("missing")
	if !errors.Is(err, ErrFailoverCandidateUnknown) {
		t.Fatalf(errFmtUnexpected, err, ErrFailoverCandidateUnknown)
	}
}

// ---------------------------------------------------------------------------
// switchover_execute.go — uncovered paths
// ---------------------------------------------------------------------------

func TestExecuteSwitchoverRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.ExecuteSwitchover(ctx, &failingDemoter{}, &recordingPromoter{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

func TestValidateSwitchoverExecutionIntentRejectsChangedIntent(t *testing.T) {
	t.Parallel()

	// Operation says alpha-1→alpha-2 but validation says alpha-1→alpha-3
	operation := cluster.Operation{
		ID:         "sw-1",
		Kind:       cluster.OperationKindSwitchover,
		FromMember: "alpha-1",
		ToMember:   "alpha-2",
	}
	validation := SwitchoverValidation{
		CurrentPrimary: cluster.MemberStatus{Name: "alpha-1"},
		Target:         SwitchoverTargetReadiness{Member: cluster.MemberStatus{Name: "alpha-3"}},
	}

	if err := validateSwitchoverExecutionIntent(operation, validation); !errors.Is(err, ErrSwitchoverIntentChanged) {
		t.Fatalf(errFmtUnexpected, err, ErrSwitchoverIntentChanged)
	}
}

func TestSwitchoverOperationForPublicationLockedRejectsNilAndMismatch(t *testing.T) {
	t.Parallel()

	t.Run("nil active operation", func(t *testing.T) {
		t.Parallel()

		store := &MemoryStateStore{}
		_, err := store.switchoverOperationForPublicationLocked(cluster.Operation{ID: "sw-1", Kind: cluster.OperationKindSwitchover})
		if !errors.Is(err, ErrSwitchoverIntentChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrSwitchoverIntentChanged)
		}
	})

	t.Run("id mismatch", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{ID: "sw-2", Kind: cluster.OperationKindSwitchover}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.switchoverOperationForPublicationLocked(cluster.Operation{ID: "sw-1", Kind: cluster.OperationKindSwitchover})
		if !errors.Is(err, ErrSwitchoverIntentChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrSwitchoverIntentChanged)
		}
	})
}

func TestSwitchoverTargetStatusLockedRejectsMissingTarget(t *testing.T) {
	t.Parallel()

	store := &MemoryStateStore{
		nodeStatuses: make(map[string]agentmodel.NodeStatus),
	}
	_, err := store.switchoverTargetStatusLocked("missing")
	if !errors.Is(err, ErrSwitchoverTargetUnknown) {
		t.Fatalf(errFmtUnexpected, err, ErrSwitchoverTargetUnknown)
	}
}

func TestMemberNodeStatusLockedReturnsSyntheticStatusForMissingNode(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC)
	store := &MemoryStateStore{
		nodeStatuses: make(map[string]agentmodel.NodeStatus),
	}

	status := store.memberNodeStatusLocked(testMissingNode, now)
	if status.NodeName != testMissingNode || status.MemberName != testMissingNode || !status.ObservedAt.Equal(now) {
		t.Fatalf("unexpected synthetic member status: %+v", status)
	}
}

func TestActiveSwitchoverOperationLockedRejectsNilAndWrongKind(t *testing.T) {
	t.Parallel()

	t.Run("nil active operation", func(t *testing.T) {
		t.Parallel()

		store := &MemoryStateStore{}
		_, err := store.activeSwitchoverOperationLocked()
		if !errors.Is(err, ErrSwitchoverIntentRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrSwitchoverIntentRequired)
		}
	})

	t.Run("wrong kind", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{
			ID:         "fo-1",
			Kind:       cluster.OperationKindFailover,
			State:      cluster.OperationStateAccepted,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.activeSwitchoverOperationLocked()
		if !errors.Is(err, ErrSwitchoverIntentRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrSwitchoverIntentRequired)
		}
	})

	t.Run("terminal state", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{
			ID:         "sw-1",
			Kind:       cluster.OperationKindSwitchover,
			State:      cluster.OperationStateCompleted,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.activeSwitchoverOperationLocked()
		if !errors.Is(err, ErrSwitchoverIntentRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrSwitchoverIntentRequired)
		}
	})
}

// ---------------------------------------------------------------------------
// rejoin_continue.go — uncovered paths
// ---------------------------------------------------------------------------

func TestExecuteRejoinStandbyConfigRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.ExecuteRejoinStandbyConfig(ctx, &recordingStandbyConfigurer{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

func TestExecuteRejoinRestartAsStandbyRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.ExecuteRejoinRestartAsStandby(ctx, &recordingStandbyRestarter{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

func TestActiveRejoinOperationLockedRejectsNilAndWrongKind(t *testing.T) {
	t.Parallel()

	t.Run("nil active operation", func(t *testing.T) {
		t.Parallel()

		store := &MemoryStateStore{}
		_, err := store.activeRejoinOperationLocked()
		if !errors.Is(err, ErrRejoinExecutionRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionRequired)
		}
	})

	t.Run("wrong kind", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{
			ID:         "sw-1",
			Kind:       cluster.OperationKindSwitchover,
			State:      cluster.OperationStateRunning,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.activeRejoinOperationLocked()
		if !errors.Is(err, ErrRejoinExecutionRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionRequired)
		}
	})

	t.Run("terminal state", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{
			ID:         "rj-1",
			Kind:       cluster.OperationKindRejoin,
			State:      cluster.OperationStateCompleted,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
		}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.activeRejoinOperationLocked()
		if !errors.Is(err, ErrRejoinExecutionRequired) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionRequired)
		}
	})
}

func TestRejoinOperationForPublicationLockedRejectsNilAndMismatch(t *testing.T) {
	t.Parallel()

	t.Run("nil active operation", func(t *testing.T) {
		t.Parallel()

		store := &MemoryStateStore{}
		_, err := store.rejoinOperationForPublicationLocked(cluster.Operation{ID: "rj-1", Kind: cluster.OperationKindRejoin})
		if !errors.Is(err, ErrRejoinExecutionChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionChanged)
		}
	})

	t.Run("id mismatch", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{ID: "rj-2", Kind: cluster.OperationKindRejoin, State: cluster.OperationStateRunning}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.rejoinOperationForPublicationLocked(cluster.Operation{ID: "rj-1", Kind: cluster.OperationKindRejoin})
		if !errors.Is(err, ErrRejoinExecutionChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionChanged)
		}
	})

	t.Run("wrong kind", func(t *testing.T) {
		t.Parallel()

		op := cluster.Operation{ID: "rj-1", Kind: cluster.OperationKindSwitchover, State: cluster.OperationStateRunning}
		store := &MemoryStateStore{activeOperation: &op}
		_, err := store.rejoinOperationForPublicationLocked(cluster.Operation{ID: "rj-1", Kind: cluster.OperationKindRejoin})
		if !errors.Is(err, ErrRejoinExecutionChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionChanged)
		}
	})
}

// ---------------------------------------------------------------------------
// rejoin_finalize.go — uncovered paths
// ---------------------------------------------------------------------------

func TestVerifyRejoinReplicationRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.VerifyRejoinReplication(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

func TestCompleteRejoinRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewMemoryStateStore()
	_, err := store.CompleteRejoin(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(errFmtUnexpected, err, context.Canceled)
	}
}

func TestPrepareVerifiedRejoinExecutionLockedRejectsChangedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 11, 0, 0, 0, time.UTC)

	// Build a minimal store that has an active rejoin operation but whose
	// in-memory topology does not match what the operation expects.  We call
	// the locked method directly to avoid the seededRestartedRejoinStore
	// timing dependency.

	t.Run("member target unknown in cluster status", func(t *testing.T) {
		t.Parallel()

		// Active operation references alpha-1 but cluster status has no members.
		op := cluster.Operation{
			ID:         "rj-1",
			Kind:       cluster.OperationKindRejoin,
			State:      cluster.OperationStateRunning,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
			StartedAt:  now,
		}
		store := &MemoryStateStore{
			now:          func() time.Time { return now },
			nodeStatuses: make(map[string]agentmodel.NodeStatus),
			clusterStatus: &cluster.ClusterStatus{
				Members:      []cluster.MemberStatus{},
				CurrentEpoch: 7,
			},
			activeOperation: &op,
		}

		store.mu.Lock()
		_, err := store.prepareVerifiedRejoinExecutionLocked(now)
		store.mu.Unlock()

		if !errors.Is(err, ErrRejoinTargetUnknown) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinTargetUnknown)
		}
	})

	t.Run("operation members mismatch cluster topology", func(t *testing.T) {
		t.Parallel()

		// Operation says alpha-1→alpha-2 but cluster status shows alpha-3 as
		// the primary, causing the member-name equality check to fail.
		op := cluster.Operation{
			ID:         "rj-2",
			Kind:       cluster.OperationKindRejoin,
			State:      cluster.OperationStateRunning,
			FromMember: "alpha-1",
			ToMember:   "alpha-2",
			StartedAt:  now,
		}
		store := &MemoryStateStore{
			now: func() time.Time { return now },
			nodeStatuses: map[string]agentmodel.NodeStatus{
				"alpha-1": {
					NodeName: "alpha-1",
					Postgres: agentmodel.PostgresStatus{Managed: true},
				},
				"alpha-3": {
					NodeName: "alpha-3",
					Postgres: agentmodel.PostgresStatus{Managed: true, Up: true},
				},
			},
			clusterStatus: &cluster.ClusterStatus{
				Members: []cluster.MemberStatus{
					{Name: "alpha-1", NeedsRejoin: true},
					{Name: "alpha-3", Role: cluster.MemberRolePrimary, Healthy: true},
				},
				CurrentPrimary: "alpha-3",
				CurrentEpoch:   7,
			},
			activeOperation: &op,
		}

		store.mu.Lock()
		_, err := store.prepareVerifiedRejoinExecutionLocked(now)
		store.mu.Unlock()

		if !errors.Is(err, ErrRejoinExecutionChanged) {
			t.Fatalf(errFmtUnexpected, err, ErrRejoinExecutionChanged)
		}
	})
}

func TestAssessRejoinReplicationVerificationReasonsCoversRemainingPaths(t *testing.T) {
	t.Parallel()

	t.Run("no member node returns early", func(t *testing.T) {
		t.Parallel()

		// assessRejoinMemberReasons is called first; if hasMemberNode == false it returns early
		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
			},
			currentPrimary: cluster.MemberStatus{
				Name:    "alpha-2",
				Healthy: true,
			},
			hasCurrentPrimary: true,
			hasMemberNode:     false,
		})

		if containsString(reasons, reasonRejoinRestartPending) {
			t.Fatalf("expected early return without member node, got reasons %v", reasons)
		}
	})

	t.Run("missing current primary node observation", func(t *testing.T) {
		t.Parallel()

		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    11,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					Up:            true,
					RecoveryKnown: true,
					InRecovery:    true,
					Role:          cluster.MemberRoleReplica,
					Details: agentmodel.PostgresDetails{
						SystemIdentifier: testSysAlpha,
					},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary:     true,
			hasCurrentPrimaryNode: false,
		})

		if !containsString(reasons, reasonCurrentPrimaryStateNotObserved) {
			t.Fatalf("expected current primary not observed reason, got %v", reasons)
		}
	})

	t.Run("unknown member system identifier", func(t *testing.T) {
		t.Parallel()

		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    11,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					Up:            true,
					RecoveryKnown: true,
					InRecovery:    true,
					Role:          cluster.MemberRoleReplica,
					Details: agentmodel.PostgresDetails{
						SystemIdentifier: "", // empty — triggers the unknown branch
					},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(reasons, reasonMemberSystemIdentifierUnknown) {
			t.Fatalf("expected member system identifier unknown reason, got %v", reasons)
		}
	})

	t.Run("unknown current primary system identifier", func(t *testing.T) {
		t.Parallel()

		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    11,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					Up:            true,
					RecoveryKnown: true,
					InRecovery:    true,
					Role:          cluster.MemberRoleReplica,
					Details: agentmodel.PostgresDetails{
						SystemIdentifier: testSysAlpha,
					},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: ""}, // empty
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(reasons, reasonCurrentPrimarySystemIdentifierUnknown) {
			t.Fatalf("expected current primary system identifier unknown reason, got %v", reasons)
		}
	})

	t.Run("unknown member timeline", func(t *testing.T) {
		t.Parallel()

		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    0, // unknown
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					Up:            true,
					RecoveryKnown: true,
					InRecovery:    true,
					Role:          cluster.MemberRoleReplica,
					Details: agentmodel.PostgresDetails{
						SystemIdentifier: testSysAlpha,
					},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(reasons, reasonMemberTimelineUnknown) {
			t.Fatalf("expected member timeline unknown reason, got %v", reasons)
		}
	})

	t.Run("unknown current primary timeline", func(t *testing.T) {
		t.Parallel()

		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    11,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					Up:            true,
					RecoveryKnown: true,
					InRecovery:    true,
					Role:          cluster.MemberRoleReplica,
					Details: agentmodel.PostgresDetails{
						SystemIdentifier: testSysAlpha,
					},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 0, // unknown
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(reasons, reasonCurrentPrimaryTimelineUnknown) {
			t.Fatalf("expected current primary timeline unknown reason, got %v", reasons)
		}
	})
}

// ---------------------------------------------------------------------------
// rejoin_plan.go — uncovered paths in buildRejoinDivergenceAssessment
// ---------------------------------------------------------------------------

func TestBuildRejoinDivergenceAssessmentCoversRemainingPaths(t *testing.T) {
	t.Parallel()

	t.Run("unknown current primary system identifier", func(t *testing.T) {
		t.Parallel()

		assessment := buildRejoinDivergenceAssessment(rejoinInputs{
			checkedAt: time.Date(2026, time.April, 14, 12, 0, 0, 0, time.UTC),
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    10,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Postgres: agentmodel.PostgresStatus{
					Managed: true,
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				NodeName: "alpha-2",
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: ""}, // empty
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(assessment.Reasons, reasonCurrentPrimarySystemIdentifierUnknown) {
			t.Fatalf("expected current primary system identifier unknown reason, got %v", assessment.Reasons)
		}

		if assessment.Compared || assessment.Diverged {
			t.Fatalf("expected comparison blocked, got %+v", assessment)
		}
	})

	t.Run("unknown member timeline blocks comparison", func(t *testing.T) {
		t.Parallel()

		assessment := buildRejoinDivergenceAssessment(rejoinInputs{
			checkedAt: time.Date(2026, time.April, 14, 12, 1, 0, 0, time.UTC),
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    0, // unknown
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Postgres: agentmodel.PostgresStatus{
					Managed: true,
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				NodeName: "alpha-2",
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(assessment.Reasons, reasonMemberTimelineUnknown) {
			t.Fatalf("expected member timeline unknown reason, got %v", assessment.Reasons)
		}
	})

	t.Run("unknown current primary timeline blocks comparison", func(t *testing.T) {
		t.Parallel()

		assessment := buildRejoinDivergenceAssessment(rejoinInputs{
			checkedAt: time.Date(2026, time.April, 14, 12, 2, 0, 0, time.UTC),
			member: cluster.MemberStatus{
				Name:        "alpha-1",
				NeedsRejoin: true,
				Timeline:    10,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: "alpha-1",
				Postgres: agentmodel.PostgresStatus{
					Managed: true,
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 0, // unknown
			},
			hasCurrentPrimary: true,
			currentPrimaryNode: agentmodel.NodeStatus{
				NodeName: "alpha-2",
				Postgres: agentmodel.PostgresStatus{
					Details: agentmodel.PostgresDetails{SystemIdentifier: testSysAlpha},
				},
			},
			hasCurrentPrimaryNode: true,
		})

		if !containsString(assessment.Reasons, reasonCurrentPrimaryTimelineUnknown) {
			t.Fatalf("expected current primary timeline unknown reason, got %v", assessment.Reasons)
		}
	})
}

// ---------------------------------------------------------------------------
// switchover_execute.go — full happy-path execution
// ---------------------------------------------------------------------------

func TestMemoryStateStoreExecuteSwitchoverRunsDemotionPromotionAndAdvancesEpoch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 13, 0, 0, 0, time.UTC)

	// Set the test clock BEFORE publishing node statuses so DCS entries are
	// stored with expiresAt = testNow + leaseDuration (14:05 UTC), not
	// realNow + leaseDuration. The DCS sweeper also runs with testNow, so if
	// entries were written at real time and testNow is in the past relative to
	// real time the sweeper would expire them and delete alpha-2 from
	// store.nodeStatuses before publishSwitchoverCompletion reads it.
	store := NewMemoryStateStore()
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })
	setTestLeaseDuration(store, time.Hour)

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	for _, ns := range []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 21),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 21, 0),
	} {
		if _, err := store.PublishNodeStatus(context.Background(), ns); err != nil {
			t.Fatalf("publish node status for %q: %v", ns.NodeName, err)
		}
	}

	if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
		Candidate:   "alpha-2",
		RequestedBy: "operator",
		Reason:      "maintenance",
	}); err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 5
	store.mu.Unlock()

	demoter := &recordingDemoter{}
	promoter := &recordingPromoter{}

	execution, err := store.ExecuteSwitchover(context.Background(), demoter, promoter)
	if err != nil {
		t.Fatalf("execute switchover: %v", err)
	}

	if execution.CurrentPrimary != "alpha-1" || execution.Candidate != "alpha-2" {
		t.Fatalf("unexpected switchover execution members: %+v", execution)
	}

	if !execution.Demoted || !execution.Promoted {
		t.Fatalf("expected demoted and promoted execution, got %+v", execution)
	}

	if execution.PreviousEpoch != 5 || execution.CurrentEpoch != 6 {
		t.Fatalf("unexpected switchover epoch transition: %+v", execution)
	}

	if len(demoter.requests) != 1 || demoter.requests[0].Candidate != "alpha-2" {
		t.Fatalf("unexpected demotion requests: %+v", demoter.requests)
	}

	if len(promoter.requests) != 1 || promoter.requests[0].Candidate != "alpha-2" {
		t.Fatalf("unexpected promotion requests: %+v", promoter.requests)
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected completed switchover to clear active operation")
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after switchover execution")
	}

	if status.CurrentPrimary != "alpha-2" || status.CurrentEpoch != 6 {
		t.Fatalf("expected promoted primary and advanced epoch, got %+v", status)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected switchover history entry after execution, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindSwitchover || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected switchover history entry: %+v", history[0])
	}
}
