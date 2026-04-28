package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
	dcsmemory "github.com/polkiloo/pacman/internal/dcs/memory"
)

func TestApplyNodeStatusEventLockedTracksDeleteRevisionAndAcceptsRecreatedNode(t *testing.T) {
	t.Parallel()

	keyspace, err := dcs.NewKeySpace("alpha")
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}

	payload, err := json.Marshal(agentmodel.NodeStatus{
		Role:  cluster.MemberRoleReplica,
		State: cluster.MemberStateStreaming,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		},
	})
	if err != nil {
		t.Fatalf("marshal node status: %v", err)
	}

	store := &MemoryStateStore{
		keyspace: keyspace,
		nodeStatuses: map[string]agentmodel.NodeStatus{
			testNodeName: {NodeName: testNodeName},
		},
		nodeStatusRevisions: map[string]int64{
			testNodeName: 2,
		},
	}

	if err := store.applyNodeStatusEventLocked(dcs.WatchEvent{
		Type:     dcs.EventDelete,
		Key:      keyspace.Status(testNodeName),
		Revision: 4,
	}); err != nil {
		t.Fatalf("apply delete node status event: %v", err)
	}

	if _, ok := store.nodeStatuses[testNodeName]; ok {
		t.Fatal("expected delete event to clear cached node status")
	}

	if got := store.nodeStatusRevisions[testNodeName]; got != 4 {
		t.Fatalf("unexpected delete revision: got %d, want %d", got, 4)
	}

	if err := store.applyNodeStatusEventLocked(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      keyspace.Status(testNodeName),
		Value:    payload,
		Revision: 1,
	}); err != nil {
		t.Fatalf("apply recreated node status event: %v", err)
	}

	recreated, ok := store.nodeStatuses[testNodeName]
	if !ok {
		t.Fatal("expected recreated node status to be cached")
	}

	if recreated.NodeName != testNodeName || recreated.State != cluster.MemberStateStreaming {
		t.Fatalf("unexpected recreated node status: %+v", recreated)
	}

	if got := store.nodeStatusRevisions[testNodeName]; got != 1 {
		t.Fatalf("unexpected recreated revision: got %d, want %d", got, 1)
	}
}

func TestForceRefreshCachePreservesFinishedOperationRevision(t *testing.T) {
	t.Parallel()

	backend := dcsmemory.New(dcsmemory.Config{})
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	keyspace, err := dcs.NewKeySpace("alpha")
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}

	store := &MemoryStateStore{
		dcs:              backend,
		keyspace:         keyspace,
		activeOpRevision: 7,
		now:              time.Now,
	}

	if err := store.forceRefreshCache(context.Background()); err != nil {
		t.Fatalf("force refresh cache: %v", err)
	}

	if store.activeOperation != nil {
		t.Fatalf("expected no active operation after refresh, got %+v", store.activeOperation)
	}

	if store.activeOpRevision != 7 {
		t.Fatalf("expected finished operation revision tombstone to be preserved, got %d", store.activeOpRevision)
	}
}

func TestDeleteKeyClearsActiveOperationWhenOperationKeyMissing(t *testing.T) {
	t.Parallel()

	backend := dcsmemory.New(dcsmemory.Config{})
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	keyspace, err := dcs.NewKeySpace("alpha")
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}

	store := &MemoryStateStore{
		dcs:           backend,
		keyspace:      keyspace,
		now:           time.Now,
		logger:        nil,
		registrations: make(map[string]MemberRegistration),
		nodeStatuses:  make(map[string]agentmodel.NodeStatus),
		activeOperation: &cluster.Operation{
			ID:   "op-1",
			Kind: cluster.OperationKindFailover,
		},
	}

	if err := store.deleteKey(context.Background(), keyspace.Operation()); err != nil {
		t.Fatalf("delete operation key: %v", err)
	}

	if store.activeOperation != nil {
		t.Fatalf("expected deleteKey to clear local active operation cache, got %+v", store.activeOperation)
	}
}

func TestPersistNodeStatusTracksLocalRevisionAndClonesPayload(t *testing.T) {
	t.Parallel()

	backend := dcsmemory.New(dcsmemory.Config{})
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	keyspace, err := dcs.NewKeySpace("alpha")
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}

	store := &MemoryStateStore{
		dcs:                 backend,
		keyspace:            keyspace,
		now:                 time.Now,
		leaseDuration:       time.Minute,
		nodeStatuses:        make(map[string]agentmodel.NodeStatus),
		nodeStatusRevisions: make(map[string]int64),
	}

	status := agentmodel.NodeStatus{
		NodeName: testNodeName,
		Tags: map[string]any{
			"zone": "a",
		},
		ObservedAt: time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC),
	}

	if err := store.persistNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("persist first node status: %v", err)
	}

	status.Tags["zone"] = "mutated"

	if got := store.nodeStatusRevisions[testNodeName]; got != 1 {
		t.Fatalf("unexpected first local node status revision: got %d, want %d", got, 1)
	}

	if store.nodeStatuses[testNodeName].Tags["zone"] != "a" {
		t.Fatalf("expected cached node status to be detached, got %+v", store.nodeStatuses[testNodeName].Tags)
	}

	if err := store.persistNodeStatus(context.Background(), store.nodeStatuses[testNodeName]); err != nil {
		t.Fatalf("persist second node status: %v", err)
	}

	if got := store.nodeStatusRevisions[testNodeName]; got != 2 {
		t.Fatalf("unexpected second local node status revision: got %d, want %d", got, 2)
	}
}

func TestPublishNodeStatusTracksNodeStatusRevision(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	setTestLeaseDuration(store, time.Hour)

	observedAt := time.Date(2026, time.April, 14, 11, 0, 0, 0, time.UTC)
	status := agentmodel.NodeStatus{
		NodeName:   testNodeName,
		Role:       cluster.MemberRoleReplica,
		State:      cluster.MemberStateStreaming,
		ObservedAt: observedAt,
	}

	if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("publish first node status: %v", err)
	}

	if got := store.nodeStatusRevisions[testNodeName]; got != 1 {
		t.Fatalf("unexpected first published node status revision: got %d, want %d", got, 1)
	}

	status.ObservedAt = observedAt.Add(time.Second)
	if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("publish second node status: %v", err)
	}

	if got := store.nodeStatusRevisions[testNodeName]; got != 2 {
		t.Fatalf("unexpected second published node status revision: got %d, want %d", got, 2)
	}
}

func TestFailoverPlanHelperBranches(t *testing.T) {
	t.Parallel()

	t.Run("quorum falls back to observed members", func(t *testing.T) {
		t.Parallel()

		total, reachable := quorumVoteCounts(cluster.ClusterSpec{}, cluster.ClusterStatus{
			Members: []cluster.MemberStatus{
				{Name: "alpha-1", Healthy: true},
				{Name: "alpha-2", Healthy: false},
			},
		})

		if total != 2 || reachable != 1 {
			t.Fatalf("unexpected observed quorum counts: total=%d reachable=%d", total, reachable)
		}
	})

	t.Run("requested failover candidate handling", func(t *testing.T) {
		t.Parallel()

		candidates := []FailoverCandidate{
			{Member: cluster.MemberStatus{Name: "alpha-1"}, Eligible: false},
			{Member: cluster.MemberStatus{Name: "alpha-2"}, Eligible: true},
		}

		selected, err := selectFailoverCandidate(candidates, "alpha-2")
		if err != nil {
			t.Fatalf("select requested candidate: %v", err)
		}

		if selected.Member.Name != "alpha-2" {
			t.Fatalf("unexpected selected candidate: %+v", selected)
		}

		if _, err := selectFailoverCandidate(candidates, "alpha-1"); !errors.Is(err, ErrFailoverNoEligibleCandidates) {
			t.Fatalf("unexpected ineligible candidate error: got %v want %v", err, ErrFailoverNoEligibleCandidates)
		}

		if _, err := selectFailoverCandidate(candidates, "missing"); !errors.Is(err, ErrFailoverCandidateUnknown) {
			t.Fatalf("unexpected unknown candidate error: got %v want %v", err, ErrFailoverCandidateUnknown)
		}
	})

	t.Run("operation messages cover generic branches", func(t *testing.T) {
		t.Parallel()

		if got := failoverOperationMessage("", "alpha-2"); got != "automatic failover accepted" {
			t.Fatalf("unexpected failover operation message: %q", got)
		}

		if got := cancelledSwitchoverMessage(cluster.Operation{
			ID:          "sw-1",
			State:       cluster.OperationStateAccepted,
			RequestedAt: time.Date(2026, time.April, 14, 12, 0, 0, 0, time.UTC),
		}); got != "switchover cancelled" {
			t.Fatalf("unexpected cancelled switchover message: %q", got)
		}

		if got := maintenanceOperationMessage(false); got != "maintenance mode disabled" {
			t.Fatalf("unexpected maintenance disabled message: %q", got)
		}
	})
}

func TestSwitchoverObservedReadinessReasonBranches(t *testing.T) {
	t.Parallel()

	t.Run("missing node observation", func(t *testing.T) {
		t.Parallel()

		reasons := appendSwitchoverObservedReadinessReasons(nil, agentmodel.NodeStatus{}, false)
		if !containsString(reasons, reasonNodeStateNotObserved) {
			t.Fatalf("expected missing observation reason, got %v", reasons)
		}
	})

	t.Run("unmanaged postgres returns early", func(t *testing.T) {
		t.Parallel()

		reasons := appendSwitchoverObservedReadinessReasons(nil, agentmodel.NodeStatus{
			Postgres: agentmodel.PostgresStatus{},
		}, true)
		if len(reasons) != 1 || reasons[0] != reasonPostgresNotManaged {
			t.Fatalf("unexpected unmanaged postgres reasons: %v", reasons)
		}
	})

	t.Run("postgres health details", func(t *testing.T) {
		t.Parallel()

		reasons := appendSwitchoverObservedReadinessReasons(nil, agentmodel.NodeStatus{
			Postgres: agentmodel.PostgresStatus{
				Managed:       true,
				Up:            false,
				RecoveryKnown: false,
				InRecovery:    false,
				Role:          cluster.MemberRolePrimary,
			},
		}, true)

		for _, want := range []string{
			reasonPostgresNotUp,
			reasonRecoveryStateUnknown,
			reasonPostgresRoleNotStandby,
		} {
			if !containsString(reasons, want) {
				t.Fatalf("expected reason %q in %v", want, reasons)
			}
		}
	})

	t.Run("known primary role in recovery check", func(t *testing.T) {
		t.Parallel()

		reasons := appendSwitchoverObservedReadinessReasons(nil, agentmodel.NodeStatus{
			Postgres: agentmodel.PostgresStatus{
				Managed:       true,
				Up:            true,
				RecoveryKnown: true,
				InRecovery:    false,
				Role:          cluster.MemberRoleStandbyLeader,
			},
		}, true)

		if !containsString(reasons, reasonNotInRecovery) {
			t.Fatalf("expected not-in-recovery reason, got %v", reasons)
		}

		if !isSwitchoverStandbyRole(cluster.MemberRoleStandbyLeader) {
			t.Fatal("expected standby leader role to be treated as a valid standby role")
		}
	})
}

func TestRejoinHelperBranches(t *testing.T) {
	t.Parallel()

	t.Run("divergence requires current primary node state", func(t *testing.T) {
		t.Parallel()

		assessment := buildRejoinDivergenceAssessment(rejoinInputs{
			checkedAt: time.Date(2026, time.April, 14, 13, 0, 0, 0, time.UTC),
			member: cluster.MemberStatus{
				Name:        testNodeName,
				NeedsRejoin: true,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: testNodeName,
				Postgres: agentmodel.PostgresStatus{Managed: true},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:    "alpha-2",
				Healthy: true,
			},
			hasCurrentPrimary: true,
		})

		if !containsString(assessment.Reasons, reasonCurrentPrimaryStateNotObserved) {
			t.Fatalf("expected current primary state reason, got %v", assessment.Reasons)
		}
	})

	t.Run("divergence detects system identifier mismatch", func(t *testing.T) {
		t.Parallel()

		assessment := buildRejoinDivergenceAssessment(rejoinInputs{
			checkedAt: time.Date(2026, time.April, 14, 13, 1, 0, 0, time.UTC),
			member: cluster.MemberStatus{
				Name:        testNodeName,
				NeedsRejoin: true,
				Timeline:    11,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName: testNodeName,
				Postgres: agentmodel.PostgresStatus{
					Managed: true,
					Details: agentmodel.PostgresDetails{SystemIdentifier: "sys-old"},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 11,
			},
			hasCurrentPrimary:     true,
			currentPrimaryNode:    rejoinPrimaryStatus("alpha-2", time.Time{}, 11, "sys-new"),
			hasCurrentPrimaryNode: true,
		})

		if !assessment.Compared || !assessment.Diverged || !assessment.RequiresReclone {
			t.Fatalf("expected reclone divergence assessment, got %+v", assessment)
		}

		if !containsString(assessment.Reasons, reasonSystemIdentifierMismatch) {
			t.Fatalf("expected system identifier mismatch reason, got %v", assessment.Reasons)
		}
	})

	t.Run("replication verification reasons accumulate unhealthy state", func(t *testing.T) {
		t.Parallel()

		reasons := assessRejoinReplicationVerificationReasons(rejoinInputs{
			member: cluster.MemberStatus{
				Name:        testNodeName,
				NeedsRejoin: true,
				Timeline:    1,
			},
			memberNode: agentmodel.NodeStatus{
				NodeName:       testNodeName,
				Role:           cluster.MemberRolePrimary,
				State:          cluster.MemberStateStarting,
				PendingRestart: true,
				NeedsRejoin:    true,
				ObservedAt:     time.Date(2026, time.April, 14, 13, 2, 0, 0, time.UTC),
				Postgres: agentmodel.PostgresStatus{
					Managed:       true,
					Up:            false,
					RecoveryKnown: false,
					InRecovery:    false,
					Role:          cluster.MemberRolePrimary,
					Details: agentmodel.PostgresDetails{
						PendingRestart:   true,
						SystemIdentifier: "sys-old",
					},
				},
			},
			hasMemberNode: true,
			currentPrimary: cluster.MemberStatus{
				Name:     "alpha-2",
				Healthy:  true,
				Timeline: 2,
			},
			hasCurrentPrimary:     true,
			currentPrimaryNode:    rejoinPrimaryStatus("alpha-2", time.Time{}, 2, "sys-new"),
			hasCurrentPrimaryNode: true,
		})

		for _, want := range []string{
			reasonRejoinRestartPending,
			reasonRoleNotStandby,
			reasonRejoinReplicationNotStreaming,
			reasonPostgresNotUp,
			reasonRecoveryStateUnknown,
			reasonNotInRecovery,
			reasonPostgresRoleNotStandby,
			reasonSystemIdentifierMismatch,
			reasonTimelineMismatch,
		} {
			if !containsString(reasons, want) {
				t.Fatalf("expected reason %q in %v", want, reasons)
			}
		}
	})

	t.Run("standby config helpers", func(t *testing.T) {
		t.Parallel()

		spec := cluster.ClusterSpec{
			Postgres: cluster.PostgresPolicy{
				Parameters: map[string]any{
					"restore_command":          " restore_command ",
					"recovery_target_timeline": " latest ",
				},
			},
		}

		standby, err := buildRejoinStandbyConfig(spec, "Alpha 1", "10.0.0.10:5432")
		if err != nil {
			t.Fatalf("build standby config: %v", err)
		}

		if standby.PrimaryConnInfo != "host=10.0.0.10 port=5432 application_name=Alpha 1" {
			t.Fatalf("unexpected primary conninfo: %q", standby.PrimaryConnInfo)
		}

		if standby.PrimarySlotName != "alpha_1" {
			t.Fatalf("unexpected primary slot name: %q", standby.PrimarySlotName)
		}

		if standby.RestoreCommand != "restore_command" || standby.RecoveryTargetTimeline != "latest" {
			t.Fatalf("unexpected standby postgres parameters: %+v", standby)
		}

		if _, err := buildRejoinStandbyConfig(spec, "alpha-1", ""); !errors.Is(err, ErrRejoinCurrentPrimaryAddressRequired) {
			t.Fatalf("unexpected missing primary address error: got %v want %v", err, ErrRejoinCurrentPrimaryAddressRequired)
		}

		if got := rejoinPrimarySlotName("___"); got != "pacman_rejoin" {
			t.Fatalf("unexpected fallback primary slot name: %q", got)
		}

		if got := stringPostgresParameter(map[string]any{"restore_command": 42}, "restore_command"); got != "" {
			t.Fatalf("expected non-string postgres parameter to be ignored, got %q", got)
		}
	})
}

func TestMemoryStateStoreCancelSwitchoverCancelsImmediateIntent(t *testing.T) {
	t.Parallel()

	clock := newMutableTestClock(time.Date(2026, time.April, 14, 14, 0, 0, 0, time.UTC))
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", clock.Now(), 19),
		readyStandbyStatus("alpha-2", clock.Now().Add(time.Second), 19, 0),
	})
	setTestNow(store, clock.Now)

	if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
		Candidate: "alpha-2",
	}); err != nil {
		t.Fatalf("create immediate switchover intent: %v", err)
	}

	cancelledAt := clock.Advance(time.Minute)
	cancelled, err := store.CancelSwitchover(context.Background())
	if err != nil {
		t.Fatalf("cancel immediate switchover: %v", err)
	}

	if cancelled.Message != "switchover cancelled" {
		t.Fatalf("unexpected immediate cancellation message: %q", cancelled.Message)
	}

	if !cancelled.CompletedAt.Equal(cancelledAt) {
		t.Fatalf("unexpected cancellation time: got %v want %v", cancelled.CompletedAt, cancelledAt)
	}
}

func TestMemoryStateStoreExecuteFailoverReturnsFencingError(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 15, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:            cluster.FailoverModeAutomatic,
			FencingRequired: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2", Priority: 100},
		},
	}, []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 12, 0),
		failoverNodeStatus("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateStreaming, now, true, 12, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })

	if _, err := store.CreateFailoverIntent(context.Background(), FailoverIntentRequest{}); err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 4
	store.mu.Unlock()

	wantErr := errors.New("fencing failed")
	_, err := store.ExecuteFailover(context.Background(), &recordingPromoter{}, failingFencer{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected failover execution error: got %v want %v", err, wantErr)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.State != cluster.OperationStateRunning || active.StartedAt.IsZero() {
		t.Fatalf("expected failover execution to remain running after fencing failure, got ok=%v operation=%+v", ok, active)
	}
}

func TestMemoryStateStoreExecuteSwitchoverReturnsDemotionError(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 16, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 20),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 20, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(10 * time.Second) })
	setTestLeaseDuration(store, time.Hour)

	if _, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{
		Candidate: "alpha-2",
	}); err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 9
	store.mu.Unlock()

	wantErr := errors.New("demotion failed")
	_, err := store.ExecuteSwitchover(context.Background(), failingDemoter{err: wantErr}, &recordingPromoter{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected switchover execution error: got %v want %v", err, wantErr)
	}

	active, ok := store.ActiveOperation()
	if !ok || active.State != cluster.OperationStateRunning || active.StartedAt.IsZero() {
		t.Fatalf("expected switchover execution to remain running after demotion failure, got ok=%v operation=%+v", ok, active)
	}
}

type failingFencer struct {
	err error
}

func (failing failingFencer) Fence(context.Context, FencingRequest) error {
	return failing.err
}

type failingDemoter struct {
	err error
}

func (failing failingDemoter) Demote(context.Context, DemotionRequest) error {
	return failing.err
}
