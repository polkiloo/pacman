package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
)

func TestNewControlPlaneDefaultsClusterNameAndInitializesBackend(t *testing.T) {
	t.Parallel()

	backend := &fixtureControlPlaneDCS{
		watchErr: dcs.ErrBackendUnavailable,
	}

	store := NewControlPlane(backend, "   ", nil)
	if store == nil {
		t.Fatal("expected control plane instance")
	}

	if backend.initializeCalls != 1 {
		t.Fatalf("unexpected initialize calls: got %d want %d", backend.initializeCalls, 1)
	}

	if store.clusterName != defaultControlPlaneClusterName {
		t.Fatalf("unexpected default cluster name: got %q want %q", store.clusterName, defaultControlPlaneClusterName)
	}

	if store.keyspace.Root() != "/pacman/"+defaultControlPlaneClusterName {
		t.Fatalf("unexpected keyspace root: %q", store.keyspace.Root())
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if backend.watchCount() >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected cache watch startup to attempt watch and retry, got %d calls", backend.watchCount())
}

func TestControlPlaneConstructorAndContextErrorBranches(t *testing.T) {
	t.Parallel()

	t.Run("new control plane panics on nil backend", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected nil backend panic")
			}
		}()

		_ = NewControlPlane(nil, "alpha", nil)
	})

	t.Run("new control plane panics on initialize error", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected initialize panic")
			}
		}()

		_ = NewControlPlane(&fixtureControlPlaneDCS{initializeErr: errors.New("boom")}, "alpha", nil)
	})

	t.Run("cache helpers respect context cancellation", func(t *testing.T) {
		t.Parallel()

		store, _ := newFixtureControlPlaneStore(t, "alpha", time.Date(2026, time.April, 15, 10, 30, 0, 0, time.UTC))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := store.ensureCacheFresh(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected ensureCacheFresh error: got %v want %v", err, context.Canceled)
		}

		if err := store.refreshCache(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected refreshCache error: got %v want %v", err, context.Canceled)
		}
	})

	t.Run("unknown watch key is ignored", func(t *testing.T) {
		t.Parallel()

		store, _ := newFixtureControlPlaneStore(t, "alpha", time.Date(2026, time.April, 15, 10, 45, 0, 0, time.UTC))
		if err := store.applyWatchEventLocked(dcs.WatchEvent{Type: dcs.EventPut, Key: "/pacman/alpha/unknown"}); err != nil {
			t.Fatalf("unexpected unknown watch error: %v", err)
		}
	})
}

func TestForceRefreshCacheLoadsStateFromDCS(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 10, 0, 0, 0, time.UTC)
	store, backend := newFixtureControlPlaneStore(t, "alpha", now)

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Maintenance: cluster.MaintenanceDesiredState{
			Enabled:       true,
			DefaultReason: "operator maintenance",
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}
	maintenance := cluster.MaintenanceModeStatus{
		Enabled:     true,
		Reason:      "operator maintenance",
		RequestedBy: "operator",
		UpdatedAt:   now.Add(-time.Minute),
	}
	operation := cluster.Operation{
		ID:          "maint-1",
		Kind:        cluster.OperationKindMaintenanceChange,
		State:       cluster.OperationStateRunning,
		Result:      cluster.OperationResultPending,
		RequestedBy: "operator",
		RequestedAt: now.Add(-2 * time.Minute),
		StartedAt:   now.Add(-time.Minute),
		Message:     "maintenance mode enabled",
	}
	history := []cluster.HistoryEntry{
		{
			OperationID: "op-2",
			Kind:        cluster.OperationKindSwitchover,
			FromMember:  "alpha-1",
			ToMember:    "alpha-2",
			Result:      cluster.OperationResultSucceeded,
			FinishedAt:  now.Add(-time.Minute),
		},
		{
			OperationID: "op-1",
			Kind:        cluster.OperationKindFailover,
			FromMember:  "alpha-2",
			ToMember:    "alpha-1",
			Result:      cluster.OperationResultSucceeded,
			FinishedAt:  now.Add(-2 * time.Minute),
		},
	}

	backend.lists[store.keyspace.MembersPrefix()] = []dcs.KeyValue{
		{Key: store.keyspace.Member("alpha-2"), Value: mustMarshalControlPlaneJSON(t, MemberRegistration{
			NodeName:       "alpha-2",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.12:8080",
			ControlAddress: "10.0.0.12:9090",
			RegisteredAt:   now.Add(-3 * time.Minute),
		})},
		{Key: store.keyspace.Member("alpha-1"), Value: mustMarshalControlPlaneJSON(t, MemberRegistration{
			NodeName:       "alpha-1",
			NodeRole:       cluster.NodeRoleData,
			APIAddress:     "10.0.0.11:8080",
			ControlAddress: "10.0.0.11:9090",
			RegisteredAt:   now.Add(-4 * time.Minute),
		})},
	}
	backend.lists[store.keyspace.StatusPrefix()] = []dcs.KeyValue{
		{
			Key:      store.keyspace.Status("alpha-1"),
			Revision: 7,
			Value:    mustMarshalControlPlaneJSON(t, readyPrimaryStatus("alpha-1", now.Add(-15*time.Second), 18)),
		},
		{
			Key:      store.keyspace.Status("alpha-2"),
			Revision: 8,
			Value:    mustMarshalControlPlaneJSON(t, readyStandbyStatus("alpha-2", now.Add(-10*time.Second), 18, 0)),
		},
	}
	backend.lists[store.keyspace.HistoryPrefix()] = []dcs.KeyValue{
		{Key: store.keyspace.History("op-2"), Value: mustMarshalControlPlaneJSON(t, history[0])},
		{Key: store.keyspace.History("op-1"), Value: mustMarshalControlPlaneJSON(t, history[1])},
	}
	backend.gets[store.keyspace.Config()] = dcs.KeyValue{
		Key:      store.keyspace.Config(),
		Revision: 5,
		Value:    mustMarshalControlPlaneJSON(t, spec),
	}
	backend.gets[store.keyspace.Maintenance()] = dcs.KeyValue{
		Key:      store.keyspace.Maintenance(),
		Revision: 3,
		Value:    mustMarshalControlPlaneJSON(t, maintenance),
	}
	backend.gets[store.keyspace.Operation()] = dcs.KeyValue{
		Key:      store.keyspace.Operation(),
		Revision: 4,
		Value:    mustMarshalControlPlaneJSON(t, operation),
	}
	backend.leaderOK = true
	backend.leaderLease = dcs.LeaderLease{
		Leader:    "alpha-1",
		Term:      2,
		Acquired:  now.Add(-time.Minute),
		Renewed:   now.Add(-30 * time.Second),
		ExpiresAt: now.Add(time.Minute),
	}

	if err := store.forceRefreshCache(context.Background()); err != nil {
		t.Fatalf("force refresh cache: %v", err)
	}

	if store.clusterSpec == nil || store.clusterSpec.ClusterName != "alpha" {
		t.Fatalf("unexpected cached cluster spec: %+v", store.clusterSpec)
	}

	if store.clusterSpecRevision != 5 || store.maintenanceRevision != 3 || store.activeOpRevision != 4 {
		t.Fatalf("unexpected cached revisions: config=%d maintenance=%d operation=%d", store.clusterSpecRevision, store.maintenanceRevision, store.activeOpRevision)
	}

	if len(store.registrations) != 2 || len(store.nodeStatuses) != 2 || len(store.history) != 2 {
		t.Fatalf("unexpected cached state sizes: registrations=%d statuses=%d history=%d", len(store.registrations), len(store.nodeStatuses), len(store.history))
	}

	if store.history[0].OperationID != "op-1" || store.history[1].OperationID != "op-2" {
		t.Fatalf("expected sorted history after refresh, got %+v", store.history)
	}

	if store.nodeStatusRevisions["alpha-1"] != 7 || store.nodeStatusRevisions["alpha-2"] != 8 {
		t.Fatalf("unexpected node status revisions: %+v", store.nodeStatusRevisions)
	}

	if store.leaderLease.LeaderNode != "alpha-1" || store.leaderLease.Term != 2 {
		t.Fatalf("unexpected cached leader lease: %+v", store.leaderLease)
	}

	if store.clusterStatus == nil || store.clusterStatus.ClusterName != "alpha" {
		t.Fatalf("expected observed cluster status after refresh, got %+v", store.clusterStatus)
	}

	if store.clusterStatus.Phase != cluster.ClusterPhaseMaintenance {
		t.Fatalf("unexpected cluster phase after refresh: %+v", store.clusterStatus)
	}

	if !store.lastDCSSeenAt.Equal(now) || !store.cacheRefreshedAt.Equal(now) || store.cacheDirty {
		t.Fatalf("unexpected cache timestamps: last=%v refreshed=%v dirty=%t", store.lastDCSSeenAt, store.cacheRefreshedAt, store.cacheDirty)
	}
}

func TestApplyWatchEventUpdatesAndDeletesCachedState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 11, 0, 0, 0, time.UTC)
	store, _ := newFixtureControlPlaneStore(t, "alpha", now)
	store.cacheRefreshedAt = now.Add(-time.Minute)
	store.clusterSpec = &cluster.ClusterSpec{ClusterName: "alpha"}
	store.clusterSpecRevision = 1
	store.maintenanceRevision = 1
	store.activeOpRevision = 1

	spec := cluster.ClusterSpec{
		ClusterName: "alpha",
		Maintenance: cluster.MaintenanceDesiredState{
			Enabled:       true,
			DefaultReason: "planned maintenance",
		},
		Members: []cluster.MemberSpec{{Name: "alpha-1"}},
	}
	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.Config(),
		Revision: 2,
		Value:    mustMarshalControlPlaneJSON(t, spec),
	}); err != nil {
		t.Fatalf("apply cluster spec watch: %v", err)
	}

	if store.clusterSpec == nil || len(store.clusterSpec.Members) != 1 {
		t.Fatalf("unexpected watched cluster spec: %+v", store.clusterSpec)
	}

	maintenance := cluster.MaintenanceModeStatus{Enabled: true, UpdatedAt: now}
	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.Maintenance(),
		Revision: 3,
		Value:    mustMarshalControlPlaneJSON(t, maintenance),
	}); err != nil {
		t.Fatalf("apply maintenance watch: %v", err)
	}

	if !store.maintenance.Enabled || store.maintenanceRevision != 3 {
		t.Fatalf("unexpected watched maintenance: %+v revision=%d", store.maintenance, store.maintenanceRevision)
	}

	operation := cluster.Operation{
		ID:          "sw-1",
		Kind:        cluster.OperationKindSwitchover,
		State:       cluster.OperationStateAccepted,
		Result:      cluster.OperationResultPending,
		RequestedAt: now,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
	}
	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.Operation(),
		Revision: 4,
		Value:    mustMarshalControlPlaneJSON(t, operation),
	}); err != nil {
		t.Fatalf("apply operation watch: %v", err)
	}

	if store.activeOperation == nil || store.activeOperation.ID != "sw-1" {
		t.Fatalf("unexpected watched operation: %+v", store.activeOperation)
	}

	registration := MemberRegistration{
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.11:8080",
		ControlAddress: "10.0.0.11:9090",
		RegisteredAt:   now,
	}
	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.Member("alpha-1"),
		Revision: 5,
		Value:    mustMarshalControlPlaneJSON(t, registration),
	}); err != nil {
		t.Fatalf("apply registration watch: %v", err)
	}

	if got := store.registrations["alpha-1"]; got.NodeName != "alpha-1" {
		t.Fatalf("expected watched registration fallback node name, got %+v", got)
	}

	status := readyPrimaryStatus("alpha-1", now, 19)
	status.NodeName = ""
	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.Status("alpha-1"),
		Revision: 6,
		Value:    mustMarshalControlPlaneJSON(t, status),
	}); err != nil {
		t.Fatalf("apply node status watch: %v", err)
	}

	if got := store.nodeStatuses["alpha-1"]; got.NodeName != "alpha-1" || store.nodeStatusRevisions["alpha-1"] != 6 {
		t.Fatalf("unexpected watched node status: %+v revisions=%+v", got, store.nodeStatusRevisions)
	}

	entry := cluster.HistoryEntry{
		Kind:       cluster.OperationKindSwitchover,
		Result:     cluster.OperationResultSucceeded,
		FinishedAt: now,
	}
	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.History("sw-1"),
		Revision: 7,
		Value:    mustMarshalControlPlaneJSON(t, entry),
	}); err != nil {
		t.Fatalf("apply history watch: %v", err)
	}

	if len(store.history) != 1 || store.history[0].OperationID != "sw-1" {
		t.Fatalf("unexpected watched history: %+v", store.history)
	}

	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventDelete,
		Key:      store.keyspace.Operation(),
		Revision: 8,
	}); err != nil {
		t.Fatalf("apply operation delete watch: %v", err)
	}

	if store.activeOperation != nil || store.activeOpRevision != 8 {
		t.Fatalf("expected watched delete to clear active operation, got %+v revision=%d", store.activeOperation, store.activeOpRevision)
	}

	if err := store.applyWatchEvent(dcs.WatchEvent{
		Type:     dcs.EventExpired,
		Key:      store.keyspace.Status("alpha-1"),
		Revision: 9,
	}); err != nil {
		t.Fatalf("apply status expiration watch: %v", err)
	}

	if _, ok := store.nodeStatuses["alpha-1"]; ok || store.nodeStatusRevisions["alpha-1"] != 9 {
		t.Fatalf("expected watched status delete to clear node status, got statuses=%+v revisions=%+v", store.nodeStatuses, store.nodeStatusRevisions)
	}

	store.clusterSpecRevision = 10
	if err := store.applyClusterSpecEventLocked(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      store.keyspace.Config(),
		Revision: 5,
		Value:    mustMarshalControlPlaneJSON(t, cluster.ClusterSpec{ClusterName: "ignored"}),
	}); err != nil {
		t.Fatalf("apply stale cluster spec watch: %v", err)
	}

	if store.clusterSpec.ClusterName != "alpha" {
		t.Fatalf("expected stale cluster spec event to be ignored, got %+v", store.clusterSpec)
	}
}

func TestApplyWatchEventMarksCacheDirtyForInitialAndInvalidStates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC)

	t.Run("cache not yet initialized", func(t *testing.T) {
		store, _ := newFixtureControlPlaneStore(t, "alpha", now)

		if err := store.applyWatchEvent(dcs.WatchEvent{Type: dcs.EventPut, Key: store.keyspace.Config()}); err != nil {
			t.Fatalf("apply pre-refresh watch: %v", err)
		}

		if !store.cacheDirty {
			t.Fatal("expected pre-refresh watch to mark cache dirty")
		}
	})

	t.Run("invalid watch payload", func(t *testing.T) {
		store, _ := newFixtureControlPlaneStore(t, "alpha", now)
		store.cacheRefreshedAt = now.Add(-time.Minute)

		err := store.applyWatchEvent(dcs.WatchEvent{
			Type:     dcs.EventPut,
			Key:      store.keyspace.Config(),
			Revision: 2,
			Value:    []byte("{"),
		})
		if err == nil || !strings.Contains(err.Error(), "decode watched") {
			t.Fatalf("unexpected invalid watch error: %v", err)
		}

		if !store.cacheDirty {
			t.Fatal("expected invalid watch payload to invalidate cache")
		}
	})
}

func TestPublisherViewsReturnZeroWhenRefreshFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 13, 0, 0, 0, time.UTC)
	store, backend := newFixtureControlPlaneStore(t, "alpha", now)
	backend.listErr[store.keyspace.MembersPrefix()] = errors.New("list members failed")
	store.cacheDirty = true

	if _, ok := store.RegisteredMember("alpha-1"); ok {
		t.Fatal("expected registered member lookup to fail closed on refresh error")
	}

	if members := store.RegisteredMembers(); members != nil {
		t.Fatalf("expected registered members to be nil on refresh error, got %+v", members)
	}

	if _, ok := store.ClusterSpec(); ok {
		t.Fatal("expected cluster spec lookup to fail closed on refresh error")
	}

	if _, ok := store.NodeStatus("alpha-1"); ok {
		t.Fatal("expected node status lookup to fail closed on refresh error")
	}

	if statuses := store.NodeStatuses(); statuses != nil {
		t.Fatalf("expected node statuses to be nil on refresh error, got %+v", statuses)
	}

	if truth := store.SourceOfTruth(); (truth != ClusterSourceOfTruth{}) {
		t.Fatalf("expected empty source of truth on refresh error, got %+v", truth)
	}

	if _, ok := store.Member("alpha-1"); ok {
		t.Fatal("expected member lookup to fail closed on refresh error")
	}

	if members := store.Members(); members != nil {
		t.Fatalf("expected members to be nil on refresh error, got %+v", members)
	}

	if _, ok := store.Leader(); ok {
		t.Fatal("expected leader lookup to fail closed on refresh error")
	}
}

func TestCancelSwitchoverPropagatesPersistenceErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 14, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover:  cluster.SwitchoverPolicy{AllowScheduled: true},
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 20),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 20, 0),
	})
	setTestNow(store, func() time.Time { return now })

	intent, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"})
	if err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	wantErr := errors.New("persist cancelled switchover")
	store.dcs = &failingControlPlaneDCS{
		DCS: store.dcs,
		failSetOnCall: map[string]map[int]error{
			store.keyspace.History(intent.Operation.ID): {1: wantErr},
		},
	}

	_, err = store.CancelSwitchover(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected switchover cancellation persistence error: got %v want %v", err, wantErr)
	}
}

func TestControlPlaneLoadHelpersPropagateDecodeAndBackendErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 15, 0, 0, 0, time.UTC)

	t.Run("member registration decode error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.lists[store.keyspace.MembersPrefix()] = []dcs.KeyValue{
			{Key: store.keyspace.Member("alpha-1"), Value: []byte("{")},
		}

		_, err := store.loadRegistrations(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode member registration") {
			t.Fatalf("unexpected registration decode error: %v", err)
		}
	})

	t.Run("node status decode error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.lists[store.keyspace.StatusPrefix()] = []dcs.KeyValue{
			{Key: store.keyspace.Status("alpha-1"), Value: []byte("{")},
		}

		_, _, err := store.loadNodeStatuses(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode node status") {
			t.Fatalf("unexpected node status decode error: %v", err)
		}
	})

	t.Run("cluster spec decode error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.gets[store.keyspace.Config()] = dcs.KeyValue{Key: store.keyspace.Config(), Revision: 1, Value: []byte("{")}

		_, _, err := store.loadClusterSpec(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode cluster spec") {
			t.Fatalf("unexpected cluster spec decode error: %v", err)
		}
	})

	t.Run("maintenance decode error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.gets[store.keyspace.Maintenance()] = dcs.KeyValue{Key: store.keyspace.Maintenance(), Revision: 1, Value: []byte("{")}

		_, _, err := store.loadMaintenance(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode maintenance status") {
			t.Fatalf("unexpected maintenance decode error: %v", err)
		}
	})

	t.Run("active operation decode error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.gets[store.keyspace.Operation()] = dcs.KeyValue{Key: store.keyspace.Operation(), Revision: 1, Value: []byte("{")}

		_, _, err := store.loadActiveOperation(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode active operation") {
			t.Fatalf("unexpected active operation decode error: %v", err)
		}
	})

	t.Run("history decode error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.lists[store.keyspace.HistoryPrefix()] = []dcs.KeyValue{
			{Key: store.keyspace.History("op-1"), Value: []byte("{")},
		}

		_, err := store.loadHistory(context.Background())
		if err == nil || !strings.Contains(err.Error(), "decode history entry") {
			t.Fatalf("unexpected history decode error: %v", err)
		}
	})

	t.Run("leader lease backend error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		backend.leaderErr = errors.New("leader read failed")

		_, err := store.loadLeaderLease(context.Background())
		if err == nil || !strings.Contains(err.Error(), "load leader lease") {
			t.Fatalf("unexpected leader lease load error: %v", err)
		}
	})
}

func TestControlPlaneCompareAndStoreHistoryAndPersistenceHelpers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 15, 30, 0, 0, time.UTC)

	t.Run("compare and store set branch plus revision helpers", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)

		if err := store.compareAndStoreJSON(context.Background(), store.keyspace.Config(), -1, cluster.ClusterSpec{ClusterName: "alpha"}); err != nil {
			t.Fatalf("compareAndStoreJSON set branch: %v", err)
		}

		if backend.setCalls[store.keyspace.Config()] != 1 || store.clusterSpecRevision != 1 {
			t.Fatalf("unexpected set branch bookkeeping: setCalls=%+v configRevision=%d", backend.setCalls, store.clusterSpecRevision)
		}

		store.bumpLocalRevision(store.keyspace.Maintenance())
		store.setLocalRevision(store.keyspace.Operation(), 9)
		store.clearLocalRevision(store.keyspace.Config())
		store.clearLocalRevision(store.keyspace.Maintenance())

		if store.clusterSpecRevision != -1 || store.maintenanceRevision != -1 || store.activeOpRevision != 9 {
			t.Fatalf("unexpected revision helper state: config=%d maintenance=%d operation=%d", store.clusterSpecRevision, store.maintenanceRevision, store.activeOpRevision)
		}
	})

	t.Run("compare and store compare-and-set error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		wantErr := errors.New("compare failed")
		backend.compareErr[store.keyspace.Maintenance()] = wantErr

		err := store.compareAndStoreJSON(context.Background(), store.keyspace.Maintenance(), 3, cluster.MaintenanceModeStatus{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected compare-and-set error: got %v want %v", err, wantErr)
		}
	})

	t.Run("compare and store compare-and-set success", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)

		if err := store.compareAndStoreJSON(context.Background(), store.keyspace.Maintenance(), 3, cluster.MaintenanceModeStatus{Enabled: true}); err != nil {
			t.Fatalf("compareAndStoreJSON compare branch: %v", err)
		}

		if backend.compareCalls[store.keyspace.Maintenance()] != 1 || store.maintenanceRevision != 4 {
			t.Fatalf("unexpected compare branch bookkeeping: compareCalls=%+v maintenanceRevision=%d", backend.compareCalls, store.maintenanceRevision)
		}
	})

	t.Run("upsert history entry replaces and sorts", func(t *testing.T) {
		store, _ := newFixtureControlPlaneStore(t, "alpha", now)
		store.history = []cluster.HistoryEntry{
			{OperationID: "op-2", FinishedAt: now.Add(-time.Minute)},
			{OperationID: "op-1", FinishedAt: now},
		}

		store.upsertHistoryEntryLocked(cluster.HistoryEntry{OperationID: "op-1", FinishedAt: now.Add(-2 * time.Minute)})
		store.upsertHistoryEntryLocked(cluster.HistoryEntry{OperationID: "op-3", FinishedAt: now.Add(-30 * time.Second)})

		gotIDs := []string{store.history[0].OperationID, store.history[1].OperationID, store.history[2].OperationID}
		wantIDs := []string{"op-1", "op-2", "op-3"}
		if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
			t.Fatalf("unexpected sorted history ids: got %v want %v", gotIDs, wantIDs)
		}
	})

	t.Run("persist node statuses propagates second write error", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		statusOne := readyPrimaryStatus("alpha-1", now, 20)
		statusTwo := readyStandbyStatus("alpha-2", now, 20, 0)
		wantErr := errors.New("persist second node status")
		backend.setErr[store.keyspace.Status("alpha-2")] = wantErr

		err := store.persistNodeStatuses(context.Background(), statusOne, statusTwo)
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected persist node statuses error: got %v want %v", err, wantErr)
		}
	})
}

func TestControlPlaneDeleteAndErrorHelperBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 15, 16, 0, 0, 0, time.UTC)

	t.Run("delete branches clear cached state", func(t *testing.T) {
		store, _ := newFixtureControlPlaneStore(t, "alpha", now)
		store.registrations["alpha-1"] = MemberRegistration{NodeName: "alpha-1"}
		store.nodeStatuses["alpha-1"] = readyPrimaryStatus("alpha-1", now, 20)
		store.nodeStatusRevisions["alpha-1"] = 7
		store.history = []cluster.HistoryEntry{{OperationID: "op-1", FinishedAt: now}}
		store.activeOperation = &cluster.Operation{ID: "op-1", Kind: cluster.OperationKindSwitchover}
		store.activeOpRevision = 5
		store.maintenance = cluster.MaintenanceModeStatus{Enabled: true, UpdatedAt: now}
		store.maintenanceRevision = 4

		if err := store.applyMaintenanceEventLocked(dcs.WatchEvent{Type: dcs.EventDelete, Key: store.keyspace.Maintenance()}); err != nil {
			t.Fatalf("delete maintenance event: %v", err)
		}

		if store.maintenance.Enabled || store.maintenanceRevision != -1 {
			t.Fatalf("expected maintenance delete to reset state, got %+v revision=%d", store.maintenance, store.maintenanceRevision)
		}

		if err := store.applyOperationEventLocked(dcs.WatchEvent{Type: dcs.EventDelete, Key: store.keyspace.Operation()}); err != nil {
			t.Fatalf("delete operation event: %v", err)
		}

		if store.activeOperation != nil || store.activeOpRevision != -1 {
			t.Fatalf("expected operation delete to clear active state, got %+v revision=%d", store.activeOperation, store.activeOpRevision)
		}

		if err := store.applyMemberRegistrationEventLocked(dcs.WatchEvent{Type: dcs.EventDelete, Key: store.keyspace.Member("alpha-1")}); err != nil {
			t.Fatalf("delete registration event: %v", err)
		}

		if _, ok := store.registrations["alpha-1"]; ok {
			t.Fatalf("expected registration delete to remove member, got %+v", store.registrations)
		}

		if err := store.applyNodeStatusEventLocked(dcs.WatchEvent{Type: dcs.EventDelete, Key: store.keyspace.Status("alpha-1")}); err != nil {
			t.Fatalf("delete node status event: %v", err)
		}

		if _, ok := store.nodeStatuses["alpha-1"]; ok {
			t.Fatalf("expected node status delete to remove cached status, got %+v", store.nodeStatuses)
		}
		if _, ok := store.nodeStatusRevisions["alpha-1"]; ok {
			t.Fatalf("expected zero-revision delete to clear cached revision, got %+v", store.nodeStatusRevisions)
		}

		if err := store.applyHistoryEventLocked(dcs.WatchEvent{Type: dcs.EventDelete, Key: store.keyspace.History("op-1")}); err != nil {
			t.Fatalf("delete history event: %v", err)
		}

		if len(store.history) != 0 {
			t.Fatalf("expected history delete to clear cached history, got %+v", store.history)
		}
	})

	t.Run("set get sync and consume watch error paths", func(t *testing.T) {
		store, backend := newFixtureControlPlaneStore(t, "alpha", now)
		setErr := errors.New("set failed")
		getErr := errors.New("get failed")
		leaderErr := errors.New("leader failed")
		backend.setErr[store.keyspace.Config()] = setErr
		backend.getErr[store.keyspace.Config()] = getErr
		backend.leaderErr = leaderErr

		if err := store.setJSON(context.Background(), store.keyspace.Config(), cluster.ClusterSpec{ClusterName: "alpha"}); !errors.Is(err, setErr) {
			t.Fatalf("unexpected setJSON error: got %v want %v", err, setErr)
		}

		if _, _, err := store.getKey(context.Background(), store.keyspace.Config()); !errors.Is(err, getErr) {
			t.Fatalf("unexpected getKey error: got %v want %v", err, getErr)
		}

		if err := store.syncLeaderLease(context.Background()); !errors.Is(err, leaderErr) {
			t.Fatalf("unexpected syncLeaderLease error: got %v want %v", err, leaderErr)
		}

		keyspace, err := dcs.NewKeySpace("alpha")
		if err != nil {
			t.Fatalf("keyspace: %v", err)
		}

		events := make(chan dcs.WatchEvent, 1)
		events <- dcs.WatchEvent{
			Type:  dcs.EventPut,
			Key:   keyspace.Member("alpha-1"),
			Value: mustMarshalControlPlaneJSON(t, MemberRegistration{NodeName: "alpha-1"}),
		}
		close(events)

		watchBackend := &scriptedWatchDCS{
			results: []watchResult{
				{events: events},
				{err: dcs.ErrBackendUnavailable},
			},
		}

		store = &MemoryStateStore{
			dcs:                 watchBackend,
			keyspace:            keyspace,
			logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
			registrations:       make(map[string]MemberRegistration),
			nodeStatuses:        make(map[string]agentmodel.NodeStatus),
			nodeStatusRevisions: make(map[string]int64),
			clusterSpec:         &cluster.ClusterSpec{ClusterName: "alpha"},
			now:                 func() time.Time { return now },
			cacheRefreshedAt:    now.Add(-time.Minute),
		}

		done := make(chan struct{})
		go func() {
			store.consumeCacheWatch(keyspace.Root(), events)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for consumeCacheWatch to stop")
		}

		if watchBackend.watchCalls() != 2 {
			t.Fatalf("unexpected watch retry count: got %d want %d", watchBackend.watchCalls(), 2)
		}

		if _, ok := store.registrations["alpha-1"]; !ok || !store.cacheDirty {
			t.Fatalf("expected consumeCacheWatch to apply event and invalidate cache on stream close, got registrations=%+v dirty=%t", store.registrations, store.cacheDirty)
		}
	})
}

type fixtureControlPlaneDCS struct {
	mu              sync.Mutex
	lists           map[string][]dcs.KeyValue
	listErr         map[string]error
	gets            map[string]dcs.KeyValue
	getErr          map[string]error
	setErr          map[string]error
	compareErr      map[string]error
	setCalls        map[string]int
	compareCalls    map[string]int
	leaderLease     dcs.LeaderLease
	leaderOK        bool
	leaderErr       error
	watchErr        error
	initializeErr   error
	initializeCalls int
	watchCalls      int
}

func (backend *fixtureControlPlaneDCS) Get(_ context.Context, key string) (dcs.KeyValue, error) {
	if err, ok := backend.getErr[key]; ok {
		return dcs.KeyValue{}, err
	}

	value, ok := backend.gets[key]
	if !ok {
		return dcs.KeyValue{}, dcs.ErrKeyNotFound
	}

	return value.Clone(), nil
}

func (backend *fixtureControlPlaneDCS) Set(_ context.Context, key string, _ []byte, _ ...dcs.SetOption) error {
	if len(backend.setCalls) == 0 {
		backend.setCalls = make(map[string]int)
	}
	if len(backend.setErr) == 0 {
		backend.setErr = make(map[string]error)
	}
	return backend.setWithKey(key)
}

func (backend *fixtureControlPlaneDCS) setWithKey(key string) error {
	backend.setCalls[key]++
	if err, ok := backend.setErr[key]; ok {
		return err
	}
	return nil
}

func (backend *fixtureControlPlaneDCS) CompareAndSet(_ context.Context, key string, _ []byte, _ int64) error {
	if len(backend.compareCalls) == 0 {
		backend.compareCalls = make(map[string]int)
	}
	if len(backend.compareErr) == 0 {
		backend.compareErr = make(map[string]error)
	}
	backend.compareCalls[key]++
	if err, ok := backend.compareErr[key]; ok {
		return err
	}
	return nil
}

func (backend *fixtureControlPlaneDCS) Delete(context.Context, string) error {
	return nil
}

func (backend *fixtureControlPlaneDCS) List(_ context.Context, prefix string) ([]dcs.KeyValue, error) {
	if err, ok := backend.listErr[prefix]; ok {
		return nil, err
	}

	values := backend.lists[prefix]
	cloned := make([]dcs.KeyValue, len(values))
	for index, value := range values {
		cloned[index] = value.Clone()
	}
	return cloned, nil
}

func (backend *fixtureControlPlaneDCS) Campaign(context.Context, string) (dcs.LeaderLease, bool, error) {
	return dcs.LeaderLease{}, false, nil
}

func (backend *fixtureControlPlaneDCS) Leader(context.Context) (dcs.LeaderLease, bool, error) {
	if backend.leaderErr != nil {
		return dcs.LeaderLease{}, false, backend.leaderErr
	}

	return backend.leaderLease.Clone(), backend.leaderOK, nil
}

func (backend *fixtureControlPlaneDCS) Resign(context.Context) error {
	return nil
}

func (backend *fixtureControlPlaneDCS) Touch(context.Context, string) error {
	return nil
}

func (backend *fixtureControlPlaneDCS) Alive(context.Context, string) (bool, error) {
	return false, nil
}

func (backend *fixtureControlPlaneDCS) Watch(context.Context, string) (<-chan dcs.WatchEvent, error) {
	backend.mu.Lock()
	backend.watchCalls++
	backend.mu.Unlock()
	return nil, backend.watchErr
}

func (backend *fixtureControlPlaneDCS) watchCount() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.watchCalls
}

func (backend *fixtureControlPlaneDCS) Initialize(context.Context) error {
	backend.initializeCalls++
	return backend.initializeErr
}

func (backend *fixtureControlPlaneDCS) Close() error {
	return nil
}

func newFixtureControlPlaneStore(t *testing.T, clusterName string, now time.Time) (*MemoryStateStore, *fixtureControlPlaneDCS) {
	t.Helper()

	keyspace, err := dcs.NewKeySpace(clusterName)
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}

	backend := &fixtureControlPlaneDCS{
		lists:        make(map[string][]dcs.KeyValue),
		listErr:      make(map[string]error),
		gets:         make(map[string]dcs.KeyValue),
		getErr:       make(map[string]error),
		setErr:       make(map[string]error),
		compareErr:   make(map[string]error),
		setCalls:     make(map[string]int),
		compareCalls: make(map[string]int),
	}

	store := &MemoryStateStore{
		dcs:                 backend,
		keyspace:            keyspace,
		clusterName:         clusterName,
		logger:              slog.Default(),
		registrations:       make(map[string]MemberRegistration),
		nodeStatuses:        make(map[string]agentmodel.NodeStatus),
		nodeStatusRevisions: make(map[string]int64),
		now:                 func() time.Time { return now },
		leaseDuration:       time.Minute,
		cacheDirty:          true,
		clusterSpecRevision: -1,
		maintenanceRevision: -1,
		activeOpRevision:    -1,
	}

	return store, backend
}

func mustMarshalControlPlaneJSON(t *testing.T, value any) []byte {
	t.Helper()

	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test payload: %v", err)
	}

	return payload
}
