package controlplane

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
)

func TestCreateSwitchoverIntentPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.April, 14, 9, 0, 0, 0, time.UTC)
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
		readyPrimaryStatus("alpha-1", now, 18),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
	})
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

	wantErr := errors.New("persist switchover operation")
	store.dcs = &failingControlPlaneDCS{
		DCS: store.dcs,
		failSetOnCall: map[string]map[int]error{
			store.keyspace.Operation(): {1: wantErr},
		},
	}

	_, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected create switchover persistence error: got %v want %v", err, wantErr)
	}
}

func TestExecuteRejoinStandbyConfigPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC)

	newStore := func(t *testing.T) *MemoryStateStore {
		t.Helper()
		store, _ := preparedRejoinStoreForCoverage(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, now)
		return store
	}

	t.Run("persist active operation", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist rejoin standby operation")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected standby config operation error: got %v want %v", err, wantErr)
		}
	})
}

func TestExecuteRejoinRestartAsStandbyPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.March, 30, 10, 30, 0, 0, time.UTC)

	newStore := func(t *testing.T) *MemoryStateStore {
		t.Helper()

		store, _ := configuredRejoinStoreForCoverage(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, now)

		return store
	}

	t.Run("persist active operation", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist rejoin restart operation")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.ExecuteRejoinRestartAsStandby(context.Background(), &recordingStandbyRestarter{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected standby restart operation error: got %v want %v", err, wantErr)
		}
	})
}

func TestVerifyRejoinReplicationPropagatesPersistenceErrors(t *testing.T) {
	now := time.Date(2026, time.March, 30, 12, 30, 0, 0, time.UTC)

	newStore := func(t *testing.T) *MemoryStateStore {
		t.Helper()

		store, _ := restartedRejoinStoreForCoverage(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, now)
		publishVerifiedRejoinReplica(t, store, now.Add(30*time.Second))
		return store
	}

	t.Run("persist active operation before publication", func(t *testing.T) {
		store := newStore(t)
		wantErr := errors.New("persist verify rejoin operation")
		store.dcs = &failingControlPlaneDCS{
			DCS: store.dcs,
			failSetOnCall: map[string]map[int]error{
				store.keyspace.Operation(): {1: wantErr},
			},
		}

		_, err := store.VerifyRejoinReplication(context.Background())
		if !errors.Is(err, wantErr) {
			t.Fatalf("unexpected verify rejoin operation error: got %v want %v", err, wantErr)
		}
	})
}

type failingControlPlaneDCS struct {
	dcs.DCS
	failSetOnCall map[string]map[int]error
	failCASOnCall map[string]map[int]error
	setCalls      map[string]int
	casCalls      map[string]int
}

func (backend *failingControlPlaneDCS) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	trimmedKey := strings.TrimSpace(key)

	if backend.setCalls == nil {
		backend.setCalls = make(map[string]int)
	}
	backend.setCalls[trimmedKey]++

	if failures, ok := backend.failSetOnCall[trimmedKey]; ok {
		if err, ok := failures[backend.setCalls[trimmedKey]]; ok {
			return err
		}
	}

	return backend.DCS.Set(ctx, key, value, options...)
}

func (backend *failingControlPlaneDCS) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	trimmedKey := strings.TrimSpace(key)

	if backend.casCalls == nil {
		backend.casCalls = make(map[string]int)
	}
	backend.casCalls[trimmedKey]++

	if failures, ok := backend.failCASOnCall[trimmedKey]; ok {
		if err, ok := failures[backend.casCalls[trimmedKey]]; ok {
			return err
		}
	}

	return backend.DCS.CompareAndSet(ctx, key, value, expectedRevision)
}

func preparedRejoinStoreForCoverage(t *testing.T, spec cluster.ClusterSpec, now time.Time) (*MemoryStateStore, *mutableTestClock) {
	t.Helper()

	clock := newMutableTestClock(now)
	store := NewMemoryStateStore()
	setTestNow(store, clock.Now)
	setTestLeaseDuration(store, time.Hour)

	if _, err := store.StoreClusterSpec(context.Background(), spec); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	for _, status := range []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now.Add(-time.Minute), 10, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(-time.Minute+time.Second), 11, "sys-alpha"),
	} {
		if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
			t.Fatalf("publish node status for %q: %v", status.NodeName, err)
		}
	}

	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 7
	store.activeOperation = &cluster.Operation{
		ID:          "rejoin-prepared",
		Kind:        cluster.OperationKindRejoin,
		State:       cluster.OperationStateRunning,
		Result:      cluster.OperationResultPending,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		RequestedAt: now.Add(-15 * time.Second),
		StartedAt:   now.Add(-10 * time.Second),
		Message:     rejoinRewindCompletedMessage("alpha-1", "alpha-2"),
	}
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	clock.Advance(10 * time.Second)
	return store, clock
}

func configuredRejoinStoreForCoverage(t *testing.T, spec cluster.ClusterSpec, now time.Time) (*MemoryStateStore, *mutableTestClock) {
	t.Helper()

	store, clock := preparedRejoinStoreForCoverage(t, spec, now)
	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status")
	}

	store.mu.Lock()
	store.nodeStatuses["alpha-1"] = configuredFormerPrimaryStandbyStatus(member, now)
	store.activeOperation = &cluster.Operation{
		ID:          "rejoin-configured",
		Kind:        cluster.OperationKindRejoin,
		State:       cluster.OperationStateRunning,
		Result:      cluster.OperationResultPending,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		RequestedAt: now.Add(-15 * time.Second),
		StartedAt:   now.Add(-10 * time.Second),
		Message:     rejoinStandbyConfigCompletedMessage("alpha-1", "alpha-2"),
	}
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	clock.Advance(10 * time.Second)
	return store, clock
}

func restartedRejoinStoreForCoverage(t *testing.T, spec cluster.ClusterSpec, now time.Time) (*MemoryStateStore, *mutableTestClock) {
	t.Helper()

	store, clock := configuredRejoinStoreForCoverage(t, spec, now)
	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status")
	}

	store.mu.Lock()
	store.nodeStatuses["alpha-1"] = restartingFormerPrimaryStandbyStatus(member, now)
	store.activeOperation = &cluster.Operation{
		ID:          "rejoin-restarted",
		Kind:        cluster.OperationKindRejoin,
		State:       cluster.OperationStateRunning,
		Result:      cluster.OperationResultPending,
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
		RequestedAt: now.Add(-15 * time.Second),
		StartedAt:   now.Add(-10 * time.Second),
		Message:     rejoinRestartCompletedMessage("alpha-1", "alpha-2"),
	}
	store.refreshSourceOfTruthLocked(now)
	store.mu.Unlock()

	clock.Advance(10 * time.Second)
	return store, clock
}
