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
		return seededPreparedRejoinStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, now, now.Add(10*time.Second))
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

		store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, now, now.Add(10*time.Second), now.Add(20*time.Second))
		if _, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{}); err != nil {
			t.Fatalf("execute standby config: %v", err)
		}

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

		store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
		}, now, now.Add(10*time.Second), now.Add(20*time.Second))
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
	setCalls      map[string]int
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
