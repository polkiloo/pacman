package controlplane

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
)

const testNodeName = "alpha-1"

func TestRunCacheWatchRetriesAndInvalidatesCache(t *testing.T) {
	t.Parallel()

	keyspace, err := dcs.NewKeySpace("alpha")
	if err != nil {
		t.Fatalf("keyspace: %v", err)
	}

	closedEvents := make(chan dcs.WatchEvent)
	close(closedEvents)

	backend := &scriptedWatchDCS{
		results: []watchResult{
			{err: context.DeadlineExceeded},
			{events: closedEvents},
			{err: dcs.ErrBackendUnavailable},
		},
	}

	store := &MemoryStateStore{
		dcs:                 backend,
		keyspace:            keyspace,
		logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		registrations:       make(map[string]MemberRegistration),
		nodeStatuses:        make(map[string]agentmodel.NodeStatus),
		nodeStatusRevisions: make(map[string]int64),
		now:                 time.Now,
		cacheDirty:          false,
	}

	finished := make(chan struct{})
	go func() {
		store.runCacheWatch()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for cache watch retry loop")
	}

	if got := backend.watchCalls(); got != 3 {
		t.Fatalf("unexpected watch retry count: got %d, want %d", got, 3)
	}

	store.mu.RLock()
	cacheDirty := store.cacheDirty
	store.mu.RUnlock()
	if !cacheDirty {
		t.Fatal("expected cache watch retry loop to invalidate cache")
	}
}

func TestRemoveHistoryEntryLocked(t *testing.T) {
	t.Parallel()
	store := &MemoryStateStore{
		history: []cluster.HistoryEntry{
			{OperationID: "op-1"},
			{OperationID: "op-2"},
			{OperationID: "op-3"},
		},
	}

	store.removeHistoryEntryLocked("op-2")
	if len(store.history) != 2 || store.history[0].OperationID != "op-1" || store.history[1].OperationID != "op-3" {
		t.Fatalf("unexpected filtered history: %+v", store.history)
	}

	store.removeHistoryEntryLocked("missing")
	if len(store.history) != 2 {
		t.Fatalf("expected missing removal to leave history unchanged, got %+v", store.history)
	}
}

func TestCurrentPrimaryNameLocked(t *testing.T) {
	t.Parallel()
	store := &MemoryStateStore{
		registrations: make(map[string]MemberRegistration),
		nodeStatuses: map[string]agentmodel.NodeStatus{
			testNodeName: {
				NodeName: testNodeName,
				Role:     cluster.MemberRolePrimary,
				State:    cluster.MemberStateRunning,
				Postgres: agentmodel.PostgresStatus{Managed: true, Up: true},
			},
			"alpha-2": {
				NodeName: "alpha-2",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Postgres: agentmodel.PostgresStatus{Managed: true, Up: true},
			},
		},
	}

	if got := store.currentPrimaryNameLocked(); got != testNodeName {
		t.Fatalf("unexpected inferred primary: got %q, want %q", got, testNodeName)
	}

	store.clusterStatus = &cluster.ClusterStatus{CurrentPrimary: "beta-1"}
	if got := store.currentPrimaryNameLocked(); got != "beta-1" {
		t.Fatalf("expected cluster status primary override, got %q", got)
	}
}

func TestMemberWALLSNLocked(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		status agentmodel.NodeStatus
		want   string
	}{
		{
			name: "flush",
			status: agentmodel.NodeStatus{
				NodeName: testNodeName,
				Postgres: agentmodel.PostgresStatus{WAL: agentmodel.WALProgress{FlushLSN: "0/10"}},
			},
			want: "0/10",
		},
		{
			name: "replay fallback",
			status: agentmodel.NodeStatus{
				NodeName: testNodeName,
				Postgres: agentmodel.PostgresStatus{WAL: agentmodel.WALProgress{ReplayLSN: "0/20"}},
			},
			want: "0/20",
		},
		{
			name: "write fallback",
			status: agentmodel.NodeStatus{
				NodeName: testNodeName,
				Postgres: agentmodel.PostgresStatus{WAL: agentmodel.WALProgress{WriteLSN: "0/30"}},
			},
			want: "0/30",
		},
		{
			name: "receive fallback",
			status: agentmodel.NodeStatus{
				NodeName: testNodeName,
				Postgres: agentmodel.PostgresStatus{WAL: agentmodel.WALProgress{ReceiveLSN: "0/40"}},
			},
			want: "0/40",
		},
		{
			name: "missing",
			status: agentmodel.NodeStatus{
				NodeName: testNodeName,
			},
			want: "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := &MemoryStateStore{
				nodeStatuses: map[string]agentmodel.NodeStatus{
					testNodeName: testCase.status,
				},
			}

			if got := store.memberWALLSNLocked(testNodeName); got != testCase.want {
				t.Fatalf("unexpected wal lsn: got %q, want %q", got, testCase.want)
			}
		})
	}

	t.Run("missing node", func(t *testing.T) {
		t.Parallel()

		store := &MemoryStateStore{nodeStatuses: map[string]agentmodel.NodeStatus{}}
		if got := store.memberWALLSNLocked("missing"); got != "" {
			t.Fatalf("expected missing node wal lsn to be empty, got %q", got)
		}
	})
}

type watchResult struct {
	events <-chan dcs.WatchEvent
	err    error
}

type scriptedWatchDCS struct {
	mu      sync.Mutex
	results []watchResult
	calls   int
}

func (backend *scriptedWatchDCS) Get(context.Context, string) (dcs.KeyValue, error) {
	return dcs.KeyValue{}, dcs.ErrKeyNotFound
}

func (backend *scriptedWatchDCS) Set(context.Context, string, []byte, ...dcs.SetOption) error {
	return nil
}

func (backend *scriptedWatchDCS) CompareAndSet(context.Context, string, []byte, int64) error {
	return nil
}

func (backend *scriptedWatchDCS) Delete(context.Context, string) error {
	return nil
}

func (backend *scriptedWatchDCS) List(context.Context, string) ([]dcs.KeyValue, error) {
	return nil, nil
}

func (backend *scriptedWatchDCS) Campaign(context.Context, string) (dcs.LeaderLease, bool, error) {
	return dcs.LeaderLease{}, false, nil
}

func (backend *scriptedWatchDCS) Leader(context.Context) (dcs.LeaderLease, bool, error) {
	return dcs.LeaderLease{}, false, nil
}

func (backend *scriptedWatchDCS) Resign(context.Context) error {
	return nil
}

func (backend *scriptedWatchDCS) Touch(context.Context, string) error {
	return nil
}

func (backend *scriptedWatchDCS) Alive(context.Context, string) (bool, error) {
	return false, nil
}

func (backend *scriptedWatchDCS) Watch(context.Context, string) (<-chan dcs.WatchEvent, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()

	backend.calls++
	index := backend.calls - 1
	if index >= len(backend.results) {
		return nil, dcs.ErrBackendUnavailable
	}

	result := backend.results[index]
	return result.events, result.err
}

func (backend *scriptedWatchDCS) Initialize(context.Context) error {
	return nil
}

func (backend *scriptedWatchDCS) Close() error {
	return nil
}

func (backend *scriptedWatchDCS) watchCalls() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.calls
}
