package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/polkiloo/pacman/internal/dcs"
)

const watchBufferSize = 16

// Backend implements the DCS interface on top of an embedded HashiCorp Raft
// node.
type Backend struct {
	config    Config
	raft      *hraft.Raft
	fsm       *fsm
	transport *hraft.NetworkTransport
	store     *raftboltdb.BoltStore
	snapshots *hraft.FileSnapshotStore
	watchers  *watchBroker

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

var _ dcs.DCS = (*Backend)(nil)

// New constructs an embedded Raft DCS backend.
func New(config Config) (*Backend, error) {
	defaulted := config.WithDefaults()
	if err := defaulted.Validate(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(defaulted.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft data dir: %w", err)
	}

	snapshotDir := filepath.Join(defaulted.DataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft snapshot dir: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(defaulted.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("open raft bolt store: %w", err)
	}

	transport, err := newTransport(defaulted)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("create raft transport: %w", err)
	}

	snapshots, err := hraft.NewFileSnapshotStoreWithLogger(snapshotDir, defaulted.SnapshotRetention, hclog.NewNullLogger())
	if err != nil {
		_ = transport.Close()
		_ = store.Close()
		return nil, fmt.Errorf("create raft snapshot store: %w", err)
	}

	stopCh := make(chan struct{})
	watchers := newWatchBroker(stopCh)
	fsm := newFSM(watchers)

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(strings.TrimSpace(defaulted.BindAddress))
	raftConfig.HeartbeatTimeout = defaulted.HeartbeatTimeout
	raftConfig.ElectionTimeout = defaulted.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = defaulted.LeaderLeaseTimeout
	raftConfig.SnapshotInterval = defaulted.SnapshotInterval
	raftConfig.SnapshotThreshold = defaulted.SnapshotThreshold
	raftConfig.TrailingLogs = defaulted.TrailingLogs
	raftConfig.Logger = hclog.NewNullLogger()

	node, err := hraft.NewRaft(raftConfig, fsm, store, store, snapshots, transport)
	if err != nil {
		_ = transport.Close()
		_ = store.Close()
		return nil, fmt.Errorf("create raft node: %w", err)
	}

	backend := &Backend{
		config:    defaulted,
		raft:      node,
		fsm:       fsm,
		transport: transport,
		store:     store,
		snapshots: snapshots,
		watchers:  watchers,
		stopCh:    stopCh,
		doneCh:    make(chan struct{}),
	}

	go backend.expireLoop()

	return backend, nil
}

// Initialize bootstraps a new embedded Raft cluster when requested. Existing
// state is left untouched.
func (backend *Backend) Initialize(ctx context.Context) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	hasState, err := hraft.HasExistingState(backend.store, backend.store, backend.snapshots)
	if err != nil {
		return fmt.Errorf("inspect raft state: %w", err)
	}

	if !hasState && backend.config.Bootstrap {
		future := backend.raft.BootstrapCluster(backend.bootstrapConfiguration())
		if err := future.Error(); err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
			return backend.mapRaftError(err)
		}
	}

	if backend.config.Bootstrap && backend.isSingleNode() {
		return backend.waitForLocalLeader(ctx)
	}

	return nil
}

// Close releases backend resources and closes active watches.
func (backend *Backend) Close() error {
	var closeErr error

	backend.closeOnce.Do(func() {
		backend.mu.Lock()
		backend.closed = true
		close(backend.stopCh)
		backend.mu.Unlock()

		<-backend.doneCh

		var errs []error

		if backend.raft != nil {
			if err := backend.raft.Shutdown().Error(); err != nil && !errors.Is(err, hraft.ErrRaftShutdown) {
				errs = append(errs, err)
			}
		}

		if backend.transport != nil {
			if err := backend.transport.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if backend.store != nil {
			if err := backend.store.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		backend.watchers.closeAll()
		closeErr = errors.Join(errs...)
	})

	return closeErr
}

// Get reads the current value for a key.
func (backend *Backend) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	if err := backend.prepareRead(ctx); err != nil {
		return dcs.KeyValue{}, err
	}

	current, ok := backend.fsm.Get(strings.TrimSpace(key), backend.config.nowUTC())
	if !ok {
		return dcs.KeyValue{}, dcs.ErrKeyNotFound
	}

	return current, nil
}

// Set writes a key unconditionally.
func (backend *Backend) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	applied := dcs.ApplySetOptions(options...)
	_, err := backend.apply(ctx, command{
		Type:  commandSet,
		Key:   key,
		Value: append([]byte(nil), value...),
		TTL:   applied.TTL,
		Now:   backend.config.nowUTC(),
	})
	return err
}

// CompareAndSet writes a key only if the expected revision still matches.
func (backend *Backend) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	_, err := backend.apply(ctx, command{
		Type:             commandCompareAndSet,
		Key:              key,
		Value:            append([]byte(nil), value...),
		ExpectedRevision: expectedRevision,
		Now:              backend.config.nowUTC(),
	})
	return err
}

// Delete removes a key.
func (backend *Backend) Delete(ctx context.Context, key string) error {
	_, err := backend.apply(ctx, command{
		Type: commandDelete,
		Key:  key,
		Now:  backend.config.nowUTC(),
	})
	return err
}

// List returns all visible keys under a prefix.
func (backend *Backend) List(ctx context.Context, prefix string) ([]dcs.KeyValue, error) {
	if err := backend.prepareRead(ctx); err != nil {
		return nil, err
	}

	return backend.fsm.List(prefix, backend.config.nowUTC()), nil
}

// Campaign attempts to acquire or renew the logical PACMAN leader lease.
func (backend *Backend) Campaign(ctx context.Context, candidate string) (dcs.LeaderLease, bool, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	if backend.raft.State() != hraft.Leader {
		lease, _ := backend.fsm.Leader(backend.config.nowUTC())
		return lease, false, nil
	}

	response, err := backend.apply(ctx, command{
		Type:      commandCampaign,
		Candidate: candidate,
		TTL:       backend.config.TTL,
		Now:       backend.config.nowUTC(),
	})
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}

	result, ok := response.(campaignResult)
	if !ok {
		return dcs.LeaderLease{}, false, fmt.Errorf("unexpected campaign response type %T", response)
	}

	return result.Lease, result.Held, nil
}

// Leader returns the current logical PACMAN leader lease, if any.
func (backend *Backend) Leader(ctx context.Context) (dcs.LeaderLease, bool, error) {
	if err := backend.prepareRead(ctx); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	lease, ok := backend.fsm.Leader(backend.config.nowUTC())
	return lease, ok, nil
}

// Resign releases the logical PACMAN leader lease.
func (backend *Backend) Resign(ctx context.Context) error {
	_, err := backend.apply(ctx, command{
		Type: commandResign,
		Now:  backend.config.nowUTC(),
	})
	return err
}

// Touch refreshes a session TTL for a member.
func (backend *Backend) Touch(ctx context.Context, member string) error {
	_, err := backend.apply(ctx, command{
		Type:   commandTouch,
		Member: member,
		TTL:    backend.config.TTL,
		Now:    backend.config.nowUTC(),
	})
	return err
}

// Alive reports whether a member session is still live.
func (backend *Backend) Alive(ctx context.Context, member string) (bool, error) {
	if err := backend.prepareRead(ctx); err != nil {
		return false, err
	}

	return backend.fsm.Alive(member, backend.config.nowUTC()), nil
}

// Watch subscribes to changes under a key prefix.
func (backend *Backend) Watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return nil, err
	}

	return backend.watchers.watch(ctx, prefix)
}

func (backend *Backend) prepareRead(ctx context.Context) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	if backend.raft.State() != hraft.Leader {
		return nil
	}

	return backend.verifyLeader()
}

func (backend *Backend) verifyLeader() error {
	if backend.raft.State() != hraft.Leader {
		return dcs.ErrNotLeader
	}

	if err := backend.raft.VerifyLeader().Error(); err != nil {
		return backend.mapRaftError(err)
	}

	if err := backend.raft.Barrier(backend.config.ApplyTimeout).Error(); err != nil {
		return backend.mapRaftError(err)
	}

	return nil
}

func (backend *Backend) apply(ctx context.Context, cmd command) (interface{}, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return nil, err
	}

	if backend.raft.State() != hraft.Leader {
		return nil, dcs.ErrNotLeader
	}

	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("encode raft command: %w", err)
	}

	future := backend.raft.Apply(payload, backend.config.ApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, backend.mapRaftError(err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok {
		return nil, responseErr
	}

	return response, nil
}

func (backend *Backend) bootstrapConfiguration() hraft.Configuration {
	servers := make([]hraft.Server, 0, len(backend.config.Peers))
	seen := make(map[string]struct{}, len(backend.config.Peers))

	for _, peer := range backend.config.Peers {
		trimmed := strings.TrimSpace(peer)
		if trimmed == "" {
			continue
		}

		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}

		servers = append(servers, hraft.Server{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(trimmed),
			Address:  hraft.ServerAddress(trimmed),
		})
	}

	return hraft.Configuration{Servers: servers}
}

func (backend *Backend) isSingleNode() bool {
	self := strings.TrimSpace(backend.config.BindAddress)
	peers := backend.bootstrapConfiguration().Servers
	return len(peers) == 1 && string(peers[0].Address) == self
}

func (backend *Backend) waitForLocalLeader(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := backend.checkAvailable(ctx); err != nil {
			return err
		}

		if backend.raft.State() == hraft.Leader {
			if err := backend.verifyLeader(); err == nil {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backend.stopCh:
			return dcs.ErrBackendUnavailable
		case <-ticker.C:
		}
	}
}

func (backend *Backend) expireLoop() {
	ticker := time.NewTicker(backend.config.ExpiryInterval)
	defer ticker.Stop()
	defer close(backend.doneCh)

	for {
		select {
		case <-ticker.C:
			backend.expireOnce()
		case <-backend.stopCh:
			return
		}
	}
}

func (backend *Backend) expireOnce() {
	if backend.raft.State() != hraft.Leader {
		return
	}

	now := backend.config.nowUTC()
	expiredKeys, expiredSessions := backend.fsm.Expired(now)
	for _, current := range expiredKeys {
		if _, err := backend.apply(context.Background(), command{
			Type:              commandExpireKey,
			Key:               current.Key,
			ExpectedRevision:  current.Revision,
			ExpectedExpiresAt: current.ExpiresAt,
			Now:               now,
		}); err != nil && !errors.Is(err, dcs.ErrNotLeader) && !errors.Is(err, dcs.ErrBackendUnavailable) {
			backend.logWarn("apply key expiration", "key", current.Key, "error", err)
		}
	}

	for _, current := range expiredSessions {
		if _, err := backend.apply(context.Background(), command{
			Type:              commandExpireSession,
			Member:            current.Member,
			ExpectedExpiresAt: current.ExpiresAt,
			Now:               now,
		}); err != nil && !errors.Is(err, dcs.ErrNotLeader) && !errors.Is(err, dcs.ErrBackendUnavailable) {
			backend.logWarn("apply session expiration", "member", current.Member, "error", err)
		}
	}
}

func (backend *Backend) checkAvailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()

	if backend.closed {
		return dcs.ErrBackendUnavailable
	}

	return nil
}

func (backend *Backend) mapRaftError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, hraft.ErrNotLeader), errors.Is(err, hraft.ErrLeadershipLost), errors.Is(err, hraft.ErrLeadershipTransferInProgress), errors.Is(err, hraft.ErrNotVoter):
		return dcs.ErrNotLeader
	case errors.Is(err, hraft.ErrRaftShutdown):
		return dcs.ErrBackendUnavailable
	default:
		return err
	}
}

func (backend *Backend) logWarn(message string, args ...any) {
	if backend.config.Logger == nil {
		return
	}

	backend.config.Logger.Warn(message, args...)
}

type watchBroker struct {
	mu       sync.Mutex
	watchers map[uint64]watchSubscription
	nextID   uint64
	closed   bool
	stopCh   <-chan struct{}
}

type watchSubscription struct {
	prefix string
	ch     chan dcs.WatchEvent
}

func newWatchBroker(stopCh <-chan struct{}) *watchBroker {
	return &watchBroker{
		watchers: make(map[uint64]watchSubscription),
		stopCh:   stopCh,
	}
}

func (broker *watchBroker) watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return nil, dcs.ErrBackendUnavailable
	}

	id := broker.nextID
	broker.nextID++
	ch := make(chan dcs.WatchEvent, watchBufferSize)
	broker.watchers[id] = watchSubscription{
		prefix: strings.TrimSpace(prefix),
		ch:     ch,
	}
	broker.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			broker.remove(id)
		case <-broker.stopCh:
			broker.remove(id)
		}
	}()

	return ch, nil
}

func (broker *watchBroker) remove(id uint64) {
	broker.mu.Lock()
	current, ok := broker.watchers[id]
	if ok {
		delete(broker.watchers, id)
	}
	broker.mu.Unlock()

	if ok {
		close(current.ch)
	}
}

func (broker *watchBroker) broadcast(event dcs.WatchEvent) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	for _, watcher := range broker.watchers {
		if !strings.HasPrefix(event.Key, watcher.prefix) {
			continue
		}

		select {
		case watcher.ch <- event.Clone():
		default:
		}
	}
}

func (broker *watchBroker) closeAll() {
	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return
	}

	broker.closed = true
	channels := make([]chan dcs.WatchEvent, 0, len(broker.watchers))
	for id, watcher := range broker.watchers {
		channels = append(channels, watcher.ch)
		delete(broker.watchers, id)
	}
	broker.mu.Unlock()

	for _, ch := range channels {
		close(ch)
	}
}
