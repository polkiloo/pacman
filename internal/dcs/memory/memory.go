package memory

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
)

const (
	DefaultTTL           = 100 * time.Millisecond
	DefaultSweepInterval = 10 * time.Millisecond
	watchBufferSize      = 16
)

// Config tunes the in-memory DCS test backend.
type Config struct {
	TTL           time.Duration
	TTLFunc       func() time.Duration
	SweepInterval time.Duration
	Now           func() time.Time
}

type store struct {
	mu            sync.RWMutex
	entries       map[string]entry
	sessions      map[string]time.Time
	leader        dcs.LeaderLease
	ttl           time.Duration
	ttlFunc       func() time.Duration
	sweepInterval time.Duration
	now           func() time.Time
	watchers      map[uint64]watcher
	nextWatcherID uint64
	closed        bool
	stopCh        chan struct{}
	doneCh        chan struct{}
}

type entry struct {
	value     dcs.KeyValue
	expiresAt time.Time
}

type watcher struct {
	prefix string
	ch     chan dcs.WatchEvent
}

var _ dcs.DCS = (*store)(nil)

// New constructs an in-memory DCS backend suitable for unit tests and shared
// conformance coverage.
func New(config Config) dcs.DCS {
	defaulted := config.withDefaults()

	backend := &store{
		entries:       make(map[string]entry),
		sessions:      make(map[string]time.Time),
		ttl:           defaulted.TTL,
		ttlFunc:       defaulted.TTLFunc,
		sweepInterval: defaulted.SweepInterval,
		now:           defaulted.Now,
		watchers:      make(map[uint64]watcher),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	go backend.expireLoop()

	return backend
}

// Initialize performs one-time backend setup. The in-memory backend is ready
// immediately, so initialization is a no-op.
func (store *store) Initialize(ctx context.Context) error {
	return store.checkAvailable(ctx)
}

// Close releases backend resources and closes active watches.
func (store *store) Close() error {
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil
	}

	store.closed = true
	close(store.stopCh)

	watcherChannels := make([]chan dcs.WatchEvent, 0, len(store.watchers))
	for id, watcher := range store.watchers {
		watcherChannels = append(watcherChannels, watcher.ch)
		delete(store.watchers, id)
	}
	store.mu.Unlock()

	<-store.doneCh

	for _, ch := range watcherChannels {
		close(ch)
	}

	return nil
}

// Get reads the current key value.
func (store *store) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	if err := store.checkAvailable(ctx); err != nil {
		return dcs.KeyValue{}, err
	}

	store.expireEntries(store.nowUTC())

	store.mu.RLock()
	defer store.mu.RUnlock()

	current, ok := store.entries[strings.TrimSpace(key)]
	if !ok {
		return dcs.KeyValue{}, dcs.ErrKeyNotFound
	}

	return current.value.Clone(), nil
}

// Set writes a key unconditionally.
func (store *store) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	if err := store.checkAvailable(ctx); err != nil {
		return err
	}

	applied := dcs.ApplySetOptions(options...)
	now := store.nowUTC()
	store.expireEntries(now)

	store.mu.Lock()
	defer store.mu.Unlock()

	revision := int64(1)
	trimmedKey := strings.TrimSpace(key)
	if current, ok := store.entries[trimmedKey]; ok {
		revision = current.value.Revision + 1
	}

	next := dcs.KeyValue{
		Key:      trimmedKey,
		Value:    append([]byte(nil), value...),
		Revision: revision,
		TTL:      applied.TTL,
	}

	currentEntry := entry{value: next}
	if applied.TTL > 0 {
		currentEntry.expiresAt = now.Add(applied.TTL)
	}

	store.entries[trimmedKey] = currentEntry
	store.broadcastLocked(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      trimmedKey,
		Value:    append([]byte(nil), value...),
		Revision: revision,
	})

	return nil
}

// CompareAndSet writes a key only if its current revision matches.
func (store *store) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	if err := store.checkAvailable(ctx); err != nil {
		return err
	}

	now := store.nowUTC()
	store.expireEntries(now)

	store.mu.Lock()
	defer store.mu.Unlock()

	trimmedKey := strings.TrimSpace(key)
	current, ok := store.entries[trimmedKey]
	if !ok || current.value.Revision != expectedRevision {
		return dcs.ErrRevisionMismatch
	}

	next := current.value.Clone()
	next.Value = append([]byte(nil), value...)
	next.Revision++
	store.entries[trimmedKey] = entry{
		value:     next,
		expiresAt: current.expiresAt,
	}

	store.broadcastLocked(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      trimmedKey,
		Value:    append([]byte(nil), value...),
		Revision: next.Revision,
	})

	return nil
}

// Delete removes a key.
func (store *store) Delete(ctx context.Context, key string) error {
	if err := store.checkAvailable(ctx); err != nil {
		return err
	}

	store.expireEntries(store.nowUTC())

	store.mu.Lock()
	defer store.mu.Unlock()

	trimmedKey := strings.TrimSpace(key)
	current, ok := store.entries[trimmedKey]
	if !ok {
		return dcs.ErrKeyNotFound
	}

	delete(store.entries, trimmedKey)
	store.broadcastLocked(dcs.WatchEvent{
		Type:     dcs.EventDelete,
		Key:      trimmedKey,
		Revision: current.value.Revision + 1,
	})

	return nil
}

// List returns all keys matching the supplied prefix.
func (store *store) List(ctx context.Context, prefix string) ([]dcs.KeyValue, error) {
	if err := store.checkAvailable(ctx); err != nil {
		return nil, err
	}

	store.expireEntries(store.nowUTC())

	store.mu.RLock()
	defer store.mu.RUnlock()

	trimmedPrefix := strings.TrimSpace(prefix)
	var listed []dcs.KeyValue
	for key, current := range store.entries {
		if strings.HasPrefix(key, trimmedPrefix) {
			listed = append(listed, current.value.Clone())
		}
	}

	slices.SortFunc(listed, func(a, b dcs.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})

	return listed, nil
}

// Campaign acquires or renews the leader lease for a candidate.
func (store *store) Campaign(ctx context.Context, candidate string) (dcs.LeaderLease, bool, error) {
	if err := store.checkAvailable(ctx); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	now := store.nowUTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.expireLeaderLocked(now)

	trimmedCandidate := strings.TrimSpace(candidate)
	if store.leader.Leader == "" {
		nextTerm := store.leader.Term + 1
		if nextTerm == 0 {
			nextTerm = 1
		}

		store.leader = dcs.LeaderLease{
			Leader:    trimmedCandidate,
			Term:      nextTerm,
			Acquired:  now,
			Renewed:   now,
			ExpiresAt: now.Add(store.ttlDuration()),
		}

		return store.leader.Clone(), true, nil
	}

	if store.leader.Leader == trimmedCandidate {
		store.leader.Renewed = now
		store.leader.ExpiresAt = now.Add(store.ttlDuration())
		return store.leader.Clone(), true, nil
	}

	return store.leader.Clone(), false, nil
}

// Leader returns the currently active leader lease.
func (store *store) Leader(ctx context.Context) (dcs.LeaderLease, bool, error) {
	if err := store.checkAvailable(ctx); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	now := store.nowUTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.expireLeaderLocked(now)
	if store.leader.Leader == "" {
		return dcs.LeaderLease{}, false, nil
	}

	return store.leader.Clone(), true, nil
}

// Resign releases the active leader lease.
func (store *store) Resign(ctx context.Context) error {
	if err := store.checkAvailable(ctx); err != nil {
		return err
	}

	now := store.nowUTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.expireLeaderLocked(now)
	if store.leader.Leader == "" {
		return dcs.ErrNoLeader
	}

	store.leader.Leader = ""
	store.leader.Acquired = time.Time{}
	store.leader.Renewed = time.Time{}
	store.leader.ExpiresAt = time.Time{}

	return nil
}

// Touch refreshes a member session TTL.
func (store *store) Touch(ctx context.Context, member string) error {
	if err := store.checkAvailable(ctx); err != nil {
		return err
	}

	now := store.nowUTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.expireSessionsLocked(now)
	store.sessions[strings.TrimSpace(member)] = now.Add(store.ttlDuration())

	return nil
}

// Alive reports whether a member session is currently live.
func (store *store) Alive(ctx context.Context, member string) (bool, error) {
	if err := store.checkAvailable(ctx); err != nil {
		return false, err
	}

	now := store.nowUTC()

	store.mu.Lock()
	defer store.mu.Unlock()

	store.expireSessionsLocked(now)
	_, ok := store.sessions[strings.TrimSpace(member)]
	return ok, nil
}

// Watch subscribes to changes under a key prefix.
func (store *store) Watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	if err := store.checkAvailable(ctx); err != nil {
		return nil, err
	}

	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil, dcs.ErrBackendUnavailable
	}

	id := store.nextWatcherID
	store.nextWatcherID++
	ch := make(chan dcs.WatchEvent, watchBufferSize)
	store.watchers[id] = watcher{
		prefix: strings.TrimSpace(prefix),
		ch:     ch,
	}
	stopCh := store.stopCh
	store.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			store.removeWatcher(id)
		case <-stopCh:
		}
	}()

	return ch, nil
}

func (config Config) withDefaults() Config {
	defaulted := config

	if defaulted.TTL <= 0 {
		defaulted.TTL = DefaultTTL
	}

	if defaulted.TTLFunc == nil {
		ttl := defaulted.TTL
		defaulted.TTLFunc = func() time.Duration {
			return ttl
		}
	}

	if defaulted.SweepInterval <= 0 {
		defaulted.SweepInterval = DefaultSweepInterval
	}

	if defaulted.Now == nil {
		defaulted.Now = time.Now
	}

	return defaulted
}

func (store *store) nowUTC() time.Time {
	return store.now().UTC()
}

func (store *store) ttlDuration() time.Duration {
	return store.ttlFunc()
}

func (store *store) checkAvailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	if store.closed {
		return dcs.ErrBackendUnavailable
	}

	return nil
}

func (store *store) expireLoop() {
	ticker := time.NewTicker(store.sweepInterval)
	defer ticker.Stop()
	defer close(store.doneCh)

	for {
		select {
		case <-ticker.C:
			now := store.nowUTC()
			store.expireEntries(now)
			store.expireSessions(now)
		case <-store.stopCh:
			return
		}
	}
}

func (store *store) expireEntries(now time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()

	for key, current := range store.entries {
		if current.expiresAt.IsZero() || now.Before(current.expiresAt) {
			continue
		}

		delete(store.entries, key)
		store.broadcastLocked(dcs.WatchEvent{
			Type:     dcs.EventExpired,
			Key:      key,
			Revision: current.value.Revision + 1,
		})
	}
}

func (store *store) expireLeaderLocked(now time.Time) {
	if store.leader.ExpiresAt.IsZero() || now.Before(store.leader.ExpiresAt) {
		return
	}

	store.leader.Leader = ""
	store.leader.Acquired = time.Time{}
	store.leader.Renewed = time.Time{}
	store.leader.ExpiresAt = time.Time{}
}

func (store *store) expireSessions(now time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.expireSessionsLocked(now)
}

func (store *store) expireSessionsLocked(now time.Time) {
	for member, expiresAt := range store.sessions {
		if now.Before(expiresAt) {
			continue
		}

		delete(store.sessions, member)
	}
}

func (store *store) removeWatcher(id uint64) {
	store.mu.Lock()
	defer store.mu.Unlock()

	current, ok := store.watchers[id]
	if !ok {
		return
	}

	delete(store.watchers, id)
	close(current.ch)
}

func (store *store) broadcastLocked(event dcs.WatchEvent) {
	for _, watcher := range store.watchers {
		if !strings.HasPrefix(event.Key, watcher.prefix) {
			continue
		}

		select {
		case watcher.ch <- event.Clone():
		default:
		}
	}
}
