package controlplane

import (
	"sync"
	"time"
)

func setTestNow(store *MemoryStateStore, now func() time.Time) {
	store.mu.Lock()
	store.now = now
	store.mu.Unlock()
}

func setTestLeaseDuration(store *MemoryStateStore, leaseDuration time.Duration) {
	store.mu.Lock()
	store.leaseDuration = leaseDuration
	store.mu.Unlock()
}

func setTestCacheMaxAge(store *MemoryStateStore, cacheMaxAge time.Duration) {
	store.mu.Lock()
	store.cacheMaxAge = cacheMaxAge
	store.mu.Unlock()
}

type mutableTestClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newMutableTestClock(now time.Time) *mutableTestClock {
	return &mutableTestClock{now: now}
}

func (clock *mutableTestClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

func (clock *mutableTestClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (clock *mutableTestClock) Advance(duration time.Duration) time.Time {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	now := clock.now
	clock.mu.Unlock()
	return now
}
