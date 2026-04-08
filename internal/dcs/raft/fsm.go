package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/polkiloo/pacman/internal/dcs"
)

type commandType string

const (
	commandSet           commandType = "set"
	commandCompareAndSet commandType = "compare_and_set"
	commandDelete        commandType = "delete"
	commandExpireKey     commandType = "expire_key"
	commandCampaign      commandType = "campaign"
	commandResign        commandType = "resign"
	commandTouch         commandType = "touch"
	commandExpireSession commandType = "expire_session"
)

type command struct {
	Type              commandType   `json:"type"`
	Key               string        `json:"key,omitempty"`
	Value             []byte        `json:"value,omitempty"`
	TTL               time.Duration `json:"ttl,omitempty"`
	ExpectedRevision  int64         `json:"expectedRevision,omitempty"`
	ExpectedExpiresAt time.Time     `json:"expectedExpiresAt,omitempty"`
	Candidate         string        `json:"candidate,omitempty"`
	Member            string        `json:"member,omitempty"`
	Now               time.Time     `json:"now,omitempty"`
}

type campaignResult struct {
	Lease dcs.LeaderLease
	Held  bool
}

type snapshotState struct {
	Entries  map[string]keyState     `json:"entries"`
	Sessions map[string]sessionState `json:"sessions"`
	Leader   leaderState             `json:"leader"`
}

type keyState struct {
	Key       string        `json:"key"`
	Value     []byte        `json:"value"`
	Revision  int64         `json:"revision"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expiresAt,omitempty"`
}

type sessionState struct {
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

type leaderState struct {
	Leader    string    `json:"leader"`
	Term      uint64    `json:"term"`
	Acquired  time.Time `json:"acquired,omitempty"`
	Renewed   time.Time `json:"renewed,omitempty"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

type expiredKey struct {
	Key       string
	Revision  int64
	ExpiresAt time.Time
}

type expiredSession struct {
	Member    string
	ExpiresAt time.Time
}

type fsm struct {
	mu       sync.RWMutex
	state    snapshotState
	watchers *watchBroker
}

func newFSM(watchers *watchBroker) *fsm {
	return &fsm{
		state: snapshotState{
			Entries:  make(map[string]keyState),
			Sessions: make(map[string]sessionState),
		},
		watchers: watchers,
	}
}

func (fsm *fsm) Apply(log *hraft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("decode raft command: %w", err)
	}

	switch cmd.Type {
	case commandSet:
		return fsm.applySet(cmd)
	case commandCompareAndSet:
		return fsm.applyCompareAndSet(cmd)
	case commandDelete:
		return fsm.applyDelete(cmd)
	case commandExpireKey:
		return fsm.applyExpireKey(cmd)
	case commandCampaign:
		return fsm.applyCampaign(cmd)
	case commandResign:
		return fsm.applyResign(cmd)
	case commandTouch:
		return fsm.applyTouch(cmd)
	case commandExpireSession:
		return fsm.applyExpireSession(cmd)
	default:
		return fmt.Errorf("unknown raft command type %q", cmd.Type)
	}
}

func (fsm *fsm) Snapshot() (hraft.FSMSnapshot, error) {
	fsm.mu.RLock()
	state := fsm.state.clone()
	fsm.mu.RUnlock()

	return &snapshot{state: state}, nil
}

func (fsm *fsm) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	var restored snapshotState
	if err := json.NewDecoder(reader).Decode(&restored); err != nil {
		return err
	}

	restored.normalize()

	fsm.mu.Lock()
	fsm.state = restored.clone()
	fsm.mu.Unlock()

	return nil
}

func (fsm *fsm) Get(key string, now time.Time) (dcs.KeyValue, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	current, ok := fsm.state.Entries[strings.TrimSpace(key)]
	if !ok || current.expired(now) {
		return dcs.KeyValue{}, false
	}

	return current.keyValue(), true
}

func (fsm *fsm) List(prefix string, now time.Time) []dcs.KeyValue {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	trimmedPrefix := strings.TrimSpace(prefix)
	listed := make([]dcs.KeyValue, 0, len(fsm.state.Entries))
	for key, current := range fsm.state.Entries {
		if !strings.HasPrefix(key, trimmedPrefix) || current.expired(now) {
			continue
		}

		listed = append(listed, current.keyValue())
	}

	slices.SortFunc(listed, func(left, right dcs.KeyValue) int {
		return strings.Compare(left.Key, right.Key)
	})

	return listed
}

func (fsm *fsm) Leader(now time.Time) (dcs.LeaderLease, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	return fsm.state.Leader.lease(now)
}

func (fsm *fsm) Alive(member string, now time.Time) bool {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	current, ok := fsm.state.Sessions[strings.TrimSpace(member)]
	if !ok || current.expired(now) {
		return false
	}

	return true
}

func (fsm *fsm) Expired(now time.Time) ([]expiredKey, []expiredSession) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	keys := make([]expiredKey, 0)
	for key, current := range fsm.state.Entries {
		if !current.expired(now) {
			continue
		}

		keys = append(keys, expiredKey{
			Key:       key,
			Revision:  current.Revision,
			ExpiresAt: current.ExpiresAt,
		})
	}

	sessions := make([]expiredSession, 0)
	for member, current := range fsm.state.Sessions {
		if !current.expired(now) {
			continue
		}

		sessions = append(sessions, expiredSession{
			Member:    member,
			ExpiresAt: current.ExpiresAt,
		})
	}

	return keys, sessions
}

func (fsm *fsm) applySet(cmd command) interface{} {
	trimmedKey := strings.TrimSpace(cmd.Key)
	now := cmd.Now.UTC()

	fsm.mu.Lock()
	current, ok := fsm.state.Entries[trimmedKey]
	if ok && current.expired(now) {
		delete(fsm.state.Entries, trimmedKey)
		ok = false
	}

	revision := int64(1)
	if ok {
		revision = current.Revision + 1
	}

	next := keyState{
		Key:      trimmedKey,
		Value:    append([]byte(nil), cmd.Value...),
		Revision: revision,
		TTL:      cmd.TTL,
	}
	if cmd.TTL > 0 {
		next.ExpiresAt = now.Add(cmd.TTL)
	}

	fsm.state.Entries[trimmedKey] = next
	fsm.mu.Unlock()

	fsm.watchers.broadcast(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      trimmedKey,
		Value:    append([]byte(nil), cmd.Value...),
		Revision: revision,
	})

	return nil
}

func (fsm *fsm) applyCompareAndSet(cmd command) interface{} {
	trimmedKey := strings.TrimSpace(cmd.Key)
	now := cmd.Now.UTC()

	fsm.mu.Lock()
	current, ok := fsm.state.Entries[trimmedKey]
	if !ok || current.expired(now) || current.Revision != cmd.ExpectedRevision {
		if ok && current.expired(now) {
			delete(fsm.state.Entries, trimmedKey)
		}
		fsm.mu.Unlock()
		return dcs.ErrRevisionMismatch
	}

	current.Value = append([]byte(nil), cmd.Value...)
	current.Revision++
	fsm.state.Entries[trimmedKey] = current
	fsm.mu.Unlock()

	fsm.watchers.broadcast(dcs.WatchEvent{
		Type:     dcs.EventPut,
		Key:      trimmedKey,
		Value:    append([]byte(nil), cmd.Value...),
		Revision: current.Revision,
	})

	return nil
}

func (fsm *fsm) applyDelete(cmd command) interface{} {
	trimmedKey := strings.TrimSpace(cmd.Key)
	now := cmd.Now.UTC()

	fsm.mu.Lock()
	current, ok := fsm.state.Entries[trimmedKey]
	if !ok || current.expired(now) {
		if ok && current.expired(now) {
			delete(fsm.state.Entries, trimmedKey)
		}
		fsm.mu.Unlock()
		return dcs.ErrKeyNotFound
	}

	delete(fsm.state.Entries, trimmedKey)
	revision := current.Revision + 1
	fsm.mu.Unlock()

	fsm.watchers.broadcast(dcs.WatchEvent{
		Type:     dcs.EventDelete,
		Key:      trimmedKey,
		Revision: revision,
	})

	return nil
}

func (fsm *fsm) applyExpireKey(cmd command) interface{} {
	trimmedKey := strings.TrimSpace(cmd.Key)
	now := cmd.Now.UTC()

	fsm.mu.Lock()
	current, ok := fsm.state.Entries[trimmedKey]
	if !ok || !current.expired(now) || current.Revision != cmd.ExpectedRevision || !current.ExpiresAt.Equal(cmd.ExpectedExpiresAt) {
		fsm.mu.Unlock()
		return nil
	}

	delete(fsm.state.Entries, trimmedKey)
	revision := current.Revision + 1
	fsm.mu.Unlock()

	fsm.watchers.broadcast(dcs.WatchEvent{
		Type:     dcs.EventExpired,
		Key:      trimmedKey,
		Revision: revision,
	})

	return nil
}

func (fsm *fsm) applyCampaign(cmd command) interface{} {
	now := cmd.Now.UTC()
	trimmedCandidate := strings.TrimSpace(cmd.Candidate)

	fsm.mu.Lock()
	leader := fsm.state.Leader
	leader.expire(now)

	switch {
	case leader.Leader == "":
		nextTerm := leader.Term + 1
		if nextTerm == 0 {
			nextTerm = 1
		}

		leader = leaderState{
			Leader:    trimmedCandidate,
			Term:      nextTerm,
			Acquired:  now,
			Renewed:   now,
			ExpiresAt: now.Add(cmd.TTL),
		}
		fsm.state.Leader = leader
		fsm.mu.Unlock()

		lease, _ := leader.lease(now)
		return campaignResult{Lease: lease, Held: true}
	case leader.Leader == trimmedCandidate:
		leader.Renewed = now
		leader.ExpiresAt = now.Add(cmd.TTL)
		fsm.state.Leader = leader
		fsm.mu.Unlock()

		lease, _ := leader.lease(now)
		return campaignResult{Lease: lease, Held: true}
	default:
		fsm.state.Leader = leader
		fsm.mu.Unlock()

		lease, _ := leader.lease(now)
		return campaignResult{Lease: lease, Held: false}
	}
}

func (fsm *fsm) applyResign(cmd command) interface{} {
	now := cmd.Now.UTC()

	fsm.mu.Lock()
	leader := fsm.state.Leader
	leader.expire(now)
	if leader.Leader == "" {
		fsm.state.Leader = leader
		fsm.mu.Unlock()
		return dcs.ErrNoLeader
	}

	leader.Leader = ""
	leader.Acquired = time.Time{}
	leader.Renewed = time.Time{}
	leader.ExpiresAt = time.Time{}
	fsm.state.Leader = leader
	fsm.mu.Unlock()

	return nil
}

func (fsm *fsm) applyTouch(cmd command) interface{} {
	now := cmd.Now.UTC()
	trimmedMember := strings.TrimSpace(cmd.Member)

	fsm.mu.Lock()
	fsm.state.Sessions[trimmedMember] = sessionState{
		ExpiresAt: now.Add(cmd.TTL),
	}
	fsm.mu.Unlock()

	return nil
}

func (fsm *fsm) applyExpireSession(cmd command) interface{} {
	now := cmd.Now.UTC()
	trimmedMember := strings.TrimSpace(cmd.Member)

	fsm.mu.Lock()
	current, ok := fsm.state.Sessions[trimmedMember]
	if !ok || !current.expired(now) || !current.ExpiresAt.Equal(cmd.ExpectedExpiresAt) {
		fsm.mu.Unlock()
		return nil
	}

	delete(fsm.state.Sessions, trimmedMember)
	fsm.mu.Unlock()

	return nil
}

func (state *snapshotState) normalize() {
	if state.Entries == nil {
		state.Entries = make(map[string]keyState)
	}

	if state.Sessions == nil {
		state.Sessions = make(map[string]sessionState)
	}
}

func (state snapshotState) clone() snapshotState {
	cloned := snapshotState{
		Entries:  make(map[string]keyState, len(state.Entries)),
		Sessions: make(map[string]sessionState, len(state.Sessions)),
		Leader:   state.Leader,
	}

	for key, current := range state.Entries {
		cloned.Entries[key] = current.clone()
	}

	for member, current := range state.Sessions {
		cloned.Sessions[member] = current
	}

	return cloned
}

func (state keyState) expired(now time.Time) bool {
	return !state.ExpiresAt.IsZero() && !now.Before(state.ExpiresAt)
}

func (state keyState) keyValue() dcs.KeyValue {
	return dcs.KeyValue{
		Key:      state.Key,
		Value:    append([]byte(nil), state.Value...),
		Revision: state.Revision,
		TTL:      state.TTL,
	}
}

func (state keyState) clone() keyState {
	cloned := state
	cloned.Value = append([]byte(nil), state.Value...)
	return cloned
}

func (state sessionState) expired(now time.Time) bool {
	return !state.ExpiresAt.IsZero() && !now.Before(state.ExpiresAt)
}

func (state leaderState) lease(now time.Time) (dcs.LeaderLease, bool) {
	if state.Leader == "" || (!state.ExpiresAt.IsZero() && !now.Before(state.ExpiresAt)) {
		return dcs.LeaderLease{}, false
	}

	return dcs.LeaderLease{
		Leader:    state.Leader,
		Term:      state.Term,
		Acquired:  state.Acquired,
		Renewed:   state.Renewed,
		ExpiresAt: state.ExpiresAt,
	}, true
}

func (state *leaderState) expire(now time.Time) {
	if state.Leader == "" || state.ExpiresAt.IsZero() || now.Before(state.ExpiresAt) {
		return
	}

	state.Leader = ""
	state.Acquired = time.Time{}
	state.Renewed = time.Time{}
	state.ExpiresAt = time.Time{}
}
