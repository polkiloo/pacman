package dcs

import (
	"context"
	"time"
)

// DCS is the backend-neutral distributed configuration store contract used by
// the control plane.
type DCS interface {
	Get(context.Context, string) (KeyValue, error)
	Set(context.Context, string, []byte, ...SetOption) error
	CompareAndSet(context.Context, string, []byte, int64) error
	Delete(context.Context, string) error
	List(context.Context, string) ([]KeyValue, error)
	Campaign(context.Context, string) (LeaderLease, bool, error)
	Leader(context.Context) (LeaderLease, bool, error)
	Resign(context.Context) error
	Touch(context.Context, string) error
	Alive(context.Context, string) (bool, error)
	Watch(context.Context, string) (<-chan WatchEvent, error)
	Initialize(context.Context) error
	Close() error
}

// KeyValue represents a versioned DCS entry.
type KeyValue struct {
	Key      string
	Value    []byte
	Revision int64
	TTL      time.Duration
}

// Clone returns a detached copy of the key-value entry.
func (entry KeyValue) Clone() KeyValue {
	clone := entry
	clone.Value = append([]byte(nil), entry.Value...)
	return clone
}

// LeaderLease describes the current DCS leader election state.
type LeaderLease struct {
	Leader    string
	Term      uint64
	Acquired  time.Time
	Renewed   time.Time
	ExpiresAt time.Time
}

// Clone returns a detached copy of the leader lease.
func (lease LeaderLease) Clone() LeaderLease {
	return lease
}

// EventType identifies the kind of change observed by a watch.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
	EventExpired
)

// WatchEvent represents a change observed by Watch.
type WatchEvent struct {
	Type     EventType
	Key      string
	Value    []byte
	Revision int64
}

// Clone returns a detached copy of the watch event.
func (event WatchEvent) Clone() WatchEvent {
	clone := event
	clone.Value = append([]byte(nil), event.Value...)
	return clone
}

// SetOption configures a DCS Set operation.
type SetOption func(*SetOptions)

// SetOptions holds the concrete settings applied to a Set operation.
type SetOptions struct {
	TTL time.Duration
}

// WithTTL attaches a TTL to the key written by Set.
func WithTTL(ttl time.Duration) SetOption {
	return func(options *SetOptions) {
		options.TTL = ttl
	}
}

// ApplySetOptions resolves a variadic SetOption list into concrete settings.
func ApplySetOptions(options ...SetOption) SetOptions {
	var applied SetOptions
	for _, option := range options {
		if option != nil {
			option(&applied)
		}
	}

	return applied
}
