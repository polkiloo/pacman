package dcs

import (
	"testing"
	"time"
)

func TestKeyValueCloneDetachesValue(t *testing.T) {
	t.Parallel()

	entry := KeyValue{
		Key:      "/pacman/alpha/config",
		Value:    []byte("config"),
		Revision: 7,
		TTL:      30 * time.Second,
	}

	clone := entry.Clone()
	clone.Value[0] = 'C'

	if string(entry.Value) != "config" {
		t.Fatalf("expected clone to detach value slice, got %q", string(entry.Value))
	}
}

func TestWatchEventCloneDetachesValue(t *testing.T) {
	t.Parallel()

	event := WatchEvent{
		Type:     EventPut,
		Key:      "/pacman/alpha/status/alpha-1",
		Value:    []byte("status"),
		Revision: 11,
	}

	clone := event.Clone()
	clone.Value[0] = 'S'

	if string(event.Value) != "status" {
		t.Fatalf("expected clone to detach value slice, got %q", string(event.Value))
	}
}

func TestLeaderLeaseClone(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	lease := LeaderLease{
		Leader:    "alpha-1",
		Term:      3,
		Acquired:  now,
		Renewed:   now.Add(5 * time.Second),
		ExpiresAt: now.Add(30 * time.Second),
	}

	clone := lease.Clone()
	if clone != lease {
		t.Fatalf("unexpected lease clone: got %+v, want %+v", clone, lease)
	}
}

func TestApplySetOptions(t *testing.T) {
	t.Parallel()

	options := applySetOptions(nil, WithTTL(45*time.Second))
	if options.TTL != 45*time.Second {
		t.Fatalf("unexpected applied ttl: got %s, want %s", options.TTL, 45*time.Second)
	}
}
