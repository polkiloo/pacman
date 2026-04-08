package raft

import (
	"net"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/dcstest"
)

func TestBackendConformance(t *testing.T) {
	t.Parallel()

	const ttl = 150 * time.Millisecond

	dcstest.Run(t, dcstest.Config{
		TTL: ttl,
		New: func(t *testing.T) dcs.DCS {
			t.Helper()

			address := reserveTCPAddress(t)
			backend, err := New(Config{
				ClusterName:        "alpha",
				TTL:                ttl,
				RetryTimeout:       2 * time.Second,
				DataDir:            t.TempDir(),
				BindAddress:        address,
				Peers:              []string{address},
				Bootstrap:          true,
				ExpiryInterval:     25 * time.Millisecond,
				HeartbeatTimeout:   75 * time.Millisecond,
				ElectionTimeout:    75 * time.Millisecond,
				LeaderLeaseTimeout: 75 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("create raft backend: %v", err)
			}

			return backend
		},
	})
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp address: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}
