package controlplane

import (
	"context"
	"strings"
	"time"
)

const defaultLeaderLeaseDuration = 5 * time.Second

// LeaderElector exposes the control-plane leader election state backed by the
// shared store.
type LeaderElector interface {
	CampaignLeader(context.Context, string) (LeaderLease, bool, error)
	Leader() (LeaderLease, bool)
}

// LeaderLease describes the current control-plane leadership term.
type LeaderLease struct {
	LeaderNode string
	Term       uint64
	AcquiredAt time.Time
	RenewedAt  time.Time
}

// Clone returns a detached copy of the leader lease.
func (lease LeaderLease) Clone() LeaderLease {
	return lease
}

func (lease LeaderLease) isActiveAt(now time.Time, duration time.Duration) bool {
	if strings.TrimSpace(lease.LeaderNode) == "" {
		return false
	}

	reference := lease.RenewedAt
	if reference.IsZero() {
		reference = lease.AcquiredAt
	}

	if reference.IsZero() {
		return false
	}

	if duration <= 0 {
		return true
	}

	return !reference.Add(duration).Before(now.UTC())
}
