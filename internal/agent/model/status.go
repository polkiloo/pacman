package model

import (
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// Startup describes the local daemon identity and startup state after the
// daemon has been initialized successfully.
type Startup struct {
	NodeName        string
	NodeRole        cluster.NodeRole
	APIAddress      string
	ControlAddress  string
	ManagesPostgres bool
	StartedAt       time.Time
}

// PostgresStatus describes the latest local PostgreSQL observation collected
// by the agent heartbeat loop.
type PostgresStatus struct {
	Managed           bool
	Up                bool
	Address           string
	CheckedAt         time.Time
	AvailabilityError string
	Role              cluster.MemberRole
	RecoveryKnown     bool
	InRecovery        bool
	SystemIdentifier  string
	Timeline          int64
	StateError        string
}

// Heartbeat describes the latest local agent heartbeat.
type Heartbeat struct {
	Sequence   uint64
	ObservedAt time.Time
	Postgres   PostgresStatus
}
