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
	Managed       bool
	Address       string
	CheckedAt     time.Time
	Up            bool
	Role          cluster.MemberRole
	RecoveryKnown bool
	InRecovery    bool
	Details       PostgresDetails
	WAL           WALProgress
	Errors        PostgresErrors
}

// PostgresDetails describes the latest local PostgreSQL instance details.
type PostgresDetails struct {
	ServerVersion       int
	PendingRestart      bool
	SystemIdentifier    string
	Timeline            int64
	PostmasterStartAt   time.Time
	DatabaseSizeBytes   int64
	ReplicationLagBytes int64
}

// WALProgress describes the latest locally observed WAL positions.
type WALProgress struct {
	WriteLSN        string
	FlushLSN        string
	ReceiveLSN      string
	ReplayLSN       string
	ReplayTimestamp time.Time
}

// PostgresErrors describes the latest PostgreSQL probe errors.
type PostgresErrors struct {
	Availability string
	State        string
}

// ControlPlaneStatus describes the local outcome of publishing the latest
// observed node state to the control plane.
type ControlPlaneStatus struct {
	ClusterReachable bool
	Leader           bool
	LastHeartbeatAt  time.Time
	LastDCSSeenAt    time.Time
	PublishError     string
}

// NodeStatus describes the full local node observation published by pacmand.
type NodeStatus struct {
	NodeName       string
	MemberName     string
	Role           cluster.MemberRole
	State          cluster.MemberState
	PendingRestart bool
	NeedsRejoin    bool
	Tags           map[string]any
	Postgres       PostgresStatus
	ControlPlane   ControlPlaneStatus
	ObservedAt     time.Time
}

// Clone returns a detached copy of the node status.
func (status NodeStatus) Clone() NodeStatus {
	clone := status
	clone.Tags = cloneTags(status.Tags)
	return clone
}

// Heartbeat describes the latest local agent heartbeat.
type Heartbeat struct {
	Sequence     uint64
	ObservedAt   time.Time
	Postgres     PostgresStatus
	ControlPlane ControlPlaneStatus
}

func cloneTags(tags map[string]any) map[string]any {
	if tags == nil {
		return nil
	}

	cloned := make(map[string]any, len(tags))
	for key, value := range tags {
		cloned[key] = value
	}

	return cloned
}
