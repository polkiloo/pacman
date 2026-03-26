package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

// DesiredStateStore persists the desired cluster spec in the replicated
// control-plane store.
type DesiredStateStore interface {
	ClusterSpec() (cluster.ClusterSpec, bool)
	StoreClusterSpec(context.Context, cluster.ClusterSpec) (cluster.ClusterSpec, error)
}

// SourceOfTruthStore exposes the cluster-wide source of truth snapshot stored
// in the control plane.
type SourceOfTruthStore interface {
	SourceOfTruth() ClusterSourceOfTruth
}

// ReplicatedStateStore is the control-plane state surface shared across
// membership, leader election, desired state, and future observed state.
type ReplicatedStateStore interface {
	NodeStatePublisher
	MemberRegistrar
	MemberDiscovery
	LeaderElector
	DesiredStateStore
	SourceOfTruthStore
}

// ClusterSourceOfTruth captures the desired and observed cluster state accepted
// by the control plane. Observed state may remain unset until aggregation is
// implemented.
type ClusterSourceOfTruth struct {
	Desired   *cluster.ClusterSpec
	Observed  *cluster.ClusterStatus
	UpdatedAt time.Time
}

// Validate reports whether the source-of-truth snapshot is coherent enough to
// publish from the control plane.
func (truth ClusterSourceOfTruth) Validate() error {
	if truth.Desired == nil && truth.Observed == nil {
		return ErrSourceOfTruthStateRequired
	}

	if truth.UpdatedAt.IsZero() {
		return ErrSourceOfTruthUpdatedAtRequired
	}

	if truth.Desired != nil {
		if err := truth.Desired.Validate(); err != nil {
			return fmt.Errorf("validate desired cluster state: %w", err)
		}
	}

	if truth.Observed != nil {
		if err := truth.Observed.Validate(); err != nil {
			return fmt.Errorf("validate observed cluster state: %w", err)
		}
	}

	if truth.Desired != nil && truth.Observed != nil && truth.Desired.ClusterName != truth.Observed.ClusterName {
		return ErrSourceOfTruthClusterNameMismatch
	}

	return nil
}

// Clone returns a detached copy of the source-of-truth snapshot.
func (truth ClusterSourceOfTruth) Clone() ClusterSourceOfTruth {
	clone := truth

	if truth.Desired != nil {
		desired := truth.Desired.Clone()
		clone.Desired = &desired
	}

	if truth.Observed != nil {
		observed := truth.Observed.Clone()
		clone.Observed = &observed
	}

	return clone
}

// ClusterName resolves the cluster name from the desired or observed state.
func (truth ClusterSourceOfTruth) ClusterName() string {
	if truth.Desired != nil {
		return truth.Desired.ClusterName
	}

	if truth.Observed != nil {
		return truth.Observed.ClusterName
	}

	return ""
}
