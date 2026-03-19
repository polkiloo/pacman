package cluster

import (
	"fmt"
	"strings"
	"time"
)

// ClusterStatus describes the current cluster-wide observed state. It
// intentionally models only cluster-level observations; per-member observed
// state lands in MemberStatus.
type ClusterStatus struct {
	ClusterName    string
	Phase          ClusterPhase
	CurrentPrimary string
	CurrentEpoch   Epoch
	Maintenance    MaintenanceModeStatus
	Members        []MemberStatus
	ObservedAt     time.Time
}

// Validate reports whether the observed cluster state is coherent enough to be
// published as the control-plane source of truth.
func (status ClusterStatus) Validate() error {
	if strings.TrimSpace(status.ClusterName) == "" {
		return ErrClusterNameRequired
	}

	if status.Phase.IsZero() {
		return ErrClusterPhaseRequired
	}

	if !status.Phase.IsValid() {
		return ErrInvalidClusterPhase
	}

	if err := status.CurrentEpoch.Validate(); err != nil {
		return err
	}

	if status.ObservedAt.IsZero() {
		return ErrClusterObservedAtRequired
	}

	for _, member := range status.Members {
		if err := member.Validate(); err != nil {
			return fmt.Errorf("member %q status is invalid: %w", member.Name, err)
		}
	}

	return nil
}

// Clone returns a copy of the status with detached mutable fields.
func (status ClusterStatus) Clone() ClusterStatus {
	clone := status
	clone.Members = cloneMemberStatuses(status.Members)

	return clone
}

// ClusterPhase describes the high-level health or transition state of the
// cluster.
type ClusterPhase string

const (
	ClusterPhaseInitializing  ClusterPhase = "initializing"
	ClusterPhaseHealthy       ClusterPhase = "healthy"
	ClusterPhaseDegraded      ClusterPhase = "degraded"
	ClusterPhaseFailingOver   ClusterPhase = "failing_over"
	ClusterPhaseSwitchingOver ClusterPhase = "switching_over"
	ClusterPhaseMaintenance   ClusterPhase = "maintenance"
	ClusterPhaseRecovering    ClusterPhase = "recovering"
	ClusterPhaseUnknown       ClusterPhase = "unknown"
)

var clusterPhases = []ClusterPhase{
	ClusterPhaseInitializing,
	ClusterPhaseHealthy,
	ClusterPhaseDegraded,
	ClusterPhaseFailingOver,
	ClusterPhaseSwitchingOver,
	ClusterPhaseMaintenance,
	ClusterPhaseRecovering,
	ClusterPhaseUnknown,
}

// ClusterPhases returns the full set of cluster phases known to PACMAN.
func ClusterPhases() []ClusterPhase {
	return append([]ClusterPhase(nil), clusterPhases...)
}

func (phase ClusterPhase) String() string {
	return string(phase)
}

// IsValid reports whether the value is a supported cluster phase.
func (phase ClusterPhase) IsValid() bool {
	switch phase {
	case ClusterPhaseInitializing, ClusterPhaseHealthy, ClusterPhaseDegraded, ClusterPhaseFailingOver, ClusterPhaseSwitchingOver, ClusterPhaseMaintenance, ClusterPhaseRecovering, ClusterPhaseUnknown:
		return true
	default:
		return false
	}
}

// IsZero reports whether the phase was left unspecified.
func (phase ClusterPhase) IsZero() bool {
	return phase == ""
}

// MaintenanceModeStatus captures the currently effective maintenance mode for
// the cluster.
type MaintenanceModeStatus struct {
	Enabled     bool
	Reason      string
	RequestedBy string
	UpdatedAt   time.Time
}

func cloneMemberStatuses(members []MemberStatus) []MemberStatus {
	if members == nil {
		return nil
	}

	cloned := make([]MemberStatus, len(members))
	for index, member := range members {
		cloned[index] = member.Clone()
	}

	return cloned
}
