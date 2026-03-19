package cluster

import (
	"fmt"
	"strings"
)

// ClusterSpec describes the desired cluster-wide configuration that the control
// plane should converge to. It intentionally models only cluster-level
// policies; per-member desired state lands in MemberSpec.
type ClusterSpec struct {
	ClusterName string
	Generation  int64
	Maintenance MaintenanceDesiredState
	Failover    FailoverPolicy
	Switchover  SwitchoverPolicy
	Postgres    PostgresPolicy
	Members     []MemberSpec
}

// Validate reports whether the desired cluster configuration is internally
// coherent enough to be accepted by the control plane.
func (spec ClusterSpec) Validate() error {
	if strings.TrimSpace(spec.ClusterName) == "" {
		return ErrClusterNameRequired
	}

	if spec.Generation < 0 {
		return ErrClusterGenerationNegative
	}

	if !spec.Failover.Mode.IsZero() && !spec.Failover.Mode.IsValid() {
		return ErrInvalidFailoverMode
	}

	if !spec.Postgres.SynchronousMode.IsZero() && !spec.Postgres.SynchronousMode.IsValid() {
		return ErrInvalidSynchronousMode
	}

	for _, member := range spec.Members {
		if err := member.Validate(); err != nil {
			return fmt.Errorf("member %q spec is invalid: %w", member.Name, err)
		}
	}

	return nil
}

// Clone returns a copy of the spec with detached mutable fields.
func (spec ClusterSpec) Clone() ClusterSpec {
	clone := spec
	clone.Postgres.Parameters = clonePostgresParameters(spec.Postgres.Parameters)
	clone.Members = cloneMemberSpecs(spec.Members)

	return clone
}

// MaintenanceDesiredState captures the desired maintenance mode for the cluster.
type MaintenanceDesiredState struct {
	Enabled       bool
	DefaultReason string
}

// FailoverMode controls whether and how PACMAN may perform automatic failover.
type FailoverMode string

const (
	FailoverModeAutomatic  FailoverMode = "automatic"
	FailoverModeManualOnly FailoverMode = "manual_only"
	FailoverModeDisabled   FailoverMode = "disabled"
)

var failoverModes = []FailoverMode{
	FailoverModeAutomatic,
	FailoverModeManualOnly,
	FailoverModeDisabled,
}

// FailoverModes returns the full set of failover modes known to PACMAN.
func FailoverModes() []FailoverMode {
	return append([]FailoverMode(nil), failoverModes...)
}

func (mode FailoverMode) String() string {
	return string(mode)
}

// IsValid reports whether the value is a supported failover mode.
func (mode FailoverMode) IsValid() bool {
	switch mode {
	case FailoverModeAutomatic, FailoverModeManualOnly, FailoverModeDisabled:
		return true
	default:
		return false
	}
}

// IsZero reports whether the mode was left unspecified.
func (mode FailoverMode) IsZero() bool {
	return mode == ""
}

// FailoverPolicy defines the safety envelope for automatic failover.
type FailoverPolicy struct {
	Mode            FailoverMode
	MaximumLagBytes int64
	CheckTimeline   bool
	RequireQuorum   bool
	FencingRequired bool
}

// SwitchoverPolicy defines the safety envelope for planned topology changes.
type SwitchoverPolicy struct {
	AllowScheduled                            bool
	RequireSpecificCandidateDuringMaintenance bool
}

// SynchronousMode controls how PACMAN expects PostgreSQL synchronous
// replication to be configured.
type SynchronousMode string

const (
	SynchronousModeDisabled SynchronousMode = "disabled"
	SynchronousModeQuorum   SynchronousMode = "quorum"
	SynchronousModeStrict   SynchronousMode = "strict"
)

var synchronousModes = []SynchronousMode{
	SynchronousModeDisabled,
	SynchronousModeQuorum,
	SynchronousModeStrict,
}

// SynchronousModes returns the full set of synchronous replication modes known
// to PACMAN.
func SynchronousModes() []SynchronousMode {
	return append([]SynchronousMode(nil), synchronousModes...)
}

func (mode SynchronousMode) String() string {
	return string(mode)
}

// IsValid reports whether the value is a supported synchronous replication
// mode.
func (mode SynchronousMode) IsValid() bool {
	switch mode {
	case SynchronousModeDisabled, SynchronousModeQuorum, SynchronousModeStrict:
		return true
	default:
		return false
	}
}

// IsZero reports whether the mode was left unspecified.
func (mode SynchronousMode) IsZero() bool {
	return mode == ""
}

// PostgresPolicy defines the desired PostgreSQL-level settings that influence
// cluster safety and rejoin behavior.
type PostgresPolicy struct {
	SynchronousMode SynchronousMode
	UsePgRewind     bool
	Parameters      map[string]any
}

func clonePostgresParameters(parameters map[string]any) map[string]any {
	if parameters == nil {
		return nil
	}

	cloned := make(map[string]any, len(parameters))
	for key, value := range parameters {
		cloned[key] = value
	}

	return cloned
}

func cloneMemberSpecs(members []MemberSpec) []MemberSpec {
	if members == nil {
		return nil
	}

	cloned := make([]MemberSpec, len(members))
	for index, member := range members {
		cloned[index] = member.Clone()
	}

	return cloned
}
