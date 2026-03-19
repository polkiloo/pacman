package cluster

import (
	"net/url"
	"strings"
	"time"
)

// MemberSpec describes the desired configuration for an individual cluster
// member.
type MemberSpec struct {
	Name       string
	Priority   int
	NoFailover bool
	Tags       map[string]any
}

// Validate reports whether the desired member configuration is internally
// coherent enough to be accepted by the control plane.
func (spec MemberSpec) Validate() error {
	if strings.TrimSpace(spec.Name) == "" {
		return ErrMemberNameRequired
	}

	if spec.Priority < 0 {
		return ErrMemberPriorityNegative
	}

	return nil
}

// Clone returns a copy of the spec with detached mutable fields.
func (spec MemberSpec) Clone() MemberSpec {
	clone := spec
	clone.Tags = cloneMemberTags(spec.Tags)

	return clone
}

// MemberState describes the observed runtime state of an individual cluster
// member.
type MemberState string

const (
	MemberStateRunning     MemberState = "running"
	MemberStateStreaming   MemberState = "streaming"
	MemberStateStarting    MemberState = "starting"
	MemberStateStopping    MemberState = "stopping"
	MemberStateFailed      MemberState = "failed"
	MemberStateUnreachable MemberState = "unreachable"
	MemberStateNeedsRejoin MemberState = "needs_rejoin"
	MemberStateUnknown     MemberState = "unknown"
)

var memberStates = []MemberState{
	MemberStateRunning,
	MemberStateStreaming,
	MemberStateStarting,
	MemberStateStopping,
	MemberStateFailed,
	MemberStateUnreachable,
	MemberStateNeedsRejoin,
	MemberStateUnknown,
}

// MemberStates returns the full set of member states known to PACMAN.
func MemberStates() []MemberState {
	return append([]MemberState(nil), memberStates...)
}

func (state MemberState) String() string {
	return string(state)
}

// IsValid reports whether the value is a supported member runtime state.
func (state MemberState) IsValid() bool {
	switch state {
	case MemberStateRunning, MemberStateStreaming, MemberStateStarting, MemberStateStopping, MemberStateFailed, MemberStateUnreachable, MemberStateNeedsRejoin, MemberStateUnknown:
		return true
	default:
		return false
	}
}

// IsZero reports whether the state was left unspecified.
func (state MemberState) IsZero() bool {
	return state == ""
}

// MemberStatus describes the currently observed state of an individual cluster
// member.
type MemberStatus struct {
	Name        string
	APIURL      string
	Host        string
	Port        int
	Role        MemberRole
	State       MemberState
	Healthy     bool
	Leader      bool
	Timeline    int64
	LagBytes    int64
	Priority    int
	NoFailover  bool
	NeedsRejoin bool
	Tags        map[string]any
	LastSeenAt  time.Time
}

// Validate reports whether the observed member state is coherent enough to be
// published by the control plane.
func (status MemberStatus) Validate() error {
	if strings.TrimSpace(status.Name) == "" {
		return ErrMemberNameRequired
	}

	if status.APIURL != "" {
		parsed, err := url.Parse(status.APIURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return ErrMemberAPIURLInvalid
		}
	}

	if status.Port != 0 && (status.Port < 1 || status.Port > 65535) {
		return ErrMemberPortOutOfRange
	}

	if status.Role.String() == "" {
		return ErrMemberRoleRequired
	}

	if !status.Role.IsValid() {
		return ErrInvalidMemberRole
	}

	if status.State.IsZero() {
		return ErrMemberStateRequired
	}

	if !status.State.IsValid() {
		return ErrInvalidMemberState
	}

	if status.Timeline < 0 {
		return ErrMemberTimelineNegative
	}

	if status.LagBytes < 0 {
		return ErrMemberLagNegative
	}

	if status.Priority < 0 {
		return ErrMemberPriorityNegative
	}

	if status.LastSeenAt.IsZero() {
		return ErrMemberLastSeenAtRequired
	}

	return nil
}

// Clone returns a copy of the status with detached mutable fields.
func (status MemberStatus) Clone() MemberStatus {
	clone := status
	clone.Tags = cloneMemberTags(status.Tags)

	return clone
}

func cloneMemberTags(tags map[string]any) map[string]any {
	if tags == nil {
		return nil
	}

	cloned := make(map[string]any, len(tags))
	for key, value := range tags {
		cloned[key] = value
	}

	return cloned
}
