package cluster

// MemberRole describes a member's authoritative role in the current cluster
// topology. Unlike NodeRole, this value is expected to change during
// failover, switchover, and rejoin flows.
type MemberRole string

const (
	MemberRolePrimary       MemberRole = "primary"
	MemberRoleReplica       MemberRole = "replica"
	MemberRoleStandbyLeader MemberRole = "standby_leader"
	MemberRoleWitness       MemberRole = "witness"
	MemberRoleUnknown       MemberRole = "unknown"
)

var memberRoles = []MemberRole{
	MemberRolePrimary,
	MemberRoleReplica,
	MemberRoleStandbyLeader,
	MemberRoleWitness,
	MemberRoleUnknown,
}

// MemberRoles returns the full set of cluster member roles known to PACMAN.
func MemberRoles() []MemberRole {
	return append([]MemberRole(nil), memberRoles...)
}

func (role MemberRole) String() string {
	return string(role)
}

// IsValid reports whether the value is a supported cluster member role.
func (role MemberRole) IsValid() bool {
	switch role {
	case MemberRolePrimary, MemberRoleReplica, MemberRoleStandbyLeader, MemberRoleWitness, MemberRoleUnknown:
		return true
	default:
		return false
	}
}

// IsDataBearing reports whether the member is expected to host PostgreSQL
// state, WAL, and replication metadata.
func (role MemberRole) IsDataBearing() bool {
	switch role {
	case MemberRolePrimary, MemberRoleReplica, MemberRoleStandbyLeader:
		return true
	default:
		return false
	}
}

// IsWritable reports whether the role should currently accept writes.
func (role MemberRole) IsWritable() bool {
	return role == MemberRolePrimary
}

// NodeRole describes the static responsibility of a PACMAN node. Unlike
// MemberRole, this value is tied to node capabilities and should not change
// during normal topology transitions.
type NodeRole string

const (
	NodeRoleData    NodeRole = "data"
	NodeRoleWitness NodeRole = "witness"
	NodeRoleUnknown NodeRole = "unknown"
)

var nodeRoles = []NodeRole{
	NodeRoleData,
	NodeRoleWitness,
	NodeRoleUnknown,
}

// NodeRoles returns the full set of node roles known to PACMAN.
func NodeRoles() []NodeRole {
	return append([]NodeRole(nil), nodeRoles...)
}

func (role NodeRole) String() string {
	return string(role)
}

// IsValid reports whether the value is a supported node role.
func (role NodeRole) IsValid() bool {
	switch role {
	case NodeRoleData, NodeRoleWitness, NodeRoleUnknown:
		return true
	default:
		return false
	}
}

// HasLocalPostgres reports whether the node should manage a colocated
// PostgreSQL instance.
func (role NodeRole) HasLocalPostgres() bool {
	return role == NodeRoleData
}

// SupportsMemberRole reports whether a node with the given static capabilities
// may advertise the provided cluster member role.
func (role NodeRole) SupportsMemberRole(memberRole MemberRole) bool {
	switch role {
	case NodeRoleData:
		switch memberRole {
		case MemberRolePrimary, MemberRoleReplica, MemberRoleStandbyLeader, MemberRoleUnknown:
			return true
		default:
			return false
		}
	case NodeRoleWitness:
		switch memberRole {
		case MemberRoleWitness, MemberRoleUnknown:
			return true
		default:
			return false
		}
	case NodeRoleUnknown:
		return memberRole == MemberRoleUnknown
	default:
		return false
	}
}
