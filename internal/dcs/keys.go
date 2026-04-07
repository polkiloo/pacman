package dcs

import "strings"

const RootPrefix = "/pacman"

// KeySpace provides stable PACMAN key layout helpers rooted at a cluster
// prefix.
type KeySpace struct {
	clusterName string
}

// NewKeySpace constructs a key layout helper for the given cluster.
func NewKeySpace(clusterName string) (KeySpace, error) {
	trimmed := strings.TrimSpace(clusterName)
	if trimmed == "" {
		return KeySpace{}, ErrClusterNameRequired
	}

	return KeySpace{clusterName: trimmed}, nil
}

// Root returns the cluster-scoped DCS prefix.
func (space KeySpace) Root() string {
	return RootPrefix + "/" + space.clusterName
}

// Config returns the desired cluster-spec key.
func (space KeySpace) Config() string {
	return space.Root() + "/config"
}

// Leader returns the leader-lease key.
func (space KeySpace) Leader() string {
	return space.Root() + "/leader"
}

// MembersPrefix returns the static member-registration prefix.
func (space KeySpace) MembersPrefix() string {
	return space.Root() + "/members/"
}

// Member returns the static member-registration key for the given node.
func (space KeySpace) Member(nodeName string) string {
	return space.MembersPrefix() + strings.TrimSpace(nodeName)
}

// StatusPrefix returns the heartbeat/status prefix.
func (space KeySpace) StatusPrefix() string {
	return space.Root() + "/status/"
}

// Status returns the heartbeat/status key for the given node.
func (space KeySpace) Status(nodeName string) string {
	return space.StatusPrefix() + strings.TrimSpace(nodeName)
}

// Operation returns the active-operation key.
func (space KeySpace) Operation() string {
	return space.Root() + "/operation"
}

// HistoryPrefix returns the operation-history prefix.
func (space KeySpace) HistoryPrefix() string {
	return space.Root() + "/history/"
}

// History returns the history-entry key for the given operation id.
func (space KeySpace) History(operationID string) string {
	return space.HistoryPrefix() + strings.TrimSpace(operationID)
}

// Maintenance returns the maintenance-mode key.
func (space KeySpace) Maintenance() string {
	return space.Root() + "/maintenance"
}

// Epoch returns the current epoch key.
func (space KeySpace) Epoch() string {
	return space.Root() + "/epoch"
}
