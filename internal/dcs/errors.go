package dcs

import "errors"

var (
	ErrKeyNotFound             = errors.New("dcs: key not found")
	ErrRevisionMismatch        = errors.New("dcs: revision mismatch")
	ErrNotLeader               = errors.New("dcs: not the leader")
	ErrNoLeader                = errors.New("dcs: no leader elected")
	ErrSessionExpired          = errors.New("dcs: session expired")
	ErrBackendUnavailable      = errors.New("dcs: backend unavailable")
	ErrBackendRequired         = errors.New("dcs: backend is required")
	ErrBackendInvalid          = errors.New("dcs: backend is invalid")
	ErrClusterNameRequired     = errors.New("dcs: clusterName is required")
	ErrTTLRequired             = errors.New("dcs: ttl must be greater than zero")
	ErrRetryTimeoutRequired    = errors.New("dcs: retryTimeout must be greater than zero")
	ErrRaftConfigRequired      = errors.New("dcs: raft config is required for raft backend")
	ErrRaftConfigUnexpected    = errors.New("dcs: raft config is only valid for raft backend")
	ErrRaftDataDirRequired     = errors.New("dcs: raft dataDir is required")
	ErrRaftBindAddressRequired = errors.New("dcs: raft bindAddress is required")
	ErrRaftBindAddressInvalid  = errors.New("dcs: raft bindAddress is invalid")
	ErrRaftPeersRequired       = errors.New("dcs: raft peers must contain at least one address")
	ErrRaftPeerInvalid         = errors.New("dcs: raft peers contain an invalid address")
	ErrEtcdConfigRequired      = errors.New("dcs: etcd config is required for etcd backend")
	ErrEtcdConfigUnexpected    = errors.New("dcs: etcd config is only valid for etcd backend")
	ErrEtcdEndpointsRequired   = errors.New("dcs: etcd endpoints must contain at least one endpoint")
	ErrEtcdEndpointInvalid     = errors.New("dcs: etcd endpoints contain an invalid endpoint")
)
