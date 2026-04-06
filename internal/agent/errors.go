package agent

import "errors"

var (
	ErrLoggerRequired         = errors.New("agent logger is required")
	ErrPostgresConfigRequired = errors.New("agent postgres config is required for data nodes")
	ErrAPIServerTLSRequired   = errors.New("agent api server tls config is required when tls is enabled")
	ErrPeerServerTLSRequired  = errors.New("agent peer server tls config is required when member mTLS is enabled")
	ErrPeerClientTLSRequired  = errors.New("agent peer client tls config is required when member mTLS is enabled")
	ErrDaemonAlreadyStarted   = errors.New("agent daemon is already started")
)
