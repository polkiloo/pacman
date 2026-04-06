package agent

import "errors"

var (
	ErrLoggerRequired         = errors.New("agent logger is required")
	ErrPostgresConfigRequired = errors.New("agent postgres config is required for data nodes")
	ErrAPIServerTLSRequired   = errors.New("agent api server tls config is required when tls is enabled")
	ErrDaemonAlreadyStarted   = errors.New("agent daemon is already started")
)
