package agent

import "errors"

var (
	ErrLoggerRequired         = errors.New("agent logger is required")
	ErrPostgresConfigRequired = errors.New("agent postgres config is required for data nodes")
	ErrDaemonAlreadyStarted   = errors.New("agent daemon is already started")
)
