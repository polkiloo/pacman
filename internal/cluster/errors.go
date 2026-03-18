package cluster

import "errors"

var (
	ErrClusterNameRequired       = errors.New("cluster name is required")
	ErrClusterGenerationNegative = errors.New("cluster generation must be non-negative")
	ErrInvalidFailoverMode       = errors.New("failover mode is invalid")
	ErrInvalidSynchronousMode    = errors.New("postgres synchronous mode is invalid")
)
