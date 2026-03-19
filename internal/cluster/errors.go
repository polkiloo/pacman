package cluster

import "errors"

var (
	ErrClusterNameRequired       = errors.New("cluster name is required")
	ErrClusterGenerationNegative = errors.New("cluster generation must be non-negative")
	ErrClusterEpochNegative      = errors.New("cluster epoch must be non-negative")
	ErrClusterObservedAtRequired = errors.New("cluster observed time is required")
	ErrClusterPhaseRequired      = errors.New("cluster phase is required")
	ErrMemberNameRequired        = errors.New("member name is required")
	ErrMemberPriorityNegative    = errors.New("member priority must be non-negative")
	ErrMemberRoleRequired        = errors.New("member role is required")
	ErrMemberStateRequired       = errors.New("member state is required")
	ErrMemberTimelineNegative    = errors.New("member timeline must be non-negative")
	ErrMemberLagNegative         = errors.New("member lag must be non-negative")
	ErrMemberPortOutOfRange      = errors.New("member port must be between 1 and 65535")
	ErrMemberLastSeenAtRequired  = errors.New("member last seen time is required")
	ErrMemberAPIURLInvalid       = errors.New("member api url is invalid")
	ErrInvalidFailoverMode       = errors.New("failover mode is invalid")
	ErrInvalidClusterPhase       = errors.New("cluster phase is invalid")
	ErrInvalidMemberRole         = errors.New("member role is invalid")
	ErrInvalidMemberState        = errors.New("member state is invalid")
	ErrInvalidSynchronousMode    = errors.New("postgres synchronous mode is invalid")
)
