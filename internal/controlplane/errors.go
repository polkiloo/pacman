package controlplane

import "errors"

var (
	ErrMemberRegistrationTimeRequired   = errors.New("member registration time is required")
	ErrClusterSpecRequired              = errors.New("cluster desired state is not initialized")
	ErrFailoverObservedStateRequired    = errors.New("failover evaluation requires observed cluster state")
	ErrAutomaticFailoverNotAllowed      = errors.New("automatic failover is not allowed by cluster policy")
	ErrFailoverPrimaryUnknown           = errors.New("failover cannot proceed without a current primary")
	ErrFailoverPrimaryHealthy           = errors.New("failover primary has not been confirmed failed")
	ErrFailoverQuorumUnavailable        = errors.New("failover quorum is not reachable")
	ErrFailoverMaintenanceEnabled       = errors.New("failover is blocked while maintenance mode is enabled")
	ErrFailoverOperationInProgress      = errors.New("failover is blocked while another cluster operation is active")
	ErrFailoverNoEligibleCandidates     = errors.New("no eligible failover candidates are available")
	ErrLeaderCandidateRequired          = errors.New("leader candidate is required")
	ErrLeaderCandidateUnknown           = errors.New("leader candidate is not registered")
	ErrSourceOfTruthStateRequired       = errors.New("cluster source of truth requires desired or observed state")
	ErrSourceOfTruthUpdatedAtRequired   = errors.New("cluster source of truth updated time is required")
	ErrSourceOfTruthClusterNameMismatch = errors.New("cluster source of truth cluster names must match")
)
