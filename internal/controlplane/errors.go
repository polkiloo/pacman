package controlplane

import "errors"

var (
	ErrMemberRegistrationTimeRequired   = errors.New("member registration time is required")
	ErrClusterSpecRequired              = errors.New("cluster desired state is not initialized")
	ErrLeaderCandidateRequired          = errors.New("leader candidate is required")
	ErrLeaderCandidateUnknown           = errors.New("leader candidate is not registered")
	ErrSourceOfTruthStateRequired       = errors.New("cluster source of truth requires desired or observed state")
	ErrSourceOfTruthUpdatedAtRequired   = errors.New("cluster source of truth updated time is required")
	ErrSourceOfTruthClusterNameMismatch = errors.New("cluster source of truth cluster names must match")
)
