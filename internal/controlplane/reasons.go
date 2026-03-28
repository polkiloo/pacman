package controlplane

const (
	reasonCurrentPrimary              = "member is the current primary"
	reasonRoleNotPromotable           = "member role is not promotable"
	reasonMemberUnhealthy             = "member is not healthy"
	reasonMemberRequiresRejoin        = "member requires rejoin"
	reasonNoFailoverTagged            = "member is tagged no-failover"
	reasonLagExceedsFailoverPolicy    = "member replication lag exceeds failover policy"
	reasonTimelineMismatch            = "member timeline does not match current primary"
	reasonRoleNotStandby              = "member role is not a standby"
	reasonStateNotReadyForSwitchover  = "member state is not ready for switchover"
	reasonLagExceedsSwitchoverMaximum = "member replication lag exceeds configured maximum"
	reasonNodeStateNotObserved        = "member node state has not been observed"
	reasonPostgresNotManaged          = "member postgres is not managed"
	reasonPostgresNotUp               = "member postgres is not up"
	reasonRecoveryStateUnknown        = "member recovery state is unknown"
	reasonNotInRecovery               = "member is not currently in recovery"
	reasonPostgresRoleNotStandby      = "member postgres role is not a standby"
)
