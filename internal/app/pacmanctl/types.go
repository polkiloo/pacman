package pacmanctl

import nativeapi "github.com/polkiloo/pacman/internal/api/native"

type apiErrorResponse = nativeapi.ErrorResponse
type clusterStatusResponse = nativeapi.ClusterStatusResponse
type membersResponse = nativeapi.MembersResponse
type historyResponse = nativeapi.HistoryResponse
type maintenanceModeUpdateRequestJSON = nativeapi.MaintenanceModeUpdateRequest
type memberStatusJSON = nativeapi.MemberStatus
type historyEntryJSON = nativeapi.HistoryEntry
type maintenanceModeStatusJSON = nativeapi.MaintenanceModeStatus
type operationJSON = nativeapi.Operation
type scheduledSwitchoverJSON = nativeapi.ScheduledSwitchover
type clusterSpecResponse = nativeapi.ClusterSpecResponse
type maintenanceDesiredJSON = nativeapi.MaintenanceDesiredState
type failoverPolicyJSON = nativeapi.FailoverPolicy
type switchoverPolicyJSON = nativeapi.SwitchoverPolicy
type postgresPolicyJSON = nativeapi.PostgresPolicy
type memberSpecJSON = nativeapi.MemberSpec
type nodeStatusResponse = nativeapi.NodeStatusResponse
type postgresLocalStatusJSON = nativeapi.PostgresLocalStatus
type postgresDetailsJSON = nativeapi.PostgresDetails
type walProgressJSON = nativeapi.WalProgress
type controlPlaneLocalStatusJSON = nativeapi.ControlPlaneLocalStatus
type diagnosticsSummaryJSON = nativeapi.DiagnosticsSummary
type memberDiagnosticSummaryJSON = nativeapi.MemberDiagnosticSummary
type switchoverRequestJSON = nativeapi.SwitchoverRequest
type failoverRequestJSON = nativeapi.FailoverRequest
type operationAcceptedResponse = nativeapi.OperationAcceptedResponse
