package httpapi

import nativeapi "github.com/polkiloo/pacman/internal/api/native"

type clusterStatusResponse = nativeapi.ClusterStatusResponse
type memberStatusJSON = nativeapi.MemberStatus
type maintenanceModeStatusJSON = nativeapi.MaintenanceModeStatus
type operationJSON = nativeapi.Operation
type scheduledSwitchoverJSON = nativeapi.ScheduledSwitchover
type membersResponse = nativeapi.MembersResponse
type nodeStatusResponse = nativeapi.NodeStatusResponse
type postgresLocalStatusJSON = nativeapi.PostgresLocalStatus
type postgresDetailsJSON = nativeapi.PostgresDetails
type walProgressJSON = nativeapi.WalProgress
type controlPlaneLocalStatusJSON = nativeapi.ControlPlaneLocalStatus
type postgresErrorsJSON = nativeapi.PostgresErrors
type errorResponseJSON = nativeapi.ErrorResponse
type clusterSpecResponse = nativeapi.ClusterSpecResponse
type maintenanceDesiredJSON = nativeapi.MaintenanceDesiredState
type failoverPolicyJSON = nativeapi.FailoverPolicy
type switchoverPolicyJSON = nativeapi.SwitchoverPolicy
type postgresPolicyJSON = nativeapi.PostgresPolicy
type memberSpecJSON = nativeapi.MemberSpec
type historyResponse = nativeapi.HistoryResponse
type historyEntryJSON = nativeapi.HistoryEntry
type maintenanceModeUpdateRequestJSON = nativeapi.MaintenanceModeUpdateRequest
type diagnosticsSummaryJSON = nativeapi.DiagnosticsSummary
type memberDiagnosticSummaryJSON = nativeapi.MemberDiagnosticSummary
type switchoverRequestJSON = nativeapi.SwitchoverRequest
type failoverRequestJSON = nativeapi.FailoverRequest
type operationAcceptedResponse = nativeapi.OperationAcceptedResponse
