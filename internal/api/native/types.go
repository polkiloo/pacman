package nativeapi

import "time"

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type ClusterStatusResponse struct {
	ClusterName         string                `json:"clusterName"`
	Phase               string                `json:"phase"`
	CurrentPrimary      string                `json:"currentPrimary,omitempty"`
	CurrentEpoch        int64                 `json:"currentEpoch"`
	ObservedAt          time.Time             `json:"observedAt"`
	Maintenance         MaintenanceModeStatus `json:"maintenance"`
	ActiveOperation     *Operation            `json:"activeOperation,omitempty"`
	ScheduledSwitchover *ScheduledSwitchover  `json:"scheduledSwitchover,omitempty"`
	Members             []MemberStatus        `json:"members"`
}

type MemberStatus struct {
	Name        string         `json:"name"`
	APIURL      string         `json:"apiUrl,omitempty"`
	Host        string         `json:"host,omitempty"`
	Port        int            `json:"port,omitempty"`
	Role        string         `json:"role"`
	State       string         `json:"state"`
	Healthy     bool           `json:"healthy"`
	Leader      bool           `json:"leader,omitempty"`
	Timeline    int64          `json:"timeline,omitempty"`
	LagBytes    int64          `json:"lagBytes,omitempty"`
	Priority    int            `json:"priority,omitempty"`
	NoFailover  bool           `json:"noFailover,omitempty"`
	NeedsRejoin bool           `json:"needsRejoin,omitempty"`
	Tags        map[string]any `json:"tags,omitempty"`
	LastSeenAt  time.Time      `json:"lastSeenAt"`
}

type MaintenanceModeStatus struct {
	Enabled     bool       `json:"enabled"`
	Reason      string     `json:"reason,omitempty"`
	RequestedBy string     `json:"requestedBy,omitempty"`
	UpdatedAt   *time.Time `json:"updatedAt,omitempty"`
}

type Operation struct {
	ID          string     `json:"id"`
	Kind        string     `json:"kind"`
	State       string     `json:"state"`
	RequestedBy string     `json:"requestedBy,omitempty"`
	RequestedAt time.Time  `json:"requestedAt"`
	Reason      string     `json:"reason,omitempty"`
	FromMember  string     `json:"fromMember,omitempty"`
	ToMember    string     `json:"toMember,omitempty"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Result      string     `json:"result,omitempty"`
	Message     string     `json:"message,omitempty"`
}

type ScheduledSwitchover struct {
	At   time.Time `json:"at"`
	From string    `json:"from"`
	To   string    `json:"to,omitempty"`
}

type MembersResponse struct {
	Items []MemberStatus `json:"items"`
}

type NodeStatusResponse struct {
	NodeName       string                  `json:"nodeName"`
	MemberName     string                  `json:"memberName,omitempty"`
	Role           string                  `json:"role"`
	State          string                  `json:"state"`
	PendingRestart bool                    `json:"pendingRestart,omitempty"`
	NeedsRejoin    bool                    `json:"needsRejoin,omitempty"`
	Tags           map[string]any          `json:"tags,omitempty"`
	Postgres       PostgresLocalStatus     `json:"postgres"`
	ControlPlane   ControlPlaneLocalStatus `json:"controlPlane"`
	ObservedAt     time.Time               `json:"observedAt"`
}

type PostgresLocalStatus struct {
	Managed       bool            `json:"managed"`
	Address       string          `json:"address,omitempty"`
	CheckedAt     time.Time       `json:"checkedAt"`
	Up            bool            `json:"up"`
	Role          string          `json:"role"`
	RecoveryKnown bool            `json:"recoveryKnown"`
	InRecovery    bool            `json:"inRecovery"`
	Details       PostgresDetails `json:"details"`
	WAL           WalProgress     `json:"wal"`
	Errors        PostgresErrors  `json:"errors"`
}

type PostgresDetails struct {
	ServerVersion       int        `json:"serverVersion,omitempty"`
	PendingRestart      bool       `json:"pendingRestart,omitempty"`
	SystemIdentifier    string     `json:"systemIdentifier,omitempty"`
	Timeline            int64      `json:"timeline,omitempty"`
	PostmasterStartAt   *time.Time `json:"postmasterStartAt,omitempty"`
	ReplicationLagBytes int64      `json:"replicationLagBytes,omitempty"`
}

type WalProgress struct {
	WriteLSN        string     `json:"writeLsn,omitempty"`
	FlushLSN        string     `json:"flushLsn,omitempty"`
	ReceiveLSN      string     `json:"receiveLsn,omitempty"`
	ReplayLSN       string     `json:"replayLsn,omitempty"`
	ReplayTimestamp *time.Time `json:"replayTimestamp,omitempty"`
}

type ControlPlaneLocalStatus struct {
	ClusterReachable bool       `json:"clusterReachable"`
	Leader           bool       `json:"leader,omitempty"`
	LastHeartbeatAt  *time.Time `json:"lastHeartbeatAt,omitempty"`
	LastDCSSeenAt    *time.Time `json:"lastDcsSeenAt,omitempty"`
	PublishError     string     `json:"publishError,omitempty"`
}

type PostgresErrors struct {
	Availability string `json:"availability,omitempty"`
	State        string `json:"state,omitempty"`
}

type ClusterSpecResponse struct {
	ClusterName string                  `json:"clusterName"`
	Generation  int64                   `json:"generation"`
	Maintenance MaintenanceDesiredState `json:"maintenance"`
	Failover    FailoverPolicy          `json:"failover"`
	Switchover  SwitchoverPolicy        `json:"switchover"`
	Postgres    PostgresPolicy          `json:"postgres"`
	Members     []MemberSpec            `json:"members,omitempty"`
}

type MaintenanceDesiredState struct {
	Enabled       bool   `json:"enabled,omitempty"`
	DefaultReason string `json:"defaultReason,omitempty"`
}

type FailoverPolicy struct {
	Mode            string `json:"mode,omitempty"`
	MaximumLagBytes int64  `json:"maximumLagBytes,omitempty"`
	CheckTimeline   bool   `json:"checkTimeline,omitempty"`
	RequireQuorum   bool   `json:"requireQuorum,omitempty"`
	FencingRequired bool   `json:"fencingRequired,omitempty"`
}

type SwitchoverPolicy struct {
	AllowScheduled                            bool `json:"allowScheduled,omitempty"`
	RequireSpecificCandidateDuringMaintenance bool `json:"requireSpecificCandidateDuringMaintenance,omitempty"`
}

type PostgresPolicy struct {
	SynchronousMode string         `json:"synchronousMode,omitempty"`
	UsePgRewind     bool           `json:"usePgRewind,omitempty"`
	Parameters      map[string]any `json:"parameters,omitempty"`
}

type MemberSpec struct {
	Name       string         `json:"name"`
	Priority   int            `json:"priority,omitempty"`
	NoFailover bool           `json:"noFailover,omitempty"`
	Tags       map[string]any `json:"tags,omitempty"`
}

type HistoryResponse struct {
	Items []HistoryEntry `json:"items"`
}

type HistoryEntry struct {
	OperationID string    `json:"operationId"`
	Kind        string    `json:"kind"`
	Timeline    int64     `json:"timeline,omitempty"`
	WALLSN      string    `json:"walLsn,omitempty"`
	FromMember  string    `json:"fromMember,omitempty"`
	ToMember    string    `json:"toMember,omitempty"`
	Reason      string    `json:"reason,omitempty"`
	Result      string    `json:"result"`
	FinishedAt  time.Time `json:"finishedAt"`
}

type MaintenanceModeUpdateRequest struct {
	Enabled     bool   `json:"enabled"`
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requestedBy,omitempty"`
}

type DiagnosticsSummary struct {
	ClusterName        string                    `json:"clusterName"`
	GeneratedAt        time.Time                 `json:"generatedAt"`
	ControlPlaneLeader string                    `json:"controlPlaneLeader,omitempty"`
	QuorumReachable    *bool                     `json:"quorumReachable,omitempty"`
	Warnings           []string                  `json:"warnings,omitempty"`
	Members            []MemberDiagnosticSummary `json:"members"`
}

type MemberDiagnosticSummary struct {
	Name        string     `json:"name"`
	Role        string     `json:"role"`
	State       string     `json:"state"`
	LagBytes    int64      `json:"lagBytes,omitempty"`
	LastSeenAt  *time.Time `json:"lastSeenAt,omitempty"`
	NeedsRejoin bool       `json:"needsRejoin,omitempty"`
}

type SwitchoverRequest struct {
	Candidate   string     `json:"candidate"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	RequestedBy string     `json:"requestedBy,omitempty"`
}

type FailoverRequest struct {
	Candidate   string `json:"candidate,omitempty"`
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requestedBy,omitempty"`
}

type OperationAcceptedResponse struct {
	Message   string    `json:"message,omitempty"`
	Operation Operation `json:"operation"`
}
