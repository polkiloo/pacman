package pacmanctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type apiClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

type apiErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type clusterStatusResponse struct {
	ClusterName         string                    `json:"clusterName"`
	Phase               string                    `json:"phase"`
	CurrentPrimary      string                    `json:"currentPrimary,omitempty"`
	CurrentEpoch        int64                     `json:"currentEpoch"`
	ObservedAt          time.Time                 `json:"observedAt"`
	Maintenance         maintenanceModeStatusJSON `json:"maintenance"`
	ActiveOperation     *operationJSON            `json:"activeOperation,omitempty"`
	ScheduledSwitchover *scheduledSwitchoverJSON  `json:"scheduledSwitchover,omitempty"`
	Members             []memberStatusJSON        `json:"members"`
}

type membersResponse struct {
	Items []memberStatusJSON `json:"items"`
}

type historyResponse struct {
	Items []historyEntryJSON `json:"items"`
}

type maintenanceModeUpdateRequestJSON struct {
	Enabled     bool   `json:"enabled"`
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requestedBy,omitempty"`
}

type memberStatusJSON struct {
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

type historyEntryJSON struct {
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

type maintenanceModeStatusJSON struct {
	Enabled     bool       `json:"enabled"`
	Reason      string     `json:"reason,omitempty"`
	RequestedBy string     `json:"requestedBy,omitempty"`
	UpdatedAt   *time.Time `json:"updatedAt,omitempty"`
}

type operationJSON struct {
	ID          string     `json:"id"`
	Kind        string     `json:"kind"`
	State       string     `json:"state"`
	RequestedBy string     `json:"requestedBy,omitempty"`
	RequestedAt *time.Time `json:"requestedAt,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	FromMember  string     `json:"fromMember,omitempty"`
	ToMember    string     `json:"toMember,omitempty"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Result      string     `json:"result,omitempty"`
	Message     string     `json:"message,omitempty"`
}

type scheduledSwitchoverJSON struct {
	At   time.Time `json:"at"`
	From string    `json:"from"`
	To   string    `json:"to,omitempty"`
}

type clusterSpecResponse struct {
	ClusterName string                 `json:"clusterName"`
	Generation  int64                  `json:"generation"`
	Maintenance maintenanceDesiredJSON `json:"maintenance"`
	Failover    failoverPolicyJSON     `json:"failover"`
	Switchover  switchoverPolicyJSON   `json:"switchover"`
	Postgres    postgresPolicyJSON     `json:"postgres"`
	Members     []memberSpecJSON       `json:"members,omitempty"`
}

type maintenanceDesiredJSON struct {
	Enabled       bool   `json:"enabled,omitempty"`
	DefaultReason string `json:"defaultReason,omitempty"`
}

type failoverPolicyJSON struct {
	Mode            string `json:"mode,omitempty"`
	MaximumLagBytes int64  `json:"maximumLagBytes,omitempty"`
	CheckTimeline   bool   `json:"checkTimeline,omitempty"`
	RequireQuorum   bool   `json:"requireQuorum,omitempty"`
	FencingRequired bool   `json:"fencingRequired,omitempty"`
}

type switchoverPolicyJSON struct {
	AllowScheduled                            bool `json:"allowScheduled,omitempty"`
	RequireSpecificCandidateDuringMaintenance bool `json:"requireSpecificCandidateDuringMaintenance,omitempty"`
}

type postgresPolicyJSON struct {
	SynchronousMode string         `json:"synchronousMode,omitempty"`
	UsePgRewind     bool           `json:"usePgRewind,omitempty"`
	Parameters      map[string]any `json:"parameters,omitempty"`
}

type memberSpecJSON struct {
	Name       string         `json:"name"`
	Priority   int            `json:"priority,omitempty"`
	NoFailover bool           `json:"noFailover,omitempty"`
	Tags       map[string]any `json:"tags,omitempty"`
}

type nodeStatusResponse struct {
	NodeName       string                      `json:"nodeName"`
	MemberName     string                      `json:"memberName,omitempty"`
	Role           string                      `json:"role"`
	State          string                      `json:"state"`
	PendingRestart bool                        `json:"pendingRestart,omitempty"`
	NeedsRejoin    bool                        `json:"needsRejoin,omitempty"`
	Tags           map[string]any              `json:"tags,omitempty"`
	Postgres       postgresLocalStatusJSON     `json:"postgres"`
	ControlPlane   controlPlaneLocalStatusJSON `json:"controlPlane"`
	ObservedAt     time.Time                   `json:"observedAt"`
}

type postgresLocalStatusJSON struct {
	Managed       bool                `json:"managed"`
	Address       string              `json:"address,omitempty"`
	CheckedAt     time.Time           `json:"checkedAt"`
	Up            bool                `json:"up"`
	Role          string              `json:"role"`
	RecoveryKnown bool                `json:"recoveryKnown"`
	InRecovery    bool                `json:"inRecovery"`
	Details       postgresDetailsJSON `json:"details"`
	WAL           walProgressJSON     `json:"wal"`
	Errors        postgresErrorsJSON  `json:"errors"`
}

type postgresDetailsJSON struct {
	ServerVersion       int        `json:"serverVersion,omitempty"`
	PendingRestart      bool       `json:"pendingRestart,omitempty"`
	SystemIdentifier    string     `json:"systemIdentifier,omitempty"`
	Timeline            int64      `json:"timeline,omitempty"`
	PostmasterStartAt   *time.Time `json:"postmasterStartAt,omitempty"`
	ReplicationLagBytes int64      `json:"replicationLagBytes,omitempty"`
}

type walProgressJSON struct {
	WriteLSN        string     `json:"writeLsn,omitempty"`
	FlushLSN        string     `json:"flushLsn,omitempty"`
	ReceiveLSN      string     `json:"receiveLsn,omitempty"`
	ReplayLSN       string     `json:"replayLsn,omitempty"`
	ReplayTimestamp *time.Time `json:"replayTimestamp,omitempty"`
}

type controlPlaneLocalStatusJSON struct {
	ClusterReachable bool       `json:"clusterReachable"`
	Leader           bool       `json:"leader,omitempty"`
	LastHeartbeatAt  *time.Time `json:"lastHeartbeatAt,omitempty"`
	LastDCSSeenAt    *time.Time `json:"lastDcsSeenAt,omitempty"`
	PublishError     string     `json:"publishError,omitempty"`
}

type postgresErrorsJSON struct {
	Availability string `json:"availability,omitempty"`
	State        string `json:"state,omitempty"`
}

type diagnosticsSummaryJSON struct {
	ClusterName        string                        `json:"clusterName"`
	GeneratedAt        time.Time                     `json:"generatedAt"`
	ControlPlaneLeader string                        `json:"controlPlaneLeader,omitempty"`
	QuorumReachable    *bool                         `json:"quorumReachable,omitempty"`
	Warnings           []string                      `json:"warnings,omitempty"`
	Members            []memberDiagnosticSummaryJSON `json:"members"`
}

type memberDiagnosticSummaryJSON struct {
	Name        string     `json:"name"`
	Role        string     `json:"role"`
	State       string     `json:"state"`
	LagBytes    int64      `json:"lagBytes,omitempty"`
	LastSeenAt  *time.Time `json:"lastSeenAt,omitempty"`
	NeedsRejoin bool       `json:"needsRejoin,omitempty"`
}

type switchoverRequestJSON struct {
	Candidate   string     `json:"candidate"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	RequestedBy string     `json:"requestedBy,omitempty"`
}

type failoverRequestJSON struct {
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requestedBy,omitempty"`
}

type operationAcceptedResponse struct {
	Message   string        `json:"message,omitempty"`
	Operation operationJSON `json:"operation"`
}

func newAPIClient(rawBaseURL string, httpClient *http.Client) (*apiClient, error) {
	trimmed := strings.TrimSpace(rawBaseURL)
	if trimmed == "" {
		return nil, errAPIURLRequired
	}

	baseURL, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parse pacmanctl api-url %q: %w", rawBaseURL, err)
	}

	if baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("pacmanctl api-url %q must include scheme and host", rawBaseURL)
	}

	if httpClient == nil {
		httpClient = &http.Client{Timeout: httpRequestTimeout}
	}

	return &apiClient{
		baseURL:    baseURL,
		httpClient: httpClient,
	}, nil
}

func (client *apiClient) clusterStatus(ctx context.Context) (clusterStatusResponse, error) {
	var response clusterStatusResponse
	if err := client.getJSON(ctx, "/api/v1/cluster", &response); err != nil {
		return clusterStatusResponse{}, err
	}

	return response, nil
}

func (client *apiClient) members(ctx context.Context) (membersResponse, error) {
	var response membersResponse
	if err := client.getJSON(ctx, "/api/v1/members", &response); err != nil {
		return membersResponse{}, err
	}

	return response, nil
}

func (client *apiClient) history(ctx context.Context) (historyResponse, error) {
	var response historyResponse
	if err := client.getJSON(ctx, "/api/v1/history", &response); err != nil {
		return historyResponse{}, err
	}

	return response, nil
}

func (client *apiClient) clusterSpec(ctx context.Context) (clusterSpecResponse, error) {
	var response clusterSpecResponse
	if err := client.getJSON(ctx, "/api/v1/cluster/spec", &response); err != nil {
		return clusterSpecResponse{}, err
	}

	return response, nil
}

func (client *apiClient) nodeStatus(ctx context.Context, nodeName string) (nodeStatusResponse, error) {
	var response nodeStatusResponse
	if err := client.getJSON(ctx, fmt.Sprintf("/api/v1/nodes/%s", url.PathEscape(strings.TrimSpace(nodeName))), &response); err != nil {
		return nodeStatusResponse{}, err
	}

	return response, nil
}

func (client *apiClient) diagnostics(ctx context.Context, includeMembers bool) (diagnosticsSummaryJSON, error) {
	var response diagnosticsSummaryJSON
	path := "/api/v1/diagnostics"
	if !includeMembers {
		path += "?includeMembers=false"
	}

	if err := client.getJSON(ctx, path, &response); err != nil {
		return diagnosticsSummaryJSON{}, err
	}

	return response, nil
}

func (client *apiClient) updateMaintenance(ctx context.Context, request maintenanceModeUpdateRequestJSON) (maintenanceModeStatusJSON, error) {
	var response maintenanceModeStatusJSON
	if err := client.doJSON(ctx, http.MethodPut, "/api/v1/maintenance", request, &response); err != nil {
		return maintenanceModeStatusJSON{}, err
	}

	return response, nil
}

func (client *apiClient) switchover(ctx context.Context, request switchoverRequestJSON) (operationAcceptedResponse, error) {
	var response operationAcceptedResponse
	if err := client.doJSON(ctx, http.MethodPost, "/api/v1/operations/switchover", request, &response); err != nil {
		return operationAcceptedResponse{}, err
	}

	return response, nil
}

func (client *apiClient) failover(ctx context.Context, request failoverRequestJSON) (operationAcceptedResponse, error) {
	var response operationAcceptedResponse
	if err := client.doJSON(ctx, http.MethodPost, "/api/v1/operations/failover", request, &response); err != nil {
		return operationAcceptedResponse{}, err
	}

	return response, nil
}

func (client *apiClient) getJSON(ctx context.Context, path string, target any) error {
	return client.doJSON(ctx, http.MethodGet, path, nil, target)
}

func (client *apiClient) doJSON(ctx context.Context, method, path string, body any, target any) error {
	var requestBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode %s %s request: %w", method, path, err)
		}
		requestBody = bytes.NewReader(payload)
	}

	relativeURL, err := url.Parse(path)
	if err != nil {
		return fmt.Errorf("parse request path %q: %w", path, err)
	}

	requestURL := client.baseURL.ResolveReference(relativeURL)
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), requestBody)
	if err != nil {
		return fmt.Errorf("build request %s %s: %w", method, path, err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("perform %s %s: %w", method, path, err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return decodeAPIError(method, path, response)
	}

	if target == nil {
		return nil
	}

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}

	return nil
}

func decodeAPIError(method, path string, response *http.Response) error {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read %s %s error response: %w", method, path, err)
	}

	var apiError apiErrorResponse
	if json.Unmarshal(body, &apiError) == nil {
		if apiError.Message != "" {
			return fmt.Errorf("%s %s returned %d: %s", method, path, response.StatusCode, apiError.Message)
		}
		if apiError.Error != "" {
			return fmt.Errorf("%s %s returned %d: %s", method, path, response.StatusCode, apiError.Error)
		}
	}

	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return fmt.Errorf("%s %s returned %d", method, path, response.StatusCode)
	}

	return fmt.Errorf("%s %s returned %d: %s", method, path, response.StatusCode, trimmed)
}
