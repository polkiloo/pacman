package pacmanctl

import (
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

type memberStatusJSON struct {
	Name       string    `json:"name"`
	Role       string    `json:"role"`
	State      string    `json:"state"`
	Healthy    bool      `json:"healthy"`
	Leader     bool      `json:"leader,omitempty"`
	Timeline   int64     `json:"timeline,omitempty"`
	LagBytes   int64     `json:"lagBytes,omitempty"`
	LastSeenAt time.Time `json:"lastSeenAt"`
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

type scheduledSwitchoverJSON struct {
	At   time.Time `json:"at"`
	From string    `json:"from"`
	To   string    `json:"to,omitempty"`
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

func (client *apiClient) getJSON(ctx context.Context, path string, target any) error {
	requestURL := client.baseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return fmt.Errorf("build request GET %s: %w", path, err)
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("perform GET %s: %w", path, err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return decodeAPIError(path, response)
	}

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode GET %s response: %w", path, err)
	}

	return nil
}

func decodeAPIError(path string, response *http.Response) error {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read GET %s error response: %w", path, err)
	}

	var apiError apiErrorResponse
	if json.Unmarshal(body, &apiError) == nil {
		if apiError.Message != "" {
			return fmt.Errorf("GET %s returned %d: %s", path, response.StatusCode, apiError.Message)
		}
		if apiError.Error != "" {
			return fmt.Errorf("GET %s returned %d: %s", path, response.StatusCode, apiError.Error)
		}
	}

	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return fmt.Errorf("GET %s returned %d", path, response.StatusCode)
	}

	return fmt.Errorf("GET %s returned %d: %s", path, response.StatusCode, trimmed)
}
