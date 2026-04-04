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
)

type apiClient struct {
	baseURL    *url.URL
	httpClient *http.Client
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

func (client *apiClient) maintenanceStatus(ctx context.Context) (maintenanceModeStatusJSON, error) {
	var response maintenanceModeStatusJSON
	if err := client.getJSON(ctx, "/api/v1/maintenance", &response); err != nil {
		return maintenanceModeStatusJSON{}, err
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

func (client *apiClient) cancelSwitchover(ctx context.Context) (operationAcceptedResponse, error) {
	var response operationAcceptedResponse
	if err := client.doJSON(ctx, http.MethodDelete, "/api/v1/operations/switchover", nil, &response); err != nil {
		return operationAcceptedResponse{}, err
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
