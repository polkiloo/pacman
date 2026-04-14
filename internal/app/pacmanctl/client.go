package pacmanctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type apiClient struct {
	baseURL    *url.URL
	apiToken   string
	httpClient *http.Client
	logger     *slog.Logger
}

func newAPIClient(rawBaseURL, apiToken string, httpClient *http.Client) (*apiClient, error) {
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
		apiToken:   strings.TrimSpace(apiToken),
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
	startedAt := time.Now().UTC()

	var requestBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			err = fmt.Errorf("encode %s %s request: %w", method, path, err)
			client.logRequest(ctx, slog.LevelError, "pacmanctl api request failed", method, path, startedAt, 0, err)
			return err
		}
		requestBody = bytes.NewReader(payload)
	}

	relativeURL, err := url.Parse(path)
	if err != nil {
		err = fmt.Errorf("parse request path %q: %w", path, err)
		client.logRequest(ctx, slog.LevelError, "pacmanctl api request failed", method, path, startedAt, 0, err)
		return err
	}

	requestURL := client.baseURL.ResolveReference(relativeURL)
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), requestBody)
	if err != nil {
		err = fmt.Errorf("build request %s %s: %w", method, path, err)
		client.logRequest(ctx, slog.LevelError, "pacmanctl api request failed", method, path, startedAt, 0, err)
		return err
	}
	if client.apiToken != "" {
		request.Header.Set("Authorization", "Bearer "+client.apiToken)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		err = fmt.Errorf("perform %s %s: %w", method, path, err)
		client.logRequest(ctx, slog.LevelError, "pacmanctl api request failed", method, path, startedAt, 0, err)
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		err = decodeAPIError(method, path, response)
		level := slog.LevelWarn
		if response.StatusCode >= http.StatusInternalServerError {
			level = slog.LevelError
		}
		client.logRequest(ctx, level, "pacmanctl api request failed", method, path, startedAt, response.StatusCode, err)
		return err
	}

	if target == nil {
		client.logRequest(ctx, slog.LevelInfo, "completed pacmanctl api request", method, path, startedAt, response.StatusCode, nil)
		return nil
	}

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		err = fmt.Errorf("decode %s %s response: %w", method, path, err)
		client.logRequest(ctx, slog.LevelError, "pacmanctl api request failed", method, path, startedAt, response.StatusCode, err)
		return err
	}

	client.logRequest(ctx, slog.LevelInfo, "completed pacmanctl api request", method, path, startedAt, response.StatusCode, nil)
	return nil
}

func (client *apiClient) logRequest(ctx context.Context, level slog.Level, message, method, path string, startedAt time.Time, status int, err error) {
	if client.logger == nil {
		return
	}

	attributes := []slog.Attr{
		slog.String("method", method),
		slog.String("path", path),
		slog.Duration("duration", time.Since(startedAt)),
	}
	if status > 0 {
		attributes = append(attributes, slog.Int("status", status))
	}
	if err != nil {
		attributes = append(attributes, slog.String("error", err.Error()))
	}

	client.logger.LogAttrs(ctx, level, message, attributes...)
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
