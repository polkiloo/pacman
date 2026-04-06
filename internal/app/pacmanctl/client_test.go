package pacmanctl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientMaintenanceStatus(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, time.April, 5, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/maintenance" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		if request.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", request.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{
			Enabled:     true,
			Reason:      "rolling upgrade",
			RequestedBy: "ops",
			UpdatedAt:   &updatedAt,
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := newAPIClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	status, err := client.maintenanceStatus(context.Background())
	if err != nil {
		t.Fatalf("maintenance status: %v", err)
	}
	if !status.Enabled {
		t.Fatal("expected enabled=true")
	}
	if status.Reason != "rolling upgrade" {
		t.Fatalf("reason: got %q, want rolling upgrade", status.Reason)
	}
}

func TestClientNewAPIClientNoHost(t *testing.T) {
	t.Parallel()

	// "//" parses as a URL with empty scheme and empty host — both required fields are absent.
	_, err := newAPIClient("//", "", nil)
	if err == nil {
		t.Fatal("expected error for URL missing scheme and host")
	}
}

func TestClientNewAPIClientNoScheme(t *testing.T) {
	t.Parallel()

	_, err := newAPIClient("example.com/path", "", nil)
	if err == nil {
		t.Fatal("expected error for missing scheme")
	}
}

func TestClientNewAPIClientWithNilHTTPClient(t *testing.T) {
	t.Parallel()

	client, err := newAPIClient("http://example.com", "", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestClientDecodeAPIErrorWithOnlyErrorField(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusUnprocessableEntity)
		if err := json.NewEncoder(writer).Encode(apiErrorResponse{
			Error: "invalid_request",
		}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer server.Close()

	client, err := newAPIClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	err = client.getJSON(context.Background(), "/api/v1/cluster", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	assertContains(t, err.Error(), "invalid_request")
}

func TestClientAddsBearerAuthorizationHeader(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("authorization header: got %q, want %q", got, "Bearer secret-token")
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := newAPIClient(server.URL, " secret-token ", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if _, err := client.clusterStatus(context.Background()); err != nil {
		t.Fatalf("cluster status: %v", err)
	}
}
