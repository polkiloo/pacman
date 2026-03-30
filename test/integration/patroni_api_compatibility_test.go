//go:build integration

package integration_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/polkiloo/pacman/test/testenv"
)

func TestPatroniProbeCompatibilityWithContainerFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	document := loadContractDocument(t)
	env := testenv.New(t)

	primary := startPatroniCompatibilityFixture(t, env, "patroni-primary", "primary")
	syncReplica := startPatroniCompatibilityFixture(t, env, "patroni-sync", "sync_replica")
	asyncReplica := startPatroniCompatibilityFixture(t, env, "patroni-async", "async_replica")
	standbyLeader := startPatroniCompatibilityFixture(t, env, "patroni-standby-leader", "standby_leader")

	testCases := []struct {
		name   string
		base   string
		path   string
		method string
		status int
	}{
		{name: "primary health", base: serviceBaseURL(t, primary), path: "/health", method: http.MethodGet, status: http.StatusOK},
		{name: "primary root", base: serviceBaseURL(t, primary), path: "/", method: http.MethodGet, status: http.StatusOK},
		{name: "primary read write", base: serviceBaseURL(t, primary), path: "/read-write", method: http.MethodGet, status: http.StatusOK},
		{name: "primary leader", base: serviceBaseURL(t, primary), path: "/leader", method: http.MethodGet, status: http.StatusOK},
		{name: "primary replica rejected", base: serviceBaseURL(t, primary), path: "/replica", method: http.MethodGet, status: http.StatusServiceUnavailable},
		{name: "primary read only accepts tags", base: serviceBaseURL(t, primary), path: "/read-only?clonefrom=true", method: http.MethodGet, status: http.StatusOK},
		{name: "sync replica ready", base: serviceBaseURL(t, syncReplica), path: "/readiness?lag=16MB&mode=apply", method: http.MethodGet, status: http.StatusOK},
		{name: "sync replica probe", base: serviceBaseURL(t, syncReplica), path: "/replica?lag=16MB&replication_state=streaming&clonefrom=true", method: http.MethodGet, status: http.StatusOK},
		{name: "sync replica tag mismatch", base: serviceBaseURL(t, syncReplica), path: "/replica?clonefrom=false", method: http.MethodGet, status: http.StatusServiceUnavailable},
		{name: "sync replica synchronous", base: serviceBaseURL(t, syncReplica), path: "/sync", method: http.MethodGet, status: http.StatusOK},
		{name: "sync replica quorum", base: serviceBaseURL(t, syncReplica), path: "/quorum", method: http.MethodGet, status: http.StatusOK},
		{name: "async replica async", base: serviceBaseURL(t, asyncReplica), path: "/async?lag=2MB", method: http.MethodGet, status: http.StatusOK},
		{name: "async replica lag rejected", base: serviceBaseURL(t, asyncReplica), path: "/async?lag=1", method: http.MethodGet, status: http.StatusServiceUnavailable},
		{name: "standby leader", base: serviceBaseURL(t, standbyLeader), path: "/standby-leader", method: http.MethodGet, status: http.StatusOK},
		{name: "standby leader alias", base: serviceBaseURL(t, standbyLeader), path: "/standby_leader", method: http.MethodGet, status: http.StatusOK},
		{name: "standby leader leader", base: serviceBaseURL(t, standbyLeader), path: "/leader", method: http.MethodGet, status: http.StatusOK},
		{name: "head primary", base: serviceBaseURL(t, primary), path: "/primary", method: http.MethodHead, status: http.StatusOK},
		{name: "head replica rejected", base: serviceBaseURL(t, primary), path: "/replica", method: http.MethodHead, status: http.StatusServiceUnavailable},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response := performHTTPRequest(t, testCase.method, testCase.base+testCase.path, nil, nil)
			defer response.Body.Close()

			if response.StatusCode != testCase.status {
				t.Fatalf("unexpected status for %s %s: got %d want %d", testCase.method, testCase.path, response.StatusCode, testCase.status)
			}

			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("read response body for %s %s: %v", testCase.method, testCase.path, err)
			}

			if testCase.method == http.MethodGet {
				requireResponseMatchesContract(t, document, strings.Split(testCase.path, "?")[0], "get", response, body)
			}
		})
	}

	optionsCases := []struct {
		base string
		path string
	}{
		{base: serviceBaseURL(t, primary), path: "/health"},
		{base: serviceBaseURL(t, primary), path: "/leader"},
		{base: serviceBaseURL(t, syncReplica), path: "/replica"},
	}

	for _, testCase := range optionsCases {
		response := performHTTPRequest(t, http.MethodOptions, testCase.base+testCase.path, nil, nil)
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Fatalf("unexpected OPTIONS status for %s: got %d want %d", testCase.path, response.StatusCode, http.StatusOK)
		}

		if allow := response.Header.Get("Allow"); !strings.Contains(allow, "GET") || !strings.Contains(allow, "HEAD") {
			t.Fatalf("unexpected Allow header for %s: %q", testCase.path, allow)
		}
	}
}

func TestPatroniMonitoringDocumentsWithContainerFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	document := loadContractDocument(t)
	env := testenv.New(t)
	primary := startPatroniCompatibilityFixture(t, env, "patroni-primary", "primary")

	testCases := []struct {
		path string
	}{
		{path: "/patroni"},
		{path: "/cluster"},
		{path: "/history"},
		{path: "/config"},
		{path: "/metrics"},
	}

	for _, testCase := range testCases {
		response := performHTTPRequest(t, http.MethodGet, serviceBaseURL(t, primary)+testCase.path, nil, nil)
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Fatalf("unexpected GET status for %s: got %d want %d", testCase.path, response.StatusCode, http.StatusOK)
		}

		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read response body for %s: %v", testCase.path, err)
		}

		requireResponseMatchesContract(t, document, testCase.path, "get", response, body)
	}
}

func TestPatroniAdminCompatibilityWithContainerFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	document := loadContractDocument(t)
	env := testenv.New(t)
	primary := startPatroniCompatibilityFixture(t, env, "patroni-primary", "primary")
	syncReplica := startPatroniCompatibilityFixture(t, env, "patroni-sync", "sync_replica")

	type adminCase struct {
		name        string
		base        string
		path        string
		method      string
		body        string
		status      int
		contentType string
	}

	testCases := []adminCase{
		{name: "switchover immediate", base: serviceBaseURL(t, primary), path: "/switchover", method: http.MethodPost, body: `{"leader":"patroni-primary"}`, status: http.StatusOK, contentType: "application/json"},
		{name: "switchover scheduled", base: serviceBaseURL(t, primary), path: "/switchover", method: http.MethodPost, body: `{"leader":"patroni-primary","scheduled_at":"2026-03-30T12:00:00Z"}`, status: http.StatusAccepted, contentType: "application/json"},
		{name: "switchover delete scheduled", base: serviceBaseURL(t, primary), path: "/switchover", method: http.MethodDelete, status: http.StatusOK},
		{name: "switchover delete missing", base: serviceBaseURL(t, primary), path: "/switchover", method: http.MethodDelete, status: http.StatusNotFound},
		{name: "failover", base: serviceBaseURL(t, primary), path: "/failover", method: http.MethodPost, body: `{"candidate":"patroni-sync"}`, status: http.StatusOK, contentType: "application/json"},
		{name: "failover with leader triggers switchover semantics", base: serviceBaseURL(t, primary), path: "/failover", method: http.MethodPost, body: `{"candidate":"patroni-sync","leader":"patroni-primary"}`, status: http.StatusOK, contentType: "application/json"},
		{name: "config patch", base: serviceBaseURL(t, primary), path: "/config", method: http.MethodPatch, body: `{"loop_wait":5,"ttl":20,"postgresql":{"parameters":{"max_connections":null}}}`, status: http.StatusOK, contentType: "application/json"},
		{name: "config rewrite", base: serviceBaseURL(t, primary), path: "/config", method: http.MethodPut, body: `{"ttl":20,"loop_wait":3,"retry_timeout":10,"maximum_lag_on_failover":1048576,"postgresql":{"use_slots":true,"use_pg_rewind":true,"parameters":{"hot_standby":"on","wal_level":"hot_standby","max_wal_senders":5}}}`, status: http.StatusOK, contentType: "application/json"},
		{name: "restart scheduled", base: serviceBaseURL(t, primary), path: "/restart", method: http.MethodPost, body: `{"schedule":"2026-03-30T13:00:00Z","restart_pending":true,"role":"primary","postgres_version":170005,"timeout":120}`, status: http.StatusAccepted, contentType: "application/json"},
		{name: "restart delete scheduled", base: serviceBaseURL(t, primary), path: "/restart", method: http.MethodDelete, status: http.StatusOK},
		{name: "restart delete missing", base: serviceBaseURL(t, primary), path: "/restart", method: http.MethodDelete, status: http.StatusNotFound},
		{name: "reload", base: serviceBaseURL(t, primary), path: "/reload", method: http.MethodPost, status: http.StatusOK},
		{name: "reinitialize replica", base: serviceBaseURL(t, syncReplica), path: "/reinitialize", method: http.MethodPost, body: `{"force":true,"from-leader":true}`, status: http.StatusOK, contentType: "application/json"},
		{name: "reinitialize primary rejected", base: serviceBaseURL(t, primary), path: "/reinitialize", method: http.MethodPost, body: `{"force":true,"from-leader":true}`, status: http.StatusServiceUnavailable, contentType: "application/json"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var body []byte
			var headers map[string]string
			if testCase.body != "" {
				body = []byte(testCase.body)
				headers = map[string]string{"Content-Type": testCase.contentType}
				if testCase.method == http.MethodPost || testCase.method == http.MethodPatch || testCase.method == http.MethodPut {
					requireRequestMatchesContract(t, document, testCase.path, strings.ToLower(testCase.method), testCase.contentType, body)
				}
			}

			response := performHTTPRequest(t, testCase.method, testCase.base+testCase.path, body, headers)
			defer response.Body.Close()

			if response.StatusCode != testCase.status {
				t.Fatalf("unexpected status for %s %s: got %d want %d", testCase.method, testCase.path, response.StatusCode, testCase.status)
			}

			payload, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatalf("read response body for %s %s: %v", testCase.method, testCase.path, err)
			}

			requireResponseMatchesContract(t, document, testCase.path, strings.ToLower(testCase.method), response, payload)
		})
	}
}
