//go:build integration

package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPacmandHTTPAPIServesHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	daemon := startSingleNodeDaemon(t, "alpha-http")
	document := loadContractDocument(t)
	waitForProbeStatus(t, daemon.Client, daemon.Base+"/health", http.StatusOK, pacmandStartupTimeout)

	resp, err := daemon.Client.Get(daemon.Base + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	var payload struct {
		State   string `json:"state"`
		Role    string `json:"role"`
		Patroni struct {
			Name  string `json:"name"`
			Scope string `json:"scope"`
		} `json:"patroni"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode /health payload: %v", err)
	}

	if payload.State != "running" {
		t.Errorf("/health state: got %q, want %q", payload.State, "running")
	}
	if payload.Role != "primary" {
		t.Errorf("/health role: got %q, want %q", payload.Role, "primary")
	}
	if payload.Patroni.Name != "alpha-http" {
		t.Errorf("/health patroni.name: got %q, want %q", payload.Patroni.Name, "alpha-http")
	}

	t.Run("metrics", func(t *testing.T) {
		metricsResp, err := daemon.Client.Get(daemon.Base + "/metrics")
		if err != nil {
			t.Fatalf("GET /metrics: %v", err)
		}
		defer metricsResp.Body.Close()

		metricsBody, err := io.ReadAll(metricsResp.Body)
		if err != nil {
			t.Fatalf("read /metrics body: %v", err)
		}

		if metricsResp.StatusCode != http.StatusOK {
			t.Fatalf("/metrics: got status %d, want %d", metricsResp.StatusCode, http.StatusOK)
		}

		requireResponseMatchesContract(t, document, "/metrics", "get", metricsResp, metricsBody)

		text := string(metricsBody)
		for _, want := range []string{
			"pacman_cluster_spec_members_desired 1",
			"pacman_cluster_members_observed 1",
			`pacman_cluster_phase{phase="healthy"} 1`,
			`pacman_cluster_primary{member="alpha-http"} 1`,
			`pacman_member_info{member="alpha-http",role="primary",state="running"} 1`,
			`pacman_node_info{member="alpha-http",node="alpha-http",role="primary",state="running"} 1`,
			`pacman_node_postgres_up{node="alpha-http"} 1`,
			`pacman_node_controlplane_reachable{node="alpha-http"} 1`,
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("expected /metrics to contain %q, got:\n%s", want, text)
			}
		}
	})
}

// TestPacmandPrimaryAndReplicaProbes verifies that the PACMAN daemon exposes
// Patroni-compatible /primary and /replica probe endpoints whose response
// shapes and status codes match the OpenAPI contract.
func TestPacmandPrimaryAndReplicaProbes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	daemon := startSingleNodeDaemon(t, "alpha-primary")
	document := loadContractDocument(t)

	waitForProbeStatus(t, daemon.Client, daemon.Base+"/health", http.StatusOK, pacmandStartupTimeout)

	probes := []struct {
		path           string
		method         string
		wantStatus     int
		contractPath   string
		contractMethod string
	}{
		// /primary → 200 when node is the primary
		{path: "/primary", method: http.MethodGet, wantStatus: http.StatusOK, contractPath: "/primary", contractMethod: "get"},
		// /replica → 503 because this node is the primary, not a replica
		{path: "/replica", method: http.MethodGet, wantStatus: http.StatusServiceUnavailable, contractPath: "/replica", contractMethod: "get"},
		// HEAD mirrors GET status codes; fasthttp suppresses the body automatically
		{path: "/primary", method: http.MethodHead, wantStatus: http.StatusOK},
		{path: "/replica", method: http.MethodHead, wantStatus: http.StatusServiceUnavailable},
		// OPTIONS returns 200 with an Allow header for all probe routes
		{path: "/primary", method: http.MethodOptions, wantStatus: http.StatusOK},
		{path: "/replica", method: http.MethodOptions, wantStatus: http.StatusOK},
	}

	for _, probe := range probes {
		req, err := http.NewRequest(probe.method, daemon.Base+probe.path, nil)
		if err != nil {
			t.Fatalf("build request %s %s: %v", probe.method, probe.path, err)
		}

		resp, err := daemon.Client.Do(req)
		if err != nil {
			t.Fatalf("perform %s %s: %v", probe.method, probe.path, err)
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read body %s %s: %v", probe.method, probe.path, readErr)
		}

		if resp.StatusCode != probe.wantStatus {
			t.Errorf("%s %s: got status %d, want %d (body: %s)",
				probe.method, probe.path, resp.StatusCode, probe.wantStatus, body)
		}

		if probe.method == http.MethodOptions {
			allow := resp.Header.Get("Allow")
			for _, m := range []string{"GET", "HEAD", "OPTIONS"} {
				if !strings.Contains(allow, m) {
					t.Errorf("OPTIONS %s Allow header %q missing %s", probe.path, allow, m)
				}
			}
		}

		if probe.method == http.MethodGet && probe.contractPath != "" {
			requireResponseMatchesContract(t, document, probe.contractPath, probe.contractMethod, resp, body)
		}
	}

	// Verify the primary probe response has the expected Patroni-compatible fields.
	primaryResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/primary", nil, nil)
	primaryBody, _ := io.ReadAll(primaryResp.Body)
	primaryResp.Body.Close()

	var primaryPayload struct {
		State   string `json:"state"`
		Role    string `json:"role"`
		Patroni struct {
			Name  string `json:"name"`
			Scope string `json:"scope"`
		} `json:"patroni"`
	}
	if err := json.Unmarshal(primaryBody, &primaryPayload); err != nil {
		t.Fatalf("decode /primary payload: %v", err)
	}
	if primaryPayload.State != "running" {
		t.Errorf("/primary state: got %q, want %q", primaryPayload.State, "running")
	}
	if primaryPayload.Role != "primary" {
		t.Errorf("/primary role: got %q, want %q", primaryPayload.Role, "primary")
	}
	if primaryPayload.Patroni.Name != "alpha-primary" {
		t.Errorf("/primary patroni.name: got %q, want %q", primaryPayload.Patroni.Name, "alpha-primary")
	}
}

func TestPacmandNativeNodeAndMembersAPIWithRealPostgresOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	daemon := startSingleNodeDaemon(t, "alpha-node")
	document := loadContractDocument(t)

	waitForProbeStatus(t, daemon.Client, daemon.Base+"/health", http.StatusOK, pacmandStartupTimeout)
	waitForProbeStatus(t, daemon.Client, daemon.Base+"/primary", http.StatusOK, pacmandStartupTimeout)
	waitForProbeStatus(t, daemon.Client, daemon.Base+"/api/v1/members", http.StatusOK, pacmandStartupTimeout)

	db := openFixtureDB(t, daemon.Postgres)
	defer db.Close()

	for _, stmt := range []string{
		`create table if not exists api_smoke (id integer primary key, note text not null)`,
		`insert into api_smoke (id, note) values (1, 'patroni-compatible') on conflict (id) do update set note = excluded.note`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec sql: %v", err)
		}
	}

	var rows int
	if err := db.QueryRow(`select count(*) from api_smoke where note = 'patroni-compatible'`).Scan(&rows); err != nil {
		t.Fatalf("verify postgres write/read: %v", err)
	}
	if rows != 1 {
		t.Fatalf("unexpected postgres row count: got %d, want 1", rows)
	}

	// Validate /primary shape and Patroni-compatible response fields.
	primaryResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/primary", nil, nil)
	primaryBody, err := io.ReadAll(primaryResp.Body)
	primaryResp.Body.Close()
	if err != nil {
		t.Fatalf("read /primary response: %v", err)
	}
	if primaryResp.StatusCode != http.StatusOK {
		t.Fatalf("/primary: got status %d, want %d", primaryResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/primary", "get", primaryResp, primaryBody)

	var primaryPayload struct {
		State   string `json:"state"`
		Role    string `json:"role"`
		Patroni struct {
			Name  string `json:"name"`
			Scope string `json:"scope"`
		} `json:"patroni"`
	}
	if err := json.Unmarshal(primaryBody, &primaryPayload); err != nil {
		t.Fatalf("decode /primary payload: %v", err)
	}
	if primaryPayload.Role != "primary" || primaryPayload.State != "running" {
		t.Fatalf("/primary unexpected payload: %+v", primaryPayload)
	}

	// Validate /api/v1/nodes/{nodeName} returns full node status with postgres details.
	nodeResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/nodes/alpha-node", nil, nil)
	nodeBody, err := io.ReadAll(nodeResp.Body)
	nodeResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/nodes response: %v", err)
	}
	if nodeResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/nodes/alpha-node: got status %d, want %d", nodeResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/nodes/{nodeName}", "get", nodeResp, nodeBody)

	var nodePayload struct {
		NodeName string `json:"nodeName"`
		Role     string `json:"role"`
		State    string `json:"state"`
		Postgres struct {
			Managed bool `json:"managed"`
			Up      bool `json:"up"`
			Details struct {
				ServerVersion    int    `json:"serverVersion"`
				SystemIdentifier string `json:"systemIdentifier"`
			} `json:"details"`
		} `json:"postgres"`
	}
	if err := json.Unmarshal(nodeBody, &nodePayload); err != nil {
		t.Fatalf("decode /api/v1/nodes payload: %v", err)
	}
	if nodePayload.NodeName != "alpha-node" || nodePayload.Role != "primary" {
		t.Fatalf("/api/v1/nodes/alpha-node unexpected payload: %+v", nodePayload)
	}
	if !nodePayload.Postgres.Managed || !nodePayload.Postgres.Up {
		t.Fatalf("/api/v1/nodes/alpha-node unexpected postgres payload: %+v", nodePayload.Postgres)
	}
	if nodePayload.Postgres.Details.ServerVersion == 0 || nodePayload.Postgres.Details.SystemIdentifier == "" {
		t.Fatalf("expected postgres details from real postgres observation, got %+v", nodePayload.Postgres.Details)
	}

	// Validate /api/v1/members returns the single registered member.
	membersResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/members", nil, nil)
	membersBody, err := io.ReadAll(membersResp.Body)
	membersResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/members response: %v", err)
	}
	if membersResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/members: got status %d, want %d", membersResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/members", "get", membersResp, membersBody)

	var membersPayload struct {
		Items []struct {
			Name    string `json:"name"`
			Role    string `json:"role"`
			State   string `json:"state"`
			Healthy bool   `json:"healthy"`
		} `json:"items"`
	}
	if err := json.Unmarshal(membersBody, &membersPayload); err != nil {
		t.Fatalf("decode /api/v1/members payload: %v", err)
	}
	if len(membersPayload.Items) != 1 {
		t.Fatalf("/api/v1/members: got %d items, want 1", len(membersPayload.Items))
	}
	member := membersPayload.Items[0]
	if member.Name != "alpha-node" || member.Role != "primary" || !member.Healthy {
		t.Fatalf("/api/v1/members unexpected member payload: %+v", member)
	}
}

func TestPacmandHistoryMaintenanceAndDiagnosticsAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	daemon := startSingleNodeDaemon(t, "alpha-admin")
	document := loadContractDocument(t)

	waitForProbeStatus(t, daemon.Client, daemon.Base+"/health", http.StatusOK, pacmandStartupTimeout)
	waitForProbeStatus(t, daemon.Client, daemon.Base+"/api/v1/members", http.StatusOK, pacmandStartupTimeout)

	// History must be empty at startup.
	historyResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/history", nil, nil)
	historyBody, err := io.ReadAll(historyResp.Body)
	historyResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/history response: %v", err)
	}
	if historyResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/history: got status %d, want %d", historyResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/history", "get", historyResp, historyBody)

	var initialHistory struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(historyBody, &initialHistory); err != nil {
		t.Fatalf("decode initial history: %v", err)
	}
	if len(initialHistory.Items) != 0 {
		t.Fatalf("expected empty initial history, got %+v", initialHistory.Items)
	}

	// Maintenance must be disabled at startup.
	maintenanceResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/maintenance", nil, nil)
	maintenanceBody, err := io.ReadAll(maintenanceResp.Body)
	maintenanceResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/maintenance response: %v", err)
	}
	if maintenanceResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/maintenance GET: got status %d, want %d", maintenanceResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/maintenance", "get", maintenanceResp, maintenanceBody)

	var maintenancePayload struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.Unmarshal(maintenanceBody, &maintenancePayload); err != nil {
		t.Fatalf("decode maintenance payload: %v", err)
	}
	if maintenancePayload.Enabled {
		t.Fatal("expected maintenance mode to start disabled")
	}

	// Enable maintenance mode and verify the response.
	maintenanceRequest := []byte(`{"enabled":true,"reason":"integration maintenance","requestedBy":"integration"}`)
	requireRequestMatchesContract(t, document, "/api/v1/maintenance", "put", "application/json", maintenanceRequest)

	putResp := performHTTPRequest(t, http.MethodPut, daemon.Base+"/api/v1/maintenance", maintenanceRequest, map[string]string{
		"Content-Type": "application/json",
	})
	putBody, err := io.ReadAll(putResp.Body)
	putResp.Body.Close()
	if err != nil {
		t.Fatalf("read PUT /api/v1/maintenance response: %v", err)
	}
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT /api/v1/maintenance: got status %d, want %d (body: %s)", putResp.StatusCode, http.StatusOK, putBody)
	}
	requireResponseMatchesContract(t, document, "/api/v1/maintenance", "put", putResp, putBody)

	var updatedMaintenance struct {
		Enabled     bool   `json:"enabled"`
		Reason      string `json:"reason"`
		RequestedBy string `json:"requestedBy"`
	}
	if err := json.Unmarshal(putBody, &updatedMaintenance); err != nil {
		t.Fatalf("decode maintenance update payload: %v", err)
	}
	if !updatedMaintenance.Enabled || updatedMaintenance.Reason != "integration maintenance" || updatedMaintenance.RequestedBy != "integration" {
		t.Fatalf("PUT /api/v1/maintenance unexpected payload: %+v", updatedMaintenance)
	}

	// History must contain the maintenance change event.
	historyAfterResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/history", nil, nil)
	historyAfterBody, err := io.ReadAll(historyAfterResp.Body)
	historyAfterResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/history after maintenance response: %v", err)
	}
	if historyAfterResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/history after maintenance: got status %d, want %d", historyAfterResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/history", "get", historyAfterResp, historyAfterBody)

	var historyAfter struct {
		Items []struct {
			Kind   string `json:"kind"`
			Result string `json:"result"`
		} `json:"items"`
	}
	if err := json.Unmarshal(historyAfterBody, &historyAfter); err != nil {
		t.Fatalf("decode history-after payload: %v", err)
	}
	if len(historyAfter.Items) != 1 {
		t.Fatalf("expected one history entry after maintenance change, got %+v", historyAfter.Items)
	}
	if historyAfter.Items[0].Kind != "maintenance_change" || historyAfter.Items[0].Result != "succeeded" {
		t.Fatalf("unexpected history entry: %+v", historyAfter.Items[0])
	}

	// Diagnostics must report cluster name, member list, and warnings when maintenance is on.
	diagnosticsResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/diagnostics", nil, nil)
	diagnosticsBody, err := io.ReadAll(diagnosticsResp.Body)
	diagnosticsResp.Body.Close()
	if err != nil {
		t.Fatalf("read /api/v1/diagnostics response: %v", err)
	}
	if diagnosticsResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/diagnostics: got status %d, want %d", diagnosticsResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/diagnostics", "get", diagnosticsResp, diagnosticsBody)

	var diagnosticsPayload struct {
		ClusterName string   `json:"clusterName"`
		Warnings    []string `json:"warnings"`
		Members     []struct {
			Name string `json:"name"`
			Role string `json:"role"`
		} `json:"members"`
	}
	if err := json.Unmarshal(diagnosticsBody, &diagnosticsPayload); err != nil {
		t.Fatalf("decode diagnostics payload: %v", err)
	}
	if diagnosticsPayload.ClusterName != "alpha" {
		t.Fatalf("/api/v1/diagnostics clusterName: got %q, want %q", diagnosticsPayload.ClusterName, "alpha")
	}
	if len(diagnosticsPayload.Members) != 1 || diagnosticsPayload.Members[0].Name != "alpha-admin" {
		t.Fatalf("/api/v1/diagnostics unexpected members: %+v", diagnosticsPayload.Members)
	}
	if len(diagnosticsPayload.Warnings) == 0 {
		t.Fatal("expected diagnostics warnings after enabling maintenance")
	}

	// ?includeMembers=false must return an empty members list.
	compactResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/api/v1/diagnostics?includeMembers=false", nil, nil)
	compactBody, err := io.ReadAll(compactResp.Body)
	compactResp.Body.Close()
	if err != nil {
		t.Fatalf("read compact /api/v1/diagnostics response: %v", err)
	}
	if compactResp.StatusCode != http.StatusOK {
		t.Fatalf("/api/v1/diagnostics?includeMembers=false: got status %d, want %d", compactResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/api/v1/diagnostics", "get", compactResp, compactBody)

	var compactPayload struct {
		Members []map[string]any `json:"members"`
	}
	if err := json.Unmarshal(compactBody, &compactPayload); err != nil {
		t.Fatalf("decode compact diagnostics payload: %v", err)
	}
	if len(compactPayload.Members) != 0 {
		t.Fatalf("expected empty compact diagnostics members, got %+v", compactPayload.Members)
	}
}

func TestPacmandOperationsAndPublishedOpenAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	daemon := startSingleNodeDaemon(t, "alpha-ops")
	document := loadContractDocument(t)

	waitForProbeStatus(t, daemon.Client, daemon.Base+"/health", http.StatusOK, pacmandStartupTimeout)
	waitForProbeStatus(t, daemon.Client, daemon.Base+"/api/v1/members", http.StatusOK, pacmandStartupTimeout)

	db := openFixtureDB(t, daemon.Postgres)
	defer db.Close()

	for _, stmt := range []string{
		`create table if not exists api_operations_smoke (id integer primary key, note text not null)`,
		`insert into api_operations_smoke (id, note) values (1, 'published-openapi') on conflict (id) do update set note = excluded.note`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec operations sql: %v", err)
		}
	}

	var note string
	if err := db.QueryRow(`select note from api_operations_smoke where id = 1`).Scan(&note); err != nil {
		t.Fatalf("verify operations postgres write/read: %v", err)
	}
	if note != "published-openapi" {
		t.Fatalf("unexpected operations postgres note: got %q", note)
	}

	openAPIResp := performHTTPRequest(t, http.MethodGet, daemon.Base+"/openapi.yaml", nil, nil)
	openAPIBody, err := io.ReadAll(openAPIResp.Body)
	openAPIResp.Body.Close()
	if err != nil {
		t.Fatalf("read /openapi.yaml response: %v", err)
	}
	if openAPIResp.StatusCode != http.StatusOK {
		t.Fatalf("/openapi.yaml: got status %d, want %d", openAPIResp.StatusCode, http.StatusOK)
	}
	requireResponseMatchesContract(t, document, "/openapi.yaml", "get", openAPIResp, openAPIBody)

	openAPIDoc := string(openAPIBody)
	for _, want := range []string{"/openapi.yaml:", "/api/v1/operations/switchover:", "/api/v1/operations/failover:"} {
		if !strings.Contains(openAPIDoc, want) {
			t.Fatalf("published openapi document is missing %q", want)
		}
	}

	switchoverRequest := []byte(`{"candidate":"alpha-2","reason":"integration switchover","requestedBy":"integration"}`)
	requireRequestMatchesContract(t, document, "/api/v1/operations/switchover", "post", "application/json", switchoverRequest)

	switchoverResp := performHTTPRequest(t, http.MethodPost, daemon.Base+"/api/v1/operations/switchover", switchoverRequest, map[string]string{
		"Content-Type": "application/json",
	})
	switchoverBody, err := io.ReadAll(switchoverResp.Body)
	switchoverResp.Body.Close()
	if err != nil {
		t.Fatalf("read POST /api/v1/operations/switchover response: %v", err)
	}
	if switchoverResp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("POST /api/v1/operations/switchover: got status %d, want %d (body: %s)", switchoverResp.StatusCode, http.StatusPreconditionFailed, switchoverBody)
	}
	requireResponseMatchesContract(t, document, "/api/v1/operations/switchover", "post", switchoverResp, switchoverBody)

	var switchoverError struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(switchoverBody, &switchoverError); err != nil {
		t.Fatalf("decode switchover error payload: %v", err)
	}
	if switchoverError.Error != "switchover_precondition_failed" {
		t.Fatalf("unexpected switchover error: %+v", switchoverError)
	}

	cancelResp := performHTTPRequest(t, http.MethodDelete, daemon.Base+"/api/v1/operations/switchover", nil, nil)
	cancelBody, err := io.ReadAll(cancelResp.Body)
	cancelResp.Body.Close()
	if err != nil {
		t.Fatalf("read DELETE /api/v1/operations/switchover response: %v", err)
	}
	if cancelResp.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETE /api/v1/operations/switchover: got status %d, want %d (body: %s)", cancelResp.StatusCode, http.StatusNotFound, cancelBody)
	}
	requireResponseMatchesContract(t, document, "/api/v1/operations/switchover", "delete", cancelResp, cancelBody)

	var cancelError struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(cancelBody, &cancelError); err != nil {
		t.Fatalf("decode switchover cancel error payload: %v", err)
	}
	if cancelError.Error != "scheduled_switchover_not_found" {
		t.Fatalf("unexpected switchover cancel error: %+v", cancelError)
	}

	failoverRequest := []byte(`{"reason":"integration failover","requestedBy":"integration"}`)
	requireRequestMatchesContract(t, document, "/api/v1/operations/failover", "post", "application/json", failoverRequest)

	failoverResp := performHTTPRequest(t, http.MethodPost, daemon.Base+"/api/v1/operations/failover", failoverRequest, map[string]string{
		"Content-Type": "application/json",
	})
	failoverBody, err := io.ReadAll(failoverResp.Body)
	failoverResp.Body.Close()
	if err != nil {
		t.Fatalf("read POST /api/v1/operations/failover response: %v", err)
	}
	if failoverResp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("POST /api/v1/operations/failover: got status %d, want %d (body: %s)", failoverResp.StatusCode, http.StatusPreconditionFailed, failoverBody)
	}
	requireResponseMatchesContract(t, document, "/api/v1/operations/failover", "post", failoverResp, failoverBody)

	var failoverError struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(failoverBody, &failoverError); err != nil {
		t.Fatalf("decode failover error payload: %v", err)
	}
	if failoverError.Error != "failover_precondition_failed" {
		t.Fatalf("unexpected failover error: %+v", failoverError)
	}
}
