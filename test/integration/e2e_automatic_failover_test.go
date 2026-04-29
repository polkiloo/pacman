//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"

	nativeapi "github.com/polkiloo/pacman/internal/api/native"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
	dcsetcd "github.com/polkiloo/pacman/internal/dcs/etcd"
	"github.com/polkiloo/pacman/test/testenv"
)

const automaticFailoverObservationWindow = 6 * time.Second

const (
	e2eAutomaticFailoverRequestedBy = "pacmand"
	e2eAutomaticFailoverReason      = "automatic failover reconciliation"
)

type e2eAutomaticFailoverScenario struct {
	Primary     *e2eSwitchoverNode
	Standby     *e2eSwitchoverNode
	Etcd        *testenv.Service
	NetworkName string
}

func TestEndToEndAutomaticFailover(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	scenario := startEndToEndAutomaticFailoverScenario(t, "etcd-e2e-failover-positive")
	initialStatus := waitForEndToEndSwitchoverInitialTopology(t, scenario.Standby)

	execServiceSQL(t, scenario.Primary.Service, `
CREATE TABLE IF NOT EXISTS e2e_failover_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execServiceSQL(t, scenario.Primary.Service, `
INSERT INTO e2e_failover_marker (id, payload)
VALUES (1, 'before-failover')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_failover_marker WHERE id = 1`, "before-failover")

	stopServicePostgres(t, scenario.Primary.Service)

	intentStatus := waitForEndToEndAutomaticFailoverIntent(t, scenario.Standby)
	finalStatus := waitForEndToEndAutomaticFailoverCompletion(t, scenario.Standby, intentStatus.ActiveOperation.ID)
	waitForServicePostgresRecovery(t, scenario.Standby.Service, false)
	waitForServicePostgresUnavailable(t, scenario.Primary.Service)
	execServiceSQL(t, scenario.Standby.Service, `
INSERT INTO e2e_failover_marker (id, payload)
VALUES (2, 'after-failover')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	historyEntry := waitForEndToEndAutomaticFailoverHistory(t, scenario.Standby, intentStatus.ActiveOperation.ID)
	formerPrimaryStatus := waitForEndToEndSwitchoverStatus(t, scenario.Standby, "former primary needs rejoin", func(status nativeapi.ClusterStatusResponse) bool {
		member := e2eSwitchoverMember(status, e2eSwitchoverSource)
		return member != nil && member.Role == "replica" && member.NeedsRejoin && !member.Healthy
	})

	positiveCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "positive source writes replicate to standby before failure",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_failover_marker WHERE id = 1`, "before-failover")
			},
		},
		{
			name: "positive automatic failover intent records cluster transition metadata",
			run: func(t *testing.T) {
				operation := intentStatus.ActiveOperation
				if operation == nil {
					t.Fatal("expected active failover operation")
				}
				if operation.Kind != "failover" || operation.FromMember != e2eSwitchoverSource || operation.ToMember != e2eSwitchoverTarget {
					t.Fatalf("unexpected failover operation members: %+v", operation)
				}
				if operation.RequestedBy != e2eAutomaticFailoverRequestedBy || operation.Reason != e2eAutomaticFailoverReason {
					t.Fatalf("unexpected failover operation metadata: %+v", operation)
				}
				if operation.State != "accepted" && operation.State != "running" {
					t.Fatalf("expected accepted or running failover operation, got %+v", operation)
				}
			},
		},
		{
			name: "positive cluster converges on promoted standby and advances epoch",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, finalStatus, e2eSwitchoverTarget)
				if finalStatus.CurrentPrimary != e2eSwitchoverTarget || finalStatus.ActiveOperation != nil {
					t.Fatalf("unexpected final failover status: %+v", finalStatus)
				}
				if finalStatus.CurrentEpoch <= initialStatus.CurrentEpoch {
					t.Fatalf("expected failover to advance epoch from %d, got %+v", initialStatus.CurrentEpoch, finalStatus)
				}
				if target.Role != "primary" || !target.Healthy {
					t.Fatalf("unexpected promoted standby status: %+v", target)
				}
			},
		},
		{
			name: "positive promoted standby is writable after failover",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_failover_marker WHERE id = 2`, "after-failover")
			},
		},
		{
			name: "positive history records success and former primary requires rejoin",
			run: func(t *testing.T) {
				if historyEntry.Kind != "failover" || historyEntry.FromMember != e2eSwitchoverSource || historyEntry.ToMember != e2eSwitchoverTarget || historyEntry.Result != "succeeded" {
					t.Fatalf("unexpected failover history entry: %+v", historyEntry)
				}
				source := requireE2ESwitchoverMember(t, formerPrimaryStatus, e2eSwitchoverSource)
				if source.Role != "replica" || !source.NeedsRejoin || source.Healthy {
					t.Fatalf("expected former primary to require rejoin and remain unhealthy, got %+v", source)
				}
			},
		},
	}

	for _, testCase := range positiveCases {
		t.Run(testCase.name, testCase.run)
	}
}

func TestEndToEndAutomaticFailoverNegativeCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	negativeCases := []struct {
		name    string
		prepare func(*testing.T, e2eAutomaticFailoverScenario)
		stopDB  bool
		assert  func(*testing.T, e2eAutomaticFailoverScenario, nativeapi.ClusterStatusResponse)
	}{
		{
			name:   "negative healthy primary does not trigger automatic failover",
			stopDB: false,
			assert: func(t *testing.T, scenario e2eAutomaticFailoverScenario, status nativeapi.ClusterStatusResponse) {
				target := requireE2ESwitchoverMember(t, status, e2eSwitchoverTarget)
				if status.CurrentPrimary != e2eSwitchoverSource || status.ActiveOperation != nil {
					t.Fatalf("expected healthy cluster to avoid failover, got %+v", status)
				}
				if target.Role != "replica" || !target.Healthy {
					t.Fatalf("expected standby to remain healthy replica, got %+v", target)
				}
			},
		},
		{
			name: "negative maintenance mode blocks automatic failover",
			prepare: func(t *testing.T, scenario e2eAutomaticFailoverScenario) {
				setEndToEndAutomaticFailoverMaintenance(t, scenario.Standby, true)
			},
			stopDB: true,
			assert: func(t *testing.T, scenario e2eAutomaticFailoverScenario, status nativeapi.ClusterStatusResponse) {
				target := requireE2ESwitchoverMember(t, status, e2eSwitchoverTarget)
				if !status.Maintenance.Enabled || target.Role != "replica" {
					t.Fatalf("expected maintenance to hold topology steady, got %+v", status)
				}
			},
		},
		{
			name: "negative manual only policy blocks automatic failover",
			prepare: func(t *testing.T, scenario e2eAutomaticFailoverScenario) {
				updateEndToEndAutomaticFailoverSpec(t, scenario, func(spec *cluster.ClusterSpec) {
					spec.Failover.Mode = cluster.FailoverModeManualOnly
				})
				waitForEndToEndAutomaticFailoverSpec(t, scenario.Standby, "manual only failover mode", func(spec nativeapi.ClusterSpecResponse) bool {
					return spec.Failover.Mode == string(cluster.FailoverModeManualOnly)
				})
			},
			stopDB: true,
			assert: func(t *testing.T, scenario e2eAutomaticFailoverScenario, status nativeapi.ClusterStatusResponse) {
				target := requireE2ESwitchoverMember(t, status, e2eSwitchoverTarget)
				if target.Role != "replica" || status.CurrentPrimary != e2eSwitchoverSource {
					t.Fatalf("expected manual-only policy to block failover, got %+v", status)
				}
			},
		},
		{
			name: "negative no failover tag blocks the only standby candidate",
			prepare: func(t *testing.T, scenario e2eAutomaticFailoverScenario) {
				updateEndToEndAutomaticFailoverSpec(t, scenario, func(spec *cluster.ClusterSpec) {
					for index := range spec.Members {
						if spec.Members[index].Name == e2eSwitchoverTarget {
							spec.Members[index].NoFailover = true
						}
					}
				})
				waitForEndToEndAutomaticFailoverSpec(t, scenario.Standby, "standby no-failover tag", func(spec nativeapi.ClusterSpecResponse) bool {
					for _, member := range spec.Members {
						if member.Name == e2eSwitchoverTarget {
							return member.NoFailover
						}
					}
					return false
				})
			},
			stopDB: true,
			assert: func(t *testing.T, scenario e2eAutomaticFailoverScenario, status nativeapi.ClusterStatusResponse) {
				target := requireE2ESwitchoverMember(t, status, e2eSwitchoverTarget)
				if !target.NoFailover || target.Role != "replica" {
					t.Fatalf("expected no-failover policy to keep standby ineligible, got %+v", target)
				}
			},
		},
		{
			name: "negative quorum requirement blocks automatic failover in a two-node cluster",
			prepare: func(t *testing.T, scenario e2eAutomaticFailoverScenario) {
				updateEndToEndAutomaticFailoverSpec(t, scenario, func(spec *cluster.ClusterSpec) {
					spec.Failover.RequireQuorum = true
				})
				waitForEndToEndAutomaticFailoverSpec(t, scenario.Standby, "required failover quorum", func(spec nativeapi.ClusterSpecResponse) bool {
					return spec.Failover.RequireQuorum
				})
			},
			stopDB: true,
			assert: func(t *testing.T, scenario e2eAutomaticFailoverScenario, status nativeapi.ClusterStatusResponse) {
				target := requireE2ESwitchoverMember(t, status, e2eSwitchoverTarget)
				if target.Role != "replica" || status.CurrentPrimary != e2eSwitchoverSource {
					t.Fatalf("expected quorum requirement to block failover, got %+v", status)
				}
			},
		},
	}

	for _, testCase := range negativeCases {
		t.Run(testCase.name, func(t *testing.T) {
			scenario := startEndToEndAutomaticFailoverScenario(t, "etcd-"+sanitizeTestName(t.Name()))
			if testCase.prepare != nil {
				testCase.prepare(t, scenario)
			}
			if testCase.stopDB {
				stopServicePostgres(t, scenario.Primary.Service)
			}

			status := ensureNoEndToEndAutomaticFailover(t, scenario.Standby, automaticFailoverObservationWindow)
			testCase.assert(t, scenario, status)
		})
	}
}

func startEndToEndAutomaticFailoverScenario(t *testing.T, etcdAlias string) e2eAutomaticFailoverScenario {
	t.Helper()

	env := testenv.New(t)
	etcd := startEndToEndAutomaticFailoverEtcd(t, env, etcdAlias)

	members := []string{e2eSwitchoverSource, e2eSwitchoverTarget}
	primary := startEndToEndSwitchoverPrimary(t, env, etcdAlias, members)
	standby := startEndToEndSwitchoverStandby(t, env, etcdAlias, members)

	waitForEndToEndSwitchoverInitialTopology(t, primary)
	waitForEndToEndSwitchoverInitialTopology(t, standby)
	waitForServicePostgresRecovery(t, standby.Service, true)

	return e2eAutomaticFailoverScenario{
		Primary:     primary,
		Standby:     standby,
		Etcd:        etcd,
		NetworkName: env.NetworkName(),
	}
}

func startEndToEndAutomaticFailoverEtcd(t *testing.T, env *testenv.Environment, alias string) *testenv.Service {
	t.Helper()

	return env.StartService(t, testenv.ServiceConfig{
		Name:       alias,
		Image:      testEtcdImage,
		Aliases:    []string{alias},
		Entrypoint: []string{"etcd"},
		Cmd: []string{
			"--name=default",
			"--data-dir=/etcd-data",
			"--listen-client-urls=http://0.0.0.0:2379",
			"--advertise-client-urls=http://" + alias + ":2379",
			"--listen-peer-urls=http://0.0.0.0:2380",
			"--initial-advertise-peer-urls=http://" + alias + ":2380",
			"--initial-cluster=default=http://" + alias + ":2380",
		},
		ExposedPorts: []string{"2379/tcp"},
		WaitStrategy: wait.ForHTTP("/health").
			WithPort("2379/tcp").
			WithStartupTimeout(60 * time.Second),
	})
}

func waitForEndToEndAutomaticFailoverIntent(t *testing.T, node *e2eSwitchoverNode) nativeapi.ClusterStatusResponse {
	t.Helper()

	return waitForEndToEndSwitchoverStatus(t, node, "automatic failover intent", func(status nativeapi.ClusterStatusResponse) bool {
		operation := status.ActiveOperation
		return operation != nil &&
			operation.Kind == "failover" &&
			operation.FromMember == e2eSwitchoverSource &&
			operation.ToMember == e2eSwitchoverTarget
	})
}

func waitForEndToEndAutomaticFailoverCompletion(t *testing.T, node *e2eSwitchoverNode, operationID string) nativeapi.ClusterStatusResponse {
	t.Helper()

	return waitForEndToEndSwitchoverStatus(t, node, "automatic failover completion", func(status nativeapi.ClusterStatusResponse) bool {
		target := e2eSwitchoverMember(status, e2eSwitchoverTarget)
		return status.CurrentPrimary == e2eSwitchoverTarget &&
			status.ActiveOperation == nil &&
			target != nil &&
			target.Role == "primary" &&
			target.Healthy &&
			operationID != ""
	})
}

func waitForEndToEndAutomaticFailoverHistory(t *testing.T, node *e2eSwitchoverNode, operationID string) nativeapi.HistoryEntry {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	var last nativeapi.HistoryResponse
	for time.Now().Before(deadline) {
		clusterJSON(t, node.Client, node.Base+"/api/v1/history", &last)
		for _, entry := range last.Items {
			if entry.OperationID == operationID && entry.Kind == "failover" && entry.Result == "succeeded" {
				return entry
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("failover history entry %q did not appear; last history: %+v", operationID, last)
	return nativeapi.HistoryEntry{}
}

func ensureNoEndToEndAutomaticFailover(t *testing.T, node *e2eSwitchoverNode, duration time.Duration) nativeapi.ClusterStatusResponse {
	t.Helper()

	deadline := time.Now().Add(duration)
	var last nativeapi.ClusterStatusResponse
	for time.Now().Before(deadline) {
		clusterJSON(t, node.Client, node.Base+topologyClusterAPI, &last)
		if last.ActiveOperation != nil && last.ActiveOperation.Kind == "failover" {
			t.Fatalf("expected no automatic failover operation, got %+v", last.ActiveOperation)
		}

		target := e2eSwitchoverMember(last, e2eSwitchoverTarget)
		if target != nil && target.Role == "primary" {
			t.Fatalf("expected standby to remain non-primary, got %+v", target)
		}

		time.Sleep(300 * time.Millisecond)
	}

	var history nativeapi.HistoryResponse
	clusterJSON(t, node.Client, node.Base+"/api/v1/history", &history)
	for _, entry := range history.Items {
		if entry.Kind == "failover" {
			t.Fatalf("expected blocked automatic failover not to record history, got %+v", history.Items)
		}
	}

	return last
}

func setEndToEndAutomaticFailoverMaintenance(t *testing.T, node *e2eSwitchoverNode, enabled bool) {
	t.Helper()

	body := []byte(`{"enabled":true,"reason":"automatic failover negative case","requestedBy":"e2e-test"}`)
	if !enabled {
		body = []byte(`{"enabled":false,"reason":"automatic failover negative case","requestedBy":"e2e-test"}`)
	}

	resp := performHTTPRequest(t, http.MethodPut, node.Base+topologyMaintenanceAPI, body, map[string]string{
		topologyContentType: topologyApplicationJSON,
	})
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read maintenance response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT %s: got status %d, want %d, body: %s", topologyMaintenanceAPI, resp.StatusCode, http.StatusOK, respBody)
	}

	waitForTopologyMaintenanceState(t, node.Client, node.Base, enabled)
}

func updateEndToEndAutomaticFailoverSpec(t *testing.T, scenario e2eAutomaticFailoverScenario, mutate func(*cluster.ClusterSpec)) cluster.ClusterSpec {
	t.Helper()

	endpoint := "http://" + scenario.Etcd.Address(t, "2379")
	backend, err := dcsetcd.New(dcs.Config{
		Backend:      dcs.BackendEtcd,
		ClusterName:  e2eSwitchoverClusterName,
		TTL:          dcs.DefaultTTL,
		RetryTimeout: 5 * time.Second,
		Etcd: &dcs.EtcdConfig{
			Endpoints: []string{endpoint},
		},
	})
	if err != nil {
		t.Fatalf("open etcd backend: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := backend.Close(); closeErr != nil {
			t.Fatalf("close etcd backend: %v", closeErr)
		}
	})

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize etcd backend: %v", err)
	}

	keyspace, err := dcs.NewKeySpace(e2eSwitchoverClusterName)
	if err != nil {
		t.Fatalf("build keyspace: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	value, err := backend.Get(ctx, keyspace.Config())
	if err != nil {
		t.Fatalf("load cluster spec from etcd: %v", err)
	}

	var spec cluster.ClusterSpec
	if err := json.Unmarshal(value.Value, &spec); err != nil {
		t.Fatalf("decode cluster spec: %v", err)
	}

	mutate(&spec)
	spec.Generation++

	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("encode cluster spec: %v", err)
	}

	if err := backend.Set(ctx, keyspace.Config(), payload); err != nil {
		t.Fatalf("store cluster spec override: %v", err)
	}

	return spec
}

func waitForEndToEndAutomaticFailoverSpec(
	t *testing.T,
	node *e2eSwitchoverNode,
	description string,
	ready func(nativeapi.ClusterSpecResponse) bool,
) nativeapi.ClusterSpecResponse {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	var last nativeapi.ClusterSpecResponse
	for time.Now().Before(deadline) {
		resp, err := node.Client.Get(node.Base + topologyClusterSpecAPI)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK && json.Unmarshal(body, &last) == nil && ready(last) {
				return last
			}
		}

		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("%s did not converge on %s; last spec: %+v", node.Name, description, last)
	return nativeapi.ClusterSpecResponse{}
}

func sanitizeTestName(raw string) string {
	sanitized := make([]rune, 0, len(raw))
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			sanitized = append(sanitized, r)
		case r >= 'A' && r <= 'Z':
			sanitized = append(sanitized, r+('a'-'A'))
		case r >= '0' && r <= '9':
			sanitized = append(sanitized, r)
		default:
			sanitized = append(sanitized, '-')
		}
	}

	name := strings.Trim(string(sanitized), "-")
	if name == "" {
		return "automatic-failover"
	}

	const maxLabelLength = 55
	if len(name) <= maxLabelLength {
		return name
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(raw))
	suffix := fmt.Sprintf("-%08x", hasher.Sum32())
	prefix := strings.TrimRight(name[:maxLabelLength-len(suffix)], "-")
	if prefix == "" {
		return "automatic" + suffix
	}

	return prefix + suffix
}
