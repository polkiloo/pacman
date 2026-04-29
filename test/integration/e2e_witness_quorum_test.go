//go:build integration

package integration_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	nativeapi "github.com/polkiloo/pacman/internal/api/native"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/test/testenv"
)

const e2eWitnessQuorumMember = "witness-1"

type e2eWitnessQuorumScenario struct {
	e2eAutomaticFailoverScenario
	Witness *e2eSwitchoverNode
}

func TestEndToEndWitnessAssistedQuorum(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	scenario := startEndToEndWitnessQuorumScenario(t, "etcd-e2e-witness-quorum-positive")
	initialStatus := waitForEndToEndWitnessQuorumInitialTopology(t, scenario)
	initialDiagnostics := fetchEndToEndDiagnostics(t, scenario.Standby)

	execServiceSQL(t, scenario.Primary.Service, `
CREATE TABLE IF NOT EXISTS e2e_witness_quorum_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execServiceSQL(t, scenario.Primary.Service, `
INSERT INTO e2e_witness_quorum_marker (id, payload)
VALUES (1, 'before-witness-quorum-failover')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_witness_quorum_marker WHERE id = 1`, "before-witness-quorum-failover")

	stopServicePostgres(t, scenario.Primary.Service)

	intentStatus := waitForEndToEndAutomaticFailoverIntent(t, scenario.Standby)
	finalStatus := waitForEndToEndAutomaticFailoverCompletion(t, scenario.Standby, intentStatus.ActiveOperation.ID)
	waitForServicePostgresRecovery(t, scenario.Standby.Service, false)
	waitForServicePostgresUnavailable(t, scenario.Primary.Service)
	execServiceSQL(t, scenario.Standby.Service, `
INSERT INTO e2e_witness_quorum_marker (id, payload)
VALUES (2, 'after-witness-quorum-failover')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	historyEntry := waitForEndToEndAutomaticFailoverHistory(t, scenario.Standby, intentStatus.ActiveOperation.ID)
	formerPrimaryStatus := waitForEndToEndSwitchoverStatus(t, scenario.Standby, "former primary needs rejoin with witness quorum", func(status nativeapi.ClusterStatusResponse) bool {
		source := e2eSwitchoverMember(status, e2eSwitchoverSource)
		witness := e2eSwitchoverMember(status, e2eWitnessQuorumMember)
		return status.CurrentPrimary == e2eSwitchoverTarget &&
			source != nil &&
			source.NeedsRejoin &&
			!source.Healthy &&
			witness != nil &&
			witness.Role == "witness" &&
			witness.Healthy
	})

	positiveCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "positive spec requires quorum and declares witness as non-failover voter",
			run: func(t *testing.T) {
				spec := waitForEndToEndAutomaticFailoverSpec(t, scenario.Standby, "witness quorum spec", func(spec nativeapi.ClusterSpecResponse) bool {
					return spec.Failover.RequireQuorum && len(spec.Members) == 3
				})
				witness := requireE2EWitnessSpecMember(t, spec, e2eWitnessQuorumMember)
				if !witness.NoFailover || witness.Priority != 10 {
					t.Fatalf("expected witness desired failover policy, got %+v", witness)
				}
			},
		},
		{
			name: "positive initial topology includes healthy witness alongside data members",
			run: func(t *testing.T) {
				witness := requireE2ESwitchoverMember(t, initialStatus, e2eWitnessQuorumMember)
				if witness.Role != "witness" || !witness.Healthy || !witness.NoFailover {
					t.Fatalf("unexpected initial witness status: %+v", witness)
				}
			},
		},
		{
			name: "positive initial quorum is reachable with all three voters online",
			run: func(t *testing.T) {
				if initialDiagnostics.QuorumReachable == nil || !*initialDiagnostics.QuorumReachable {
					t.Fatalf("expected initial quorum to be reachable, got %+v", initialDiagnostics)
				}
			},
		},
		{
			name: "positive source writes replicate before witness assisted failover",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_witness_quorum_marker WHERE id = 1`, "before-witness-quorum-failover")
			},
		},
		{
			name: "positive primary failure creates automatic failover using standby and witness quorum",
			run: func(t *testing.T) {
				operation := intentStatus.ActiveOperation
				if operation == nil {
					t.Fatal("expected active failover operation")
				}
				if operation.Kind != "failover" || operation.FromMember != e2eSwitchoverSource || operation.ToMember != e2eSwitchoverTarget {
					t.Fatalf("unexpected witness quorum failover operation: %+v", operation)
				}
				if operation.RequestedBy != e2eAutomaticFailoverRequestedBy || operation.Reason != e2eAutomaticFailoverReason {
					t.Fatalf("unexpected witness quorum failover metadata: %+v", operation)
				}
			},
		},
		{
			name: "positive standby promotes and advances epoch while witness stays healthy",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, finalStatus, e2eSwitchoverTarget)
				witness := requireE2ESwitchoverMember(t, finalStatus, e2eWitnessQuorumMember)
				if finalStatus.CurrentPrimary != e2eSwitchoverTarget || finalStatus.ActiveOperation != nil {
					t.Fatalf("unexpected final witness quorum status: %+v", finalStatus)
				}
				if finalStatus.CurrentEpoch <= initialStatus.CurrentEpoch {
					t.Fatalf("expected failover to advance epoch from %d, got %+v", initialStatus.CurrentEpoch, finalStatus)
				}
				if target.Role != "primary" || !target.Healthy {
					t.Fatalf("expected standby to promote, got %+v", target)
				}
				if witness.Role != "witness" || !witness.Healthy {
					t.Fatalf("expected witness to remain healthy after failover, got %+v", witness)
				}
			},
		},
		{
			name: "positive promoted standby is writable after witness assisted failover",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_witness_quorum_marker WHERE id = 2`, "after-witness-quorum-failover")
			},
		},
		{
			name: "positive history records failover and former primary requires rejoin",
			run: func(t *testing.T) {
				if historyEntry.Kind != "failover" || historyEntry.FromMember != e2eSwitchoverSource || historyEntry.ToMember != e2eSwitchoverTarget || historyEntry.Result != "succeeded" {
					t.Fatalf("unexpected witness quorum failover history entry: %+v", historyEntry)
				}
				source := requireE2ESwitchoverMember(t, formerPrimaryStatus, e2eSwitchoverSource)
				if formerPrimaryStatus.CurrentPrimary == e2eSwitchoverSource || !source.NeedsRejoin || source.Healthy {
					t.Fatalf("expected former primary to require rejoin, got %+v", source)
				}
			},
		},
	}

	for _, testCase := range positiveCases {
		t.Run(testCase.name, testCase.run)
	}
}

func TestEndToEndWitnessAssistedQuorumNegativeCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	scenario := startEndToEndWitnessQuorumScenario(t, "etcd-e2e-witness-quorum-negative")
	_ = waitForEndToEndWitnessQuorumInitialTopology(t, scenario)

	scenario.Witness.Service.Stop(t)
	witnessLostStatus := waitForEndToEndSwitchoverStatus(t, scenario.Standby, "witness quorum loss", func(status nativeapi.ClusterStatusResponse) bool {
		witness := e2eSwitchoverMember(status, e2eWitnessQuorumMember)
		return witness != nil && !witness.Healthy
	})
	stopServicePostgres(t, scenario.Primary.Service)

	blockedStatus := ensureNoEndToEndAutomaticFailover(t, scenario.Standby, automaticFailoverObservationWindow)
	blockedDiagnostics := fetchEndToEndDiagnostics(t, scenario.Standby)
	blockedHistory := fetchEndToEndHistory(t, scenario.Standby)

	negativeCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "negative lost witness is not healthy",
			run: func(t *testing.T) {
				witness := requireE2ESwitchoverMember(t, witnessLostStatus, e2eWitnessQuorumMember)
				if witness.Healthy {
					t.Fatalf("expected stopped witness to be unhealthy, got %+v", witness)
				}
			},
		},
		{
			name: "negative remaining standby alone does not satisfy required quorum",
			run: func(t *testing.T) {
				if blockedDiagnostics.QuorumReachable == nil || *blockedDiagnostics.QuorumReachable {
					t.Fatalf("expected quorum to be unreachable without witness, got %+v", blockedDiagnostics)
				}
			},
		},
		{
			name: "negative automatic failover operation is not created without witness quorum",
			run: func(t *testing.T) {
				if blockedStatus.ActiveOperation != nil {
					t.Fatalf("expected no active failover operation, got %+v", blockedStatus.ActiveOperation)
				}
			},
		},
		{
			name: "negative standby is not promoted when witness quorum is unavailable",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, blockedStatus, e2eSwitchoverTarget)
				if blockedStatus.CurrentPrimary != e2eSwitchoverSource || target.Role == "primary" {
					t.Fatalf("expected standby to remain non-primary without quorum, got status=%+v target=%+v", blockedStatus, target)
				}
			},
		},
		{
			name: "negative blocked witness quorum failure records no failover history",
			run: func(t *testing.T) {
				if hasEndToEndHistoryKind(blockedHistory, "failover") {
					t.Fatalf("expected no failover history without witness quorum, got %+v", blockedHistory.Items)
				}
			},
		},
		{
			name: "negative failed primary is not considered healthy while quorum is unavailable",
			run: func(t *testing.T) {
				source := requireE2ESwitchoverMember(t, blockedStatus, e2eSwitchoverSource)
				if source.Healthy {
					t.Fatalf("expected stopped primary to be unhealthy, got %+v", source)
				}
			},
		},
		{
			name: "negative diagnostics reports quorum loss for blocked failover",
			run: func(t *testing.T) {
				if !e2eWitnessDiagnosticsHasWarning(blockedDiagnostics, "quorum is not reachable") {
					t.Fatalf("expected quorum warning in diagnostics, got %+v", blockedDiagnostics.Warnings)
				}
			},
		},
		{
			name: "negative standby remains read only when witness quorum is unavailable",
			run: func(t *testing.T) {
				assertServiceSQLFails(t, scenario.Standby.Service, `
CREATE TABLE e2e_witness_quorum_blocked_write (
	id integer PRIMARY KEY
)`, "read-only")
			},
		},
	}

	for _, testCase := range negativeCases {
		t.Run(testCase.name, testCase.run)
	}
}

func startEndToEndWitnessQuorumScenario(t *testing.T, etcdAlias string) e2eWitnessQuorumScenario {
	t.Helper()

	env := testenv.New(t)
	etcd := startEndToEndAutomaticFailoverEtcd(t, env, etcdAlias)

	members := []string{e2eSwitchoverSource, e2eSwitchoverTarget, e2eWitnessQuorumMember}
	primary := startEndToEndSwitchoverPrimary(t, env, etcdAlias, members)
	standby := startEndToEndSwitchoverStandby(t, env, etcdAlias, members)
	witness := startEndToEndWitnessQuorumWitness(t, env, etcdAlias, members)

	scenario := e2eWitnessQuorumScenario{
		e2eAutomaticFailoverScenario: e2eAutomaticFailoverScenario{
			Primary:     primary,
			Standby:     standby,
			Etcd:        etcd,
			NetworkName: env.NetworkName(),
		},
		Witness: witness,
	}

	updateEndToEndAutomaticFailoverSpec(t, scenario.e2eAutomaticFailoverScenario, func(spec *cluster.ClusterSpec) {
		spec.Failover.RequireQuorum = true
		for index := range spec.Members {
			if spec.Members[index].Name == e2eWitnessQuorumMember {
				spec.Members[index].NoFailover = true
				spec.Members[index].Priority = 10
			}
		}
	})
	waitForEndToEndAutomaticFailoverSpec(t, standby, "witness quorum required", func(spec nativeapi.ClusterSpecResponse) bool {
		witness := e2eWitnessSpecMember(spec, e2eWitnessQuorumMember)
		return spec.Failover.RequireQuorum &&
			witness != nil &&
			witness.NoFailover &&
			witness.Priority == 10
	})

	waitForEndToEndWitnessQuorumInitialTopology(t, scenario)
	waitForServicePostgresRecovery(t, standby.Service, true)

	return scenario
}

func startEndToEndWitnessQuorumWitness(t *testing.T, env *testenv.Environment, etcdAlias string, members []string) *e2eSwitchoverNode {
	t.Helper()

	testenv.RequireLocalImage(t, pgextTestImage())

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         e2eWitnessQuorumMember + "-e2e-witness-quorum",
		Image:        pgextTestImage(),
		Aliases:      []string{e2eWitnessQuorumMember},
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, e2eWitnessQuorumDaemonConfig(etcdAlias, members))},
		Entrypoint:   []string{"/usr/local/bin/pacmand", "-config", "/tmp/pacmand.yaml"},
		ExposedPorts: []string{"8080/tcp"},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(2 * topologyStartupTimeout),
	})

	client := &http.Client{Timeout: 3 * time.Second}
	node := &e2eSwitchoverNode{
		Name:    e2eWitnessQuorumMember,
		Base:    "http://" + service.Address(t, "8080"),
		Client:  client,
		Service: service,
	}
	waitForProbeStatus(t, client, node.Base+"/liveness", http.StatusOK, topologyStartupTimeout)

	return node
}

func e2eWitnessQuorumDaemonConfig(etcdAlias string, members []string) string {
	return fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: %s
  role: witness
  apiAddress: %s:8080
  controlAddress: %s:9090
dcs:
  backend: etcd
  clusterName: %s
  etcd:
    endpoints:
      - http://%s:2379
bootstrap:
  clusterName: %s
  initialPrimary: %s
  seedAddresses:
    - %s:9090
  expectedMembers:
%s
`,
		e2eWitnessQuorumMember,
		e2eWitnessQuorumMember,
		e2eWitnessQuorumMember,
		e2eSwitchoverClusterName,
		etcdAlias,
		e2eSwitchoverClusterName,
		e2eSwitchoverSource,
		e2eWitnessQuorumMember,
		e2eSwitchoverMembersYAML(members),
	)
}

func waitForEndToEndWitnessQuorumInitialTopology(t *testing.T, scenario e2eWitnessQuorumScenario) nativeapi.ClusterStatusResponse {
	t.Helper()

	return waitForEndToEndSwitchoverStatus(t, scenario.Standby, "initial witness quorum topology", func(status nativeapi.ClusterStatusResponse) bool {
		source := e2eSwitchoverMember(status, e2eSwitchoverSource)
		target := e2eSwitchoverMember(status, e2eSwitchoverTarget)
		witness := e2eSwitchoverMember(status, e2eWitnessQuorumMember)

		return status.ClusterName == e2eSwitchoverClusterName &&
			status.CurrentPrimary == e2eSwitchoverSource &&
			source != nil &&
			source.Role == "primary" &&
			source.Healthy &&
			target != nil &&
			target.Role == "replica" &&
			target.Healthy &&
			witness != nil &&
			witness.Role == "witness" &&
			witness.Healthy &&
			witness.NoFailover
	})
}

func fetchEndToEndDiagnostics(t *testing.T, node *e2eSwitchoverNode) nativeapi.DiagnosticsSummary {
	t.Helper()

	var diagnostics nativeapi.DiagnosticsSummary
	clusterJSON(t, node.Client, node.Base+"/api/v1/diagnostics", &diagnostics)
	return diagnostics
}

func requireE2EWitnessSpecMember(t *testing.T, spec nativeapi.ClusterSpecResponse, memberName string) nativeapi.MemberSpec {
	t.Helper()

	member := e2eWitnessSpecMember(spec, memberName)
	if member == nil {
		t.Fatalf("expected member %q in spec: %+v", memberName, spec)
	}
	return *member
}

func e2eWitnessSpecMember(spec nativeapi.ClusterSpecResponse, memberName string) *nativeapi.MemberSpec {
	for index := range spec.Members {
		if spec.Members[index].Name == memberName {
			return &spec.Members[index]
		}
	}
	return nil
}

func e2eWitnessDiagnosticsHasWarning(diagnostics nativeapi.DiagnosticsSummary, want string) bool {
	for _, warning := range diagnostics.Warnings {
		if strings.Contains(warning, want) {
			return true
		}
	}

	return false
}

func assertServiceSQLFails(t *testing.T, service *testenv.Service, statement string, wantError string) {
	t.Helper()

	db := openServiceDB(t, service)
	defer db.Close()

	_, err := db.Exec(statement)
	if err == nil {
		t.Fatalf("expected SQL on %q to fail, statement=%q", service.Name(), statement)
	}

	if wantError != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(wantError)) {
		t.Fatalf("expected SQL on %q to fail with %q, got %v", service.Name(), wantError, err)
	}
}
