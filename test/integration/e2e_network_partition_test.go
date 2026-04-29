//go:build integration

package integration_test

import (
	"strings"
	"testing"

	nativeapi "github.com/polkiloo/pacman/internal/api/native"
)

func TestEndToEndNetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	scenario := startEndToEndAutomaticFailoverScenario(t, "etcd-e2e-network-partition")
	initialStatus := waitForEndToEndSwitchoverInitialTopology(t, scenario.Standby)

	execServiceSQL(t, scenario.Primary.Service, `
CREATE TABLE IF NOT EXISTS e2e_partition_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execServiceSQL(t, scenario.Primary.Service, `
INSERT INTO e2e_partition_marker (id, payload)
VALUES (1, 'before-partition')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_partition_marker WHERE id = 1`, "before-partition")

	scenario.Primary.Service.DisconnectNetwork(t, scenario.NetworkName)
	partitioned := true
	t.Cleanup(func() {
		if partitioned {
			scenario.Primary.Service.ConnectNetwork(t, scenario.NetworkName, e2eSwitchoverSource)
		}
	})

	intentStatus := waitForEndToEndAutomaticFailoverIntent(t, scenario.Standby)
	finalStatus := waitForEndToEndAutomaticFailoverCompletion(t, scenario.Standby, intentStatus.ActiveOperation.ID)
	waitForServicePostgresRecovery(t, scenario.Standby.Service, false)
	execServiceSQL(t, scenario.Standby.Service, `
INSERT INTO e2e_partition_marker (id, payload)
VALUES (2, 'during-partition')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	historyEntry := waitForEndToEndAutomaticFailoverHistory(t, scenario.Standby, intentStatus.ActiveOperation.ID)
	partitionedStatus := waitForEndToEndSwitchoverStatus(t, scenario.Standby, "partitioned former primary needs rejoin", func(status nativeapi.ClusterStatusResponse) bool {
		source := e2eSwitchoverMember(status, e2eSwitchoverSource)
		return status.CurrentPrimary == e2eSwitchoverTarget &&
			source != nil &&
			source.Role == "replica" &&
			source.NeedsRejoin &&
			!source.Healthy
	})
	historyDuringPartition := fetchEndToEndHistory(t, scenario.Standby)
	standbyToSourceReachable := serviceCanReachPostgresHost(t, scenario.Standby, e2eSwitchoverSource)

	positiveCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "positive pre-partition writes replicate to standby",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_partition_marker WHERE id = 1`, "before-partition")
			},
		},
		{
			name: "positive partition triggers automatic failover intent",
			run: func(t *testing.T) {
				operation := intentStatus.ActiveOperation
				if operation == nil {
					t.Fatal("expected active failover operation")
				}
				if operation.Kind != "failover" || operation.FromMember != e2eSwitchoverSource || operation.ToMember != e2eSwitchoverTarget {
					t.Fatalf("unexpected network partition failover operation: %+v", operation)
				}
				if operation.RequestedBy != e2eAutomaticFailoverRequestedBy || operation.Reason != e2eAutomaticFailoverReason {
					t.Fatalf("unexpected network partition failover metadata: %+v", operation)
				}
			},
		},
		{
			name: "positive reachable standby promotes and advances epoch",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, finalStatus, e2eSwitchoverTarget)
				if finalStatus.CurrentPrimary != e2eSwitchoverTarget || finalStatus.ActiveOperation != nil {
					t.Fatalf("unexpected partition failover status: %+v", finalStatus)
				}
				if finalStatus.CurrentEpoch <= initialStatus.CurrentEpoch {
					t.Fatalf("expected partition failover to advance epoch from %d, got %+v", initialStatus.CurrentEpoch, finalStatus)
				}
				if target.Role != "primary" || !target.Healthy {
					t.Fatalf("expected standby to promote under partition, got %+v", target)
				}
			},
		},
		{
			name: "positive promoted standby accepts writes while source is partitioned",
			run: func(t *testing.T) {
				assertServiceQueryValue(t, scenario.Standby.Service, `SELECT payload FROM e2e_partition_marker WHERE id = 2`, "during-partition")
			},
		},
		{
			name: "positive partitioned former primary is fenced for rejoin",
			run: func(t *testing.T) {
				source := requireE2ESwitchoverMember(t, partitionedStatus, e2eSwitchoverSource)
				if source.Role != "replica" || !source.NeedsRejoin || source.Healthy {
					t.Fatalf("expected partitioned former primary to be fenced for rejoin, got %+v", source)
				}
			},
		},
	}

	negativeCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "negative initial topology does not start with promoted standby",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, initialStatus, e2eSwitchoverTarget)
				if initialStatus.CurrentPrimary == e2eSwitchoverTarget || target.Role == "primary" {
					t.Fatalf("expected standby not to be primary before partition, got status=%+v target=%+v", initialStatus, target)
				}
			},
		},
		{
			name: "negative initial topology has no active failover operation",
			run: func(t *testing.T) {
				if initialStatus.ActiveOperation != nil {
					t.Fatalf("expected no active operation before partition, got %+v", initialStatus.ActiveOperation)
				}
			},
		},
		{
			name: "negative partitioned former primary is not reachable from promoted standby",
			run: func(t *testing.T) {
				if standbyToSourceReachable {
					t.Fatal("expected partitioned former primary postgres endpoint to be unreachable from standby")
				}
			},
		},
		{
			name: "negative partitioned former primary does not remain current primary in cluster view",
			run: func(t *testing.T) {
				source := requireE2ESwitchoverMember(t, partitionedStatus, e2eSwitchoverSource)
				if partitionedStatus.CurrentPrimary == e2eSwitchoverSource || source.Role == "primary" {
					t.Fatalf("expected partitioned source to lose primary role in reachable cluster view, got status=%+v source=%+v", partitionedStatus, source)
				}
			},
		},
		{
			name: "negative partition failover is not user requested",
			run: func(t *testing.T) {
				operation := intentStatus.ActiveOperation
				if operation == nil {
					t.Fatal("expected active failover operation")
				}
				if operation.RequestedBy == "e2e-test" {
					t.Fatalf("expected automatic failover requester, got %+v", operation)
				}
			},
		},
		{
			name: "negative partition failover does not reverse source and target",
			run: func(t *testing.T) {
				operation := intentStatus.ActiveOperation
				if operation == nil {
					t.Fatal("expected active failover operation")
				}
				if operation.FromMember == e2eSwitchoverTarget || operation.ToMember == e2eSwitchoverSource {
					t.Fatalf("expected failover from partitioned source to reachable standby, got %+v", operation)
				}
			},
		},
		{
			name: "negative promoted standby does not remain replica after failover",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, finalStatus, e2eSwitchoverTarget)
				if target.Role == "replica" || finalStatus.CurrentPrimary != e2eSwitchoverTarget {
					t.Fatalf("expected promoted standby to be current primary after partition failover, got status=%+v target=%+v", finalStatus, target)
				}
			},
		},
		{
			name: "negative partition failover does not leave epoch unchanged",
			run: func(t *testing.T) {
				if finalStatus.CurrentEpoch <= initialStatus.CurrentEpoch {
					t.Fatalf("expected partition failover to advance epoch from %d, got %+v", initialStatus.CurrentEpoch, finalStatus)
				}
			},
		},
		{
			name: "negative partitioned source is not considered healthy",
			run: func(t *testing.T) {
				source := requireE2ESwitchoverMember(t, partitionedStatus, e2eSwitchoverSource)
				if source.Healthy {
					t.Fatalf("expected partitioned source to be unhealthy in reachable cluster view, got %+v", source)
				}
			},
		},
		{
			name: "negative partitioned source does not clear needs rejoin",
			run: func(t *testing.T) {
				source := requireE2ESwitchoverMember(t, partitionedStatus, e2eSwitchoverSource)
				if !source.NeedsRejoin {
					t.Fatalf("expected partitioned source to remain marked for rejoin, got %+v", source)
				}
			},
		},
		{
			name: "negative successful rejoin history is absent while partition remains active",
			run: func(t *testing.T) {
				if hasSuccessfulEndToEndRejoinHistory(historyDuringPartition) {
					t.Fatalf("expected no successful rejoin history during partition, got %+v", historyDuringPartition.Items)
				}
			},
		},
		{
			name: "negative switchover history is absent for partition failover",
			run: func(t *testing.T) {
				if hasEndToEndHistoryKind(historyDuringPartition, "switchover") {
					t.Fatalf("expected network partition not to record switchover history, got %+v", historyDuringPartition.Items)
				}
			},
		},
		{
			name: "negative failover history is not attributed to switchover or wrong members",
			run: func(t *testing.T) {
				if historyEntry.Kind != "failover" || historyEntry.FromMember != e2eSwitchoverSource || historyEntry.ToMember != e2eSwitchoverTarget || historyEntry.Result != "succeeded" {
					t.Fatalf("unexpected network partition failover history entry: %+v", historyEntry)
				}
			},
		},
		{
			name: "negative completed partition failover does not retain active operation",
			run: func(t *testing.T) {
				if finalStatus.ActiveOperation != nil {
					t.Fatalf("expected completed partition failover to clear active operation, got %+v", finalStatus.ActiveOperation)
				}
			},
		},
		{
			name: "negative promoted primary is not marked for rejoin during partition",
			run: func(t *testing.T) {
				target := requireE2ESwitchoverMember(t, partitionedStatus, e2eSwitchoverTarget)
				if target.NeedsRejoin || target.Role != "primary" || !target.Healthy {
					t.Fatalf("expected promoted primary to stay healthy and not require rejoin, got %+v", target)
				}
			},
		},
	}

	for _, testCase := range positiveCases {
		t.Run(testCase.name, testCase.run)
	}

	for _, testCase := range negativeCases {
		t.Run(testCase.name, testCase.run)
	}
}

func serviceCanReachPostgresHost(t *testing.T, node *e2eSwitchoverNode, host string) bool {
	t.Helper()

	result := node.Service.Exec(t, "pg_isready", "-h", host, "-p", "5432", "-U", "pacman", "-d", "pacman", "-t", "1")
	if result.ExitCode == 0 {
		return true
	}

	output := strings.ToLower(result.Output)
	return !strings.Contains(output, "no response") &&
		!strings.Contains(output, "could not translate host name") &&
		!strings.Contains(output, "timeout expired")
}

func hasEndToEndHistoryKind(history nativeapi.HistoryResponse, kind string) bool {
	for _, entry := range history.Items {
		if entry.Kind == kind {
			return true
		}
	}

	return false
}
