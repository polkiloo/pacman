//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/test/testenv"
)

func TestPatroniInspiredWatchdogAndFencingInTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	t.Run("patroni watchdog config warns but daemon remains runnable", func(t *testing.T) {
		env := testenv.New(t)
		etcd := startTopologyEtcd(t, env, "patroni-watchdog-etcd")
		serviceName := "patroni-watchdog-node"
		pg := env.StartPostgres(t, serviceName+"-pg", serviceName+topologyPGPostgresSuffix)

		runner := startDaemonRunner(
			t,
			env,
			serviceName+"-runner",
			patroniWatchdogConfig(etcd.Alias, serviceName+topologyPGPostgresSuffix),
			nil,
			postgresConnectionEnv(pg),
		)

		result := runPacmandUntilTerminated(t, runner)
		if !strings.Contains(result.Output, "watchdog") ||
			!strings.Contains(result.Output, "not translated by PACMAN") {
			t.Fatalf("expected Patroni watchdog translation warning, got:\n%s", result.Output)
		}
		if !strings.Contains(result.Output, "loaded node configuration") {
			t.Fatalf("expected daemon to load config despite watchdog warning, got:\n%s", result.Output)
		}
		if strings.Contains(result.Output, "server stopped with error") {
			t.Fatalf("expected watchdog warning not to stop daemon with error, got:\n%s", result.Output)
		}
	})

	t.Run("required fencing blocks real failover promotion until hook succeeds", func(t *testing.T) {
		primary, standby := startReplicatedPostgresPair(t)
		store := seededRealStore(t, cluster.ClusterSpec{
			ClusterName: "alpha",
			Failover: cluster.FailoverPolicy{
				Mode:            cluster.FailoverModeAutomatic,
				MaximumLagBytes: 1024,
				CheckTimeline:   true,
				FencingRequired: true,
			},
			Members: []cluster.MemberSpec{
				{Name: "alpha-1", Priority: 100},
				{Name: "alpha-2", Priority: 90},
			},
		})

		observedAt := time.Now().UTC()
		primaryObservation := publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
		standbyObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

		primaryAddress := primary.Address(t)
		primary.Stop(t)
		waitForAddressUnavailable(t, primary.Name(), primaryAddress)

		failedPrimary := publishUnavailableNodeStatus(t, store, "alpha-1", primaryAddress, observedAt.Add(2*time.Second), primaryObservation)
		publishObservedNodeStatusFromObservation(t, store, "alpha-2", standby.Address(t), failedPrimary.ObservedAt.Add(time.Second), standbyObservation)

		intent, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
			RequestedBy: "integration-test",
			Reason:      "watchdog fencing coverage",
		})
		if err != nil {
			t.Fatalf("create failover intent: %v", err)
		}
		if intent.Candidate != "alpha-2" {
			t.Fatalf("unexpected failover candidate: %+v", intent)
		}

		_, err = store.ExecuteFailover(context.Background(), newPostgresPromotionExecutor(t, standby), nil)
		if !errors.Is(err, controlplane.ErrFailoverFencingHookRequired) {
			t.Fatalf("expected missing fencing hook error, got %v", err)
		}
		waitForPostgresRole(t, standby, cluster.MemberRoleReplica)

		fencer := &recordingIntegrationFencer{}
		execution, err := store.ExecuteFailover(context.Background(), newPostgresPromotionExecutor(t, standby), fencer)
		if err != nil {
			t.Fatalf("execute fenced failover: %v", err)
		}
		if !execution.Fenced || !execution.Promoted {
			t.Fatalf("expected fenced promotion execution, got %+v", execution)
		}
		if len(fencer.requests) != 1 || fencer.requests[0].Candidate != "alpha-2" {
			t.Fatalf("unexpected fencing requests: %+v", fencer.requests)
		}

		waitForPostgresRole(t, standby, cluster.MemberRolePrimary)
	})
}

func patroniWatchdogConfig(etcdAlias, postgresAlias string) string {
	return `
scope: patroni-watchdog
name: postgresql0
restapi:
  listen: 0.0.0.0:8080
etcd:
  host: ` + etcdAlias + `:2379
bootstrap:
  dcs:
    ttl: 30
    retry_timeout: 10
postgresql:
  listen: ` + postgresAlias + `:5432
  data_dir: /var/lib/postgresql/data
watchdog:
  mode: required
  device: /dev/watchdog
  safety_margin: 5
`
}

type recordingIntegrationFencer struct {
	requests []controlplane.FencingRequest
}

func (fencer *recordingIntegrationFencer) Fence(_ context.Context, request controlplane.FencingRequest) error {
	fencer.requests = append(fencer.requests, request)
	return nil
}
