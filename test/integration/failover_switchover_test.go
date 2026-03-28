//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/lib/pq"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	pgobs "github.com/polkiloo/pacman/internal/postgres"
	"github.com/polkiloo/pacman/test/testenv"
)

func TestSwitchoverValidationUsesRealStreamingStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	primaryObservation := publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	standbyObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	if primaryObservation.Role != cluster.MemberRolePrimary || primaryObservation.InRecovery {
		t.Fatalf("expected real primary observation, got %+v", primaryObservation)
	}

	if standbyObservation.Role != cluster.MemberRoleReplica || !standbyObservation.InRecovery {
		t.Fatalf("expected real standby observation, got %+v", standbyObservation)
	}

	readiness, err := store.SwitchoverTargetReadiness("alpha-2")
	if err != nil {
		t.Fatalf("switchover target readiness: %v", err)
	}

	if !readiness.Ready || readiness.CurrentPrimary != "alpha-1" || readiness.Member.Name != "alpha-2" {
		t.Fatalf("unexpected switchover readiness: %+v", readiness)
	}

	if len(readiness.Reasons) != 0 {
		t.Fatalf("expected ready standby with no rejection reasons, got %+v", readiness.Reasons)
	}

	validation, err := store.ValidateSwitchover(context.Background(), controlplane.SwitchoverRequest{
		RequestedBy: "operator",
		Reason:      "planned switchover integration test",
		Candidate:   "alpha-2",
		ScheduledAt: time.Now().Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("validate switchover: %v", err)
	}

	if validation.CurrentPrimary.Name != "alpha-1" || validation.Target.Member.Name != "alpha-2" || !validation.Target.Ready {
		t.Fatalf("unexpected switchover validation: %+v", validation)
	}
}

func TestFailoverPromotesRealStandbyAndRecordsHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	primaryAddress := primary.Address(t)
	primaryObservedAt := time.Now().UTC()
	primaryObservation := publishObservedNodeStatus(t, store, "alpha-1", primary, primaryObservedAt)
	standbyObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, primaryObservedAt.Add(time.Second))

	if primaryObservation.Details.Timeline == 0 {
		t.Fatalf("expected primary timeline from real postgres, got %+v", primaryObservation)
	}

	primary.Stop(t)
	waitForAddressUnavailable(t, primary.Name(), primaryAddress)

	// Preserve the last known primary identity while marking the stopped server failed.
	failedPrimaryStatus := nodeStatusFromObservation("alpha-1", primaryAddress, primaryObservedAt.Add(2*time.Second), primaryObservation)
	failedPrimaryStatus.State = cluster.MemberStateFailed
	failedPrimaryStatus.Postgres.Up = false
	failedPrimaryStatus.Postgres.CheckedAt = failedPrimaryStatus.ObservedAt
	failedPrimaryStatus.Postgres.Errors.Availability = "postgres is unavailable"

	if _, err := store.PublishNodeStatus(context.Background(), failedPrimaryStatus); err != nil {
		t.Fatalf("publish failed primary state: %v", err)
	}

	if _, err := store.PublishNodeStatus(context.Background(), nodeStatusFromObservation("alpha-2", standby.Address(t), failedPrimaryStatus.ObservedAt.Add(time.Second), standbyObservation)); err != nil {
		t.Fatalf("refresh standby state before failover: %v", err)
	}

	intent, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
		RequestedBy: "integration-test",
		Reason:      "primary container stopped",
	})
	if err != nil {
		t.Fatalf("create failover intent: %v", err)
	}

	if intent.CurrentPrimary != "alpha-1" || intent.Candidate != "alpha-2" {
		t.Fatalf("unexpected failover intent: %+v", intent)
	}

	execution, err := store.ExecuteFailover(context.Background(), newPostgresPromotionExecutor(t, standby), nil)
	if err != nil {
		t.Fatalf("execute failover: %v", err)
	}

	waitForPostgresRole(t, standby, cluster.MemberRolePrimary)

	promotedObservedAt := time.Now().UTC()
	promotedObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, promotedObservedAt)
	if promotedObservation.Role != cluster.MemberRolePrimary || promotedObservation.InRecovery {
		t.Fatalf("expected promoted standby to become primary, got %+v", promotedObservation)
	}

	execSQL(t, standby, `
CREATE TABLE IF NOT EXISTS failover_writable_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, standby, `
INSERT INTO failover_writable_marker (id, payload)
VALUES (1, 'promoted')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after failover")
	}

	if status.CurrentPrimary != "alpha-2" || status.CurrentEpoch != execution.CurrentEpoch {
		t.Fatalf("unexpected cluster status after failover: %+v", status)
	}

	formerPrimary, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary state after failover")
	}

	if formerPrimary.State != cluster.MemberStateNeedsRejoin || !formerPrimary.NeedsRejoin {
		t.Fatalf("expected former primary to require rejoin, got %+v", formerPrimary)
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failover history entry, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindFailover || history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultSucceeded {
		t.Fatalf("unexpected failover history entry: %+v", history[0])
	}
}

func startReplicatedPostgresPair(t *testing.T) (*testenv.Postgres, *testenv.Postgres) {
	t.Helper()

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "alpha-1", "alpha-1-postgres")
	standby := env.StartStreamingStandby(t, "alpha-2", "alpha-2-postgres", primary, "alpha_2")

	t.Setenv("PGDATABASE", primary.Database())
	t.Setenv("PGUSER", primary.Username())
	t.Setenv("PGPASSWORD", primary.Password())
	t.Setenv("PGSSLMODE", "disable")

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS topology_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO topology_marker (id, payload)
VALUES (1, 'replicated')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	waitForQueryValue(t, standby, `SELECT payload FROM topology_marker WHERE id = 1`, "replicated")

	return primary, standby
}

func seededRealTopologyStore(t *testing.T) *controlplane.MemoryStateStore {
	t.Helper()

	store := controlplane.NewMemoryStateStore()
	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Failover: cluster.FailoverPolicy{
			Mode:            cluster.FailoverModeAutomatic,
			MaximumLagBytes: 1024,
			CheckTimeline:   true,
		},
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1", Priority: 100},
			{Name: "alpha-2", Priority: 90},
		},
	}); err != nil {
		t.Fatalf("store real topology spec: %v", err)
	}

	return store
}

func publishObservedNodeStatus(t *testing.T, store *controlplane.MemoryStateStore, nodeName string, fixture *testenv.Postgres, observedAt time.Time) pgobs.Observation {
	t.Helper()

	observation := waitForObservation(t, fixture, func(observation pgobs.Observation) bool {
		return observation.Role != cluster.MemberRoleUnknown
	})

	if _, err := store.PublishNodeStatus(context.Background(), nodeStatusFromObservation(nodeName, fixture.Address(t), observedAt, observation)); err != nil {
		t.Fatalf("publish node status for %s: %v", nodeName, err)
	}

	return observation
}

func nodeStatusFromObservation(nodeName, address string, observedAt time.Time, observation pgobs.Observation) agentmodel.NodeStatus {
	status := agentmodel.NodeStatus{
		NodeName:   nodeName,
		MemberName: nodeName,
		Role:       observation.Role,
		Postgres: agentmodel.PostgresStatus{
			Managed:       true,
			Address:       address,
			CheckedAt:     observedAt,
			Up:            true,
			Role:          observation.Role,
			RecoveryKnown: true,
			InRecovery:    observation.InRecovery,
			Details: agentmodel.PostgresDetails{
				ServerVersion:       observation.Details.ServerVersion,
				PendingRestart:      observation.Details.PendingRestart,
				SystemIdentifier:    observation.Details.SystemIdentifier,
				Timeline:            observation.Details.Timeline,
				PostmasterStartAt:   observation.Details.PostmasterStartAt,
				ReplicationLagBytes: observation.Details.ReplicationLagBytes,
			},
			WAL: agentmodel.WALProgress{
				WriteLSN:        observation.WAL.WriteLSN,
				FlushLSN:        observation.WAL.FlushLSN,
				ReceiveLSN:      observation.WAL.ReceiveLSN,
				ReplayLSN:       observation.WAL.ReplayLSN,
				ReplayTimestamp: observation.WAL.ReplayTimestamp,
			},
		},
		ObservedAt: observedAt,
	}

	status.State = localMemberStateForObservation(observation)
	return status
}

func localMemberStateForObservation(observation pgobs.Observation) cluster.MemberState {
	if observation.Role == cluster.MemberRoleReplica && (observation.WAL.ReceiveLSN != "" || observation.WAL.ReplayLSN != "") {
		return cluster.MemberStateStreaming
	}

	return cluster.MemberStateRunning
}

func waitForObservation(t *testing.T, fixture *testenv.Postgres, predicate func(pgobs.Observation) bool) pgobs.Observation {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	address := fixture.Address(t)
	var lastObservation pgobs.Observation
	var lastErr error

	for time.Now().Before(deadline) {
		observation, err := pgobs.QueryObservation(context.Background(), address)
		if err == nil {
			lastObservation = observation
			if predicate(observation) {
				return observation
			}
		} else {
			lastErr = err
		}

		time.Sleep(200 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("observation for %q did not become ready: %v", fixture.Name(), lastErr)
	}

	t.Fatalf("observation for %q did not satisfy predicate, last observation: %+v", fixture.Name(), lastObservation)
	return pgobs.Observation{}
}

func waitForPostgresRole(t *testing.T, fixture *testenv.Postgres, role cluster.MemberRole) {
	t.Helper()

	waitForObservation(t, fixture, func(observation pgobs.Observation) bool {
		return observation.Role == role && (role != cluster.MemberRoleReplica || observation.InRecovery)
	})
}

func waitForAddressUnavailable(t *testing.T, name, address string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 250*time.Millisecond)
		if err != nil {
			return
		}

		_ = conn.Close()
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("postgres fixture %q remained reachable after stop", name)
}

func execSQL(t *testing.T, fixture *testenv.Postgres, statement string, args ...any) {
	t.Helper()

	db := openFixtureDB(t, fixture)
	defer db.Close()

	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatalf("exec SQL on %q failed: %v", fixture.Name(), err)
	}
}

func waitForQueryValue(t *testing.T, fixture *testenv.Postgres, query string, want string) {
	t.Helper()

	host := fixture.Host(t)
	port := fixture.Port(t)
	database := fixture.Database()
	username := fixture.Username()
	password := fixture.Password()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		db, err := openDB(host, port, database, username, password)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var got string
		err = db.QueryRow(query).Scan(&got)
		_ = db.Close()
		if err == nil && got == want {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("query %q on %q did not return %q before deadline", query, fixture.Name(), want)
}

func openFixtureDB(t *testing.T, fixture *testenv.Postgres) *sql.DB {
	t.Helper()

	db, err := openDB(
		fixture.Host(t),
		fixture.Port(t),
		fixture.Database(),
		fixture.Username(),
		fixture.Password(),
	)
	if err != nil {
		t.Fatalf("open postgres fixture %q: %v", fixture.Name(), err)
	}

	return db
}

type postgresPromotionExecutor struct {
	host     string
	port     int
	database string
	username string
	password string
}

func newPostgresPromotionExecutor(t *testing.T, fixture *testenv.Postgres) postgresPromotionExecutor {
	t.Helper()

	return postgresPromotionExecutor{
		host:     fixture.Host(t),
		port:     fixture.Port(t),
		database: fixture.Database(),
		username: fixture.Username(),
		password: fixture.Password(),
	}
}

func (executor postgresPromotionExecutor) Promote(_ context.Context, request controlplane.PromotionRequest) error {
	db, err := openDB(
		executor.host,
		executor.port,
		executor.database,
		executor.username,
		executor.password,
	)
	if err != nil {
		return err
	}
	defer db.Close()

	var promoted bool
	if err := db.QueryRow(`SELECT pg_promote(wait_seconds => 30)`).Scan(&promoted); err != nil {
		return err
	}

	if !promoted {
		return fmt.Errorf("pg_promote did not report success for %s", request.Candidate)
	}

	return nil
}
func openDB(host string, port int, database, username, password string) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable connect_timeout=5",
		host,
		port,
		database,
		username,
		password,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
