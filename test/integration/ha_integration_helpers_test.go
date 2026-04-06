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

func seededRealStore(t *testing.T, spec cluster.ClusterSpec) *controlplane.MemoryStateStore {
	t.Helper()

	store := controlplane.NewMemoryStateStore()
	if _, err := store.StoreClusterSpec(context.Background(), spec); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	return store
}

func seededRealTopologyStore(t *testing.T) *controlplane.MemoryStateStore {
	t.Helper()

	return seededRealStore(t, cluster.ClusterSpec{
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
	})
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

func publishObservedNodeStatusFromObservation(t *testing.T, store *controlplane.MemoryStateStore, nodeName, address string, observedAt time.Time, observation pgobs.Observation) agentmodel.NodeStatus {
	t.Helper()

	status := nodeStatusFromObservation(nodeName, address, observedAt, observation)
	if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("publish node status for %s: %v", nodeName, err)
	}

	return status
}

func publishUnavailableNodeStatus(t *testing.T, store *controlplane.MemoryStateStore, nodeName, address string, observedAt time.Time, observation pgobs.Observation) agentmodel.NodeStatus {
	t.Helper()

	status := nodeStatusFromObservation(nodeName, address, observedAt, observation)
	status.State = cluster.MemberStateFailed
	status.Postgres.Up = false
	status.Postgres.CheckedAt = observedAt
	status.Postgres.Errors.Availability = "postgres is unavailable"

	if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("publish unavailable node status for %s: %v", nodeName, err)
	}

	return status
}

func publishWitnessNodeStatus(t *testing.T, store *controlplane.MemoryStateStore, nodeName string, observedAt time.Time, state cluster.MemberState) agentmodel.NodeStatus {
	t.Helper()

	status := agentmodel.NodeStatus{
		NodeName:   nodeName,
		MemberName: nodeName,
		Role:       cluster.MemberRoleWitness,
		State:      state,
		Postgres: agentmodel.PostgresStatus{
			Managed:   false,
			CheckedAt: observedAt,
		},
		ObservedAt: observedAt,
	}

	if _, err := store.PublishNodeStatus(context.Background(), status); err != nil {
		t.Fatalf("publish witness node status for %s: %v", nodeName, err)
	}

	return status
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

func waitForPromotedPrimaryTimeline(t *testing.T, fixture *testenv.Postgres, previousTimeline int64) pgobs.Observation {
	t.Helper()

	return waitForObservation(t, fixture, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRolePrimary &&
			!observation.InRecovery &&
			observation.Details.Timeline > previousTimeline
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

type postgresDemotionExecutor struct {
	t        *testing.T
	fixture  *testenv.Postgres
	host     string
	port     int
	database string
	username string
	password string
}

func newPostgresDemotionExecutor(t *testing.T, fixture *testenv.Postgres) postgresDemotionExecutor {
	t.Helper()

	return postgresDemotionExecutor{
		t:        t,
		fixture:  fixture,
		host:     fixture.Host(t),
		port:     fixture.Port(t),
		database: fixture.Database(),
		username: fixture.Username(),
		password: fixture.Password(),
	}
}

func (executor postgresDemotionExecutor) Demote(_ context.Context, request controlplane.DemotionRequest) error {
	db, err := openDB(
		executor.host,
		executor.port,
		executor.database,
		executor.username,
		executor.password,
	)
	if err == nil {
		_, _ = db.Exec(`CHECKPOINT`)
		_ = db.Close()
	}

	address := net.JoinHostPort(executor.host, fmt.Sprintf("%d", executor.port))
	executor.fixture.Stop(executor.t)
	waitForAddressUnavailable(executor.t, executor.fixture.Name(), address)

	return nil
}

type containerPGRewindExecutor struct {
	t          *testing.T
	fixture    *testenv.Postgres
	markerPath string
}

func newContainerPGRewindExecutor(t *testing.T, fixture *testenv.Postgres) containerPGRewindExecutor {
	t.Helper()

	return containerPGRewindExecutor{
		t:          t,
		fixture:    fixture,
		markerPath: "/tmp/pacman-rejoin-rewind.ok",
	}
}

func (executor containerPGRewindExecutor) Rewind(_ context.Context, request controlplane.RewindRequest) error {
	if request.Decision.Strategy != cluster.RejoinStrategyRewind {
		return fmt.Errorf("unexpected rejoin strategy %q", request.Decision.Strategy)
	}

	if request.CurrentPrimaryNode.Postgres.Address == "" {
		return fmt.Errorf("current primary postgres address is required")
	}

	command := fmt.Sprintf(
		"pg_rewind_bin=$(command -v pg_rewind || true); "+
			"if [ -z \"$pg_rewind_bin\" ]; then "+
			"for candidate in /usr/lib/postgresql/*/bin/pg_rewind /usr/local/bin/pg_rewind; do "+
			"if [ -x \"$candidate\" ]; then pg_rewind_bin=\"$candidate\"; break; fi; "+
			"done; fi; "+
			"[ -n \"$pg_rewind_bin\" ] && \"$pg_rewind_bin\" --version >/dev/null && printf 'member=%s primary=%s\\n' > %s",
		request.Decision.Member.Name,
		request.Decision.CurrentPrimary.Name,
		executor.markerPath,
	)
	result := executor.fixture.Exec(executor.t, "sh", "-lc", command)
	if result.ExitCode != 0 {
		return fmt.Errorf("exec pg_rewind in %s returned %d: %s", executor.fixture.Name(), result.ExitCode, result.Output)
	}

	return nil
}

func (executor containerPGRewindExecutor) RequireMarker(t *testing.T) string {
	t.Helper()

	return executor.fixture.RequireExec(t, "sh", "-lc", "cat "+executor.markerPath)
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
