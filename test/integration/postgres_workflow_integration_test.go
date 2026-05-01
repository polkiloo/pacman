//go:build integration

package integration_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	pgobs "github.com/polkiloo/pacman/internal/postgres"
	"github.com/polkiloo/pacman/test/testenv"
)

func TestPostgresWorkflowPositiveCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	t.Run("primary observation exposes writable runtime details", func(t *testing.T) {
		observation, err := pgobs.QueryObservation(context.Background(), primary.Address(t))
		if err != nil {
			t.Fatalf("query primary observation: %v", err)
		}

		if observation.Role != cluster.MemberRolePrimary || observation.InRecovery {
			t.Fatalf("expected primary observation, got %+v", observation)
		}

		if observation.Details.SystemIdentifier == "" || observation.Details.Timeline == 0 || observation.Details.DatabaseSizeBytes == 0 {
			t.Fatalf("expected populated primary details, got %+v", observation.Details)
		}
	})

	t.Run("standby observation exposes streaming replay state", func(t *testing.T) {
		observation, err := pgobs.QueryObservation(context.Background(), standby.Address(t))
		if err != nil {
			t.Fatalf("query standby observation: %v", err)
		}

		if observation.Role != cluster.MemberRoleReplica || !observation.InRecovery {
			t.Fatalf("expected standby observation, got %+v", observation)
		}

		if observation.WAL.ReceiveLSN == "" || observation.WAL.ReplayLSN == "" {
			t.Fatalf("expected standby WAL receiver/replay LSNs, got %+v", observation.WAL)
		}
	})

	t.Run("primary reports active physical replication slot", func(t *testing.T) {
		assertActiveReplicationSlots(t, primary, "alpha_2")
	})

	t.Run("writes on primary replicate to standby", func(t *testing.T) {
		execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS workflow_positive_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
		execSQL(t, primary, `
INSERT INTO workflow_positive_marker (id, payload)
VALUES (1, 'positive')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
		waitForQueryValue(t, standby, `SELECT payload FROM workflow_positive_marker WHERE id = 1`, "positive")
	})

	t.Run("control plane accepts real standby as switchover target", func(t *testing.T) {
		readiness, err := store.SwitchoverTargetReadiness("alpha-2")
		if err != nil {
			t.Fatalf("switchover target readiness: %v", err)
		}

		if !readiness.Ready || readiness.CurrentPrimary != "alpha-1" || readiness.Member.Name != "alpha-2" {
			t.Fatalf("expected ready switchover target, got %+v", readiness)
		}
	})
}

func TestPostgresWorkflowNegativeCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	standbyObservation := publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	t.Run("role query returns unknown for unavailable postgres", func(t *testing.T) {
		role, err := pgobs.QueryRole(context.Background(), unusedTCPAddress(t))
		if err == nil {
			t.Fatal("expected unavailable postgres role query to fail")
		}
		if role != cluster.MemberRoleUnknown {
			t.Fatalf("expected unknown role for unavailable postgres, got %q", role)
		}
	})

	t.Run("standby rejects direct writes", func(t *testing.T) {
		assertStandbyRejectsWrite(t, standby, `
CREATE TABLE workflow_negative_standby_write (
	id integer PRIMARY KEY
)`)
	})

	t.Run("healthy primary blocks failover intent", func(t *testing.T) {
		_, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
			RequestedBy: "integration-test",
			Reason:      "negative healthy primary case",
		})
		if !errors.Is(err, controlplane.ErrFailoverPrimaryHealthy) {
			t.Fatalf("unexpected failover intent error: got %v, want %v", err, controlplane.ErrFailoverPrimaryHealthy)
		}
	})

	t.Run("maintenance mode blocks failover intent", func(t *testing.T) {
		if _, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
			Enabled:     true,
			Reason:      "negative maintenance case",
			RequestedBy: "integration-test",
		}); err != nil {
			t.Fatalf("enable maintenance: %v", err)
		}

		_, err := store.CreateFailoverIntent(context.Background(), controlplane.FailoverIntentRequest{
			RequestedBy: "integration-test",
			Reason:      "negative maintenance failover case",
		})
		if !errors.Is(err, controlplane.ErrFailoverMaintenanceEnabled) {
			t.Fatalf("unexpected maintenance failover error: got %v, want %v", err, controlplane.ErrFailoverMaintenanceEnabled)
		}

		if _, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
			Enabled:     false,
			Reason:      "negative maintenance case complete",
			RequestedBy: "integration-test",
		}); err != nil {
			t.Fatalf("disable maintenance: %v", err)
		}
	})

	t.Run("stopped standby is rejected as switchover target", func(t *testing.T) {
		standbyAddress := standby.Address(t)
		standby.Stop(t)
		waitForAddressUnavailable(t, standby.Name(), standbyAddress)
		publishUnavailableNodeStatus(t, store, "alpha-2", standbyAddress, observedAt.Add(2*time.Second), standbyObservation)

		_, err := store.ValidateSwitchover(context.Background(), controlplane.SwitchoverRequest{
			RequestedBy: "integration-test",
			Reason:      "negative stopped standby case",
			Candidate:   "alpha-2",
		})
		if !errors.Is(err, controlplane.ErrSwitchoverTargetNotReady) {
			t.Fatalf("unexpected stopped standby switchover error: got %v, want %v", err, controlplane.ErrSwitchoverTargetNotReady)
		}
	})
}

func TestPostgresWorkflowAdditionalNegativeCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	primary, standby := startReplicatedPostgresPair(t)

	t.Run("bad password blocks observation query", func(t *testing.T) {
		t.Setenv("PGPASSWORD", "definitely-wrong")

		observation, err := pgobs.QueryObservation(context.Background(), primary.Address(t))
		if err == nil {
			t.Fatal("expected observation query with bad password to fail")
		}
		if observation.Role != cluster.MemberRoleUnknown {
			t.Fatalf("expected unknown role on auth failure, got %+v", observation)
		}
	})

	t.Run("missing database blocks health query", func(t *testing.T) {
		t.Setenv("PGDATABASE", "pacman_missing_database")

		_, err := pgobs.QueryHealth(context.Background(), primary.Address(t))
		if err == nil {
			t.Fatal("expected health query against missing database to fail")
		}
	})

	t.Run("unknown user blocks system identifier query", func(t *testing.T) {
		t.Setenv("PGUSER", "pacman_missing_user")

		systemIdentifier, err := pgobs.QuerySystemIdentifier(context.Background(), primary.Address(t))
		if err == nil {
			t.Fatal("expected system identifier query with unknown user to fail")
		}
		if systemIdentifier != "" {
			t.Fatalf("expected empty system identifier on auth failure, got %q", systemIdentifier)
		}
	})

	t.Run("standby rejects insert into replicated table", func(t *testing.T) {
		assertStandbyRejectsWrite(t, standby, `
INSERT INTO topology_marker (id, payload)
VALUES (2, 'standby-write')`)
	})

	t.Run("standby rejects drop of replicated table", func(t *testing.T) {
		assertStandbyRejectsWrite(t, standby, `DROP TABLE topology_marker`)
	})

	t.Run("standby rejects create database", func(t *testing.T) {
		assertStandbyRejectsWrite(t, standby, `CREATE DATABASE pacman_negative_created_on_standby`)
	})

	t.Run("primary rejects duplicate physical replication slot", func(t *testing.T) {
		assertSQLFails(
			t,
			primary,
			`SELECT pg_create_physical_replication_slot('alpha_2')`,
			"already exists",
			"replication slot",
		)
	})
}

func TestPostgresRoleDetectionUsesRealPrimaryAndStandby(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	primary, standby := startReplicatedPostgresPair(t)

	primaryRole, err := pgobs.QueryRole(context.Background(), primary.Address(t))
	if err != nil {
		t.Fatalf("query primary role: %v", err)
	}
	if primaryRole != cluster.MemberRolePrimary {
		t.Fatalf("unexpected primary role: got %q, want %q", primaryRole, cluster.MemberRolePrimary)
	}

	standbyRole, err := pgobs.QueryRole(context.Background(), standby.Address(t))
	if err != nil {
		t.Fatalf("query standby role: %v", err)
	}
	if standbyRole != cluster.MemberRoleReplica {
		t.Fatalf("unexpected standby role: got %q, want %q", standbyRole, cluster.MemberRoleReplica)
	}

	standbyObservation, err := pgobs.QueryObservation(context.Background(), standby.Address(t))
	if err != nil {
		t.Fatalf("query standby observation: %v", err)
	}
	if !standbyObservation.InRecovery || standbyObservation.WAL.ReceiveLSN == "" || standbyObservation.WAL.ReplayLSN == "" {
		t.Fatalf("expected streaming standby observation, got %+v", standbyObservation)
	}
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate unused TCP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release unused TCP address: %v", err)
	}

	return address
}

func TestPromoteWorkflowPromotesRealStandby(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	primary, standby := startReplicatedPostgresPair(t)
	standbyBeforePromote := waitForObservation(t, standby, func(observation pgobs.Observation) bool {
		return observation.Role == cluster.MemberRoleReplica && observation.InRecovery && observation.Details.Timeline > 0
	})

	primaryAddress := primary.Address(t)
	primary.Stop(t)
	waitForAddressUnavailable(t, primary.Name(), primaryAddress)

	if err := newPostgresPromotionExecutor(t, standby).Promote(context.Background(), controlplane.PromotionRequest{
		Candidate: "alpha-2",
	}); err != nil {
		t.Fatalf("promote standby: %v", err)
	}

	promotedObservation := waitForPromotedPrimaryTimeline(t, standby, standbyBeforePromote.Details.Timeline)
	if promotedObservation.Role != cluster.MemberRolePrimary || promotedObservation.InRecovery {
		t.Fatalf("expected promoted primary observation, got %+v", promotedObservation)
	}

	role, err := pgobs.QueryRole(context.Background(), standby.Address(t))
	if err != nil {
		t.Fatalf("query promoted standby role: %v", err)
	}
	if role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected promoted role: got %q, want %q", role, cluster.MemberRolePrimary)
	}

	execSQL(t, standby, `
CREATE TABLE IF NOT EXISTS promote_workflow_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, standby, `
INSERT INTO promote_workflow_marker (id, payload)
VALUES (1, 'promoted')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
}

func TestStandbyConfigurationRenderedByPACMANStreamsFromRealPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "rendered-primary", "rendered-primary-postgres")
	standby := env.StartRenderedStreamingStandby(t, "rendered-standby", "rendered-standby-postgres", primary, "rendered_standby")

	t.Setenv("PGDATABASE", primary.Database())
	t.Setenv("PGUSER", primary.Username())
	t.Setenv("PGPASSWORD", primary.Password())
	t.Setenv("PGSSLMODE", "disable")

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)
	waitForQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standby, `SHOW primary_slot_name`, "rendered_standby")

	autoConf := standby.RequireExec(t, "sh", "-lc", "cat \"$PGDATA/postgresql.auto.conf\"")
	for _, expected := range []string{
		"primary_conninfo = 'host=rendered-primary-postgres port=5432 user=replicator password=replicator application_name=rendered_standby'",
		"primary_slot_name = 'rendered_standby'",
		"recovery_target_timeline = 'latest'",
	} {
		if !strings.Contains(autoConf, expected) {
			t.Fatalf("expected rendered standby config to contain %q, got:\n%s", expected, autoConf)
		}
	}

	if got := strings.TrimSpace(standby.RequireExec(t, "sh", "-lc", "test -f \"$PGDATA/standby.signal\" && echo present")); got != "present" {
		t.Fatalf("expected standby.signal from rendered standby config, got %q", got)
	}

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS rendered_standby_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO rendered_standby_marker (id, payload)
VALUES (1, 'rendered')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForQueryValue(t, standby, `SELECT payload FROM rendered_standby_marker WHERE id = 1`, "rendered")
}

func TestMaintenanceModeEnableDisableWithRealTopology(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	primary, standby := startReplicatedPostgresPair(t)
	store := seededRealTopologyStore(t)

	observedAt := time.Now().UTC()
	publishObservedNodeStatus(t, store, "alpha-1", primary, observedAt)
	publishObservedNodeStatus(t, store, "alpha-2", standby, observedAt.Add(time.Second))

	enabled, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     true,
		Reason:      "operator maintenance",
		RequestedBy: "integration-test",
	})
	if err != nil {
		t.Fatalf("enable maintenance: %v", err)
	}
	if !enabled.Enabled || enabled.Reason != "operator maintenance" {
		t.Fatalf("unexpected enabled maintenance status: %+v", enabled)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after enabling maintenance")
	}
	if status.Phase != cluster.ClusterPhaseMaintenance || !status.Maintenance.Enabled {
		t.Fatalf("expected maintenance phase after enable, got %+v", status)
	}

	disabled, err := store.UpdateMaintenanceMode(context.Background(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     false,
		Reason:      "operator maintenance complete",
		RequestedBy: "integration-test",
	})
	if err != nil {
		t.Fatalf("disable maintenance: %v", err)
	}
	if disabled.Enabled {
		t.Fatalf("expected maintenance disabled, got %+v", disabled)
	}

	status, ok = store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after disabling maintenance")
	}
	if status.Phase != cluster.ClusterPhaseHealthy || status.Maintenance.Enabled {
		t.Fatalf("expected healthy phase after disabling maintenance, got %+v", status)
	}

	history := store.History()
	if len(history) != 2 {
		t.Fatalf("expected enable and disable maintenance history entries, got %+v", history)
	}
	for _, entry := range history {
		if entry.Kind != cluster.OperationKindMaintenanceChange || entry.Result != cluster.OperationResultSucceeded {
			t.Fatalf("unexpected maintenance history entry: %+v", entry)
		}
	}
}
