//go:build integration

package integration_test

import (
	"testing"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/test/testenv"
)

const (
	skipShortMode = "skipping Docker-backed integration test in short mode"

	fixtureNode1      = "pacmand-1"
	fixtureNode1Alias = "pacmand-1-postgres"
	fixtureNode2      = "pacmand-2"
	fixtureNode2Alias = "pacmand-2-postgres"
	fixtureNode3      = "pacmand-3"
	fixtureNode3Alias = "pacmand-3-postgres"
	fixtureSlot2      = "pacmand_2"
	fixtureSlot3      = "pacmand_3"
)

// ---------------------------------------------------------------------------
// Positive: fixture bootstraps and replicates correctly
// ---------------------------------------------------------------------------

// TestPostgresReplicationFixtureBootstrapsStreamingTopology verifies that a
// three-node primary+2-standby topology reaches streaming replication with the
// expected slot names and read-only enforcement on both standbys.
func TestPostgresReplicationFixtureBootstrapsStreamingTopology(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, fixtureNode1, fixtureNode1Alias)
	standbyTwo := env.StartStreamingStandby(t, fixtureNode2, fixtureNode2Alias, primary, fixtureSlot2)
	standbyThree := env.StartStreamingStandby(t, fixtureNode3, fixtureNode3Alias, primary, fixtureSlot3)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standbyTwo, cluster.MemberRoleReplica)
	waitForPostgresRole(t, standbyThree, cluster.MemberRoleReplica)

	waitForQueryValue(t, standbyTwo, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standbyThree, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standbyTwo, `SHOW primary_slot_name`, fixtureSlot2)
	waitForQueryValue(t, standbyThree, `SHOW primary_slot_name`, fixtureSlot3)
	waitForQueryValue(t, standbyTwo, `SHOW transaction_read_only`, "on")
	waitForQueryValue(t, standbyThree, `SHOW transaction_read_only`, "on")

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS replication_fixture_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO replication_fixture_marker (id, payload)
VALUES (1, 'replicated')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	waitForQueryValue(t, standbyTwo, `SELECT payload FROM replication_fixture_marker WHERE id = 1`, "replicated")
	waitForQueryValue(t, standbyThree, `SELECT payload FROM replication_fixture_marker WHERE id = 1`, "replicated")

	assertActiveReplicationSlots(t, primary, fixtureSlot2, fixtureSlot3)
}

// TestReplicationFixtureSingleStandbyBootstraps verifies the two-node case
// (primary + one standby) used by most HA integration scenarios.
func TestReplicationFixtureSingleStandbyBootstraps(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "alpha-1", "alpha-1-postgres")
	standby := env.StartStreamingStandby(t, "alpha-2", "alpha-2-postgres", primary, "alpha_2")
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)

	waitForQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standby, `SHOW primary_slot_name`, "alpha_2")
	waitForQueryValue(t, standby, `SHOW transaction_read_only`, "on")

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS replication_single_standby_marker (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO replication_single_standby_marker (id, payload)
VALUES (1, 'single-standby')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)

	waitForQueryValue(t, standby, `SELECT payload FROM replication_single_standby_marker WHERE id = 1`, "single-standby")

	assertActiveReplicationSlots(t, primary, "alpha_2")
}

// TestReplicationFixtureSubsequentWritesReplicateToAllStandbys verifies that
// writes made after the topology is established replicate correctly, including
// writes across multiple DML statements — catching bugs that only surface after
// the first checkpoint or WAL segment boundary.
func TestReplicationFixtureSubsequentWritesReplicateToAllStandbys(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, fixtureNode1, fixtureNode1Alias)
	standbyTwo := env.StartStreamingStandby(t, fixtureNode2, fixtureNode2Alias, primary, fixtureSlot2)
	standbyThree := env.StartStreamingStandby(t, fixtureNode3, fixtureNode3Alias, primary, fixtureSlot3)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standbyTwo, cluster.MemberRoleReplica)
	waitForPostgresRole(t, standbyThree, cluster.MemberRoleReplica)

	waitForQueryValue(t, standbyTwo, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standbyThree, `SELECT status FROM pg_stat_wal_receiver`, "streaming")

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS replication_subsequent_writes (
	id serial PRIMARY KEY,
	payload text NOT NULL
)`)

	for _, payload := range []string{"write-one", "write-two", "write-three"} {
		execSQL(t, primary, `INSERT INTO replication_subsequent_writes (payload) VALUES ($1)`, payload)
		waitForQueryValue(t, standbyTwo,
			`SELECT payload FROM replication_subsequent_writes ORDER BY id DESC LIMIT 1`,
			payload,
		)
		waitForQueryValue(t, standbyThree,
			`SELECT payload FROM replication_subsequent_writes ORDER BY id DESC LIMIT 1`,
			payload,
		)
	}
}

// TestReplicationFixturePrimaryShowsActiveSendersForAllStandbys verifies that
// pg_stat_replication on the primary lists one active streaming sender per
// connected standby.
func TestReplicationFixturePrimaryShowsActiveSendersForAllStandbys(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, fixtureNode1, fixtureNode1Alias)
	standbyTwo := env.StartStreamingStandby(t, fixtureNode2, fixtureNode2Alias, primary, fixtureSlot2)
	standbyThree := env.StartStreamingStandby(t, fixtureNode3, fixtureNode3Alias, primary, fixtureSlot3)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standbyTwo, cluster.MemberRoleReplica)
	waitForPostgresRole(t, standbyThree, cluster.MemberRoleReplica)

	waitForQueryValue(t, standbyTwo, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standbyThree, `SELECT status FROM pg_stat_wal_receiver`, "streaming")

	waitForQueryValue(t,
		primary,
		`SELECT COUNT(*)::text FROM pg_stat_replication WHERE state = 'streaming'`,
		"2",
	)
}

// ---------------------------------------------------------------------------
// Negative: invalid operations and topology invariant violations
// ---------------------------------------------------------------------------

// TestReplicationFixtureStandbyRejectsDirectWrite verifies that a streaming
// standby is in read-only mode and rejects DML.  This is a critical MVP safety
// assertion: a standby that accepts writes makes "who is primary" unreliable.
func TestReplicationFixtureStandbyRejectsDirectWrite(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, fixtureNode1, fixtureNode1Alias)
	standby := env.StartStreamingStandby(t, fixtureNode2, fixtureNode2Alias, primary, fixtureSlot2)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)
	waitForQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")

	assertStandbyRejectsWrite(t, standby,
		`CREATE TABLE IF NOT EXISTS standby_write_rejected (id integer PRIMARY KEY)`)
}

// TestReplicationFixtureStandbyHasNoOwnReplicationSlots verifies that a
// streaming standby carries no physical replication slots of its own.  A
// standby with dangling slots accumulates WAL indefinitely — a symptom of
// a misconfigured or partially-failed rejoin.
func TestReplicationFixtureStandbyHasNoOwnReplicationSlots(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, fixtureNode1, fixtureNode1Alias)
	standby := env.StartStreamingStandby(t, fixtureNode2, fixtureNode2Alias, primary, fixtureSlot2)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)
	waitForQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")

	waitForQueryValue(t, standby, `SELECT COUNT(*)::text FROM pg_replication_slots`, "0")
}

// TestReplicationFixturePrimaryHasNoInactiveSlots verifies that after all
// standbys have connected and are streaming, the primary holds no inactive
// replication slots.  Inactive slots on the primary block WAL removal and
// indicate a standby that is disconnected or was never attached — a condition
// that must be detected before HA operations can safely proceed.
func TestReplicationFixturePrimaryHasNoInactiveSlots(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, fixtureNode1, fixtureNode1Alias)
	standbyTwo := env.StartStreamingStandby(t, fixtureNode2, fixtureNode2Alias, primary, fixtureSlot2)
	standbyThree := env.StartStreamingStandby(t, fixtureNode3, fixtureNode3Alias, primary, fixtureSlot3)
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standbyTwo, cluster.MemberRoleReplica)
	waitForPostgresRole(t, standbyThree, cluster.MemberRoleReplica)

	waitForQueryValue(t, standbyTwo, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standbyThree, `SELECT status FROM pg_stat_wal_receiver`, "streaming")

	waitForQueryValue(t, primary,
		`SELECT COUNT(*)::text FROM pg_replication_slots WHERE NOT active`,
		"0",
	)
}

// TestReplicationFixturePatroniInspiredNegativeSafetyCases verifies negative
// HA/replication invariants against a real primary+standby topology. These
// cases mirror the Patroni-style emphasis on not treating a replica as writable,
// not allowing duplicate slots, and not hiding unhealthy slot state.
func TestReplicationFixturePatroniInspiredNegativeSafetyCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	primary := env.StartReplicationPrimary(t, "patroni-negative-1", "patroni-negative-1-postgres")
	standby := env.StartStreamingStandby(t, "patroni-negative-2", "patroni-negative-2-postgres", primary, "patroni_negative_2")
	setPostgresObservationEnv(t, primary)

	waitForPostgresRole(t, primary, cluster.MemberRolePrimary)
	waitForPostgresRole(t, standby, cluster.MemberRoleReplica)
	waitForQueryValue(t, standby, `SELECT status FROM pg_stat_wal_receiver`, "streaming")
	waitForQueryValue(t, standby, `SHOW primary_slot_name`, "patroni_negative_2")

	execSQL(t, primary, `
CREATE TABLE IF NOT EXISTS patroni_negative_guard (
	id integer PRIMARY KEY,
	payload text NOT NULL
)`)
	execSQL(t, primary, `
INSERT INTO patroni_negative_guard (id, payload)
VALUES (1, 'replicated-negative-fixture')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`)
	waitForQueryValue(t, standby, `SELECT payload FROM patroni_negative_guard WHERE id = 1`, "replicated-negative-fixture")

	negativeCases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "negative standby rejects create table",
			run: func(t *testing.T) {
				assertSQLFails(t, standby,
					`CREATE TABLE patroni_negative_create_rejected (id integer PRIMARY KEY)`,
					"read-only transaction", "recovery mode", "standby",
				)
			},
		},
		{
			name: "negative standby rejects insert",
			run: func(t *testing.T) {
				assertSQLFails(t, standby,
					`INSERT INTO patroni_negative_guard (id, payload) VALUES (2, 'must-fail')`,
					"read-only transaction", "recovery mode", "standby",
				)
			},
		},
		{
			name: "negative standby rejects update",
			run: func(t *testing.T) {
				assertSQLFails(t, standby,
					`UPDATE patroni_negative_guard SET payload = 'must-fail' WHERE id = 1`,
					"read-only transaction", "recovery mode", "standby",
				)
			},
		},
		{
			name: "negative standby rejects truncate",
			run: func(t *testing.T) {
				assertSQLFails(t, standby,
					`TRUNCATE patroni_negative_guard`,
					"read-only transaction", "recovery mode", "standby",
				)
			},
		},
		{
			name: "negative primary rejects duplicate standby slot",
			run: func(t *testing.T) {
				assertSQLFails(t, primary,
					`SELECT pg_create_physical_replication_slot('patroni_negative_2')`,
					"already exists", "replication slot",
				)
			},
		},
		{
			name: "negative standby has no local replication slots",
			run: func(t *testing.T) {
				waitForQueryValue(t, standby, `SELECT COUNT(*)::text FROM pg_replication_slots`, "0")
			},
		},
		{
			name: "negative active standby slot is not inactive on primary",
			run: func(t *testing.T) {
				waitForQueryValue(t, primary,
					`SELECT COUNT(*)::text FROM pg_replication_slots WHERE slot_name = 'patroni_negative_2' AND NOT active`,
					"0",
				)
			},
		},
	}

	for _, testCase := range negativeCases {
		t.Run(testCase.name, testCase.run)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func setPostgresObservationEnv(t *testing.T, fixture *testenv.Postgres) {
	t.Helper()

	t.Setenv("PGDATABASE", fixture.Database())
	t.Setenv("PGUSER", fixture.Username())
	t.Setenv("PGPASSWORD", fixture.Password())
	t.Setenv("PGSSLMODE", "disable")
}

func assertActiveReplicationSlots(t *testing.T, fixture *testenv.Postgres, wantSlots ...string) {
	t.Helper()

	db := openFixtureDB(t, fixture)
	defer db.Close()

	rows, err := db.Query(`SELECT slot_name, active FROM pg_replication_slots ORDER BY slot_name`)
	if err != nil {
		t.Fatalf("query replication slots on %q: %v", fixture.Name(), err)
	}
	defer rows.Close()

	slotStates := make(map[string]bool, len(wantSlots))
	for rows.Next() {
		var slotName string
		var active bool
		if err := rows.Scan(&slotName, &active); err != nil {
			t.Fatalf("scan replication slot on %q: %v", fixture.Name(), err)
		}

		slotStates[slotName] = active
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate replication slots on %q: %v", fixture.Name(), err)
	}

	for _, wantSlot := range wantSlots {
		active, ok := slotStates[wantSlot]
		if !ok {
			t.Fatalf("expected replication slot %q on %q, got %v", wantSlot, fixture.Name(), slotStates)
		}

		if !active {
			t.Fatalf("expected replication slot %q on %q to be active, got %v", wantSlot, fixture.Name(), slotStates)
		}
	}
}

// assertStandbyRejectsWrite opens a direct connection to fixture and asserts
// that the provided statement fails with a read-only / recovery-mode error.
func assertStandbyRejectsWrite(t *testing.T, fixture *testenv.Postgres, statement string) {
	t.Helper()

	assertSQLFails(t, fixture, statement, "read-only transaction", "cannot execute", "recovery mode", "standby")
}

func assertSQLFails(t *testing.T, fixture *testenv.Postgres, statement string, wantSubstrings ...string) {
	t.Helper()

	db := openFixtureDB(t, fixture)
	defer db.Close()

	_, err := db.Exec(statement)
	if err == nil {
		t.Fatalf("expected SQL on %q to fail, but it succeeded: %s", fixture.Name(), statement)
	}

	errMsg := err.Error()
	if !containsAny(errMsg, wantSubstrings...) {
		t.Fatalf("expected SQL error from %q to contain one of %v, got: %v", fixture.Name(), wantSubstrings, err)
	}
}

func containsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
