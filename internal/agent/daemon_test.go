package agent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestNewDaemonRejectsNilLogger(t *testing.T) {
	t.Parallel()

	_, err := NewDaemon(validDataConfig(), nil)
	if !errors.Is(err, ErrLoggerRequired) {
		t.Fatalf("expected nil logger error, got %v", err)
	}
}

func TestNewDaemonRejectsMissingPostgresConfigForDataNode(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name: "alpha-1",
			Role: cluster.NodeRoleData,
		},
	}

	_, err := NewDaemon(cfg, logging.New("pacmand", &bytes.Buffer{}))
	if !errors.Is(err, ErrPostgresConfigRequired) {
		t.Fatalf("expected postgres config error, got %v", err)
	}
}

func TestNewDaemonAllowsWitnessWithoutPostgresConfig(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name: "alpha-3",
			Role: cluster.NodeRoleWitness,
		},
	}

	daemon, err := NewDaemon(cfg, logging.New("pacmand", &bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	startup := daemon.Startup()
	if !startup.StartedAt.IsZero() {
		t.Fatalf("expected zero startup state before start, got %+v", startup)
	}
}

func TestDaemonStartRecordsStartupStateAndHeartbeat(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	now := time.Date(2026, time.March, 21, 10, 30, 0, 0, time.UTC)
	cfg := validDataConfig()

	daemon, err := NewDaemon(
		cfg,
		logging.New("pacmand", &logs),
		withNow(func() time.Time { return now }),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{
				Role:       cluster.MemberRolePrimary,
				InRecovery: false,
				Details: postgres.Details{
					ServerVersion:       170002,
					PendingRestart:      false,
					SystemIdentifier:    "7599025879359099984",
					Timeline:            1,
					PostmasterStartAt:   now.Add(-2 * time.Hour),
					ReplicationLagBytes: 0,
				},
				WAL: postgres.WALProgress{
					WriteLSN: "0/3000148",
					FlushLSN: "0/3000148",
				},
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	startup := daemon.Startup()
	if startup.NodeName != "alpha-1" {
		t.Fatalf("unexpected node name: got %q, want %q", startup.NodeName, "alpha-1")
	}

	if startup.NodeRole != cluster.NodeRoleData {
		t.Fatalf("unexpected node role: got %q, want %q", startup.NodeRole, cluster.NodeRoleData)
	}

	if startup.APIAddress != cfg.Node.APIAddress {
		t.Fatalf("unexpected api address: got %q, want %q", startup.APIAddress, cfg.Node.APIAddress)
	}

	if startup.ControlAddress != config.DefaultControlAddress {
		t.Fatalf("unexpected control address: got %q, want %q", startup.ControlAddress, config.DefaultControlAddress)
	}

	if !startup.ManagesPostgres {
		t.Fatal("expected data node daemon to manage postgres")
	}

	if !startup.StartedAt.Equal(now) {
		t.Fatalf("unexpected startedAt: got %v, want %v", startup.StartedAt, now)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.Sequence != 1 {
		t.Fatalf("unexpected heartbeat sequence: got %d, want %d", heartbeat.Sequence, 1)
	}

	if !heartbeat.ObservedAt.Equal(now) {
		t.Fatalf("unexpected heartbeat observedAt: got %v, want %v", heartbeat.ObservedAt, now)
	}

	if !heartbeat.Postgres.Managed {
		t.Fatal("expected heartbeat to manage postgres")
	}

	if !heartbeat.Postgres.Up {
		t.Fatalf("expected postgres to be available, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected postgres role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRolePrimary)
	}

	if !heartbeat.Postgres.RecoveryKnown {
		t.Fatalf("expected known recovery state, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.InRecovery {
		t.Fatalf("expected primary recovery state, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Details.SystemIdentifier != "7599025879359099984" {
		t.Fatalf("unexpected system identifier: got %q", heartbeat.Postgres.Details.SystemIdentifier)
	}

	if heartbeat.Postgres.Details.Timeline != 1 {
		t.Fatalf("unexpected timeline: got %d, want %d", heartbeat.Postgres.Details.Timeline, 1)
	}

	if heartbeat.Postgres.Details.ServerVersion != 170002 {
		t.Fatalf("unexpected server version: got %d", heartbeat.Postgres.Details.ServerVersion)
	}

	if heartbeat.Postgres.Details.PendingRestart {
		t.Fatalf("expected pending restart to be false, got %+v", heartbeat.Postgres)
	}

	if !heartbeat.Postgres.Details.PostmasterStartAt.Equal(now.Add(-2 * time.Hour)) {
		t.Fatalf("unexpected postmaster start time: got %v", heartbeat.Postgres.Details.PostmasterStartAt)
	}

	if heartbeat.Postgres.WAL.WriteLSN != "0/3000148" {
		t.Fatalf("unexpected write lsn: got %q", heartbeat.Postgres.WAL.WriteLSN)
	}

	if heartbeat.Postgres.WAL.FlushLSN != "0/3000148" {
		t.Fatalf("unexpected flush lsn: got %q", heartbeat.Postgres.WAL.FlushLSN)
	}

	if heartbeat.Postgres.Details.ReplicationLagBytes != 0 {
		t.Fatalf("unexpected replication lag bytes: got %d", heartbeat.Postgres.Details.ReplicationLagBytes)
	}

	if heartbeat.Postgres.Address != "127.0.0.1:5432" {
		t.Fatalf("unexpected postgres probe address: got %q", heartbeat.Postgres.Address)
	}

	if !heartbeat.ControlPlane.ClusterReachable {
		t.Fatalf("expected control plane publication to succeed, got %+v", heartbeat.ControlPlane)
	}

	assertContains(t, logs.String(), `"msg":"started local agent daemon"`)
	assertContains(t, logs.String(), `"msg":"observed PostgreSQL availability"`)
	assertContains(t, logs.String(), `"msg":"published local state to control plane"`)
	assertContains(t, logs.String(), `"heartbeat_sequence":1`)
	assertContains(t, logs.String(), `"postgres_up":true`)
	assertContains(t, logs.String(), `"member_role":"primary"`)
	assertContains(t, logs.String(), `"in_recovery":false`)
	assertContains(t, logs.String(), `"system_identifier":"7599025879359099984"`)
	assertContains(t, logs.String(), `"timeline":1`)
	assertContains(t, logs.String(), `"write_lsn":"0/3000148"`)
	assertContains(t, logs.String(), `"flush_lsn":"0/3000148"`)
	assertContains(t, logs.String(), `"replication_lag_bytes":0`)
	assertContains(t, logs.String(), `"cluster_reachable":true`)

	cancel()
	daemon.Wait()
}

func TestDaemonStartRecordsWitnessHeartbeatWithoutLocalPostgres(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	now := time.Date(2026, time.March, 21, 10, 30, 0, 0, time.UTC)

	daemon, err := NewDaemon(
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name:       "witness-1",
				Role:       cluster.NodeRoleWitness,
				APIAddress: reserveLoopbackAddress(),
			},
		},
		logging.New("pacmand", &logs),
		withNow(func() time.Time { return now }),
		withHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.Sequence != 1 {
		t.Fatalf("unexpected heartbeat sequence: got %d, want %d", heartbeat.Sequence, 1)
	}

	if heartbeat.Postgres.Managed {
		t.Fatalf("expected witness heartbeat without postgres, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Role != cluster.MemberRoleWitness {
		t.Fatalf("unexpected witness role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRoleWitness)
	}

	if heartbeat.Postgres.Up {
		t.Fatalf("expected postgres availability to be false, got %+v", heartbeat.Postgres)
	}

	assertContains(t, logs.String(), `"msg":"observed heartbeat without local PostgreSQL"`)
	assertContains(t, logs.String(), `"postgres_managed":false`)
	assertContains(t, logs.String(), `"msg":"published local state to control plane"`)

	cancel()
	daemon.Wait()
}

func TestDaemonStartDetectsReplicaRecoveryState(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &bytes.Buffer{}),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{
				Role:       cluster.MemberRoleReplica,
				InRecovery: true,
				Details: postgres.Details{
					ServerVersion:       170002,
					PendingRestart:      true,
					SystemIdentifier:    "7599025879359099984",
					Timeline:            7,
					PostmasterStartAt:   time.Date(2026, time.March, 21, 8, 0, 0, 0, time.UTC),
					ReplicationLagBytes: 256,
				},
				WAL: postgres.WALProgress{
					FlushLSN:        "0/7000200",
					ReceiveLSN:      "0/7000200",
					ReplayLSN:       "0/7000100",
					ReplayTimestamp: time.Date(2026, time.March, 21, 10, 29, 0, 0, time.UTC),
				},
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.Postgres.Role != cluster.MemberRoleReplica {
		t.Fatalf("unexpected postgres role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRoleReplica)
	}

	if !heartbeat.Postgres.RecoveryKnown {
		t.Fatalf("expected known recovery state, got %+v", heartbeat.Postgres)
	}

	if !heartbeat.Postgres.InRecovery {
		t.Fatalf("expected in-recovery state, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Details.SystemIdentifier != "7599025879359099984" {
		t.Fatalf("unexpected system identifier: got %q", heartbeat.Postgres.Details.SystemIdentifier)
	}

	if heartbeat.Postgres.Details.Timeline != 7 {
		t.Fatalf("unexpected timeline: got %d, want %d", heartbeat.Postgres.Details.Timeline, 7)
	}

	if !heartbeat.Postgres.Details.PendingRestart {
		t.Fatalf("expected pending restart, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.WAL.FlushLSN != "0/7000200" {
		t.Fatalf("unexpected flush lsn: got %q", heartbeat.Postgres.WAL.FlushLSN)
	}

	if heartbeat.Postgres.WAL.ReceiveLSN != "0/7000200" {
		t.Fatalf("unexpected receive lsn: got %q", heartbeat.Postgres.WAL.ReceiveLSN)
	}

	if heartbeat.Postgres.WAL.ReplayLSN != "0/7000100" {
		t.Fatalf("unexpected replay lsn: got %q", heartbeat.Postgres.WAL.ReplayLSN)
	}

	if heartbeat.Postgres.Details.ReplicationLagBytes != 256 {
		t.Fatalf("unexpected replication lag bytes: got %d", heartbeat.Postgres.Details.ReplicationLagBytes)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonStartReportsRoleDetectionFailureWhileAvailabilityIsUp(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{Role: cluster.MemberRoleUnknown}, errors.New("pq: password authentication failed")
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if !heartbeat.Postgres.Up {
		t.Fatalf("expected availability to remain up, got %+v", heartbeat.Postgres)
	}

	if heartbeat.Postgres.Role != cluster.MemberRoleUnknown {
		t.Fatalf("unexpected postgres role: got %q, want %q", heartbeat.Postgres.Role, cluster.MemberRoleUnknown)
	}

	if heartbeat.Postgres.RecoveryKnown {
		t.Fatalf("expected unknown recovery state, got %+v", heartbeat.Postgres)
	}

	assertContains(t, logs.String(), `"msg":"observed PostgreSQL availability without role state"`)
	assertContains(t, logs.String(), `"postgres_state_error":"pq: password authentication failed"`)

	cancel()
	daemon.Wait()
}

func TestDaemonHeartbeatLoopTicksAndTracksAvailabilityChanges(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	var (
		mu    sync.Mutex
		calls int
	)

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withHeartbeatInterval(10*time.Millisecond),
		withPostgresProbe(func(context.Context, string) error {
			mu.Lock()
			defer mu.Unlock()

			calls++
			if calls == 1 {
				return errors.New("dial tcp 127.0.0.1:5432: connect: connection refused")
			}

			return nil
		}),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{
				Role:       cluster.MemberRolePrimary,
				InRecovery: false,
				Details: postgres.Details{
					ServerVersion:       170002,
					PendingRestart:      false,
					SystemIdentifier:    "7599025879359099984",
					Timeline:            1,
					PostmasterStartAt:   time.Date(2026, time.March, 21, 8, 0, 0, 0, time.UTC),
					ReplicationLagBytes: 0,
				},
				WAL: postgres.WALProgress{
					WriteLSN: "0/3000148",
					FlushLSN: "0/3000148",
				},
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	waitForHeartbeat(t, daemon, func(heartbeat agentmodel.Heartbeat) bool {
		return heartbeat.Sequence >= 2 && heartbeat.Postgres.Up
	})

	cancel()
	daemon.Wait()

	heartbeat := daemon.Heartbeat()
	if heartbeat.Sequence < 2 {
		t.Fatalf("expected at least two heartbeats, got %d", heartbeat.Sequence)
	}

	if !heartbeat.Postgres.Up {
		t.Fatalf("expected postgres to recover, got %+v", heartbeat.Postgres)
	}

	assertContains(t, logs.String(), `"msg":"observed PostgreSQL unavailability"`)
	assertContains(t, logs.String(), `"msg":"observed PostgreSQL availability"`)
	assertContains(t, logs.String(), `"postgres_up":false`)
	assertContains(t, logs.String(), `"postgres_up":true`)
	assertContains(t, logs.String(), `"member_role":"primary"`)
	assertContains(t, logs.String(), `"system_identifier":"7599025879359099984"`)
	assertContains(t, logs.String(), `"timeline":1`)
	assertContains(t, logs.String(), `"msg":"published local state to control plane"`)
}

func TestDaemonPublishesNodeStatusToControlPlane(t *testing.T) {
	t.Parallel()

	store := controlplane.NewMemoryStateStore()
	cfg := validDataConfig()

	daemon, err := NewDaemon(
		cfg,
		logging.New("pacmand", &bytes.Buffer{}),
		withHeartbeatInterval(time.Hour),
		WithControlPlanePublisher(store),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{
				Role:       cluster.MemberRolePrimary,
				InRecovery: false,
				Details: postgres.Details{
					ServerVersion:       170002,
					PendingRestart:      false,
					SystemIdentifier:    "7599025879359099984",
					Timeline:            4,
					ReplicationLagBytes: 0,
				},
				WAL: postgres.WALProgress{
					WriteLSN: "0/4000200",
					FlushLSN: "0/4000200",
				},
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	registration, ok := store.RegisteredMember("alpha-1")
	if !ok {
		t.Fatal("expected registered member")
	}

	if registration.APIAddress != cfg.Node.APIAddress {
		t.Fatalf("unexpected registered api address: got %q", registration.APIAddress)
	}

	if registration.ControlAddress != config.DefaultControlAddress {
		t.Fatalf("unexpected registered control address: got %q", registration.ControlAddress)
	}

	leader, ok := store.Leader()
	if !ok {
		t.Fatal("expected elected control-plane leader")
	}

	if leader.LeaderNode != "alpha-1" {
		t.Fatalf("unexpected control-plane leader: got %+v", leader)
	}

	status, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected published node status")
	}

	if status.Role != cluster.MemberRolePrimary {
		t.Fatalf("unexpected published role: got %q", status.Role)
	}

	if status.State != cluster.MemberStateRunning {
		t.Fatalf("unexpected published state: got %q", status.State)
	}

	if !status.ControlPlane.ClusterReachable {
		t.Fatalf("expected control-plane reachable status, got %+v", status.ControlPlane)
	}

	if !status.ControlPlane.Leader {
		t.Fatalf("expected leader flag to propagate, got %+v", status.ControlPlane)
	}

	if status.Postgres.WAL.FlushLSN != "0/4000200" {
		t.Fatalf("unexpected published flush lsn: got %q", status.Postgres.WAL.FlushLSN)
	}

	member, ok := store.Member("alpha-1")
	if !ok {
		t.Fatal("expected discovered member")
	}

	if member.APIURL != "http://"+cfg.Node.APIAddress {
		t.Fatalf("unexpected discovered api url: got %q", member.APIURL)
	}

	if !member.Healthy {
		t.Fatalf("expected discovered member to be healthy, got %+v", member)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonRecordsControlPlanePublishFailure(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	publisher := failingPublisher{err: errors.New("control plane storage unavailable")}

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &logs),
		withHeartbeatInterval(time.Hour),
		WithControlPlanePublisher(publisher),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{
				Role:       cluster.MemberRolePrimary,
				InRecovery: false,
				Details: postgres.Details{
					SystemIdentifier:    "7599025879359099984",
					Timeline:            1,
					ReplicationLagBytes: 0,
				},
				WAL: postgres.WALProgress{
					WriteLSN: "0/3000148",
					FlushLSN: "0/3000148",
				},
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	heartbeat := daemon.Heartbeat()
	if heartbeat.ControlPlane.ClusterReachable {
		t.Fatalf("expected unreachable control plane, got %+v", heartbeat.ControlPlane)
	}

	if heartbeat.ControlPlane.PublishError != "control plane storage unavailable" {
		t.Fatalf("unexpected publish error: got %q", heartbeat.ControlPlane.PublishError)
	}

	assertContains(t, logs.String(), `"msg":"failed to publish local state to control plane"`)
	assertContains(t, logs.String(), `"publish_error":"control plane storage unavailable"`)

	cancel()
	daemon.Wait()
}

func TestDaemonStartRejectsSecondStart(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(validDataConfig(), logging.New("pacmand", &bytes.Buffer{}), withHeartbeatInterval(time.Hour))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	if err := daemon.Start(ctx); !errors.Is(err, ErrDaemonAlreadyStarted) {
		t.Fatalf("expected second start error, got %v", err)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonStartRejectsConcurrentSecondStart(t *testing.T) {
	t.Parallel()

	daemon, err := NewDaemon(
		validDataConfig(),
		logging.New("pacmand", &bytes.Buffer{}),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{
				Role:       cluster.MemberRolePrimary,
				InRecovery: false,
				Details: postgres.Details{
					SystemIdentifier: "7599025879359099984",
					Timeline:         1,
				},
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			errs <- daemon.Start(ctx)
		}()
	}

	var startedCount int
	var alreadyStartedCount int
	for range 2 {
		err := <-errs
		switch {
		case err == nil:
			startedCount++
		case errors.Is(err, ErrDaemonAlreadyStarted):
			alreadyStartedCount++
		default:
			t.Fatalf("unexpected start error: %v", err)
		}
	}

	if startedCount != 1 {
		t.Fatalf("expected exactly one successful start, got %d", startedCount)
	}

	if alreadyStartedCount != 1 {
		t.Fatalf("expected exactly one already-started error, got %d", alreadyStartedCount)
	}

	cancel()
	daemon.Wait()
}

func TestDaemonStartReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	daemon, err := NewDaemon(validDataConfig(), logging.New("pacmand", &bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	err = daemon.Start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}
}

func validDataConfig() config.Config {
	return config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name:       "alpha-1",
			Role:       cluster.NodeRoleData,
			APIAddress: reserveLoopbackAddress(),
		},
		Postgres: &config.PostgresLocalConfig{
			DataDir: "/var/lib/postgresql/data",
		},
	}
}

func reserveLoopbackAddress() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		panic(err)
	}

	return address
}

func waitForHeartbeat(t *testing.T, daemon *Daemon, predicate func(agentmodel.Heartbeat) bool) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if predicate(daemon.Heartbeat()) {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("heartbeat condition was not met, last heartbeat: %+v", daemon.Heartbeat())
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("expected %q to contain %q", got, want)
	}
}

type failingPublisher struct {
	err error
}

func (publisher failingPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: false}, publisher.err
}
