package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/peerapi"
	"github.com/polkiloo/pacman/internal/postgres"
	"github.com/polkiloo/pacman/internal/security"
	"github.com/polkiloo/pacman/internal/security/tlstesting"
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

func TestNewDaemonRejectsUnreadableAdminBearerTokenFile(t *testing.T) {
	t.Parallel()

	cfg := validDataConfig()
	cfg.Security = &config.SecurityConfig{
		AdminBearerTokenFile: filepath.Join(t.TempDir(), "missing.token"),
	}

	_, err := NewDaemon(cfg, logging.New("pacmand", &bytes.Buffer{}))
	if err == nil {
		t.Fatal("expected token file resolution error")
	}

	assertContains(t, err.Error(), "resolve api admin bearer token")
	assertContains(t, err.Error(), "read admin bearer token file")
}

func TestNewDaemonRejectsUnreadableAPITLSFiles(t *testing.T) {
	t.Parallel()

	cfg := validDataConfig()
	cfg.TLS = &config.TLSConfig{
		Enabled:  true,
		CertFile: filepath.Join(t.TempDir(), "missing.crt"),
		KeyFile:  filepath.Join(t.TempDir(), "missing.key"),
	}

	_, err := NewDaemon(cfg, logging.New("pacmand", &bytes.Buffer{}))
	if !errors.Is(err, ErrAPIServerTLSRequired) {
		t.Fatalf("expected tls dependency error, got %v", err)
	}
}

func TestNewDaemonRejectsMissingPeerServerTLSConfig(t *testing.T) {
	t.Parallel()

	cfg := validWitnessMemberMTLSConfig()

	_, err := NewDaemon(
		cfg,
		logging.New("pacmand", &bytes.Buffer{}),
		WithNoAPIServer(),
	)
	if !errors.Is(err, ErrPeerServerTLSRequired) {
		t.Fatalf("expected peer server tls dependency error, got %v", err)
	}
}

func TestNewDaemonRejectsMissingPeerClientTLSConfig(t *testing.T) {
	t.Parallel()

	cfg := validWitnessMemberMTLSConfig()
	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, err := security.LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.Server.CertFile,
		KeyFile:  fixture.Server.KeyFile,
	}, tls.RequireAndVerifyClientCert)
	if err != nil {
		t.Fatalf("load peer server tls config: %v", err)
	}

	_, err = NewDaemon(
		cfg,
		logging.New("pacmand", &bytes.Buffer{}),
		WithNoAPIServer(),
		WithPeerServerTLSConfig(serverTLSConfig),
	)
	if !errors.Is(err, ErrPeerClientTLSRequired) {
		t.Fatalf("expected peer client tls dependency error, got %v", err)
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
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"node_role":"data"`)
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

func TestDaemonStartProtectsAPIRoutesWhenAdminAuthConfigured(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	cfg := validDataConfig()
	cfg.Security = &config.SecurityConfig{
		AdminBearerToken: "secret-token",
	}

	daemon, err := NewDaemon(
		cfg,
		logging.New("pacmand", &logs),
		withHeartbeatInterval(time.Hour),
		withPostgresProbe(func(context.Context, string) error { return nil }),
		withPostgresStateProbe(func(context.Context, string) (postgres.Observation, error) {
			return postgres.Observation{Role: cluster.MemberRolePrimary}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		daemon.Wait()
	}()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	client := &http.Client{Timeout: time.Second}
	waitForHTTPServer(t, client, "http://"+cfg.Node.APIAddress+"/health")

	noAuthResponse := mustGET(t, client, "http://"+cfg.Node.APIAddress+"/api/v1/cluster", "")
	defer noAuthResponse.Body.Close()

	if noAuthResponse.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected unauthorized status: got %d, want %d", noAuthResponse.StatusCode, http.StatusUnauthorized)
	}

	if got := noAuthResponse.Header.Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("www-authenticate: got %q, want %q", got, "Bearer")
	}

	authedResponse := mustGET(t, client, "http://"+cfg.Node.APIAddress+"/api/v1/cluster", "Bearer secret-token")
	defer authedResponse.Body.Close()

	if authedResponse.StatusCode == http.StatusUnauthorized {
		t.Fatal("expected authorized request to pass middleware")
	}
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
	assertContains(t, logs.String(), `"node":"witness-1"`)
	assertContains(t, logs.String(), `"node_role":"witness"`)
	assertContains(t, logs.String(), `"postgres_managed":false`)
	assertContains(t, logs.String(), `"msg":"published local state to control plane"`)

	cancel()
	daemon.Wait()
}

func TestDaemonStartBootstrapsClusterSpecIntoControlPlane(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	now := time.Date(2026, time.April, 1, 10, 30, 0, 0, time.UTC)
	store := controlplane.NewMemoryStateStore()

	cfg := validDataConfig()
	cfg.Bootstrap = &config.ClusterBootstrapConfig{
		ClusterName:     "alpha",
		InitialPrimary:  "alpha-1",
		SeedAddresses:   []string{"127.0.0.1:9090"},
		ExpectedMembers: []string{"alpha-1", "alpha-2"},
	}

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
					ServerVersion:    170002,
					SystemIdentifier: "7599025879359099984",
					Timeline:         1,
				},
				WAL: postgres.WALProgress{
					WriteLSN: "0/3000148",
					FlushLSN: "0/3000148",
				},
			}, nil
		}),
		WithControlPlanePublisher(store),
		WithNoAPIServer(),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	spec, ok := store.ClusterSpec()
	if !ok {
		t.Fatal("expected bootstrap cluster spec to be stored")
	}

	if spec.ClusterName != "alpha" {
		t.Fatalf("unexpected cluster name: got %q", spec.ClusterName)
	}

	if len(spec.Members) != 2 {
		t.Fatalf("unexpected bootstrap members: got %+v", spec.Members)
	}

	if spec.Members[0].Name != "alpha-1" || spec.Members[1].Name != "alpha-2" {
		t.Fatalf("unexpected bootstrap member names: got %+v", spec.Members)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after first heartbeat")
	}

	if status.ClusterName != "alpha" {
		t.Fatalf("unexpected cluster status name: got %q", status.ClusterName)
	}

	if len(status.Members) != 1 || status.Members[0].Name != "alpha-1" {
		t.Fatalf("unexpected observed members: got %+v", status.Members)
	}

	assertContains(t, logs.String(), `"msg":"stored bootstrap cluster spec"`)

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
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"node_role":"data"`)
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
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"node_role":"data"`)
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

func TestDaemonUsesControlPlaneStateStoreForHTTPAPI(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 19, 14, 30, 0, 0, time.UTC)
	store := staticControlPlaneStore{
		clusterStatus: cluster.ClusterStatus{
			ClusterName:    "alpha",
			Phase:          cluster.ClusterPhaseHealthy,
			CurrentPrimary: "alpha-1",
			ObservedAt:     now,
			Members: []cluster.MemberStatus{
				{
					Name:       "alpha-1",
					APIURL:     "http://10.0.0.10:8080",
					Host:       "10.0.0.10",
					Port:       8080,
					Role:       cluster.MemberRolePrimary,
					State:      cluster.MemberStateRunning,
					Healthy:    true,
					Leader:     true,
					Timeline:   1,
					LastSeenAt: now,
				},
				{
					Name:       "alpha-2",
					APIURL:     "http://10.0.0.20:8080",
					Host:       "10.0.0.20",
					Port:       8080,
					Role:       cluster.MemberRoleReplica,
					State:      cluster.MemberStateStreaming,
					Healthy:    true,
					Timeline:   1,
					LastSeenAt: now,
				},
			},
		},
	}

	cfg := config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name:           "alpha-api",
			Role:           cluster.NodeRoleWitness,
			APIAddress:     reserveLoopbackAddress(),
			ControlAddress: reserveLoopbackAddress(),
		},
	}

	daemon, err := NewDaemon(
		cfg,
		logging.New("pacmand", &bytes.Buffer{}),
		WithControlPlaneStateStore(store),
		withHeartbeatInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	client := &http.Client{Timeout: 200 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}
	defer func() {
		cancel()
		daemon.Wait()
	}()

	waitForHTTPServer(t, client, "http://"+cfg.Node.APIAddress+"/health")

	response := mustGET(t, client, "http://"+cfg.Node.APIAddress+"/api/v1/members", "")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected members status: got %d want %d", response.StatusCode, http.StatusOK)
	}

	var payload struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode members response: %v", err)
	}

	if len(payload.Items) != 2 || payload.Items[0].Name != "alpha-1" || payload.Items[1].Name != "alpha-2" {
		t.Fatalf("unexpected members payload: %+v", payload.Items)
	}
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

func TestDaemonStartServesPeerIdentityOverMTLS(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, err := security.LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.Server.CertFile,
		KeyFile:  fixture.Server.KeyFile,
	}, tls.RequireAndVerifyClientCert)
	if err != nil {
		t.Fatalf("load peer server tls config: %v", err)
	}

	clientTLSConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		CertFile:   fixture.Client.CertFile,
		KeyFile:    fixture.Client.KeyFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("load peer client tls config: %v", err)
	}

	cfg := validWitnessMemberMTLSConfig()
	cfg.TLS = &config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.Server.CertFile,
		KeyFile:  fixture.Server.KeyFile,
	}
	cfg.Bootstrap = &config.ClusterBootstrapConfig{
		ClusterName:     "alpha",
		InitialPrimary:  "alpha-1",
		SeedAddresses:   []string{cfg.Node.ControlAddress},
		ExpectedMembers: []string{"alpha-1", "beta-1"},
	}

	daemon, err := NewDaemon(
		cfg,
		logging.New("pacmand", &bytes.Buffer{}),
		WithNoAPIServer(),
		WithPeerServerTLSConfig(serverTLSConfig),
		WithPeerClientTLSConfig(clientTLSConfig),
	)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: clientTLSConfig,
		},
	}

	waitForHTTPServer(t, client, "https://"+cfg.Node.ControlAddress+"/peer/v1/identity")

	response := mustGET(t, client, "https://"+cfg.Node.ControlAddress+"/peer/v1/identity", "")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected peer identity status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	var payload peerapi.IdentityResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode peer identity response: %v", err)
	}

	if payload.NodeName != "alpha-1" {
		t.Fatalf("nodeName: got %q, want %q", payload.NodeName, "alpha-1")
	}

	if payload.Peer.Subject != "beta-1" {
		t.Fatalf("peer subject: got %q, want %q", payload.Peer.Subject, "beta-1")
	}

	cancel()
	daemon.Wait()
}

func TestDaemonProbeSeedPeersLogsValidatedPeerMTLSConnection(t *testing.T) {
	t.Parallel()

	fixture := tlstesting.WriteMutual(t, "alpha-1", "beta-1")
	serverTLSConfig, err := security.LoadServerTLSConfig(config.TLSConfig{
		Enabled:  true,
		CAFile:   fixture.CAFile,
		CertFile: fixture.Server.CertFile,
		KeyFile:  fixture.Server.KeyFile,
	}, tls.RequireAndVerifyClientCert)
	if err != nil {
		t.Fatalf("load peer server tls config: %v", err)
	}

	clientTLSConfig, err := security.LoadClientTLSConfig(config.TLSConfig{
		CAFile:     fixture.CAFile,
		CertFile:   fixture.Client.CertFile,
		KeyFile:    fixture.Client.KeyFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("load peer client tls config: %v", err)
	}

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()

	server := peerapi.New("alpha-1", logging.New("peerapi", &bytes.Buffer{}), peerapi.Config{
		TLSConfig:    serverTLSConfig,
		AllowedPeers: []string{"beta-1"},
	})
	seedAddress := reserveLoopbackAddress()
	if err := server.Start(serverCtx, seedAddress); err != nil {
		t.Fatalf("start peer server: %v", err)
	}

	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: clientTLSConfig,
		},
	}
	waitForHTTPServer(t, client, "https://"+seedAddress+"/peer/v1/identity")

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:              logging.New("pacmand", &logs),
		peerClientTLSConfig: clientTLSConfig,
		peerProbeTimeout:    time.Second,
		config: config.Config{
			Node: config.NodeConfig{
				ControlAddress: reserveLoopbackAddress(),
			},
			Bootstrap: &config.ClusterBootstrapConfig{
				SeedAddresses: []string{seedAddress},
			},
		},
	}

	daemon.probeSeedPeers(context.Background())

	assertContains(t, logs.String(), `"msg":"validated peer mTLS connection"`)
	assertContains(t, logs.String(), `"seed_address":"`+seedAddress+`"`)

	cancelServer()
	if err := server.Wait(); err != nil {
		t.Fatalf("wait for peer server shutdown: %v", err)
	}
}

func TestDaemonWaitLogsPeerServerUnexpectedStop(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:     logging.New("pacmand", &logs),
		config:     validWitnessMemberMTLSConfig(),
		peerServer: waitErrorServer{waitErr: errors.New("peer stopped")},
	}

	daemon.Wait()

	assertContains(t, logs.String(), `"msg":"peer api server stopped unexpectedly"`)
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"node_role":"witness"`)
	assertContains(t, logs.String(), `"error":"peer stopped"`)
}

func TestDaemonWaitLogsAPIServerUnexpectedStop(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:    logging.New("pacmand", &logs),
		config:    validDataConfig(),
		apiServer: waitErrorServer{waitErr: errors.New("api stopped")},
	}

	daemon.Wait()

	assertContains(t, logs.String(), `"msg":"http api server stopped unexpectedly"`)
	assertContains(t, logs.String(), `"node":"alpha-1"`)
	assertContains(t, logs.String(), `"node_role":"data"`)
	assertContains(t, logs.String(), `"error":"api stopped"`)
}

func TestDaemonProbeSeedPeerLogsRequestBuildError(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:           logging.New("pacmand", &logs),
		config:           validDataConfig(),
		peerProbeTimeout: time.Second,
	}

	daemon.probeSeedPeer(context.Background(), &http.Client{Timeout: time.Second}, "bad\naddress:9090")

	assertContains(t, logs.String(), `"msg":"failed to build peer probe request"`)
	assertContains(t, logs.String(), `"seed_address":"bad\naddress:9090"`)
}

func TestDaemonProbeSeedPeerLogsTransportFailure(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:           logging.New("pacmand", &logs),
		config:           validDataConfig(),
		peerProbeTimeout: time.Second,
	}

	daemon.probeSeedPeer(context.Background(), &http.Client{Timeout: 20 * time.Millisecond}, reserveLoopbackAddress())

	assertContains(t, logs.String(), `"msg":"failed to probe peer over mTLS"`)
}

func TestDaemonProbeSeedPeerLogsUnexpectedStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:           logging.New("pacmand", &logs),
		config:           validDataConfig(),
		peerProbeTimeout: time.Second,
	}

	daemon.probeSeedPeer(context.Background(), server.Client(), strings.TrimPrefix(server.URL, "https://"))

	assertContains(t, logs.String(), `"msg":"peer probe returned unexpected status"`)
	assertContains(t, logs.String(), `"status":503`)
}

func TestDaemonProbeSeedPeerLogsDecodeFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte("{"))
	}))
	defer server.Close()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:           logging.New("pacmand", &logs),
		config:           validDataConfig(),
		peerProbeTimeout: time.Second,
	}

	daemon.probeSeedPeer(context.Background(), server.Client(), strings.TrimPrefix(server.URL, "https://"))

	assertContains(t, logs.String(), `"msg":"failed to decode peer probe response"`)
}

func TestShouldProbeSeedAddress(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		seedAddress      string
		localControlAddr string
		want             bool
	}{
		{name: "blank", seedAddress: "", localControlAddr: "127.0.0.1:9090", want: false},
		{name: "local address", seedAddress: "127.0.0.1:9090", localControlAddr: "127.0.0.1:9090", want: false},
		{name: "unspecified ip", seedAddress: "0.0.0.0:9090", localControlAddr: "127.0.0.1:9090", want: false},
		{name: "remote ip", seedAddress: "127.0.0.2:9090", localControlAddr: "127.0.0.1:9090", want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := shouldProbeSeedAddress(tc.seedAddress, tc.localControlAddr); got != tc.want {
				t.Fatalf("shouldProbeSeedAddress(%q, %q): got %t, want %t", tc.seedAddress, tc.localControlAddr, got, tc.want)
			}
		})
	}
}

func TestMemberPeerSubjects(t *testing.T) {
	t.Parallel()

	if subjects := memberPeerSubjects(config.Config{}); subjects != nil {
		t.Fatalf("expected nil subjects without bootstrap config, got %v", subjects)
	}

	cfg := config.Config{
		Bootstrap: &config.ClusterBootstrapConfig{
			ExpectedMembers: []string{"alpha-1", "beta-1"},
		},
	}
	subjects := memberPeerSubjects(cfg)
	if len(subjects) != 2 {
		t.Fatalf("unexpected subject count: got %d, want %d", len(subjects), 2)
	}

	subjects[0] = "mutated"
	if cfg.Bootstrap.ExpectedMembers[0] != "alpha-1" {
		t.Fatal("expected member peer subjects to return a copy")
	}
}

func TestWithAPIServerTLSConfig(t *testing.T) {
	t.Parallel()

	expected := &tls.Config{MinVersion: tls.VersionTLS12}
	daemon := &Daemon{}

	WithAPIServerTLSConfig(expected)(daemon)

	if daemon.apiTLSConfig != expected {
		t.Fatal("expected api tls config option to store the provided config")
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

type waitErrorServer struct {
	waitErr error
}

func (server waitErrorServer) Start(context.Context, string) error {
	return nil
}

func (server waitErrorServer) Wait() error {
	return server.waitErr
}

func validWitnessMemberMTLSConfig() config.Config {
	return config.Config{
		APIVersion: config.APIVersionV1Alpha1,
		Kind:       config.KindNodeConfig,
		Node: config.NodeConfig{
			Name:           "alpha-1",
			Role:           cluster.NodeRoleWitness,
			APIAddress:     reserveLoopbackAddress(),
			ControlAddress: reserveLoopbackAddress(),
		},
		TLS: &config.TLSConfig{
			Enabled:  true,
			CAFile:   "tls/ca.crt",
			CertFile: "tls/server.crt",
			KeyFile:  "tls/server.key",
		},
		Security: &config.SecurityConfig{
			MemberMTLSEnabled: true,
		},
		Bootstrap: &config.ClusterBootstrapConfig{
			ClusterName:     "alpha",
			InitialPrimary:  "alpha-1",
			SeedAddresses:   []string{"127.0.0.1:9090"},
			ExpectedMembers: []string{"alpha-1", "beta-1"},
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

func waitForHTTPServer(t *testing.T, client *http.Client, rawURL string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(rawURL)
		if err == nil {
			response.Body.Close()
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("http server at %q did not become ready", rawURL)
}

func mustGET(t *testing.T, client *http.Client, rawURL, authorization string) *http.Response {
	t.Helper()

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("build GET request %q: %v", rawURL, err)
	}

	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("perform GET request %q: %v", rawURL, err)
	}

	return response
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

type staticControlPlaneStore struct {
	clusterStatus cluster.ClusterStatus
}

func (store staticControlPlaneStore) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (store staticControlPlaneStore) NodeStatus(string) (agentmodel.NodeStatus, bool) {
	return agentmodel.NodeStatus{}, false
}

func (store staticControlPlaneStore) NodeStatuses() []agentmodel.NodeStatus {
	return nil
}

func (store staticControlPlaneStore) ClusterSpec() (cluster.ClusterSpec, bool) {
	return cluster.ClusterSpec{}, false
}

func (store staticControlPlaneStore) ClusterStatus() (cluster.ClusterStatus, bool) {
	return store.clusterStatus.Clone(), true
}

func (store staticControlPlaneStore) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return cluster.MaintenanceModeStatus{}
}

func (store staticControlPlaneStore) UpdateMaintenanceMode(context.Context, cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	return cluster.MaintenanceModeStatus{}, errors.New("unsupported")
}

func (store staticControlPlaneStore) History() []cluster.HistoryEntry {
	return nil
}

func (store staticControlPlaneStore) CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error) {
	return controlplane.SwitchoverIntent{}, errors.New("unsupported")
}

func (store staticControlPlaneStore) CancelSwitchover(context.Context) (cluster.Operation, error) {
	return cluster.Operation{}, errors.New("unsupported")
}

func (store staticControlPlaneStore) CreateFailoverIntent(context.Context, controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error) {
	return controlplane.FailoverIntent{}, errors.New("unsupported")
}
