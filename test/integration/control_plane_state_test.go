//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/test/testenv"
)

func TestControlPlaneAggregatesSharedDaemonStateWithRealPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)
	postgresFixture := env.StartPostgres(t, "alpha-1", "alpha-1-postgres")

	t.Setenv("PGDATABASE", postgresFixture.Database())
	t.Setenv("PGUSER", postgresFixture.Username())
	t.Setenv("PGPASSWORD", postgresFixture.Password())
	t.Setenv("PGSSLMODE", "disable")

	store := controlplane.NewMemoryStateStore()
	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{
				Name:     "alpha-1",
				Priority: 100,
				Tags: map[string]any{
					"zone": "a",
				},
			},
			{
				Name:       "witness-1",
				Priority:   10,
				NoFailover: true,
				Tags: map[string]any{
					"placement": "arbiter",
				},
			},
		},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	dataLogs := &bytes.Buffer{}
	witnessLogs := &bytes.Buffer{}

	dataDaemon, err := agent.NewDaemon(
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name:           "alpha-1",
				Role:           cluster.NodeRoleData,
				APIAddress:     "10.0.0.10:8080",
				ControlAddress: "10.0.0.10:9090",
			},
			Postgres: &config.PostgresLocalConfig{
				DataDir:       "/var/lib/postgresql/data",
				ListenAddress: postgresFixture.Host(t),
				Port:          postgresFixture.Port(t),
			},
		},
		logging.New("pacmand", dataLogs),
		agent.WithControlPlanePublisher(store),
		agent.WithNoAPIServer(),
	)
	if err != nil {
		t.Fatalf("new data daemon: %v", err)
	}

	witnessDaemon, err := agent.NewDaemon(
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name:           "witness-1",
				Role:           cluster.NodeRoleWitness,
				APIAddress:     "10.0.0.20:8080",
				ControlAddress: "10.0.0.20:9090",
			},
		},
		logging.New("pacmand", witnessLogs),
		agent.WithControlPlanePublisher(store),
		agent.WithNoAPIServer(),
	)
	if err != nil {
		t.Fatalf("new witness daemon: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		dataDaemon.Wait()
		witnessDaemon.Wait()
	})

	if err := dataDaemon.Start(ctx); err != nil {
		t.Fatalf("start data daemon: %v", err)
	}

	if err := witnessDaemon.Start(ctx); err != nil {
		t.Fatalf("start witness daemon: %v", err)
	}

	clusterStatus := waitForClusterStatus(t, store, func(status cluster.ClusterStatus) bool {
		return status.Phase == cluster.ClusterPhaseHealthy &&
			status.CurrentPrimary == "alpha-1" &&
			len(status.Members) == 2
	})

	lease, ok := store.Leader()
	if !ok {
		t.Fatal("expected elected control-plane leader")
	}

	if lease.LeaderNode != "alpha-1" || lease.Term != 1 {
		t.Fatalf("unexpected leader lease: %+v", lease)
	}

	registrations := store.RegisteredMembers()
	if len(registrations) != 2 {
		t.Fatalf("expected 2 member registrations, got %+v", registrations)
	}

	alphaStatus, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected alpha-1 node status")
	}

	if !alphaStatus.Postgres.Managed || !alphaStatus.Postgres.Up {
		t.Fatalf("expected reachable managed postgres, got %+v", alphaStatus.Postgres)
	}

	if alphaStatus.Role != cluster.MemberRolePrimary || alphaStatus.State != cluster.MemberStateRunning {
		t.Fatalf("unexpected alpha-1 published role/state: %+v", alphaStatus)
	}

	if alphaStatus.Postgres.Details.SystemIdentifier == "" || alphaStatus.Postgres.Details.ServerVersion < 170000 {
		t.Fatalf("expected real postgres details in alpha-1 status, got %+v", alphaStatus.Postgres.Details)
	}

	if alphaStatus.Postgres.Details.Timeline != 1 {
		t.Fatalf("unexpected alpha-1 timeline: got %d", alphaStatus.Postgres.Details.Timeline)
	}

	if alphaStatus.Postgres.WAL.WriteLSN == "" || alphaStatus.Postgres.WAL.FlushLSN == "" {
		t.Fatalf("expected primary wal positions in alpha-1 status, got %+v", alphaStatus.Postgres.WAL)
	}

	if !alphaStatus.ControlPlane.ClusterReachable || !alphaStatus.ControlPlane.Leader || alphaStatus.ControlPlane.PublishError != "" {
		t.Fatalf("expected successful leader publication for alpha-1, got %+v", alphaStatus.ControlPlane)
	}

	witnessStatus, ok := store.NodeStatus("witness-1")
	if !ok {
		t.Fatal("expected witness-1 node status")
	}

	if witnessStatus.Role != cluster.MemberRoleWitness || witnessStatus.State != cluster.MemberStateRunning {
		t.Fatalf("unexpected witness published role/state: %+v", witnessStatus)
	}

	if witnessStatus.Postgres.Managed || witnessStatus.Postgres.Up {
		t.Fatalf("expected unmanaged witness postgres status, got %+v", witnessStatus.Postgres)
	}

	if witnessStatus.ControlPlane.Leader {
		t.Fatalf("expected witness not to hold control-plane leadership, got %+v", witnessStatus.ControlPlane)
	}

	alphaMember, ok := store.Member("alpha-1")
	if !ok {
		t.Fatal("expected alpha-1 member view")
	}

	if alphaMember.APIURL != "http://10.0.0.10:8080" || alphaMember.Host != "10.0.0.10" || alphaMember.Port != 8080 {
		t.Fatalf("unexpected registered alpha-1 endpoint projection: %+v", alphaMember)
	}

	if !alphaMember.Healthy || !alphaMember.Leader || alphaMember.Priority != 100 || alphaMember.NoFailover {
		t.Fatalf("unexpected effective alpha-1 member policy: %+v", alphaMember)
	}

	if alphaMember.Tags["zone"] != "a" {
		t.Fatalf("expected desired alpha-1 tags in member view, got %+v", alphaMember.Tags)
	}

	witnessMember, ok := store.Member("witness-1")
	if !ok {
		t.Fatal("expected witness-1 member view")
	}

	if witnessMember.Role != cluster.MemberRoleWitness || !witnessMember.Healthy {
		t.Fatalf("unexpected witness member state: %+v", witnessMember)
	}

	if !witnessMember.NoFailover || witnessMember.Priority != 10 {
		t.Fatalf("expected witness failover policy projection, got %+v", witnessMember)
	}

	if witnessMember.Tags["placement"] != "arbiter" {
		t.Fatalf("expected desired witness tags in member view, got %+v", witnessMember.Tags)
	}

	if clusterStatus.CurrentPrimary != "alpha-1" || clusterStatus.Phase != cluster.ClusterPhaseHealthy {
		t.Fatalf("unexpected cluster status: %+v", clusterStatus)
	}

	if clusterStatus.Members[0].Name != "alpha-1" || clusterStatus.Members[1].Name != "witness-1" {
		t.Fatalf("expected sorted cluster members, got %+v", clusterStatus.Members)
	}

	truth, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile source of truth: %v", err)
	}

	if err := truth.Validate(); err != nil {
		t.Fatalf("validate reconciled source of truth: %v", err)
	}

	if truth.Observed == nil || truth.Observed.CurrentPrimary != "alpha-1" {
		t.Fatalf("expected observed source of truth for alpha-1 primary, got %+v", truth)
	}
}

func waitForClusterStatus(t *testing.T, store *controlplane.MemoryStateStore, predicate func(cluster.ClusterStatus) bool) cluster.ClusterStatus {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, ok := store.ClusterStatus()
		if ok && predicate(status) {
			return status
		}

		time.Sleep(50 * time.Millisecond)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("cluster status did not become available")
	}

	t.Fatalf("cluster status condition was not met, last status: %+v", status)
	return cluster.ClusterStatus{}
}
