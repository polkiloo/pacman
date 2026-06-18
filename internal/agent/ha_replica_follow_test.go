package agent

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestDaemonReconcileReplicaFollowPrimary(t *testing.T) {
	t.Parallel()

	t.Run("reconfigures surviving replica to follow promoted primary timeline", func(t *testing.T) {
		t.Parallel()

		daemon, logs, tracePath, dataDir := newReplicaFollowTestDaemon(t)
		daemon.stateReader = replicaFollowReader()

		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))

		assertTraceLines(t, tracePath, []string{
			"stop -D " + dataDir + " -w -m fast",
			"status -D " + dataDir,
			"start -D " + dataDir + " -W",
		})
		assertContains(t, readTestFile(t, filepath.Join(dataDir, postgres.PostgresAutoConfFileName)), "primary_conninfo = 'host=alpha-2 port=5432 application_name=alpha-3 user=replicator password=replicator-secret'")
		assertContains(t, readTestFile(t, filepath.Join(dataDir, postgres.PostgresAutoConfFileName)), "recovery_target_timeline = 'latest'")
		if _, err := os.Stat(filepath.Join(dataDir, postgres.StandbySignalFileName)); err != nil {
			t.Fatalf("expected standby.signal to be written: %v", err)
		}
		assertContains(t, logs.String(), `"msg":"replica standby reconfigured to follow promoted primary"`)
	})

	t.Run("does not repeat successful reconfiguration for the same primary timeline", func(t *testing.T) {
		t.Parallel()

		daemon, _, tracePath, dataDir := newReplicaFollowTestDaemon(t)
		daemon.stateReader = replicaFollowReader()

		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))
		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))

		assertTraceLines(t, tracePath, []string{
			"stop -D " + dataDir + " -w -m fast",
			"status -D " + dataDir,
			"start -D " + dataDir + " -W",
		})
	})

	t.Run("leaves needs-rejoin members on the rejoin path", func(t *testing.T) {
		t.Parallel()

		daemon, _, tracePath, _ := newReplicaFollowTestDaemon(t)
		reader := replicaFollowReader()
		reader.clusterStatus.Members[1].NeedsRejoin = true
		reader.clusterStatus.Members[1].State = cluster.MemberStateNeedsRejoin
		daemon.stateReader = reader

		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))

		if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
			t.Fatalf("expected no pg_ctl restart trace, got err=%v", err)
		}
	})

	t.Run("skips reconfiguration when promoted primary address is unavailable", func(t *testing.T) {
		t.Parallel()

		daemon, _, tracePath, _ := newReplicaFollowTestDaemon(t)
		reader := replicaFollowReader()
		reader.status.Postgres.Address = ""
		reader.clusterStatus.Members[0].Host = ""
		daemon.stateReader = reader

		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))

		if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
			t.Fatalf("expected no pg_ctl restart trace, got err=%v", err)
		}
	})

	t.Run("logs standby configuration failures without restart", func(t *testing.T) {
		t.Parallel()

		daemon, logs, tracePath, dataDir := newReplicaFollowTestDaemon(t)
		daemon.config.Postgres.DataDir = filepath.Join(dataDir, "missing", "postgres")
		daemon.stateReader = replicaFollowReader()

		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))

		if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
			t.Fatalf("expected no pg_ctl restart trace, got err=%v", err)
		}
		assertContains(t, logs.String(), `"msg":"replica standby reconfiguration failed"`)
	})

	t.Run("logs restart failures after standby reconfiguration", func(t *testing.T) {
		t.Parallel()

		binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
state="${trace}.state"
printf '%%s\n' "$*" >> "$trace"
case "$1" in
  stop)
    printf 'stopped\n' > "$state"
    exit 0
    ;;
  status)
    exit 3
    ;;
  start)
    exit 1
    ;;
esac
exit 0
`)
		dataDir := t.TempDir()
		var logs bytes.Buffer
		daemon := &Daemon{
			config: config.Config{
				Node: config.NodeConfig{
					Name: "alpha-3",
					Role: cluster.NodeRoleData,
				},
				Postgres: &config.PostgresLocalConfig{
					BinDir:              binDir,
					DataDir:             dataDir,
					Port:                5432,
					ReplicationUser:     "replicator",
					ReplicationPassword: "replicator-secret",
				},
			},
			logger:      logging.New("pacmand", &logs),
			pgCtl:       &postgres.PGCtl{BinDir: binDir, DataDir: dataDir},
			stateReader: replicaFollowReader(),
		}

		daemon.reconcileReplicaFollowPrimary(context.Background(), replicaFollowPostgresStatus(1))

		assertTraceLines(t, tracePath, []string{
			"stop -D " + dataDir + " -w -m fast",
			"status -D " + dataDir,
			"start -D " + dataDir + " -W",
		})
		assertContains(t, logs.String(), `"msg":"replica restart after primary follow reconfiguration failed"`)
	})
}

func TestReplicaFollowHelpers(t *testing.T) {
	t.Parallel()

	t.Run("member lookup reports misses", func(t *testing.T) {
		t.Parallel()

		if _, ok := memberByName([]cluster.MemberStatus{{Name: "alpha-1"}}, "alpha-2"); ok {
			t.Fatal("expected missing member lookup to report false")
		}
	})

	t.Run("primary address falls back through node address and default ports", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			primary     cluster.MemberStatus
			primaryNode agentmodel.NodeStatus
			fallback    int
			want        string
		}{
			{
				name:    "node address host when member host is empty",
				primary: cluster.MemberStatus{},
				primaryNode: agentmodel.NodeStatus{
					Postgres: agentmodel.PostgresStatus{Address: "10.0.0.2:5544"},
				},
				want: "10.0.0.2:5544",
			},
			{
				name: "configured fallback port when primary node has no postgres port",
				primary: cluster.MemberStatus{
					Host: "alpha-2",
				},
				fallback: 5544,
				want:     "alpha-2:5544",
			},
			{
				name: "default postgres port when no observed or configured port exists",
				primary: cluster.MemberStatus{
					Host: "alpha-2",
				},
				want: "alpha-2:5432",
			},
			{
				name:    "empty when no host is known",
				primary: cluster.MemberStatus{},
				want:    "",
			},
		}

		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				got := replicaFollowPrimaryAddress(testCase.primary, testCase.primaryNode, testCase.fallback)
				if got != testCase.want {
					t.Fatalf("unexpected primary address: got %q want %q", got, testCase.want)
				}
			})
		}
	})

	t.Run("split host port rejects malformed addresses", func(t *testing.T) {
		t.Parallel()

		for _, address := range []string{"alpha-2", "alpha-2:not-a-port"} {
			host, port := splitHostPort(address)
			if host != "" || port != 0 {
				t.Fatalf("expected malformed address %q to be rejected, got host=%q port=%d", address, host, port)
			}
		}
	})

	t.Run("standby config rejects malformed primary addresses", func(t *testing.T) {
		t.Parallel()

		if got := controlPlaneStandbyConfig("alpha-2", "alpha-3"); got.Standby != (postgres.StandbyConfig{}) {
			t.Fatalf("expected malformed primary address to produce zero request, got %+v", got)
		}
	})

	t.Run("restart returns stop errors", func(t *testing.T) {
		t.Parallel()

		binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
case "$1" in
  stop)
    exit 1
    ;;
esac
exit 0
`)
		dataDir := t.TempDir()

		err := restartReplicaPostgres(context.Background(), &postgres.PGCtl{BinDir: binDir, DataDir: dataDir})
		if err == nil || !strings.Contains(err.Error(), "stop postgres via pg_ctl") {
			t.Fatalf("expected stop error, got %v", err)
		}
		assertTraceLines(t, tracePath, []string{
			"stop -D " + dataDir + " -w -m fast",
		})
	})
}

func newReplicaFollowTestDaemon(t *testing.T) (*Daemon, *bytes.Buffer, string, string) {
	t.Helper()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
state="${trace}.state"
printf '%%s\n' "$*" >> "$trace"
case "$1" in
  stop)
    printf 'stopped\n' > "$state"
    exit 0
    ;;
  status)
    if [ "$(cat "$state" 2>/dev/null)" = "stopped" ]; then
      exit 3
    fi
    exit 0
    ;;
  start)
    printf 'running\n' > "$state"
    exit 0
    ;;
esac
exit 0
`)
	dataDir := t.TempDir()
	var logs bytes.Buffer
	daemon := &Daemon{
		config: config.Config{
			Node: config.NodeConfig{
				Name: "alpha-3",
				Role: cluster.NodeRoleData,
			},
			Postgres: &config.PostgresLocalConfig{
				BinDir:              binDir,
				DataDir:             dataDir,
				Port:                5432,
				ReplicationUser:     "replicator",
				ReplicationPassword: "replicator-secret",
			},
		},
		logger: logging.New("pacmand", &logs),
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: dataDir,
		},
	}

	return daemon, &logs, tracePath, dataDir
}

func replicaFollowReader() stubNodeStatusReader {
	return stubNodeStatusReader{
		status: agentmodel.NodeStatus{
			Postgres: agentmodel.PostgresStatus{
				Address: "127.0.0.1:5432",
			},
		},
		ok: true,
		clusterStatus: failoverTestClusterStatus("alpha-2", nil,
			cluster.MemberStatus{
				Name:     "alpha-2",
				Host:     "alpha-2",
				Role:     cluster.MemberRolePrimary,
				State:    cluster.MemberStateRunning,
				Healthy:  true,
				Timeline: 2,
			},
			cluster.MemberStatus{
				Name:     "alpha-3",
				Role:     cluster.MemberRoleReplica,
				State:    cluster.MemberStateStreaming,
				Healthy:  true,
				Timeline: 1,
			},
		),
		clusterStatusOK: true,
	}
}

func replicaFollowPostgresStatus(timeline int64) agentmodel.PostgresStatus {
	return agentmodel.PostgresStatus{
		Managed:       true,
		Up:            true,
		Role:          cluster.MemberRoleReplica,
		RecoveryKnown: true,
		InRecovery:    true,
		Details: agentmodel.PostgresDetails{
			Timeline: timeline,
		},
	}
}
