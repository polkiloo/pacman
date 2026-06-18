package agent

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestLocalDemoterDemoteUsesFastShutdown(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
exit 0
`)

	demoter := &localDemoter{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := demoter.Demote(context.Background(), controlplane.DemotionRequest{}); err != nil {
		t.Fatalf("demote local primary: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"stop -D /var/lib/postgresql/data -w -m fast",
	})
}

func TestAPIPromoterPromote(t *testing.T) {
	t.Parallel()

	t.Run("returns error when candidate is not registered", func(t *testing.T) {
		t.Parallel()

		promoter := &apiPromoter{
			client:    http.DefaultClient,
			discovery: testMemberDiscovery{},
		}

		err := promoter.Promote(context.Background(), controlplane.PromotionRequest{Candidate: "alpha-2"})
		if err == nil || !strings.Contains(err.Error(), `candidate "alpha-2" has no registered API address`) {
			t.Fatalf("unexpected missing member error: %v", err)
		}
	})

	t.Run("posts promote request with admin token", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				t.Fatalf("unexpected promote method: %s", request.Method)
			}

			if request.URL.Path != "/api/v1/promote" {
				t.Fatalf("unexpected promote path: %s", request.URL.Path)
			}

			if got := request.Header.Get("Authorization"); got != "Bearer secret-token" {
				t.Fatalf("unexpected authorization header: %q", got)
			}

			writer.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("parse httptest server URL: %v", err)
		}

		promoter := &apiPromoter{
			client:     server.Client(),
			adminToken: "secret-token",
			discovery: testMemberDiscovery{
				registration: controlplane.MemberRegistration{
					NodeName:   "alpha-2",
					APIAddress: serverURL.Host,
				},
				ok: true,
			},
		}

		if err := promoter.Promote(context.Background(), controlplane.PromotionRequest{Candidate: "alpha-2"}); err != nil {
			t.Fatalf("promote candidate over API: %v", err)
		}
	})

	t.Run("returns non-200 response as error", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		if err != nil {
			t.Fatalf("parse httptest server URL: %v", err)
		}

		promoter := &apiPromoter{
			client: server.Client(),
			discovery: testMemberDiscovery{
				registration: controlplane.MemberRegistration{
					NodeName:   "alpha-2",
					APIAddress: serverURL.Host,
				},
				ok: true,
			},
		}

		err = promoter.Promote(context.Background(), controlplane.PromotionRequest{Candidate: "alpha-2"})
		if err == nil || !strings.Contains(err.Error(), "returned status 503") {
			t.Fatalf("unexpected non-200 promote error: %v", err)
		}
	})
}

func TestPGCtlLocalPromoterPromoteLocalUsesPgCtlPromote(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
exit 0
`)

	promoter := &pgCtlLocalPromoter{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := promoter.PromoteLocal(context.Background()); err != nil {
		t.Fatalf("promote local postgres: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"promote -D /var/lib/postgresql/data -w",
	})
}

func TestPGCtlLocalPromoterPromoteUsesPgCtlPromote(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
exit 0
`)

	promoter := &pgCtlLocalPromoter{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := promoter.Promote(context.Background(), controlplane.PromotionRequest{Candidate: "alpha-2"}); err != nil {
		t.Fatalf("promote local postgres: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"promote -D /var/lib/postgresql/data -w",
	})
}

func TestMemberAPIURL(t *testing.T) {
	t.Parallel()

	if got := memberAPIURL(""); got != "" {
		t.Fatalf("empty member API URL: got %q", got)
	}

	if got := memberAPIURL("10.0.0.10:8080"); got != "http://10.0.0.10:8080" {
		t.Fatalf("unexpected member API URL: got %q", got)
	}
}

func TestDaemonReconcileFailover(t *testing.T) {
	t.Parallel()

	t.Run("returns when cluster status is unavailable", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{}
		daemon, _ := newFailoverTestDaemon(t, publisher, stubNodeStatusReader{})

		daemon.reconcileFailover(context.Background())

		if publisher.createCalls != 0 || publisher.executeCalls != 0 {
			t.Fatalf("expected failover reconciliation to be skipped, got create=%d execute=%d", publisher.createCalls, publisher.executeCalls)
		}
	})

	t.Run("returns when local member is not an eligible healthy replica", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", nil,
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, true, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, _ := newFailoverTestDaemon(t, publisher, reader)
		daemon.config.Node.Name = "alpha-1"

		daemon.reconcileFailover(context.Background())

		if publisher.createCalls != 0 || publisher.executeCalls != 0 {
			t.Fatalf("expected healthy primary to skip failover reconciliation, got create=%d execute=%d", publisher.createCalls, publisher.executeCalls)
		}
	})

	t.Run("creates automatic failover intent for healthy replica after primary failure", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", nil,
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, false, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, _ := newFailoverTestDaemon(t, publisher, reader)

		daemon.reconcileFailover(context.Background())

		if publisher.createCalls != 1 {
			t.Fatalf("expected one failover intent creation attempt, got %d", publisher.createCalls)
		}

		if publisher.executeCalls != 0 {
			t.Fatalf("expected no immediate failover execution after intent creation, got %d", publisher.executeCalls)
		}

		if publisher.createRequest.RequestedBy != automaticFailoverRequestedBy || publisher.createRequest.Reason != automaticFailoverReason {
			t.Fatalf("unexpected automatic failover request metadata: %+v", publisher.createRequest)
		}
	})

	t.Run("suppresses expected intent creation errors", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{createErr: controlplane.ErrFailoverNoEligibleCandidates}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", nil,
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, false, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, logs := newFailoverTestDaemon(t, publisher, reader)

		daemon.reconcileFailover(context.Background())

		if publisher.createCalls != 1 {
			t.Fatalf("expected one failover intent creation attempt, got %d", publisher.createCalls)
		}

		if logs.Len() != 0 {
			t.Fatalf("expected gated failover creation to be suppressed, got logs %q", logs.String())
		}
	})

	t.Run("logs unexpected intent creation error", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{createErr: errors.New("dcs write failed")}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", nil,
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, false, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, logs := newFailoverTestDaemon(t, publisher, reader)

		daemon.reconcileFailover(context.Background())

		assertContains(t, logs.String(), `"msg":"failover intent creation failed"`)
		assertContains(t, logs.String(), `"error":"dcs write failed"`)
	})

	t.Run("executes accepted failover only on selected candidate", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", &cluster.Operation{
				ID:         "failover-1",
				Kind:       cluster.OperationKindFailover,
				State:      cluster.OperationStateAccepted,
				FromMember: "alpha-1",
				ToMember:   "alpha-2",
			},
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, false, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, _ := newFailoverTestDaemon(t, publisher, reader)

		daemon.reconcileFailover(context.Background())

		if publisher.executeCalls != 1 {
			t.Fatalf("expected candidate node to execute failover, got %d calls", publisher.executeCalls)
		}

		publisher.executeCalls = 0
		daemon.config.Node.Name = "alpha-3"
		daemon.reconcileFailover(context.Background())
		if publisher.executeCalls != 0 {
			t.Fatalf("expected non-candidate node to skip failover execution, got %d calls", publisher.executeCalls)
		}
	})

	t.Run("logs unexpected execution error", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{executeErr: errors.New("promotion failed")}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", &cluster.Operation{
				ID:         "failover-1",
				Kind:       cluster.OperationKindFailover,
				State:      cluster.OperationStateAccepted,
				FromMember: "alpha-1",
				ToMember:   "alpha-2",
			},
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, false, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, logs := newFailoverTestDaemon(t, publisher, reader)

		daemon.reconcileFailover(context.Background())

		assertContains(t, logs.String(), `"msg":"failover execution failed"`)
		assertContains(t, logs.String(), `"error":"promotion failed"`)
	})

	t.Run("logs successful execution", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingFailoverPublisher{
			execution: controlplane.FailoverExecution{
				CurrentPrimary: "alpha-1",
				Candidate:      "alpha-2",
				CurrentEpoch:   9,
			},
		}
		reader := stubNodeStatusReader{
			clusterStatus: failoverTestClusterStatus("alpha-1", &cluster.Operation{
				ID:         "failover-1",
				Kind:       cluster.OperationKindFailover,
				State:      cluster.OperationStateRunning,
				FromMember: "alpha-1",
				ToMember:   "alpha-2",
			},
				failoverTestMember("alpha-1", cluster.MemberRolePrimary, false, false),
				failoverTestMember("alpha-2", cluster.MemberRoleReplica, true, false),
			),
			clusterStatusOK: true,
		}
		daemon, logs := newFailoverTestDaemon(t, publisher, reader)

		daemon.reconcileFailover(context.Background())

		if publisher.executeCalls != 1 {
			t.Fatalf("expected one failover execution attempt, got %d", publisher.executeCalls)
		}

		assertContains(t, logs.String(), `"msg":"failover executed"`)
		assertContains(t, logs.String(), `"from_primary":"alpha-1"`)
		assertContains(t, logs.String(), `"to_candidate":"alpha-2"`)
		assertContains(t, logs.String(), `"epoch":"9"`)
	})
}

func TestBuildNodeStatusPreservesLastKnownIdentityAcrossManagedPostgresProbeLoss(t *testing.T) {
	t.Parallel()

	daemon, _ := newFailoverTestDaemon(t, &recordingFailoverPublisher{}, stubNodeStatusReader{})
	daemon.nodeStatus = agentmodel.NodeStatus{
		NodeName:   "alpha-2",
		MemberName: "alpha-2",
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Role:    cluster.MemberRolePrimary,
			Details: agentmodel.PostgresDetails{
				SystemIdentifier: "sys-alpha",
				Timeline:         11,
			},
			WAL: agentmodel.WALProgress{
				WriteLSN: "0/B000148",
			},
		},
	}

	status := daemon.buildNodeStatus(clusterTime("2026-01-02T03:04:06Z"), agentmodel.PostgresStatus{
		Managed:   true,
		Up:        false,
		Role:      cluster.MemberRoleUnknown,
		CheckedAt: clusterTime("2026-01-02T03:04:06Z"),
	})

	if status.Role != cluster.MemberRolePrimary || status.State != cluster.MemberStateFailed {
		t.Fatalf("expected failed heartbeat to preserve primary identity, got %+v", status)
	}

	if status.Postgres.Role != cluster.MemberRolePrimary || status.Postgres.Details.SystemIdentifier != "sys-alpha" || status.Postgres.Details.Timeline != 11 {
		t.Fatalf("expected failed heartbeat to preserve postgres identity, got %+v", status.Postgres)
	}

	if status.Postgres.WAL.WriteLSN != "0/B000148" {
		t.Fatalf("expected failed heartbeat to preserve WAL identity, got %+v", status.Postgres.WAL)
	}

	probeLoss := daemon.buildNodeStatus(clusterTime("2026-01-02T03:04:07Z"), agentmodel.PostgresStatus{
		Managed:   true,
		Up:        true,
		Role:      cluster.MemberRoleUnknown,
		CheckedAt: clusterTime("2026-01-02T03:04:07Z"),
		Errors: agentmodel.PostgresErrors{
			State: "pq: the database system is shutting down",
		},
	})

	if probeLoss.Role != cluster.MemberRolePrimary || probeLoss.State != cluster.MemberStateUnknown {
		t.Fatalf("expected probe-loss heartbeat to preserve primary identity, got %+v", probeLoss)
	}

	if probeLoss.Postgres.Role != cluster.MemberRolePrimary || probeLoss.Postgres.Details.SystemIdentifier != "sys-alpha" || probeLoss.Postgres.Details.Timeline != 11 {
		t.Fatalf("expected probe-loss heartbeat to preserve postgres identity, got %+v", probeLoss.Postgres)
	}

	if probeLoss.Postgres.WAL.WriteLSN != "0/B000148" {
		t.Fatalf("expected probe-loss heartbeat to preserve WAL identity, got %+v", probeLoss.Postgres.WAL)
	}
}

func TestDaemonReconcileSwitchover(t *testing.T) {
	t.Parallel()

	t.Run("returns when local node is not primary", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingSwitchoverPublisher{}
		daemon, _ := newSwitchoverTestDaemon(t, publisher)
		daemon.heartbeat = agentmodel.Heartbeat{
			Postgres: agentmodel.PostgresStatus{Role: cluster.MemberRoleReplica},
		}

		daemon.reconcileSwitchover(context.Background())

		if publisher.executeCalls != 0 {
			t.Fatalf("expected switchover execution to be skipped, got %d calls", publisher.executeCalls)
		}
	})

	t.Run("suppresses missing intent error", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingSwitchoverPublisher{executeErr: controlplane.ErrSwitchoverIntentRequired}
		daemon, logs := newSwitchoverTestDaemon(t, publisher)
		daemon.heartbeat = agentmodel.Heartbeat{
			Postgres: agentmodel.PostgresStatus{Role: cluster.MemberRolePrimary},
		}

		daemon.reconcileSwitchover(context.Background())

		if publisher.executeCalls != 1 {
			t.Fatalf("expected one switchover execution attempt, got %d", publisher.executeCalls)
		}

		if logs.Len() != 0 {
			t.Fatalf("expected not-ready switchover error to be suppressed, got logs %q", logs.String())
		}
	})

	t.Run("logs unexpected execution error", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingSwitchoverPublisher{executeErr: errors.New("demotion failed")}
		daemon, logs := newSwitchoverTestDaemon(t, publisher)
		daemon.heartbeat = agentmodel.Heartbeat{
			Postgres: agentmodel.PostgresStatus{Role: cluster.MemberRolePrimary},
		}

		daemon.reconcileSwitchover(context.Background())

		assertContains(t, logs.String(), `"msg":"switchover execution failed"`)
		assertContains(t, logs.String(), `"error":"demotion failed"`)
	})

	t.Run("logs successful execution", func(t *testing.T) {
		t.Parallel()

		publisher := &recordingSwitchoverPublisher{
			execution: controlplane.SwitchoverExecution{
				CurrentPrimary: "alpha-1",
				Candidate:      "alpha-2",
				CurrentEpoch:   8,
			},
		}
		daemon, logs := newSwitchoverTestDaemon(t, publisher)
		daemon.heartbeat = agentmodel.Heartbeat{
			Postgres: agentmodel.PostgresStatus{Role: cluster.MemberRolePrimary},
		}

		daemon.reconcileSwitchover(context.Background())

		if publisher.executeCalls != 1 {
			t.Fatalf("expected one switchover execution attempt, got %d", publisher.executeCalls)
		}

		assertContains(t, logs.String(), `"msg":"switchover executed"`)
		assertContains(t, logs.String(), `"from_primary":"alpha-1"`)
		assertContains(t, logs.String(), `"to_candidate":"alpha-2"`)
		assertContains(t, logs.String(), `"epoch":"8"`)
	})
}

func TestPGRewindRewinderRewind(t *testing.T) {
	t.Parallel()

	t.Run("rejects empty source server", func(t *testing.T) {
		t.Parallel()

		rewinder := &pgRewindRewinder{
			binDir:  t.TempDir(),
			dataDir: "/var/lib/postgresql/data",
		}

		err := rewinder.Rewind(context.Background(), controlplane.RewindRequest{})
		if err == nil || !strings.Contains(err.Error(), "current primary source server is empty") {
			t.Fatalf("unexpected empty source server error: %v", err)
		}
	})

	t.Run("runs pg_rewind against the local data directory", func(t *testing.T) {
		t.Parallel()

		binDir, tracePath := writeTracingBinary(t, "pg_rewind", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
exit 0
`)

		rewinder := &pgRewindRewinder{
			binDir:  binDir,
			dataDir: "/var/lib/postgresql/data",
		}

		err := rewinder.Rewind(context.Background(), controlplane.RewindRequest{
			SourceServer: "host=alpha-2 port=5432",
		})
		if err != nil {
			t.Fatalf("run pg_rewind: %v", err)
		}

		assertTraceLines(t, tracePath, []string{
			"--target-pgdata /var/lib/postgresql/data --source-server host=alpha-2 port=5432 --progress",
		})
	})
}

func TestPGCtlStandbyRestarterRestartAsStandbyStopsRunningPostgresThenStartsWithoutWaiting(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  if [ -f "$trace.stopped" ]; then
    exit 3
  fi
  exit 0
fi
if [ "$1" = "stop" ]; then
  touch "$trace.stopped"
  exit 0
fi
exit 0
`)

	restarter := &pgCtlStandbyRestarter{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := restarter.RestartAsStandby(context.Background(), controlplane.StandbyRestartRequest{}); err != nil {
		t.Fatalf("restart standby without waiting: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
		"stop -D /var/lib/postgresql/data -w -m fast",
		"status -D /var/lib/postgresql/data",
		"start -D /var/lib/postgresql/data -W",
	})
}

func TestPGCtlStandbyRestarterRestartAsStandbyStartsStoppedPostgresWithoutStop(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  exit 3
fi
exit 0
`)

	restarter := &pgCtlStandbyRestarter{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := restarter.RestartAsStandby(context.Background(), controlplane.StandbyRestartRequest{}); err != nil {
		t.Fatalf("restart standby without waiting: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
		"status -D /var/lib/postgresql/data",
		"start -D /var/lib/postgresql/data -W",
	})
}

func TestPGCtlStandbyRestarterRestartAsStandbyDoesNotStartAfterStopFailure(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  exit 0
fi
if [ "$1" = "stop" ]; then
  echo stop failed
  exit 1
fi
exit 0
`)

	restarter := &pgCtlStandbyRestarter{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := restarter.RestartAsStandby(context.Background(), controlplane.StandbyRestartRequest{}); err == nil {
		t.Fatalf("expected stop failure")
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
		"stop -D /var/lib/postgresql/data -w -m fast",
	})
}

func TestPGCtlReinitStopperStopsRunningPostgres(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  exit 0
fi
if [ "$1" = "stop" ]; then
  exit 0
fi
exit 1
`)

	stopper := &pgCtlReinitStopper{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := stopper.StopPostgres(context.Background(), controlplane.ReinitPostgresStopRequest{}); err != nil {
		t.Fatalf("stop postgres for reinit: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
		"stop -D /var/lib/postgresql/data -w -m fast",
	})
}

func TestPGCtlReinitStopperSkipsStoppedPostgres(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "pg_ctl", `#!/bin/sh
trace=%q
printf '%%s\n' "$*" >> "$trace"
if [ "$1" = "status" ]; then
  exit 3
fi
exit 1
`)

	stopper := &pgCtlReinitStopper{
		pgCtl: &postgres.PGCtl{
			BinDir:  binDir,
			DataDir: "/var/lib/postgresql/data",
		},
	}

	if err := stopper.StopPostgres(context.Background(), controlplane.ReinitPostgresStopRequest{}); err != nil {
		t.Fatalf("stop postgres for reinit: %v", err)
	}

	assertTraceLines(t, tracePath, []string{
		"status -D /var/lib/postgresql/data",
	})
}

func TestDaemonAdvanceRejoinFinalPhases(t *testing.T) {
	t.Parallel()

	t.Run("logs completed rejoin", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{}
		daemon, logs := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.advanceRejoinFinalPhases(context.Background(), engine, "alpha-1")

		if engine.completeCalls != 1 {
			t.Fatalf("expected rejoin completion to be attempted once, got %d", engine.completeCalls)
		}

		if engine.verifyCalls != 0 {
			t.Fatalf("expected no replication verification after successful completion, got %d", engine.verifyCalls)
		}

		assertContains(t, logs.String(), `"msg":"rejoin completed"`)
	})

	t.Run("logs unexpected replication verification failure", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			completeErr: controlplane.ErrRejoinExecutionRequired,
			verifyErr:   errors.New("replication probe failed"),
		}
		daemon, logs := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.advanceRejoinFinalPhases(context.Background(), engine, "alpha-1")

		if engine.completeCalls != 1 || engine.verifyCalls != 1 {
			t.Fatalf("unexpected rejoin final-phase calls: complete=%d verify=%d", engine.completeCalls, engine.verifyCalls)
		}

		assertContains(t, logs.String(), `"msg":"rejoin replication verification failed"`)
		assertContains(t, logs.String(), `"error":"replication probe failed"`)
	})
}

func TestLocalReinitDataDirArchiverArchivesWithOperationID(t *testing.T) {
	t.Parallel()

	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	archiver := &localReinitDataDirArchiver{dataDir: dataDir}
	result, err := archiver.ArchiveDataDir(context.Background(), controlplane.ReinitDataDirArchiveRequest{
		Operation: cluster.Operation{ID: "reinit-20260617T120000Z"},
	})
	if err != nil {
		t.Fatalf("archive data dir: %v", err)
	}

	if !result.Archived || !strings.Contains(result.ArchivePath, "reinit-20260617T120000Z") {
		t.Fatalf("unexpected archive result: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(result.ArchivePath, "PG_VERSION")); err != nil {
		t.Fatalf("expected marker in archive: %v", err)
	}
}

func TestLocalReinitWALGRestorerRunsBackupFetchWithConfiguredEnvironment(t *testing.T) {
	t.Parallel()

	binDir, tracePath := writeTracingBinary(t, "wal-g", `#!/bin/sh
trace=%q
printf 'args=%%s\n' "$*" >> "$trace"
printf 'prefix=%%s\n' "$WALG_FILE_PREFIX" >> "$trace"
mkdir -p "$2"
printf '17\n' > "$2/PG_VERSION"
exit 0
`)

	dataDir := filepath.Join(t.TempDir(), "restore")
	restorer := &localReinitWALGRestorer{
		dataDir: dataDir,
		walg: config.WALGConfig{
			Binary: filepath.Join(binDir, "wal-g"),
			Repository: config.WALGRepositoryConfig{
				Provider: config.WALGRepositoryProviderFilesystem,
				Prefix:   "/backups/alpha",
			},
			Restore: config.WALGRestoreConfig{BackupName: "base_000000010000000000000005"},
		},
	}

	result, err := restorer.RestoreFromWALG(context.Background(), controlplane.ReinitWALGRestoreRequest{})
	if err != nil {
		t.Fatalf("restore from WAL-G: %v", err)
	}

	if result.DataDir != dataDir || result.BackupName != "base_000000010000000000000005" {
		t.Fatalf("unexpected WAL-G restore result: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "PG_VERSION")); err != nil {
		t.Fatalf("expected restored data dir marker: %v", err)
	}
	assertTraceLines(t, tracePath, []string{
		"args=backup-fetch " + dataDir + " base_000000010000000000000005",
		"prefix=/backups/alpha",
	})
}

func TestLocalReinitRecoveryConfiguratorWritesWALGRestoreCommand(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, postgres.PostgresAutoConfFileName), []byte(strings.Join([]string{
		"listen_addresses = '*'",
		"restore_command = 'old restore'",
		"primary_conninfo = 'old primary'",
		"",
	}, "\n")), 0o640); err != nil {
		t.Fatalf("write existing auto conf: %v", err)
	}

	configurator := &localReinitRecoveryConfigurator{
		dataDir:             dataDir,
		replicationUser:     "replicator",
		replicationPassword: "replicator secret",
		walg: config.WALGConfig{
			Binary: "/usr/local/bin/wal-g",
			Repository: config.WALGRepositoryConfig{
				Provider: config.WALGRepositoryProviderFilesystem,
				Prefix:   "/backups/alpha",
			},
		},
	}

	result, err := configurator.ConfigureReinitRecovery(context.Background(), controlplane.ReinitRecoveryConfigRequest{
		Standby: postgres.StandbyConfig{
			PrimaryConnInfo: "host=alpha-1 port=5432 application_name=alpha-2",
		},
	})
	if err != nil {
		t.Fatalf("configure reinit recovery: %v", err)
	}

	if result.DataDir != dataDir || result.RestoreCommand == "" {
		t.Fatalf("unexpected recovery config result: %+v", result)
	}

	rendered := readTestFile(t, filepath.Join(dataDir, postgres.PostgresAutoConfFileName))
	assertContains(t, rendered, "listen_addresses = '*'")
	assertContains(t, rendered, "primary_conninfo = 'host=alpha-1 port=5432 application_name=alpha-2 user=replicator password=''replicator secret'''")
	assertContains(t, rendered, "restore_command = 'env ''WALG_FILE_PREFIX=/backups/alpha'' ''/usr/local/bin/wal-g'' wal-fetch ''%f'' ''%p'''")
	assertContains(t, rendered, "recovery_target_timeline = 'latest'")
	if strings.Contains(rendered, "old restore") || strings.Contains(rendered, "old primary") {
		t.Fatalf("expected old recovery settings to be replaced, got %q", rendered)
	}
	if _, err := os.Stat(filepath.Join(dataDir, postgres.StandbySignalFileName)); err != nil {
		t.Fatalf("standby.signal not written: %v", err)
	}
}

func TestDaemonReconcileReinit(t *testing.T) {
	t.Parallel()

	t.Run("executes PostgreSQL stop for running managed target", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		})

		if engine.stopCalls != 1 || engine.stopMember != "alpha-2" {
			t.Fatalf("unexpected reinit stop calls: calls=%d member=%q", engine.stopCalls, engine.stopMember)
		}
		assertContains(t, logs.String(), `"msg":"reinit PostgreSQL stopped"`)
	})

	t.Run("archives data directory when PostgreSQL is already down", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{stopErr: controlplane.ErrReinitExecutionChanged}
		daemon, _ := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 1 || engine.archiveMember != "alpha-2" {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d member=%q", engine.stopCalls, engine.archiveCalls, engine.archiveMember)
		}
	})

	t.Run("restores from WAL-G when archive phase is already complete", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{
			stopErr:    controlplane.ErrReinitExecutionChanged,
			archiveErr: controlplane.ErrReinitExecutionChanged,
			restoreResult: controlplane.ReinitExecution{
				WALGBackupName: "LATEST",
			},
		}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 1 || engine.restoreCalls != 1 || engine.restoreMember != "alpha-2" {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d restore=%d member=%q", engine.stopCalls, engine.archiveCalls, engine.restoreCalls, engine.restoreMember)
		}
		assertContains(t, logs.String(), `"msg":"reinit WAL-G restore completed"`)
	})

	t.Run("renders recovery config when WAL-G restore phase is complete", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{
			stopErr:    controlplane.ErrReinitExecutionChanged,
			archiveErr: controlplane.ErrReinitExecutionChanged,
			restoreErr: controlplane.ErrReinitExecutionChanged,
		}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 1 || engine.restoreCalls != 1 || engine.recoveryConfigCalls != 1 || engine.recoveryConfigMember != "alpha-2" {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d restore=%d recovery=%d member=%q",
				engine.stopCalls, engine.archiveCalls, engine.restoreCalls, engine.recoveryConfigCalls, engine.recoveryConfigMember)
		}
		assertContains(t, logs.String(), `"msg":"reinit recovery configured"`)
	})

	t.Run("publishes stopped phase when PostgreSQL is already down", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
		})

		if engine.stopCalls != 1 || engine.archiveCalls != 0 {
			t.Fatalf("unexpected reinit calls: stop=%d archive=%d", engine.stopCalls, engine.archiveCalls)
		}
		assertContains(t, logs.String(), `"msg":"reinit PostgreSQL stopped"`)
	})

	t.Run("suppresses missing active reinit operation", func(t *testing.T) {
		t.Parallel()

		engine := &recordingReinitPublisher{stopErr: controlplane.ErrReinitExecutionRequired}
		daemon, logs := newReinitTestDaemon(t, engine)

		daemon.reconcileReinit(context.Background(), agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
		})

		if engine.stopCalls != 1 {
			t.Fatalf("expected one reinit stop attempt, got %d", engine.stopCalls)
		}
		if strings.Contains(logs.String(), "reinit PostgreSQL stop failed") {
			t.Fatalf("expected missing active operation to be quiet, logs=%s", logs.String())
		}
	})
}

func TestDaemonReconcileRejoin(t *testing.T) {
	t.Parallel()

	t.Run("returns when local pg_ctl is unavailable", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment: controlplane.RejoinMemberAssessment{Ready: true},
		}
		daemon, _ := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})
		daemon.pgCtl = nil

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.standbyConfigCalls != 0 || engine.decideCalls != 0 {
			t.Fatalf("expected rejoin reconciliation to stop before any engine call, got standby=%d decide=%d", engine.standbyConfigCalls, engine.decideCalls)
		}
	})

	t.Run("returns when rejoin assessment is not ready", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment: controlplane.RejoinMemberAssessment{Ready: false},
		}
		daemon, _ := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.standbyConfigCalls != 0 || engine.decideCalls != 0 {
			t.Fatalf("expected unrepaired member to skip rejoin execution, got standby=%d decide=%d", engine.standbyConfigCalls, engine.decideCalls)
		}
	})

	t.Run("finalizes already running standby", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment: controlplane.RejoinMemberAssessment{Ready: true},
		}
		daemon, _ := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{
			Up:         true,
			InRecovery: true,
		})

		if engine.completeCalls != 1 {
			t.Fatalf("expected rejoin completion to be attempted once, got %d", engine.completeCalls)
		}
	})

	t.Run("restarts standby when pending restart is set", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment: controlplane.RejoinMemberAssessment{Ready: true},
		}
		reader := stubNodeStatusReader{
			status: agentmodel.NodeStatus{
				PendingRestart: true,
			},
			ok: true,
		}
		daemon, _ := newRejoinTestDaemon(t, engine, reader)

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.restartCalls != 1 {
			t.Fatalf("expected standby restart to be attempted once, got %d", engine.restartCalls)
		}
	})

	t.Run("configures standby before starting a new execution", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment: controlplane.RejoinMemberAssessment{Ready: true},
		}
		daemon, logs := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.standbyConfigCalls != 1 {
			t.Fatalf("expected standby configuration to be attempted once, got %d", engine.standbyConfigCalls)
		}

		if engine.decideCalls != 0 {
			t.Fatalf("expected no strategy decision after successful standby configuration, got %d", engine.decideCalls)
		}

		assertContains(t, logs.String(), `"msg":"rejoin standby configured"`)
	})

	t.Run("logs unexpected standby configuration failure", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment:       controlplane.RejoinMemberAssessment{Ready: true},
			standbyConfigErr: errors.New("write standby config failed"),
		}
		daemon, logs := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.decideCalls != 0 {
			t.Fatalf("expected no strategy decision after failed standby configuration, got %d", engine.decideCalls)
		}

		assertContains(t, logs.String(), `"msg":"rejoin standby config failed"`)
		assertContains(t, logs.String(), `"error":"write standby config failed"`)
	})

	t.Run("starts direct rejoin when decision allows it", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment:       controlplane.RejoinMemberAssessment{Ready: true},
			standbyConfigErr: controlplane.ErrRejoinExecutionRequired,
			decision: controlplane.RejoinStrategyDecision{
				DirectRejoinPossible: true,
			},
		}
		daemon, _ := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.directCalls != 1 {
			t.Fatalf("expected direct rejoin to be attempted once, got %d", engine.directCalls)
		}
	})

	t.Run("suppresses direct rejoin already in progress", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment:       controlplane.RejoinMemberAssessment{Ready: true},
			standbyConfigErr: controlplane.ErrRejoinExecutionRequired,
			decision: controlplane.RejoinStrategyDecision{
				DirectRejoinPossible: true,
			},
			directErr: controlplane.ErrRejoinOperationInProgress,
		}
		daemon, logs := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.directCalls != 1 {
			t.Fatalf("expected one direct rejoin attempt, got %d", engine.directCalls)
		}

		if logs.Len() != 0 {
			t.Fatalf("expected in-progress direct rejoin to be suppressed, got logs %q", logs.String())
		}
	})

	t.Run("starts rewind when decision selects rewind", func(t *testing.T) {
		t.Parallel()

		engine := &recordingRejoinPublisher{
			assessment:       controlplane.RejoinMemberAssessment{Ready: true},
			standbyConfigErr: controlplane.ErrRejoinExecutionRequired,
			decision: controlplane.RejoinStrategyDecision{
				Decided:  true,
				Strategy: cluster.RejoinStrategyRewind,
			},
		}
		daemon, _ := newRejoinTestDaemon(t, engine, staticControlPlaneStore{})

		daemon.reconcileRejoin(context.Background(), agentmodel.PostgresStatus{})

		if engine.rewindCalls != 1 {
			t.Fatalf("expected rewind rejoin to be attempted once, got %d", engine.rewindCalls)
		}
	})
}

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

func readTestFile(t *testing.T, path string) string {
	t.Helper()

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %q: %v", path, err)
	}

	return string(payload)
}

func newSwitchoverTestDaemon(t *testing.T, publisher controlplane.NodeStatePublisher) (*Daemon, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer
	daemon := &Daemon{
		config: config.Config{
			Node: config.NodeConfig{
				Name: "alpha-1",
				Role: cluster.NodeRoleData,
			},
		},
		logger:         logging.New("pacmand", &logs),
		pgCtl:          &postgres.PGCtl{DataDir: t.TempDir()},
		statePublisher: publisher,
	}

	return daemon, &logs
}

func newFailoverTestDaemon(t *testing.T, publisher controlplane.NodeStatePublisher, reader httpapi.NodeStatusReader) (*Daemon, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer
	daemon := &Daemon{
		config: config.Config{
			Node: config.NodeConfig{
				Name: "alpha-2",
				Role: cluster.NodeRoleData,
			},
		},
		logger:         logging.New("pacmand", &logs),
		pgCtl:          &postgres.PGCtl{DataDir: t.TempDir()},
		statePublisher: publisher,
		stateReader:    reader,
	}

	return daemon, &logs
}

func newRejoinTestDaemon(t *testing.T, publisher controlplane.NodeStatePublisher, reader httpapi.NodeStatusReader) (*Daemon, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer
	daemon := &Daemon{
		config: config.Config{
			Node: config.NodeConfig{
				Name: "alpha-1",
				Role: cluster.NodeRoleData,
			},
			Postgres: &config.PostgresLocalConfig{
				BinDir:              t.TempDir(),
				DataDir:             t.TempDir(),
				ReplicationUser:     "replicator",
				ReplicationPassword: "replicator-secret",
			},
		},
		logger:         logging.New("pacmand", &logs),
		pgCtl:          &postgres.PGCtl{DataDir: t.TempDir()},
		statePublisher: publisher,
		stateReader:    reader,
	}

	return daemon, &logs
}

func newReinitTestDaemon(t *testing.T, publisher controlplane.NodeStatePublisher) (*Daemon, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer
	daemon := &Daemon{
		config: config.Config{
			Node: config.NodeConfig{
				Name: "alpha-2",
				Role: cluster.NodeRoleData,
			},
			Postgres: &config.PostgresLocalConfig{
				DataDir: t.TempDir(),
			},
			Reinit: &config.ReinitConfig{
				WALG: &config.WALGConfig{
					Binary: "/usr/local/bin/wal-g",
					Repository: config.WALGRepositoryConfig{
						Provider: config.WALGRepositoryProviderFilesystem,
						Prefix:   "/backups/alpha",
					},
				},
			},
		},
		logger:         logging.New("pacmand", &logs),
		pgCtl:          &postgres.PGCtl{DataDir: t.TempDir()},
		statePublisher: publisher,
	}

	return daemon, &logs
}

func writeTracingBinary(t *testing.T, binaryName, scriptTemplate string) (string, string) {
	t.Helper()

	binDir := t.TempDir()
	tracePath := filepath.Join(binDir, binaryName+".trace")
	scriptPath := filepath.Join(binDir, binaryName)
	tempPath := scriptPath + ".tmp"
	script := []byte(strings.TrimSpace(
		strings.ReplaceAll(
			strings.ReplaceAll(scriptTemplate, "%q", `"`+tracePath+`"`),
			"%%", "%",
		),
	) + "\n")

	if err := os.WriteFile(tempPath, script, 0o755); err != nil {
		t.Fatalf("write %s script: %v", binaryName, err)
	}
	if err := os.Rename(tempPath, scriptPath); err != nil {
		t.Fatalf("install %s script: %v", binaryName, err)
	}
	time.Sleep(10 * time.Millisecond)

	return binDir, tracePath
}

func assertTraceLines(t *testing.T, path string, want []string) {
	t.Helper()

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read command trace %q: %v", path, err)
	}

	got := strings.Split(strings.TrimSpace(string(payload)), "\n")
	if len(got) != len(want) {
		t.Fatalf("unexpected traced command count: got %v, want %v", got, want)
	}

	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("trace %d: got %q, want %q", index, got[index], want[index])
		}
	}
}

type testMemberDiscovery struct {
	registration controlplane.MemberRegistration
	ok           bool
}

func (discovery testMemberDiscovery) RegisteredMember(nodeName string) (controlplane.MemberRegistration, bool) {
	if discovery.ok && discovery.registration.NodeName == nodeName {
		return discovery.registration.Clone(), true
	}

	return controlplane.MemberRegistration{}, false
}

func (discovery testMemberDiscovery) RegisteredMembers() []controlplane.MemberRegistration {
	if !discovery.ok {
		return nil
	}

	return []controlplane.MemberRegistration{discovery.registration.Clone()}
}

func (testMemberDiscovery) Member(string) (cluster.MemberStatus, bool) {
	return cluster.MemberStatus{}, false
}

func (testMemberDiscovery) Members() []cluster.MemberStatus {
	return nil
}

type recordingSwitchoverPublisher struct {
	executeCalls int
	executeErr   error
	execution    controlplane.SwitchoverExecution
}

func (*recordingSwitchoverPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (*recordingSwitchoverPublisher) SwitchoverTargetReadiness(string) (controlplane.SwitchoverTargetReadiness, error) {
	return controlplane.SwitchoverTargetReadiness{}, nil
}

func (*recordingSwitchoverPublisher) ValidateSwitchover(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverValidation, error) {
	return controlplane.SwitchoverValidation{}, nil
}

func (*recordingSwitchoverPublisher) CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error) {
	return controlplane.SwitchoverIntent{}, nil
}

func (*recordingSwitchoverPublisher) CancelSwitchover(context.Context) (cluster.Operation, error) {
	return cluster.Operation{}, nil
}

func (publisher *recordingSwitchoverPublisher) ExecuteSwitchover(context.Context, controlplane.DemotionExecutor, controlplane.PromotionExecutor) (controlplane.SwitchoverExecution, error) {
	publisher.executeCalls++
	return publisher.execution.Clone(), publisher.executeErr
}

func (*recordingSwitchoverPublisher) RegisteredMember(string) (controlplane.MemberRegistration, bool) {
	return controlplane.MemberRegistration{}, false
}

func (*recordingSwitchoverPublisher) RegisteredMembers() []controlplane.MemberRegistration {
	return nil
}

func (*recordingSwitchoverPublisher) Member(string) (cluster.MemberStatus, bool) {
	return cluster.MemberStatus{}, false
}

func (*recordingSwitchoverPublisher) Members() []cluster.MemberStatus {
	return nil
}

type recordingFailoverPublisher struct {
	createCalls   int
	createRequest controlplane.FailoverIntentRequest
	createErr     error
	executeCalls  int
	executeErr    error
	execution     controlplane.FailoverExecution
}

func (*recordingFailoverPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (*recordingFailoverPublisher) FailoverCandidates() ([]controlplane.FailoverCandidate, error) {
	return nil, nil
}

func (*recordingFailoverPublisher) ConfirmPrimaryFailure() (controlplane.PrimaryFailureConfirmation, error) {
	return controlplane.PrimaryFailureConfirmation{}, nil
}

func (publisher *recordingFailoverPublisher) CreateFailoverIntent(_ context.Context, request controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error) {
	publisher.createCalls++
	publisher.createRequest = request
	return controlplane.FailoverIntent{}, publisher.createErr
}

func (publisher *recordingFailoverPublisher) ExecuteFailover(context.Context, controlplane.PromotionExecutor, controlplane.FencingHook) (controlplane.FailoverExecution, error) {
	publisher.executeCalls++
	return publisher.execution.Clone(), publisher.executeErr
}

type recordingRejoinPublisher struct {
	assessment         controlplane.RejoinMemberAssessment
	assessmentErr      error
	decision           controlplane.RejoinStrategyDecision
	decisionErr        error
	standbyConfigErr   error
	restartErr         error
	directErr          error
	rewindErr          error
	completeErr        error
	verifyErr          error
	standbyConfigCalls int
	restartCalls       int
	directCalls        int
	rewindCalls        int
	completeCalls      int
	verifyCalls        int
	decideCalls        int
}

func (*recordingRejoinPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (publisher *recordingRejoinPublisher) AssessRejoinMember(string) (controlplane.RejoinMemberAssessment, error) {
	return publisher.assessment.Clone(), publisher.assessmentErr
}

func (*recordingRejoinPublisher) DetectRejoinDivergence(string) (controlplane.RejoinDivergenceAssessment, error) {
	return controlplane.RejoinDivergenceAssessment{}, nil
}

func (publisher *recordingRejoinPublisher) DecideRejoinStrategy(string) (controlplane.RejoinStrategyDecision, error) {
	publisher.decideCalls++
	return publisher.decision.Clone(), publisher.decisionErr
}

func (publisher *recordingRejoinPublisher) ExecuteRejoinDirect(context.Context, controlplane.RejoinRequest) (controlplane.RejoinExecution, error) {
	publisher.directCalls++
	return controlplane.RejoinExecution{}, publisher.directErr
}

func (publisher *recordingRejoinPublisher) ExecuteRejoinRewind(context.Context, controlplane.RejoinRequest, controlplane.RewindExecutor) (controlplane.RejoinExecution, error) {
	publisher.rewindCalls++
	return controlplane.RejoinExecution{}, publisher.rewindErr
}

func (publisher *recordingRejoinPublisher) ExecuteRejoinStandbyConfig(context.Context, controlplane.StandbyConfigExecutor) (controlplane.RejoinExecution, error) {
	publisher.standbyConfigCalls++
	return controlplane.RejoinExecution{}, publisher.standbyConfigErr
}

func (publisher *recordingRejoinPublisher) ExecuteRejoinRestartAsStandby(context.Context, controlplane.StandbyRestartExecutor) (controlplane.RejoinExecution, error) {
	publisher.restartCalls++
	return controlplane.RejoinExecution{}, publisher.restartErr
}

func (publisher *recordingRejoinPublisher) VerifyRejoinReplication(context.Context) (controlplane.RejoinExecution, error) {
	publisher.verifyCalls++
	return controlplane.RejoinExecution{}, publisher.verifyErr
}

func (publisher *recordingRejoinPublisher) CompleteRejoin(context.Context) (controlplane.RejoinExecution, error) {
	publisher.completeCalls++
	return controlplane.RejoinExecution{}, publisher.completeErr
}

type recordingReinitPublisher struct {
	stopCalls            int
	stopMember           string
	stopErr              error
	archiveCalls         int
	archiveMember        string
	archiveErr           error
	restoreCalls         int
	restoreMember        string
	restoreResult        controlplane.ReinitExecution
	restoreErr           error
	recoveryConfigCalls  int
	recoveryConfigMember string
	recoveryConfigErr    error
}

func (*recordingReinitPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (*recordingReinitPublisher) ValidateReinit(context.Context, controlplane.ReinitRequest) (controlplane.ReinitValidation, error) {
	return controlplane.ReinitValidation{}, nil
}

func (*recordingReinitPublisher) CreateReinitIntent(context.Context, controlplane.ReinitRequest) (controlplane.ReinitIntent, error) {
	return controlplane.ReinitIntent{}, nil
}

func (publisher *recordingReinitPublisher) ExecuteReinitStopPostgres(_ context.Context, member string, _ controlplane.ReinitPostgresStopExecutor) (controlplane.ReinitExecution, error) {
	publisher.stopCalls++
	publisher.stopMember = member
	return controlplane.ReinitExecution{}, publisher.stopErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitArchiveDataDir(_ context.Context, member string, _ controlplane.ReinitDataDirArchiveExecutor) (controlplane.ReinitExecution, error) {
	publisher.archiveCalls++
	publisher.archiveMember = member
	return controlplane.ReinitExecution{ArchivePath: "/archive/data"}, publisher.archiveErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitWALGRestore(_ context.Context, member string, _ controlplane.ReinitWALGRestoreExecutor) (controlplane.ReinitExecution, error) {
	publisher.restoreCalls++
	publisher.restoreMember = member
	return publisher.restoreResult.Clone(), publisher.restoreErr
}

func (publisher *recordingReinitPublisher) ExecuteReinitRecoveryConfig(_ context.Context, member string, _ controlplane.ReinitRecoveryConfigExecutor) (controlplane.ReinitExecution, error) {
	publisher.recoveryConfigCalls++
	publisher.recoveryConfigMember = member
	return controlplane.ReinitExecution{RecoveryConfig: true}, publisher.recoveryConfigErr
}

type stubNodeStatusReader struct {
	status          agentmodel.NodeStatus
	ok              bool
	clusterStatus   cluster.ClusterStatus
	clusterStatusOK bool
}

func (reader stubNodeStatusReader) NodeStatus(string) (agentmodel.NodeStatus, bool) {
	if !reader.ok {
		return agentmodel.NodeStatus{}, false
	}

	return reader.status.Clone(), true
}

func (stubNodeStatusReader) NodeStatuses() []agentmodel.NodeStatus {
	return nil
}

func (stubNodeStatusReader) ClusterSpec() (cluster.ClusterSpec, bool) {
	return cluster.ClusterSpec{}, false
}

func (reader stubNodeStatusReader) ClusterStatus() (cluster.ClusterStatus, bool) {
	if !reader.clusterStatusOK {
		return cluster.ClusterStatus{}, false
	}

	return reader.clusterStatus.Clone(), true
}

func (stubNodeStatusReader) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return cluster.MaintenanceModeStatus{}
}

func (stubNodeStatusReader) UpdateMaintenanceMode(context.Context, cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	return cluster.MaintenanceModeStatus{}, errors.New("unsupported")
}

func (stubNodeStatusReader) History() []cluster.HistoryEntry {
	return nil
}

func (stubNodeStatusReader) CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error) {
	return controlplane.SwitchoverIntent{}, errors.New("unsupported")
}

func (stubNodeStatusReader) CancelSwitchover(context.Context) (cluster.Operation, error) {
	return cluster.Operation{}, errors.New("unsupported")
}

func (stubNodeStatusReader) CreateFailoverIntent(context.Context, controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error) {
	return controlplane.FailoverIntent{}, errors.New("unsupported")
}

func (stubNodeStatusReader) CreateReinitIntent(context.Context, controlplane.ReinitRequest) (controlplane.ReinitIntent, error) {
	return controlplane.ReinitIntent{}, errors.New("unsupported")
}

func failoverTestClusterStatus(currentPrimary string, active *cluster.Operation, members ...cluster.MemberStatus) cluster.ClusterStatus {
	return cluster.ClusterStatus{
		ClusterName:    "alpha",
		Phase:          cluster.ClusterPhaseDegraded,
		CurrentPrimary: currentPrimary,
		ActiveOperation: func() *cluster.Operation {
			if active == nil {
				return nil
			}
			cloned := active.Clone()
			return &cloned
		}(),
		Members:    append([]cluster.MemberStatus(nil), members...),
		ObservedAt: clusterTime("2026-01-02T03:04:05Z"),
	}
}

func failoverTestMember(name string, role cluster.MemberRole, healthy bool, needsRejoin bool) cluster.MemberStatus {
	state := cluster.MemberStateFailed
	if healthy {
		state = cluster.MemberStateRunning
		if role == cluster.MemberRoleReplica {
			state = cluster.MemberStateStreaming
		}
	}
	if needsRejoin {
		state = cluster.MemberStateNeedsRejoin
	}

	return cluster.MemberStatus{
		Name:        name,
		Role:        role,
		State:       state,
		Healthy:     healthy,
		NeedsRejoin: needsRejoin,
		LastSeenAt:  clusterTime("2026-01-02T03:04:05Z"),
	}
}

func clusterTime(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}

	return parsed
}
