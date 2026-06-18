package agent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/logging"
	"github.com/polkiloo/pacman/internal/postgres"
)

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
