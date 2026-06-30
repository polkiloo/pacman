package agent

import (
	"bytes"
	"context"
	"errors"
	"testing"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
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
		if !daemon.selfDemotedPrimaryRejoinPending() {
			t.Fatal("expected completed switchover to keep the local former primary in rejoin state")
		}

		assertContains(t, logs.String(), `"msg":"switchover executed"`)
		assertContains(t, logs.String(), `"from_primary":"alpha-1"`)
		assertContains(t, logs.String(), `"to_candidate":"alpha-2"`)
		assertContains(t, logs.String(), `"epoch":"8"`)
	})
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
