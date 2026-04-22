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

func TestMemberAPIURL(t *testing.T) {
	t.Parallel()

	if got := memberAPIURL(""); got != "" {
		t.Fatalf("empty member API URL: got %q", got)
	}

	if got := memberAPIURL("10.0.0.10:8080"); got != "http://10.0.0.10:8080" {
		t.Fatalf("unexpected member API URL: got %q", got)
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

func TestPGCtlStandbyRestarterRestartAsStandbyStartsWithoutWaiting(t *testing.T) {
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
		"start -D /var/lib/postgresql/data -W",
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

	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		t.Fatalf("open temp %s script: %v", binaryName, err)
	}

	if _, err := file.Write(script); err != nil {
		_ = file.Close()
		t.Fatalf("write temp %s script: %v", binaryName, err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close temp %s script: %v", binaryName, err)
	}

	if err := os.Rename(tempPath, scriptPath); err != nil {
		t.Fatalf("write %s script: %v", binaryName, err)
	}

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

type stubNodeStatusReader struct {
	status agentmodel.NodeStatus
	ok     bool
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

func (stubNodeStatusReader) ClusterStatus() (cluster.ClusterStatus, bool) {
	return cluster.ClusterStatus{}, false
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
