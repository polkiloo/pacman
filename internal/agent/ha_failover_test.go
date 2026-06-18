package agent

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
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
