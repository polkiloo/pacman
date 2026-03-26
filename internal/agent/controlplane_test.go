package agent

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/logging"
)

func TestDaemonRegisterMemberLogsWarningOnFailure(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:         logging.New("pacmand", &logs),
		statePublisher: registeringPublisher{registerErr: errors.New("registration unavailable")},
	}

	daemon.registerMember(context.Background(), agentmodel.Startup{
		NodeName:       "alpha-1",
		NodeRole:       cluster.NodeRoleData,
		APIAddress:     "10.0.0.10:8080",
		ControlAddress: "10.0.0.10:9090",
		StartedAt:      time.Date(2026, time.March, 27, 9, 0, 0, 0, time.UTC),
	})

	assertContains(t, logs.String(), `"msg":"failed to register local member in control plane"`)
	assertContains(t, logs.String(), `"register_error":"registration unavailable"`)
}

func TestDaemonCampaignLeaderIgnoresUnknownCandidateError(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:         logging.New("pacmand", &logs),
		statePublisher: electingPublisher{campaignErr: controlplane.ErrLeaderCandidateUnknown},
	}

	daemon.campaignLeader(context.Background(), "alpha-1")

	if logs.Len() != 0 {
		t.Fatalf("expected unknown candidate campaign error to be suppressed, got logs %q", logs.String())
	}
}

func TestDaemonCampaignLeaderLogsWarningOnFailure(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	daemon := &Daemon{
		logger:         logging.New("pacmand", &logs),
		statePublisher: electingPublisher{campaignErr: errors.New("leader election unavailable")},
	}

	daemon.campaignLeader(context.Background(), "alpha-1")

	assertContains(t, logs.String(), `"msg":"failed to campaign for control-plane leadership"`)
	assertContains(t, logs.String(), `"campaign_error":"leader election unavailable"`)
}

type registeringPublisher struct {
	registerErr error
}

func (publisher registeringPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (publisher registeringPublisher) RegisterMember(context.Context, controlplane.MemberRegistration) error {
	return publisher.registerErr
}

type electingPublisher struct {
	campaignErr error
}

func (publisher electingPublisher) PublishNodeStatus(context.Context, agentmodel.NodeStatus) (agentmodel.ControlPlaneStatus, error) {
	return agentmodel.ControlPlaneStatus{ClusterReachable: true}, nil
}

func (publisher electingPublisher) CampaignLeader(context.Context, string) (controlplane.LeaderLease, bool, error) {
	return controlplane.LeaderLease{}, false, publisher.campaignErr
}

func (publisher electingPublisher) Leader() (controlplane.LeaderLease, bool) {
	return controlplane.LeaderLease{}, false
}
