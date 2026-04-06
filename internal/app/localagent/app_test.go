package localagent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/logging"
)

func TestRunReturnsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Run(ctx, logging.New("pacmand", &bytes.Buffer{}), config.Config{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRunWrapsDaemonConstructionError(t *testing.T) {
	t.Parallel()

	err := Run(context.Background(), logging.New("pacmand", &bytes.Buffer{}), config.Config{
		Node: config.NodeConfig{
			Name: "alpha-1",
			Role: cluster.NodeRoleData,
		},
	})
	if !errors.Is(err, agent.ErrPostgresConfigRequired) {
		t.Fatalf("expected ErrPostgresConfigRequired, got %v", err)
	}

	if !strings.Contains(err.Error(), "construct local agent daemon") {
		t.Fatalf("expected error to contain wrap prefix, got %q", err)
	}
}

func TestRunStartsAndWaitsForWitnessDaemon(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := Run(
		ctx,
		logging.New("pacmand", &bytes.Buffer{}),
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name: "alpha-witness",
				Role: cluster.NodeRoleWitness,
			},
		},
		agent.WithNoAPIServer(),
	)
	if err != nil {
		t.Fatalf("run witness daemon: %v", err)
	}
}

func TestRunWrapsDaemonStartError(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	defer listener.Close()

	err = Run(
		context.Background(),
		logging.New("pacmand", &bytes.Buffer{}),
		config.Config{
			APIVersion: config.APIVersionV1Alpha1,
			Kind:       config.KindNodeConfig,
			Node: config.NodeConfig{
				Name:       "alpha-witness",
				Role:       cluster.NodeRoleWitness,
				APIAddress: listener.Addr().String(),
			},
		},
	)
	if err == nil {
		t.Fatal("expected daemon start error")
	}

	if !strings.Contains(err.Error(), "start local agent daemon") {
		t.Fatalf("expected error to contain start wrap prefix, got %q", err)
	}
}
