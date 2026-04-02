package localagent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

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
