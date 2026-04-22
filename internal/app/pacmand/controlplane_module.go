package pacmand

import (
	"context"
	"fmt"
	"log/slog"

	"go.uber.org/fx"

	"github.com/polkiloo/pacman/internal/agent"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/dcs"
	dcsetcd "github.com/polkiloo/pacman/internal/dcs/etcd"
	dcsraft "github.com/polkiloo/pacman/internal/dcs/raft"
)

type controlPlaneParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Context   context.Context
	Config    *config.Config `optional:"true"`
	Logger    *slog.Logger
}

// ControlPlaneModule wires the configured DCS backend into pacmand and exposes
// it to the daemon as a shared control-plane state store.
func ControlPlaneModule() fx.Option {
	return fx.Module(
		"pacmand.controlplane",
		fx.Provide(
			fx.Annotate(
				newControlPlaneAgentOption,
				fx.ResultTags(`group:"agent.option"`),
			),
		),
	)
}

func newControlPlaneAgentOption(params controlPlaneParams) (agent.Option, error) {
	if params.Config == nil || params.Config.DCS == nil {
		return nil, nil
	}

	backend, err := openConfiguredDCS(*params.Config.DCS)
	if err != nil {
		return nil, fmt.Errorf("open configured dcs backend: %w", err)
	}

	store, err := controlplane.OpenControlPlane(params.Context, backend, params.Config.DCS.ClusterName, params.Logger)
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("open control plane: %w", err)
	}

	params.Lifecycle.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return backend.Close()
		},
	})

	return agent.WithControlPlaneStateStore(store), nil
}

func openConfiguredDCS(cfg dcs.Config) (dcs.DCS, error) {
	switch cfg.WithDefaults().Backend {
	case dcs.BackendEtcd:
		return dcsetcd.New(cfg)
	case dcs.BackendRaft:
		raftConfig, err := dcsraft.ConfigFromDCS(cfg)
		if err != nil {
			return nil, err
		}

		return dcsraft.New(raftConfig)
	default:
		return nil, fmt.Errorf("unsupported dcs backend %q", cfg.Backend)
	}
}
