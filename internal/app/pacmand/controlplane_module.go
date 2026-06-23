package pacmand

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

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

	defaultedConfig := params.Config.WithDefaults()
	backend, err := openConfiguredDCS(defaultedConfig)
	if err != nil {
		return nil, fmt.Errorf("open configured dcs backend: %w", err)
	}

	store, err := controlplane.OpenControlPlane(params.Context, backend, defaultedConfig.DCS.ClusterName, params.Logger)
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

func openConfiguredDCS(cfg config.Config) (dcs.DCS, error) {
	defaulted := cfg.WithDefaults()
	if defaulted.DCS == nil {
		return nil, nil
	}

	switch defaulted.DCS.WithDefaults().Backend {
	case dcs.BackendEtcd:
		return dcsetcd.New(*defaulted.DCS)
	case dcs.BackendRaft:
		raftConfig, err := configuredRaftConfig(defaulted)
		if err != nil {
			return nil, err
		}

		return dcsraft.New(raftConfig)
	default:
		return nil, fmt.Errorf("unsupported dcs backend %q", defaulted.DCS.Backend)
	}
}

func configuredRaftConfig(cfg config.Config) (dcsraft.Config, error) {
	defaulted := cfg.WithDefaults()
	if defaulted.DCS == nil {
		return dcsraft.Config{}, dcs.ErrRaftConfigRequired
	}

	raftConfig, err := dcsraft.ConfigFromDCS(*defaulted.DCS)
	if err != nil {
		return dcsraft.Config{}, err
	}

	if raftBootstrapEnabled(defaulted) {
		raftConfig.Bootstrap = true
	}

	return raftConfig, nil
}

func raftBootstrapEnabled(cfg config.Config) bool {
	defaulted := cfg.WithDefaults()
	if defaulted.DCS == nil || defaulted.DCS.WithDefaults().Backend != dcs.BackendRaft || defaulted.Bootstrap == nil {
		return false
	}

	return strings.TrimSpace(defaulted.Node.Name) == strings.TrimSpace(defaulted.Bootstrap.InitialPrimary)
}
