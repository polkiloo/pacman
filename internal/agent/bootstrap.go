package agent

import (
	"context"
	"log/slog"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/controlplane"
)

func (daemon *Daemon) bootstrapClusterSpec(ctx context.Context) error {
	if daemon.config.Bootstrap == nil {
		return nil
	}

	store, ok := daemon.statePublisher.(controlplane.DesiredStateStore)
	if !ok {
		return nil
	}

	spec := cluster.ClusterSpec{
		ClusterName: daemon.config.Bootstrap.ClusterName,
		Postgres: cluster.PostgresPolicy{
			Parameters: bootstrapPostgresParameters(daemon.config),
		},
		Members: bootstrapMemberSpecs(daemon.config.Bootstrap.ExpectedMembers),
	}

	stored, err := store.StoreClusterSpec(ctx, spec)
	if err != nil {
		return err
	}

	daemon.logger.InfoContext(
		ctx,
		"stored bootstrap cluster spec",
		daemon.logArgs(
			"controlplane",
			slog.String("cluster", stored.ClusterName),
			slog.Int64("generation", int64(stored.Generation)),
			slog.Int("members", len(stored.Members)),
		)...,
	)

	return nil
}

func bootstrapMemberSpecs(memberNames []string) []cluster.MemberSpec {
	members := make([]cluster.MemberSpec, len(memberNames))
	for index, memberName := range memberNames {
		members[index] = cluster.MemberSpec{Name: memberName}
	}

	return members
}

func bootstrapPostgresParameters(cfg config.Config) map[string]any {
	if cfg.Postgres == nil || len(cfg.Postgres.Parameters) == 0 {
		return nil
	}

	parameters := make(map[string]any, len(cfg.Postgres.Parameters))
	for key, value := range cfg.Postgres.Parameters {
		parameters[key] = value
	}

	return parameters
}
