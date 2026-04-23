package agent

import (
	"context"
	"errors"
	"log/slog"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

const (
	automaticFailoverRequestedBy = "pacmand"
	automaticFailoverReason      = "automatic failover reconciliation"
)

func (daemon *Daemon) reconcileFailover(ctx context.Context) {
	if daemon.pgCtl == nil || daemon.stateReader == nil || !daemon.config.Node.Role.HasLocalPostgres() {
		return
	}

	engine, ok := daemon.statePublisher.(controlplane.FailoverEngine)
	if !ok {
		return
	}

	status, ok := daemon.stateReader.ClusterStatus()
	if !ok {
		return
	}

	if operation := status.ActiveOperation; operation != nil {
		if operation.Kind != cluster.OperationKindFailover || operation.ToMember != daemon.config.Node.Name {
			return
		}

		daemon.executeFailover(ctx, engine)
		return
	}

	local := failoverClusterMember(status, daemon.config.Node.Name)
	if local == nil || !local.Healthy || local.NeedsRejoin || local.Role != cluster.MemberRoleReplica {
		return
	}

	currentPrimary := failoverClusterMember(status, status.CurrentPrimary)
	if currentPrimary == nil || currentPrimary.Healthy {
		return
	}

	_, err := engine.CreateFailoverIntent(ctx, controlplane.FailoverIntentRequest{
		RequestedBy: automaticFailoverRequestedBy,
		Reason:      automaticFailoverReason,
	})
	if err == nil {
		return
	}

	if errors.Is(err, controlplane.ErrClusterSpecRequired) ||
		errors.Is(err, controlplane.ErrFailoverObservedStateRequired) ||
		errors.Is(err, controlplane.ErrAutomaticFailoverNotAllowed) ||
		errors.Is(err, controlplane.ErrFailoverPrimaryUnknown) ||
		errors.Is(err, controlplane.ErrFailoverPrimaryHealthy) ||
		errors.Is(err, controlplane.ErrFailoverQuorumUnavailable) ||
		errors.Is(err, controlplane.ErrFailoverMaintenanceEnabled) ||
		errors.Is(err, controlplane.ErrFailoverNoEligibleCandidates) ||
		errors.Is(err, controlplane.ErrFailoverOperationInProgress) {
		return
	}

	daemon.logger.WarnContext(ctx, "failover intent creation failed",
		daemon.logArgs("agent", slog.String("error", err.Error()))...)
}

func (daemon *Daemon) executeFailover(ctx context.Context, engine controlplane.FailoverEngine) {
	promoter := &pgCtlLocalPromoter{pgCtl: daemon.pgCtl}

	execution, err := engine.ExecuteFailover(ctx, promoter, nil)
	if err != nil {
		if errors.Is(err, controlplane.ErrFailoverIntentRequired) ||
			errors.Is(err, controlplane.ErrFailoverIntentChanged) {
			return
		}

		daemon.logger.WarnContext(ctx, "failover execution failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	}

	daemon.logger.InfoContext(ctx, "failover executed",
		daemon.logArgs("agent",
			slog.String("from_primary", execution.CurrentPrimary),
			slog.String("to_candidate", execution.Candidate),
			slog.String("epoch", execution.CurrentEpoch.String()),
		)...)
}

func failoverClusterMember(status cluster.ClusterStatus, memberName string) *cluster.MemberStatus {
	for index := range status.Members {
		if status.Members[index].Name == memberName {
			return &status.Members[index]
		}
	}

	return nil
}
