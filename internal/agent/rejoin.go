package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/postgres"
)

// pgRewindRewinder implements controlplane.RewindExecutor via pg_rewind.
type pgRewindRewinder struct {
	binDir  string
	dataDir string
}

func (r *pgRewindRewinder) Rewind(ctx context.Context, req controlplane.RewindRequest) error {
	connInfo := req.CurrentPrimaryNode.Postgres.Address
	if connInfo == "" {
		return fmt.Errorf("pg_rewind: current primary postgres address is empty")
	}

	return postgres.PGRewind{
		BinDir:       r.binDir,
		DataDir:      r.dataDir,
		SourceServer: connInfo,
	}.Run(ctx)
}

// localStandbyConfigurator implements controlplane.StandbyConfigExecutor by
// writing postgresql.auto.conf and standby.signal into the local data dir.
type localStandbyConfigurator struct {
	dataDir string
}

func (c *localStandbyConfigurator) ConfigureStandby(_ context.Context, req controlplane.StandbyConfigRequest) error {
	// Don't use a replication slot for rejoin — the slot won't pre-exist on the
	// new primary after switchover. wal_keep_size retains enough WAL to reconnect.
	standby := req.Standby
	standby.PrimarySlotName = ""

	rendered, err := postgres.RenderStandbyFiles(c.dataDir, standby)
	if err != nil {
		return fmt.Errorf("render standby files: %w", err)
	}

	merged, err := mergePostgresAutoConf(rendered.PostgresAutoConfPath, rendered.PostgresAutoConf)
	if err != nil {
		return err
	}

	if err := os.WriteFile(rendered.PostgresAutoConfPath, []byte(merged), 0640); err != nil {
		return fmt.Errorf("write postgresql.auto.conf: %w", err)
	}

	if err := os.WriteFile(rendered.StandbySignalPath, nil, 0640); err != nil {
		return fmt.Errorf("write standby.signal: %w", err)
	}

	return nil
}

// mergePostgresAutoConf reads an existing postgresql.auto.conf, strips out
// standby-related parameters, then appends the new standby config block. This
// preserves Ansible-managed settings (listen_addresses, wal_level, etc.).
func mergePostgresAutoConf(path, newStandbyBlock string) (string, error) {
	standbyParams := map[string]bool{
		"primary_conninfo":         true,
		"primary_slot_name":        true,
		"restore_command":          true,
		"recovery_target_timeline": true,
	}

	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read postgresql.auto.conf: %w", err)
	}

	var kept []string
	for _, line := range strings.Split(string(existing), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			kept = append(kept, line)
			continue
		}
		key := strings.ToLower(strings.TrimSpace(strings.SplitN(trimmed, "=", 2)[0]))
		if !standbyParams[key] {
			kept = append(kept, line)
		}
	}

	base := strings.TrimRight(strings.Join(kept, "\n"), "\n")
	if base != "" {
		base += "\n"
	}
	return base + newStandbyBlock, nil
}

// pgCtlStandbyRestarter implements controlplane.StandbyRestartExecutor via
// pg_ctl start.
type pgCtlStandbyRestarter struct {
	pgCtl *postgres.PGCtl
}

func (r *pgCtlStandbyRestarter) RestartAsStandby(ctx context.Context, _ controlplane.StandbyRestartRequest) error {
	return r.pgCtl.Start(ctx)
}

func (daemon *Daemon) reconcileRejoin(ctx context.Context, currentPostgres agentmodel.PostgresStatus) {
	if daemon.pgCtl == nil {
		return
	}

	engine, ok := daemon.statePublisher.(controlplane.RejoinEngine)
	if !ok {
		return
	}

	nodeName := daemon.config.Node.Name

	assessment, err := engine.AssessRejoinMember(nodeName)
	if err != nil || !assessment.Ready {
		return
	}

	if currentPostgres.Up && currentPostgres.InRecovery {
		daemon.advanceRejoinFinalPhases(ctx, engine, nodeName)
		return
	}

	// Read the merged node status from the store to check control-plane flags.
	storedStatus, _ := daemon.stateReader.NodeStatus(nodeName)

	if storedStatus.PendingRestart || storedStatus.Postgres.Details.PendingRestart {
		restarter := &pgCtlStandbyRestarter{pgCtl: daemon.pgCtl}
		if _, err := engine.ExecuteRejoinRestartAsStandby(ctx, restarter); err != nil {
			if !errors.Is(err, controlplane.ErrRejoinExecutionRequired) {
				daemon.logger.WarnContext(ctx, "rejoin restart as standby failed",
					daemon.logArgs("agent", slog.String("error", err.Error()))...)
			}
		}
		return
	}

	// Try standby config — fails with ErrRejoinExecutionRequired if no active op.
	configurator := &localStandbyConfigurator{dataDir: daemon.config.Postgres.DataDir}
	if _, err := engine.ExecuteRejoinStandbyConfig(ctx, configurator); err == nil {
		daemon.logger.InfoContext(ctx, "rejoin standby configured",
			daemon.logArgs("agent")...)
		return
	} else if !errors.Is(err, controlplane.ErrRejoinExecutionRequired) {
		daemon.logger.WarnContext(ctx, "rejoin standby config failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	}

	// No active operation — start rejoin.
	decision, err := engine.DecideRejoinStrategy(nodeName)
	if err != nil {
		return
	}

	req := controlplane.RejoinRequest{Member: nodeName}

	switch {
	case decision.DirectRejoinPossible:
		if _, err := engine.ExecuteRejoinDirect(ctx, req); err != nil {
			if !errors.Is(err, controlplane.ErrRejoinOperationInProgress) {
				daemon.logger.WarnContext(ctx, "rejoin direct start failed",
					daemon.logArgs("agent", slog.String("error", err.Error()))...)
			}
		}
	case decision.Decided && decision.Strategy == cluster.RejoinStrategyRewind:
		rewinder := &pgRewindRewinder{
			binDir:  daemon.config.Postgres.BinDir,
			dataDir: daemon.config.Postgres.DataDir,
		}
		if _, err := engine.ExecuteRejoinRewind(ctx, req, rewinder); err != nil {
			daemon.logger.WarnContext(ctx, "rejoin rewind failed",
				daemon.logArgs("agent", slog.String("error", err.Error()))...)
		}
	}
}

func (daemon *Daemon) advanceRejoinFinalPhases(ctx context.Context, engine controlplane.RejoinEngine, nodeName string) {
	if _, err := engine.CompleteRejoin(ctx); err == nil {
		daemon.logger.InfoContext(ctx, "rejoin completed",
			daemon.logArgs("agent", slog.String("node", nodeName))...)
		return
	}

	// Verify replication; CompleteRejoin will succeed on the next heartbeat.
	if _, err := engine.VerifyRejoinReplication(ctx); err != nil {
		if !errors.Is(err, controlplane.ErrRejoinExecutionRequired) &&
			!errors.Is(err, controlplane.ErrRejoinReplicationNotHealthy) &&
			!errors.Is(err, controlplane.ErrRejoinExecutionChanged) {
			daemon.logger.WarnContext(ctx, "rejoin replication verification failed",
				daemon.logArgs("agent", slog.String("error", err.Error()))...)
		}
	}
}
