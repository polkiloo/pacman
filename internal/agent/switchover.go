package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
	"github.com/polkiloo/pacman/internal/postgres"
)

// localDemoter stops the local primary cleanly to hand off writes.
type localDemoter struct {
	pgCtl *postgres.PGCtl
}

func (d *localDemoter) Demote(ctx context.Context, _ controlplane.DemotionRequest) error {
	return d.pgCtl.Stop(ctx, postgres.ShutdownModeFast)
}

// apiPromoter sends a promote request to the candidate's HTTP API.
type apiPromoter struct {
	client     *http.Client
	adminToken string
	discovery  controlplane.MemberDiscovery
}

func (p *apiPromoter) Promote(ctx context.Context, req controlplane.PromotionRequest) error {
	reg, ok := p.discovery.RegisteredMember(req.Candidate)
	if !ok {
		return fmt.Errorf("candidate %q has no registered API address", req.Candidate)
	}

	apiURL := memberAPIURL(reg.APIAddress) + "/api/v1/promote"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, nil)
	if err != nil {
		return fmt.Errorf("build promote request for %q: %w", req.Candidate, err)
	}
	if p.adminToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.adminToken)
	}

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("promote call to %q: %w", req.Candidate, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("promote on %q returned status %d", req.Candidate, resp.StatusCode)
	}

	return nil
}

// pgCtlLocalPromoter implements httpapi.LocalPromoter via pg_ctl promote.
type pgCtlLocalPromoter struct {
	pgCtl *postgres.PGCtl
}

func (p *pgCtlLocalPromoter) PromoteLocal(ctx context.Context) error {
	return p.pgCtl.Promote(ctx)
}

func (daemon *Daemon) reconcileSwitchover(ctx context.Context) {
	if daemon.pgCtl == nil {
		return
	}

	engine, ok := daemon.statePublisher.(controlplane.SwitchoverEngine)
	if !ok {
		return
	}

	discovery, ok := daemon.statePublisher.(controlplane.MemberDiscovery)
	if !ok {
		return
	}

	// Only the current primary drives switchover execution.
	if daemon.Heartbeat().Postgres.Role != cluster.MemberRolePrimary {
		return
	}

	demoter := &localDemoter{pgCtl: daemon.pgCtl}
	promoter := &apiPromoter{
		client:     &http.Client{Timeout: 30 * time.Second},
		adminToken: daemon.adminToken,
		discovery:  discovery,
	}

	execution, err := engine.ExecuteSwitchover(ctx, demoter, promoter)
	if err != nil {
		if errors.Is(err, controlplane.ErrSwitchoverIntentRequired) ||
			errors.Is(err, controlplane.ErrSwitchoverExecutionNotReady) {
			return
		}

		daemon.logger.WarnContext(ctx, "switchover execution failed",
			daemon.logArgs("agent", slog.String("error", err.Error()))...)
		return
	}

	daemon.logger.InfoContext(ctx, "switchover executed",
		daemon.logArgs("agent",
			slog.String("from_primary", execution.CurrentPrimary),
			slog.String("to_candidate", execution.Candidate),
			slog.String("epoch", execution.CurrentEpoch.String()),
		)...)
}

func memberAPIURL(address string) string {
	if address == "" {
		return ""
	}

	return "http://" + address
}
