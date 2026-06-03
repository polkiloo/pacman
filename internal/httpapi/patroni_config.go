package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/polkiloo/pacman/internal/cluster"
)

var errUnsupportedPatroniConfigPatch = errors.New("only pause config patches are supported")

func (srv *Server) handlePatroniConfigGet(c *fiber.Ctx) error {
	spec, ok := srv.store.ClusterSpec()
	if !ok {
		return writeAPIError(c, fiber.StatusServiceUnavailable, "cluster_spec_unavailable", "cluster spec unavailable")
	}

	return c.JSON(buildPatroniDynamicConfig(spec, srv.store.MaintenanceStatus()))
}

func (srv *Server) handlePatroniConfigPatch(c *fiber.Ctx) error {
	if len(c.Body()) == 0 {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_config_request", "config patch body must be valid JSON")
	}

	var patch map[string]json.RawMessage
	if err := json.Unmarshal(c.Body(), &patch); err != nil {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_config_request", "config patch body must be valid JSON")
	}

	pauseValue, hasPause, err := parsePatroniPausePatch(patch)
	if err != nil {
		if errors.Is(err, errUnsupportedPatroniConfigPatch) {
			return writeAPIError(c, fiber.StatusBadRequest, "unsupported_config_patch", err.Error())
		}
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_config_request", err.Error())
	}
	if !hasPause {
		return writeAPIError(c, fiber.StatusBadRequest, "unsupported_config_patch", errUnsupportedPatroniConfigPatch.Error())
	}

	updated, err := srv.store.UpdateMaintenanceMode(c.UserContext(), cluster.MaintenanceModeUpdateRequest{
		Enabled:     pauseValue,
		Reason:      "patronictl pause/resume",
		RequestedBy: "patronictl",
	})
	if err != nil {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_maintenance_request", err.Error())
	}

	srv.logRequest(
		c,
		slog.LevelInfo,
		"updated maintenance mode",
		append(
			auditLogAttrs("maintenance_mode.update"),
			slog.Bool("maintenance_enabled", updated.Enabled),
			slog.String("reason", updated.Reason),
			slog.String("requested_by", updated.RequestedBy),
		)...,
	)

	if spec, ok := srv.store.ClusterSpec(); ok {
		return c.JSON(buildPatroniDynamicConfig(spec, updated))
	}

	return c.JSON(map[string]any{"pause": updated.Enabled})
}

func parsePatroniPausePatch(patch map[string]json.RawMessage) (bool, bool, error) {
	raw, ok := patch["pause"]
	if !ok {
		return false, false, nil
	}
	if len(patch) != 1 {
		return false, true, errUnsupportedPatroniConfigPatch
	}

	if len(raw) == 0 || strings.EqualFold(strings.TrimSpace(string(raw)), "null") {
		return false, true, nil
	}

	var enabled bool
	if err := json.Unmarshal(raw, &enabled); err != nil {
		return false, true, err
	}

	return enabled, true, nil
}

func buildPatroniDynamicConfig(spec cluster.ClusterSpec, maintenance cluster.MaintenanceModeStatus) map[string]any {
	config := map[string]any{
		"pause":                   maintenance.Enabled,
		"maximum_lag_on_failover": spec.Failover.MaximumLagBytes,
		"check_timeline":          spec.Failover.CheckTimeline,
		"postgresql": map[string]any{
			"use_pg_rewind": spec.Postgres.UsePgRewind,
			"parameters":    spec.Postgres.Parameters,
		},
	}

	switch spec.Postgres.SynchronousMode {
	case cluster.SynchronousModeQuorum:
		config["synchronous_mode"] = true
		config["synchronous_mode_strict"] = false
	case cluster.SynchronousModeStrict:
		config["synchronous_mode"] = true
		config["synchronous_mode_strict"] = true
	case cluster.SynchronousModeDisabled, "":
		config["synchronous_mode"] = false
		config["synchronous_mode_strict"] = false
	default:
		config["synchronous_mode"] = string(spec.Postgres.SynchronousMode)
	}

	return config
}
