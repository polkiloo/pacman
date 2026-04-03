package httpapi

import (
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

type switchoverRequestJSON struct {
	Candidate   string     `json:"candidate"`
	ScheduledAt *time.Time `json:"scheduledAt,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	RequestedBy string     `json:"requestedBy,omitempty"`
}

type failoverRequestJSON struct {
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requestedBy,omitempty"`
}

type operationAcceptedResponse struct {
	Message   string        `json:"message,omitempty"`
	Operation operationJSON `json:"operation"`
}

func (srv *Server) handleSwitchoverCreate(c *fiber.Ctx) error {
	if len(c.Body()) == 0 {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_switchover_request", "switchover request body must be valid JSON")
	}

	var requestBody switchoverRequestJSON
	if err := c.BodyParser(&requestBody); err != nil {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_switchover_request", "switchover request body must be valid JSON")
	}

	request := controlplane.SwitchoverRequest{
		Candidate:   strings.TrimSpace(requestBody.Candidate),
		Reason:      strings.TrimSpace(requestBody.Reason),
		RequestedBy: strings.TrimSpace(requestBody.RequestedBy),
	}
	if requestBody.ScheduledAt != nil {
		request.ScheduledAt = requestBody.ScheduledAt.UTC()
	}

	intent, err := srv.store.CreateSwitchoverIntent(c.UserContext(), request)
	if err != nil {
		return writeSwitchoverCreateError(c, err)
	}

	return c.Status(fiber.StatusAccepted).JSON(buildOperationAcceptedResponse(intent.Operation))
}

func (srv *Server) handleSwitchoverCancel(c *fiber.Ctx) error {
	cancelled, err := srv.store.CancelSwitchover(c.UserContext())
	if err != nil {
		return writeSwitchoverCancelError(c, err)
	}

	return c.JSON(buildOperationAcceptedResponse(cancelled))
}

func (srv *Server) handleFailoverCreate(c *fiber.Ctx) error {
	if len(c.Body()) == 0 {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_failover_request", "failover request body must be valid JSON")
	}

	var requestBody failoverRequestJSON
	if err := c.BodyParser(&requestBody); err != nil {
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_failover_request", "failover request body must be valid JSON")
	}

	intent, err := srv.store.CreateFailoverIntent(c.UserContext(), normalizeFailoverRequest(requestBody))
	if err != nil {
		return writeFailoverCreateError(c, err)
	}

	return c.Status(fiber.StatusAccepted).JSON(buildOperationAcceptedResponse(intent.Operation))
}

func (srv *Server) handleOpenAPIDocument(c *fiber.Ctx) error {
	document, err := srv.publishedOpenAPIDocument()
	if err != nil {
		if srv.logger != nil {
			srv.logger.Error(
				"published openapi document unavailable",
				slog.String("component", "httpapi"),
				slog.String("path", c.Path()),
				slog.String("error", err.Error()),
			)
		}

		applyAPIResponseHeaders(c)
		return writeAPIError(c, fiber.StatusServiceUnavailable, "openapi_unavailable", "openapi document unavailable")
	}

	applyAPIResponseHeaders(c)
	c.Set(fiber.HeaderContentType, "application/yaml; charset=utf-8")
	return c.Send(document)
}

func (srv *Server) publishedOpenAPIDocument() ([]byte, error) {
	srv.openAPILoad.Do(func() {
		if srv.openAPIDoc == nil {
			srv.openAPIErr = errors.New("openapi document provider is nil")
			return
		}

		document, err := srv.openAPIDoc()
		if err != nil {
			srv.openAPIErr = err
			return
		}

		srv.openAPIBytes = append([]byte(nil), document...)
	})

	if srv.openAPIErr != nil {
		return nil, srv.openAPIErr
	}

	return append([]byte(nil), srv.openAPIBytes...), nil
}

func buildOperationAcceptedResponse(operation cluster.Operation) operationAcceptedResponse {
	return operationAcceptedResponse{
		Message:   strings.TrimSpace(operation.Message),
		Operation: buildOperationJSON(operation),
	}
}

func normalizeFailoverRequest(body failoverRequestJSON) controlplane.FailoverIntentRequest {
	request := controlplane.FailoverIntentRequest{
		Reason:      strings.TrimSpace(body.Reason),
		RequestedBy: strings.TrimSpace(body.RequestedBy),
	}

	if request.RequestedBy == "" {
		request.RequestedBy = "operator"
	}

	if request.Reason == "" {
		request.Reason = "manual failover"
	}

	return request
}

func writeSwitchoverCreateError(c *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, controlplane.ErrSwitchoverTargetRequired),
		errors.Is(err, controlplane.ErrSwitchoverSchedulingNotAllowed):
		return writeAPIError(c, fiber.StatusBadRequest, "invalid_switchover_request", err.Error())
	case errors.Is(err, controlplane.ErrSwitchoverOperationInProgress):
		return writeAPIError(c, fiber.StatusConflict, "switchover_conflict", err.Error())
	case errors.Is(err, controlplane.ErrSwitchoverPrimaryUnknown),
		errors.Is(err, controlplane.ErrSwitchoverPrimaryUnhealthy),
		errors.Is(err, controlplane.ErrSwitchoverTargetUnknown),
		errors.Is(err, controlplane.ErrSwitchoverTargetNotReady),
		errors.Is(err, controlplane.ErrSwitchoverTargetIsCurrentPrimary):
		return writeAPIError(c, fiber.StatusPreconditionFailed, "switchover_precondition_failed", err.Error())
	case errors.Is(err, controlplane.ErrClusterSpecRequired),
		errors.Is(err, controlplane.ErrSwitchoverObservedStateRequired):
		return writeAPIError(c, fiber.StatusServiceUnavailable, "switchover_unavailable", err.Error())
	default:
		return writeAPIError(c, fiber.StatusInternalServerError, "internal_error", "failed to create switchover intent")
	}
}

func writeSwitchoverCancelError(c *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, controlplane.ErrScheduledSwitchoverNotFound),
		errors.Is(err, controlplane.ErrSwitchoverIntentRequired):
		return writeAPIError(c, fiber.StatusNotFound, "scheduled_switchover_not_found", err.Error())
	case errors.Is(err, controlplane.ErrSwitchoverAlreadyRunning):
		return writeAPIError(c, fiber.StatusConflict, "switchover_conflict", err.Error())
	default:
		return writeAPIError(c, fiber.StatusInternalServerError, "internal_error", "failed to cancel switchover")
	}
}

func writeFailoverCreateError(c *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, controlplane.ErrFailoverOperationInProgress):
		return writeAPIError(c, fiber.StatusConflict, "failover_conflict", err.Error())
	case errors.Is(err, controlplane.ErrFailoverPrimaryUnknown),
		errors.Is(err, controlplane.ErrFailoverPrimaryHealthy),
		errors.Is(err, controlplane.ErrFailoverQuorumUnavailable),
		errors.Is(err, controlplane.ErrFailoverNoEligibleCandidates):
		return writeAPIError(c, fiber.StatusPreconditionFailed, "failover_precondition_failed", err.Error())
	case errors.Is(err, controlplane.ErrClusterSpecRequired),
		errors.Is(err, controlplane.ErrFailoverObservedStateRequired),
		errors.Is(err, controlplane.ErrFailoverMaintenanceEnabled),
		errors.Is(err, controlplane.ErrAutomaticFailoverNotAllowed):
		return writeAPIError(c, fiber.StatusServiceUnavailable, "failover_unavailable", err.Error())
	default:
		return writeAPIError(c, fiber.StatusInternalServerError, "internal_error", "failed to create failover intent")
	}
}
