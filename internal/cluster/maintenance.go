package cluster

import (
	"strings"
)

// MaintenanceModeUpdateRequest captures an explicit API or CLI request to
// change cluster-wide maintenance mode.
type MaintenanceModeUpdateRequest struct {
	Enabled     bool
	Reason      string
	RequestedBy string
}

// Validate reports whether the request is coherent enough to be accepted.
func (request MaintenanceModeUpdateRequest) Validate() error {
	return nil
}

// EffectiveReason resolves the request reason using the provided default when
// the request did not include an explicit one.
func (request MaintenanceModeUpdateRequest) EffectiveReason(defaultReason string) string {
	if reason := strings.TrimSpace(request.Reason); reason != "" {
		return reason
	}

	return strings.TrimSpace(defaultReason)
}

// Validate reports whether the desired maintenance configuration is coherent
// enough to be accepted by the control plane.
func (state MaintenanceDesiredState) Validate() error {
	return nil
}

// EffectiveReason resolves an explicit maintenance reason using the desired
// default when one was configured.
func (state MaintenanceDesiredState) EffectiveReason(reason string) string {
	if trimmed := strings.TrimSpace(reason); trimmed != "" {
		return trimmed
	}

	return strings.TrimSpace(state.DefaultReason)
}

// Validate reports whether the effective maintenance status is coherent enough
// to be published.
func (status MaintenanceModeStatus) Validate() error {
	hasMetadata := status.Enabled || strings.TrimSpace(status.Reason) != "" || strings.TrimSpace(status.RequestedBy) != ""
	if hasMetadata && status.UpdatedAt.IsZero() {
		return ErrMaintenanceUpdatedAtRequired
	}

	return nil
}
