package controlplane

import (
	"context"
	"log/slog"
	"reflect"
	"sort"
	"strings"

	"github.com/polkiloo/pacman/internal/cluster"
	paclog "github.com/polkiloo/pacman/internal/logging"
)

const (
	controlPlaneEventCategoryAudit       = "audit"
	controlPlaneEventCategoryLifecycle   = "lifecycle"
	controlPlaneEventCategoryTransition  = "state_transition"
	controlPlaneTransitionCluster        = "cluster"
	controlPlaneTransitionLeaderLease    = "leader_lease"
	controlPlaneTransitionMemberState    = "member_state"
	controlPlaneTransitionOperationState = "operation_state"
)

func (store *MemoryStateStore) log(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	if store.logger == nil {
		return
	}

	base := []slog.Attr{
		slog.String("component", "controlplane"),
		slog.String("cluster", store.clusterName),
	}
	base = append(base, paclog.AttrsFromContext(contextOrBackground(ctx))...)
	base = append(base, attrs...)

	store.logger.LogAttrs(contextOrBackground(ctx), level, msg, base...)
}

func (store *MemoryStateStore) logAudit(ctx context.Context, msg, action string, attrs ...slog.Attr) {
	base := []slog.Attr{
		slog.String("event_category", controlPlaneEventCategoryAudit),
		slog.String("audit_action", action),
	}
	base = append(base, attrs...)

	store.log(ctx, slog.LevelInfo, msg, base...)
}

func (store *MemoryStateStore) logLifecycle(ctx context.Context, msg string, attrs ...slog.Attr) {
	base := []slog.Attr{slog.String("event_category", controlPlaneEventCategoryLifecycle)}
	base = append(base, attrs...)

	store.log(ctx, slog.LevelInfo, msg, base...)
}

func (store *MemoryStateStore) logTransition(ctx context.Context, msg, transition string, attrs ...slog.Attr) {
	base := []slog.Attr{
		slog.String("event_category", controlPlaneEventCategoryTransition),
		slog.String("transition", transition),
	}
	base = append(base, attrs...)

	store.log(ctx, slog.LevelInfo, msg, base...)
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}

func operationLogAttrs(operation cluster.Operation) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("operation_id", operation.ID),
		slog.String("operation_kind", string(operation.Kind)),
		slog.String("operation_state", string(operation.State)),
	}

	if !operation.Result.IsZero() {
		attrs = append(attrs, slog.String("operation_result", string(operation.Result)))
	}
	if member := operationMember(operation); member != "" {
		attrs = append(attrs, slog.String("member", member))
	}
	if fromMember := strings.TrimSpace(operation.FromMember); fromMember != "" {
		attrs = append(attrs, slog.String("from_member", fromMember))
	}
	if toMember := strings.TrimSpace(operation.ToMember); toMember != "" {
		attrs = append(attrs, slog.String("to_member", toMember))
	}

	return attrs
}

func operationMember(operation cluster.Operation) string {
	if member := strings.TrimSpace(operation.ToMember); member != "" {
		return member
	}

	return strings.TrimSpace(operation.FromMember)
}

func clusterStatusTransitionAttrs(previous *cluster.ClusterStatus, current cluster.ClusterStatus) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("phase", string(current.Phase)),
		slog.String("current_primary", current.CurrentPrimary),
		slog.Int64("current_epoch", int64(current.CurrentEpoch)),
		slog.Int("member_count", len(current.Members)),
		slog.Bool("maintenance_enabled", current.Maintenance.Enabled),
	}

	if previous != nil {
		attrs = append(
			attrs,
			slog.String("previous_phase", string(previous.Phase)),
			slog.String("previous_primary", previous.CurrentPrimary),
			slog.Int64("previous_epoch", int64(previous.CurrentEpoch)),
			slog.Bool("previous_maintenance_enabled", previous.Maintenance.Enabled),
		)
		if previous.ActiveOperation != nil {
			attrs = append(
				attrs,
				slog.String("previous_operation_id", previous.ActiveOperation.ID),
				slog.String("previous_operation_kind", string(previous.ActiveOperation.Kind)),
				slog.String("previous_operation_state", string(previous.ActiveOperation.State)),
			)
		}
	}

	if current.ActiveOperation != nil {
		attrs = append(attrs, operationLogAttrs(*current.ActiveOperation)...)
	}

	return attrs
}

func clusterStatusChanged(previous *cluster.ClusterStatus, current cluster.ClusterStatus) bool {
	if previous == nil {
		return true
	}

	if previous.Phase != current.Phase ||
		previous.CurrentPrimary != current.CurrentPrimary ||
		previous.CurrentEpoch != current.CurrentEpoch ||
		previous.Maintenance.Enabled != current.Maintenance.Enabled ||
		len(previous.Members) != len(current.Members) {
		return true
	}

	return !sameOperationState(previous.ActiveOperation, current.ActiveOperation)
}

func sameOperationState(left, right *cluster.Operation) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return left.ID == right.ID &&
			left.Kind == right.Kind &&
			left.State == right.State &&
			left.Result == right.Result
	}
}

func clusterSpecTopologyDiff(previous *cluster.ClusterSpec, current cluster.ClusterSpec) (added []string, removed []string, updated []string) {
	previousMembers := make(map[string]cluster.MemberSpec)
	if previous != nil {
		for _, member := range previous.Members {
			previousMembers[member.Name] = member.Clone()
		}
	}

	currentMembers := make(map[string]cluster.MemberSpec, len(current.Members))
	for _, member := range current.Members {
		currentMembers[member.Name] = member.Clone()
	}

	for name, member := range currentMembers {
		previousMember, ok := previousMembers[name]
		if !ok {
			added = append(added, name)
			continue
		}

		if !reflect.DeepEqual(previousMember, member) {
			updated = append(updated, name)
		}
	}

	for name := range previousMembers {
		if _, ok := currentMembers[name]; !ok {
			removed = append(removed, name)
		}
	}

	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(updated)

	return added, removed, updated
}
