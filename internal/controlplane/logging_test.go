package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	paclog "github.com/polkiloo/pacman/internal/logging"
)

// safeBuffer wraps bytes.Buffer with a mutex so it can be used as the
// slog handler's io.Writer while a background cache-watch goroutine may
// also call Write concurrently.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *safeBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func TestMemoryStateStoreStoreClusterSpecLogsTopologyAudit(t *testing.T) {
	t.Parallel()

	var buffer safeBuffer
	store := NewMemoryStateStore()
	store.logger = slog.New(slog.NewJSONHandler(&buffer, nil))
	setTestNow(store, func() time.Time {
		return time.Date(2026, time.April, 15, 13, 0, 0, 0, time.UTC)
	})

	ctx := paclog.WithNode(
		paclog.WithRequestID(
			paclog.WithPrincipalMechanism(
				paclog.WithPrincipalSubject(context.Background(), "ops@example"),
				"bearer",
			),
			"req-topology",
		),
		"alpha-api",
	)

	_, err := store.StoreClusterSpec(ctx, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	})
	if err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	auditEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "stored cluster topology")
	assertControlPlaneLogString(t, auditEntry, "event_category", "audit")
	assertControlPlaneLogString(t, auditEntry, "audit_action", "cluster_topology.update")
	assertControlPlaneLogString(t, auditEntry, "request_id", "req-topology")
	assertControlPlaneLogString(t, auditEntry, "node", "alpha-api")
	assertControlPlaneLogString(t, auditEntry, "principal_subject", "ops@example")
	assertControlPlaneLogString(t, auditEntry, "principal_mechanism", "bearer")
	assertControlPlaneLogStrings(t, auditEntry, "added_members", []string{"alpha-1", "alpha-2"})
	assertControlPlaneLogFloat(t, auditEntry, "member_count", 2)

	clusterEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "cluster source of truth updated")
	assertControlPlaneLogString(t, clusterEntry, "event_category", "state_transition")
	assertControlPlaneLogString(t, clusterEntry, "transition", "cluster")
	assertControlPlaneLogString(t, clusterEntry, "phase", "initializing")
}

func TestMemoryStateStoreUpdateMaintenanceModeLogsAuditWithOperationCorrelation(t *testing.T) {
	t.Parallel()

	var buffer safeBuffer
	store := NewMemoryStateStore()
	store.logger = slog.New(slog.NewJSONHandler(&buffer, nil))
	setTestNow(store, func() time.Time {
		return time.Date(2026, time.April, 15, 13, 30, 0, 0, time.UTC)
	})

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
		},
	}); err != nil {
		t.Fatalf("seed cluster spec: %v", err)
	}
	buffer.Reset()

	ctx := paclog.WithNode(
		paclog.WithRequestID(
			paclog.WithPrincipalSubject(context.Background(), "ops@example"),
			"req-maintenance",
		),
		"alpha-api",
	)

	_, err := store.UpdateMaintenanceMode(ctx, cluster.MaintenanceModeUpdateRequest{
		Enabled:     true,
		Reason:      "weekly backup",
		RequestedBy: "ops@example",
	})
	if err != nil {
		t.Fatalf("update maintenance mode: %v", err)
	}

	auditEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "updated maintenance mode")
	assertControlPlaneLogString(t, auditEntry, "event_category", "audit")
	assertControlPlaneLogString(t, auditEntry, "audit_action", "maintenance_mode.update")
	assertControlPlaneLogString(t, auditEntry, "request_id", "req-maintenance")
	assertControlPlaneLogString(t, auditEntry, "node", "alpha-api")
	assertControlPlaneLogString(t, auditEntry, "operation_kind", "maintenance_change")
	assertControlPlaneLogString(t, auditEntry, "operation_state", "completed")
	assertControlPlaneLogBool(t, auditEntry, "maintenance_enabled", true)
	assertControlPlaneLogBool(t, auditEntry, "previous_enabled", false)
	assertControlPlaneLogString(t, auditEntry, "reason", "weekly backup")
	assertControlPlaneLogPrefix(t, auditEntry, "operation_id", "maintenance-")

	transitionEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "operation state changed")
	assertControlPlaneLogString(t, transitionEntry, "transition", "operation_state")
	assertControlPlaneLogString(t, transitionEntry, "operation_state", "completed")
}

func TestMemoryStateStorePublishNodeStatusLogsMemberStateTransition(t *testing.T) {
	t.Parallel()

	var buffer safeBuffer
	store := NewMemoryStateStore()
	store.logger = slog.New(slog.NewJSONHandler(&buffer, nil))
	setTestLeaseDuration(store, time.Hour)

	firstObservedAt := time.Date(2026, time.April, 15, 14, 0, 0, 0, time.UTC)
	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName:   "alpha-1",
		MemberName: "alpha-1",
		Role:       cluster.MemberRolePrimary,
		State:      cluster.MemberStateRunning,
		ObservedAt: firstObservedAt,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      true,
			Details: agentmodel.PostgresDetails{
				Timeline:            9,
				ReplicationLagBytes: 0,
			},
		},
	}); err != nil {
		t.Fatalf("publish initial node status: %v", err)
	}
	buffer.Reset()

	if _, err := store.PublishNodeStatus(context.Background(), agentmodel.NodeStatus{
		NodeName:    "alpha-1",
		MemberName:  "alpha-1",
		Role:        cluster.MemberRoleReplica,
		State:       cluster.MemberStateNeedsRejoin,
		ObservedAt:  firstObservedAt.Add(time.Minute),
		NeedsRejoin: true,
		Postgres: agentmodel.PostgresStatus{
			Managed: true,
			Up:      false,
			Details: agentmodel.PostgresDetails{
				Timeline:            10,
				ReplicationLagBytes: 128,
			},
		},
	}); err != nil {
		t.Fatalf("publish changed node status: %v", err)
	}

	entry := findControlPlaneLogEntryByMessage(t, buffer.String(), "observed member state change")
	assertControlPlaneLogString(t, entry, "event_category", "state_transition")
	assertControlPlaneLogString(t, entry, "transition", "member_state")
	assertControlPlaneLogString(t, entry, "node", "alpha-1")
	assertControlPlaneLogString(t, entry, "member", "alpha-1")
	assertControlPlaneLogString(t, entry, "state", "needs_rejoin")
	assertControlPlaneLogString(t, entry, "previous_state", "running")
	assertControlPlaneLogBool(t, entry, "healthy", false)
	assertControlPlaneLogBool(t, entry, "previous_healthy", true)
	assertControlPlaneLogBool(t, entry, "needs_rejoin", true)
	assertControlPlaneLogFloat(t, entry, "timeline", 10)
	assertControlPlaneLogFloat(t, entry, "previous_timeline", 9)
}

func TestMemoryStateStoreCreateSwitchoverIntentLogsAuditAndOperationState(t *testing.T) {
	t.Parallel()

	var buffer safeBuffer
	now := time.Date(2026, time.April, 15, 14, 30, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover: cluster.SwitchoverPolicy{
			AllowScheduled: true,
		},
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		readyPrimaryStatus("alpha-1", now, 18),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 18, 0),
	})
	store.logger = slog.New(slog.NewJSONHandler(&buffer, nil))
	setTestNow(store, func() time.Time { return now.Add(5 * time.Minute) })

	ctx := paclog.WithMember(
		paclog.WithNode(
			paclog.WithRequestID(
				paclog.WithPrincipalSubject(context.Background(), "ops@example"),
				"req-switchover",
			),
			"alpha-api",
		),
		"alpha-2",
	)

	_, err := store.CreateSwitchoverIntent(ctx, SwitchoverRequest{
		RequestedBy: "ops@example",
		Reason:      "planned maintenance",
		Candidate:   "alpha-2",
	})
	if err != nil {
		t.Fatalf("create switchover intent: %v", err)
	}

	auditEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "accepted switchover intent")
	assertControlPlaneLogString(t, auditEntry, "event_category", "audit")
	assertControlPlaneLogString(t, auditEntry, "audit_action", "switchover.requested")
	assertControlPlaneLogString(t, auditEntry, "request_id", "req-switchover")
	assertControlPlaneLogString(t, auditEntry, "node", "alpha-api")
	assertControlPlaneLogString(t, auditEntry, "principal_subject", "ops@example")
	assertControlPlaneLogString(t, auditEntry, "member", "alpha-2")
	assertControlPlaneLogString(t, auditEntry, "operation_kind", "switchover")
	assertControlPlaneLogString(t, auditEntry, "operation_state", "accepted")

	transitionEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "operation state changed")
	assertControlPlaneLogString(t, transitionEntry, "transition", "operation_state")
	assertControlPlaneLogString(t, transitionEntry, "operation_state", "accepted")

	clusterEntry := findControlPlaneLogEntryByMessage(t, buffer.String(), "cluster source of truth updated")
	assertControlPlaneLogString(t, clusterEntry, "transition", "cluster")
	assertControlPlaneLogString(t, clusterEntry, "operation_kind", "switchover")
	assertControlPlaneLogString(t, clusterEntry, "operation_state", "accepted")
}

func findControlPlaneLogEntryByMessage(t *testing.T, payload, message string) map[string]any {
	t.Helper()

	for _, entry := range parseControlPlaneLogEntries(t, payload) {
		if got, _ := entry["msg"].(string); got == message {
			return entry
		}
	}

	t.Fatalf("control-plane log message %q not found in %q", message, payload)
	return nil
}

func parseControlPlaneLogEntries(t *testing.T, payload string) []map[string]any {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(payload), "\n")
	entries := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		var entry map[string]any
		if err := json.Unmarshal([]byte(trimmed), &entry); err != nil {
			t.Fatalf("unmarshal control-plane log entry %q: %v", trimmed, err)
		}

		entries = append(entries, entry)
	}

	return entries
}

func assertControlPlaneLogString(t *testing.T, entry map[string]any, key, want string) {
	t.Helper()

	got, ok := entry[key].(string)
	if !ok {
		t.Fatalf("control-plane log field %q missing or not a string: %+v", key, entry)
	}
	if got != want {
		t.Fatalf("control-plane log field %q: got %q, want %q", key, got, want)
	}
}

func assertControlPlaneLogPrefix(t *testing.T, entry map[string]any, key, prefix string) {
	t.Helper()

	got, ok := entry[key].(string)
	if !ok {
		t.Fatalf("control-plane log field %q missing or not a string: %+v", key, entry)
	}
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("control-plane log field %q: got %q, want prefix %q", key, got, prefix)
	}
}

func assertControlPlaneLogBool(t *testing.T, entry map[string]any, key string, want bool) {
	t.Helper()

	got, ok := entry[key].(bool)
	if !ok {
		t.Fatalf("control-plane log field %q missing or not a bool: %+v", key, entry)
	}
	if got != want {
		t.Fatalf("control-plane log field %q: got %v, want %v", key, got, want)
	}
}

func assertControlPlaneLogFloat(t *testing.T, entry map[string]any, key string, want float64) {
	t.Helper()

	got, ok := entry[key].(float64)
	if !ok {
		t.Fatalf("control-plane log field %q missing or not numeric: %+v", key, entry)
	}
	if got != want {
		t.Fatalf("control-plane log field %q: got %v, want %v", key, got, want)
	}
}

func assertControlPlaneLogStrings(t *testing.T, entry map[string]any, key string, want []string) {
	t.Helper()

	items, ok := entry[key].([]any)
	if !ok {
		t.Fatalf("control-plane log field %q missing or not an array: %+v", key, entry)
	}

	got := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			t.Fatalf("control-plane log field %q contains non-string value %T", key, item)
		}
		got = append(got, value)
	}

	if len(got) != len(want) {
		t.Fatalf("control-plane log field %q: got %v, want %v", key, got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("control-plane log field %q: got %v, want %v", key, got, want)
		}
	}
}
