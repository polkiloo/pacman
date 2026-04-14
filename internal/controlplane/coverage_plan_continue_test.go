package controlplane

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// ---------------------------------------------------------------------------
// switchover_plan.go — uncovered branches
// ---------------------------------------------------------------------------

// TestSwitchoverTargetReasonsNonStandbyMemberRole exercises the
// reasonRoleNotStandby branch in switchoverTargetReasons when the target's
// cluster-status role is not a standby type.
func TestSwitchoverTargetReasonsNonStandbyMemberRole(t *testing.T) {
	t.Parallel()

	checkedAt := time.Date(2026, time.April, 14, 14, 0, 0, 0, time.UTC)

	// target has MemberRolePrimary — not a valid switchover standby
	readiness := buildSwitchoverTargetReadiness(
		cluster.ClusterSpec{ClusterName: "alpha"},
		cluster.MemberStatus{Name: "alpha-1", Role: cluster.MemberRolePrimary, Healthy: true},
		cluster.MemberStatus{
			Name:    "alpha-2",
			Role:    cluster.MemberRolePrimary, // non-standby
			State:   cluster.MemberStateRunning,
			Healthy: true,
		},
		agentmodel.NodeStatus{},
		false,
		checkedAt,
	)

	if readiness.Ready {
		t.Fatal("expected non-standby-role target to not be ready")
	}
	if !containsString(readiness.Reasons, reasonRoleNotStandby) {
		t.Fatalf("expected reasonRoleNotStandby in reasons, got %v", readiness.Reasons)
	}
}

// TestCreateSwitchoverIntentRejectsUnhealthyPrimaryInLockedPath exercises the
// store.mu.Unlock(); return path in CreateSwitchoverIntent when
// evaluateSwitchoverRequestLocked fails due to an unhealthy primary. This is
// distinct from the ValidateSwitchover path that carries the same check.
func TestCreateSwitchoverIntentRejectsUnhealthyPrimaryInLockedPath(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 14, 0, 0, 0, time.UTC)

	store := NewMemoryStateStore()
	setTestNow(store, func() time.Time { return now })
	setTestLeaseDuration(store, time.Hour)

	if _, err := store.StoreClusterSpec(context.Background(), cluster.ClusterSpec{
		ClusterName: "alpha",
		Switchover:  cluster.SwitchoverPolicy{AllowScheduled: true},
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}); err != nil {
		t.Fatalf("store cluster spec: %v", err)
	}

	// Unhealthy primary (State=Failed, up=false) so evaluateSwitchoverRequestLocked
	// returns ErrSwitchoverPrimaryUnhealthy while the mutex is held.
	for _, ns := range []agentmodel.NodeStatus{
		failoverNodeStatus("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateFailed, now, false, 19, 0),
		readyStandbyStatus("alpha-2", now.Add(time.Second), 19, 0),
	} {
		if _, err := store.PublishNodeStatus(context.Background(), ns); err != nil {
			t.Fatalf("publish node status for %q: %v", ns.NodeName, err)
		}
	}

	_, err := store.CreateSwitchoverIntent(context.Background(), SwitchoverRequest{Candidate: "alpha-2"})
	if !errors.Is(err, ErrSwitchoverPrimaryUnhealthy) {
		t.Fatalf("expected ErrSwitchoverPrimaryUnhealthy, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// rejoin_continue.go — uncovered branches
// ---------------------------------------------------------------------------

// TestExecuteRejoinStandbyConfigRecordsFailure verifies that when the standby
// configurator returns an error, ExecuteRejoinStandbyConfig journals the
// operation as failed (via failRejoinExecution) and clears the active
// operation. This covers the ConfigureStandby error path in the function.
func TestExecuteRejoinStandbyConfigRecordsFailure(t *testing.T) {
	now := time.Date(2026, time.March, 30, 9, 0, 0, 0, time.UTC)
	store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second))

	configurator := &recordingStandbyConfigurer{err: errors.New("configure failed")}
	_, err := store.ExecuteRejoinStandbyConfig(context.Background(), configurator)
	if err == nil {
		t.Fatal("expected standby config failure error")
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed standby config to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed rejoin history entry after config failure, got %+v", history)
	}
	if history[0].Kind != cluster.OperationKindRejoin || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed rejoin history entry: %+v", history[0])
	}
}

// TestActiveRejoinOperationLockedRejectsBlankMembers covers the branch in
// activeRejoinOperationLocked that returns ErrRejoinExecutionRequired when
// FromMember or ToMember is blank. The other error branches are already
// covered in TestActiveRejoinOperationLockedRejectsNilAndWrongKind.
func TestActiveRejoinOperationLockedRejectsBlankMembers(t *testing.T) {
	t.Parallel()

	op := cluster.Operation{
		ID:    "rj-blank",
		Kind:  cluster.OperationKindRejoin,
		State: cluster.OperationStateRunning,
		// FromMember and ToMember deliberately left empty
	}
	store := &MemoryStateStore{activeOperation: &op}
	_, err := store.activeRejoinOperationLocked()
	if !errors.Is(err, ErrRejoinExecutionRequired) {
		t.Fatalf("expected ErrRejoinExecutionRequired for blank members, got %v", err)
	}
}

// TestPrepareActiveRejoinContinuationLockedRejectsExpiredMemberNode covers the
// ErrRejoinExecutionChanged branch when the former-primary node status is
// absent from the in-memory cache (e.g., after a TTL expiry) at the point
// prepareActiveRejoinContinuationLocked runs.
func TestPrepareActiveRejoinContinuationLockedRejectsExpiredMemberNode(t *testing.T) {
	now := time.Date(2026, time.March, 30, 9, 30, 0, 0, time.UTC)
	store := seededPreparedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second))

	// Remove the current-primary node status so hasCurrentPrimaryNode becomes
	// false. memberLocked still finds alpha-1 via nodeStatuses, so
	// rejoinInputsLocked succeeds. ensureCacheFresh will not reload from DCS
	// (cache is not dirty), so the missing status is seen by
	// prepareActiveRejoinContinuationLocked.
	store.mu.Lock()
	delete(store.nodeStatuses, "alpha-2")
	store.mu.Unlock()

	_, err := store.ExecuteRejoinStandbyConfig(context.Background(), &recordingStandbyConfigurer{})
	if !errors.Is(err, ErrRejoinExecutionChanged) {
		t.Fatalf("expected ErrRejoinExecutionChanged when primary node missing, got %v", err)
	}
}

// TestRejoinPrimarySlotNameEdgeCases covers the three special-case branches in
// rejoinPrimarySlotName that return "pacman_rejoin" and the >63-char truncation
// path.
func TestRejoinPrimarySlotNameEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty string returns fallback", func(t *testing.T) {
		t.Parallel()
		if got := rejoinPrimarySlotName(""); got != "pacman_rejoin" {
			t.Fatalf("expected pacman_rejoin for empty string, got %q", got)
		}
	})

	t.Run("whitespace-only returns fallback", func(t *testing.T) {
		t.Parallel()
		if got := rejoinPrimarySlotName("   "); got != "pacman_rejoin" {
			t.Fatalf("expected pacman_rejoin for whitespace, got %q", got)
		}
	})

	t.Run("all-special-chars collapses to empty slot", func(t *testing.T) {
		t.Parallel()
		// "!@#" → all chars hit the default branch → single "_" → Trim("_") = "" →
		// returns the fallback.
		if got := rejoinPrimarySlotName("!@#"); got != "pacman_rejoin" {
			t.Fatalf("expected pacman_rejoin for all-special input, got %q", got)
		}
	})

	t.Run("name longer than 63 chars is truncated", func(t *testing.T) {
		t.Parallel()
		long := strings.Repeat("a", 64)
		got := rejoinPrimarySlotName(long)
		if len(got) > 63 {
			t.Fatalf("expected slot to be at most 63 chars, got len=%d %q", len(got), got)
		}
		// The first 63 'a' chars — no trailing underscores to trim.
		if got != strings.Repeat("a", 63) {
			t.Fatalf("expected 63 a's, got %q", got)
		}
	})
}

// TestStringPostgresParameterNonStringValue covers the branch where the map
// value exists but is not a string, causing the type assertion to fail and
// return "".
func TestStringPostgresParameterNonStringValue(t *testing.T) {
	t.Parallel()

	// value is an int, not a string → type assertion fails → return ""
	got := stringPostgresParameter(map[string]any{"restore_command": 42}, "restore_command")
	if got != "" {
		t.Fatalf("expected empty string for non-string parameter value, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// rejoin_finalize.go — uncovered branches
// ---------------------------------------------------------------------------

// TestVerifyRejoinReplicationRejectsExpiredMemberNode covers the missing-member
// path in prepareVerifiedRejoinExecutionLocked when the former primary has
// disappeared from observed state before verification begins.
func TestVerifyRejoinReplicationRejectsExpiredMemberNode(t *testing.T) {
	now := time.Date(2026, time.March, 30, 15, 0, 0, 0, time.UTC)
	store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second), now.Add(20*time.Second))

	// Remove the former-primary node status so hasMemberNode is false when
	// prepareVerifiedRejoinExecutionLocked runs.
	store.mu.Lock()
	delete(store.nodeStatuses, "alpha-1")
	store.mu.Unlock()

	_, err := store.VerifyRejoinReplication(context.Background())
	if !errors.Is(err, ErrRejoinTargetUnknown) {
		t.Fatalf("expected ErrRejoinTargetUnknown when member node missing at verification, got %v", err)
	}
}

// TestCompleteRejoinRejectsExpiredMemberNode covers the missing-member path in
// prepareVerifiedRejoinExecutionLocked when invoked from CompleteRejoin.
func TestCompleteRejoinRejectsExpiredMemberNode(t *testing.T) {
	now := time.Date(2026, time.March, 30, 15, 30, 0, 0, time.UTC)
	store := seededRestartedRejoinStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, now, now.Add(10*time.Second), now.Add(20*time.Second), now.Add(30*time.Second))

	// Remove the former-primary node status before CompleteRejoin.
	store.mu.Lock()
	delete(store.nodeStatuses, "alpha-1")
	store.mu.Unlock()

	_, err := store.CompleteRejoin(context.Background())
	if !errors.Is(err, ErrRejoinTargetUnknown) {
		t.Fatalf("expected ErrRejoinTargetUnknown when member node missing at completion, got %v", err)
	}
}
