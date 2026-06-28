package controlplane

import (
	"errors"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/cluster"
)

func TestNormalizeReinitRequestTrimsAndDefaultsOperatorMetadata(t *testing.T) {
	t.Parallel()

	normalized := normalizeReinitRequest(ReinitRequest{
		Member:      " alpha-2 ",
		RequestedBy: " ",
		Reason:      " ",
	})

	if normalized.Member != "alpha-2" {
		t.Fatalf("member: got %q want %q", normalized.Member, "alpha-2")
	}
	if normalized.RequestedBy != "operator" {
		t.Fatalf("requestedBy: got %q want default operator", normalized.RequestedBy)
	}
	if normalized.Reason != "replica reinitialization requested" {
		t.Fatalf("reason: got %q want default reason", normalized.Reason)
	}

	normalized = normalizeReinitRequest(ReinitRequest{
		Member:      " alpha-3 ",
		RequestedBy: " ops ",
		Reason:      " reclone ",
	})
	if normalized.Member != "alpha-3" || normalized.RequestedBy != "ops" || normalized.Reason != "reclone" {
		t.Fatalf("unexpected trimmed request: %+v", normalized)
	}
}

func TestEvaluateReinitRequestAcceptsFailedReplicaWithHealthyPrimary(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 21, 9, 0, 0, 0, time.UTC)
	status := reinitValidationStatus(now, []cluster.MemberStatus{
		reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
		reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now.Add(time.Second)),
	})
	status.CurrentEpoch = 17

	validation, err := evaluateReinitRequest(status, ReinitRequest{
		Member:      "alpha-2",
		RequestedBy: "ops",
		Reason:      "reclone",
	}, nil, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("evaluate reinit request: %v", err)
	}

	if validation.CurrentPrimary.Name != "alpha-1" || validation.Target.Name != "alpha-2" {
		t.Fatalf("unexpected validation members: %+v", validation)
	}
	if validation.Request.RequestedBy != "ops" || validation.Request.Reason != "reclone" {
		t.Fatalf("unexpected validation request metadata: %+v", validation.Request)
	}
	if validation.CurrentEpoch != 17 || !validation.ValidatedAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("unexpected validation timing/epoch: %+v", validation)
	}
}

func TestEvaluateReinitExecutionRequestAcceptsUnknownTargetAfterRuntimeRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 28, 7, 12, 0, 0, time.UTC)
	status := reinitValidationStatus(now, []cluster.MemberStatus{
		reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
		reinitValidationMember("alpha-2", cluster.MemberRoleUnknown, cluster.MemberStateFailed, false, now.Add(time.Second)),
	})

	validation, err := evaluateReinitExecutionRequest(status, ReinitRequest{Member: "alpha-2"}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("evaluate resumed reinit execution: %v", err)
	}
	if validation.Target.Name != "alpha-2" || validation.Target.Role != cluster.MemberRoleUnknown {
		t.Fatalf("unexpected resumed reinit target: %+v", validation.Target)
	}
}

func TestEvaluateReinitExecutionRequestRejectsWitnessTarget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 28, 7, 13, 0, 0, time.UTC)
	status := reinitValidationStatus(now, []cluster.MemberStatus{
		reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
		reinitValidationMember("witness-1", cluster.MemberRoleWitness, cluster.MemberStateRunning, true, now.Add(time.Second)),
	})

	_, err := evaluateReinitExecutionRequest(status, ReinitRequest{Member: "witness-1"}, now.Add(time.Minute))
	if !errors.Is(err, ErrReinitTargetIsWitness) {
		t.Fatalf("evaluate resumed witness reinit error: got %v want %v", err, ErrReinitTargetIsWitness)
	}
}

func TestEvaluateReinitRequestRejectsMissingPrimaryAndRoleMismatches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 21, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		status  cluster.ClusterStatus
		member  string
		wantErr error
	}{
		{
			name: "blank target",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now.Add(time.Second)),
			}),
			member:  "",
			wantErr: ErrReinitTargetRequired,
		},
		{
			name: "no current primary",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now),
			}),
			member:  "alpha-2",
			wantErr: ErrReinitSourcePrimaryUnknown,
		},
		{
			name: "unhealthy current primary",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, false, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now.Add(time.Second)),
			}),
			member:  "alpha-2",
			wantErr: ErrReinitSourcePrimaryUnhealthy,
		},
		{
			name: "unknown target",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now.Add(time.Second)),
			}),
			member:  "alpha-3",
			wantErr: ErrReinitTargetUnknown,
		},
		{
			name: "current primary by name",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now.Add(time.Second)),
			}),
			member:  "alpha-1",
			wantErr: ErrReinitTargetIsCurrentPrimary,
		},
		{
			name: "primary role target despite different current primary name",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now.Add(time.Second)),
			}),
			member:  "alpha-2",
			wantErr: ErrReinitTargetIsCurrentPrimary,
		},
		{
			name: "witness target",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleWitness, cluster.MemberStateRunning, true, now.Add(time.Second)),
			}),
			member:  "alpha-2",
			wantErr: ErrReinitTargetIsWitness,
		},
		{
			name: "unknown role is not data bearing",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleUnknown, cluster.MemberStateUnknown, false, now.Add(time.Second)),
			}),
			member:  "alpha-2",
			wantErr: ErrReinitTargetIsWitness,
		},
		{
			name: "active operation blocks validation before target lookup",
			status: reinitValidationStatus(now, []cluster.MemberStatus{
				reinitValidationMember("alpha-1", cluster.MemberRolePrimary, cluster.MemberStateRunning, true, now),
				reinitValidationMember("alpha-2", cluster.MemberRoleReplica, cluster.MemberStateFailed, false, now.Add(time.Second)),
			}),
			member:  "missing",
			wantErr: ErrReinitOperationInProgress,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var active *cluster.Operation
			if errors.Is(testCase.wantErr, ErrReinitOperationInProgress) {
				active = &cluster.Operation{
					ID:          "switchover-active",
					Kind:        cluster.OperationKindSwitchover,
					State:       cluster.OperationStateRunning,
					RequestedAt: now,
					Result:      cluster.OperationResultPending,
				}
			}

			_, err := evaluateReinitRequest(testCase.status, ReinitRequest{Member: testCase.member}, active, now)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("evaluate reinit request error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestBuildReinitIntentOperationPopulatesJournalMetadata(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.June, 21, 11, 0, 0, 123, time.UTC)
	validation := ReinitValidation{
		Request: ReinitRequest{
			Member:      "alpha-2",
			RequestedBy: "ops",
			Reason:      "reclone",
		},
		CurrentPrimary: cluster.MemberStatus{Name: "alpha-1"},
		Target:         cluster.MemberStatus{Name: "alpha-2"},
	}

	operation, err := buildReinitIntentOperation(now, validation)
	if err != nil {
		t.Fatalf("build reinit operation: %v", err)
	}

	if operation.ID != "reinit-20260621T110000.000000123Z" {
		t.Fatalf("operation id: got %q", operation.ID)
	}
	if operation.Kind != cluster.OperationKindReinit || operation.State != cluster.OperationStateAccepted || operation.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected operation lifecycle: %+v", operation)
	}
	if operation.FromMember != "alpha-1" || operation.ToMember != "alpha-2" || operation.RequestedBy != "ops" || operation.Reason != "reclone" {
		t.Fatalf("unexpected operation metadata: %+v", operation)
	}
	if operation.Message != reinitOperationMessage("alpha-1", "alpha-2") {
		t.Fatalf("operation message: got %q", operation.Message)
	}
}

func TestBuildReinitIntentOperationRejectsInvalidRequestedAt(t *testing.T) {
	t.Parallel()

	validation := ReinitValidation{
		Request: ReinitRequest{
			Member:      "alpha-2",
			RequestedBy: "ops",
			Reason:      "reclone",
		},
		CurrentPrimary: cluster.MemberStatus{Name: "alpha-1"},
		Target:         cluster.MemberStatus{Name: "alpha-2"},
	}

	_, err := buildReinitIntentOperation(time.Time{}, validation)
	if !errors.Is(err, cluster.ErrOperationRequestedAtRequired) {
		t.Fatalf("build reinit operation error: got %v want %v", err, cluster.ErrOperationRequestedAtRequired)
	}
}

func TestReinitInputsLockedReportsMissingState(t *testing.T) {
	t.Parallel()

	store := &MemoryStateStore{}
	if _, _, err := store.reinitInputsLocked(); !errors.Is(err, ErrClusterSpecRequired) {
		t.Fatalf("missing spec error: got %v want %v", err, ErrClusterSpecRequired)
	}

	store.clusterSpec = &cluster.ClusterSpec{ClusterName: "alpha"}
	if _, _, err := store.reinitInputsLocked(); !errors.Is(err, ErrReinitObservedStateRequired) {
		t.Fatalf("missing status error: got %v want %v", err, ErrReinitObservedStateRequired)
	}
}

func reinitValidationStatus(now time.Time, members []cluster.MemberStatus) cluster.ClusterStatus {
	return cluster.ClusterStatus{
		ClusterName: "alpha",
		Phase:       cluster.ClusterPhaseDegraded,
		Members:     members,
		ObservedAt:  now,
	}
}

func reinitValidationMember(name string, role cluster.MemberRole, state cluster.MemberState, healthy bool, seenAt time.Time) cluster.MemberStatus {
	return cluster.MemberStatus{
		Name:       name,
		Role:       role,
		State:      state,
		Healthy:    healthy,
		LastSeenAt: seenAt,
	}
}
