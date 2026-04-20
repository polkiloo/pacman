package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

func TestMemoryStateStoreAssessRejoinMemberDetectsFormerPrimaryState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 18, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
	})

	assessment, err := store.AssessRejoinMember("alpha-1")
	if err != nil {
		t.Fatalf("assess rejoin member: %v", err)
	}

	if assessment.State != cluster.RejoinStateAssessingMember || assessment.CheckedAt.IsZero() {
		t.Fatalf("unexpected rejoin assessment metadata: %+v", assessment)
	}

	if assessment.Member.Name != "alpha-1" || assessment.CurrentPrimary.Name != "alpha-2" {
		t.Fatalf("unexpected rejoin member topology: %+v", assessment)
	}

	if !assessment.FormerPrimary || !assessment.ManagedPostgres || assessment.PostgresUp {
		t.Fatalf("unexpected former primary assessment flags: %+v", assessment)
	}

	if !assessment.Ready || len(assessment.Reasons) != 0 {
		t.Fatalf("expected former primary to be ready for divergence detection, got %+v", assessment)
	}
}

func TestMemoryStateStoreAssessRejoinMemberReportsBlockedReasons(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 18, 30, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		prepare func(t *testing.T) *MemoryStateStore
		target  string
		reasons []string
	}{
		{
			name: "member does not require rejoin",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members: []cluster.MemberSpec{
						{Name: "alpha-1"},
						{Name: "alpha-2"},
					},
				}, []agentmodel.NodeStatus{
					readyStandbyStatus("alpha-1", now, 12, 0),
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 12, "sys-alpha"),
				})
			},
			target:  "alpha-1",
			reasons: []string{reasonMemberDoesNotRequireRejoin},
		},
		{
			name: "current primary unknown",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members: []cluster.MemberSpec{
						{Name: "alpha-1"},
						{Name: "alpha-2"},
					},
				}, []agentmodel.NodeStatus{
					rejoinFormerPrimaryStatus("alpha-1", now, 12, "sys-alpha"),
				})
			},
			target:  "alpha-1",
			reasons: []string{reasonCurrentPrimaryUnknown},
		},
		{
			name: "member node state not observed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				store := seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members: []cluster.MemberSpec{
						{Name: "alpha-1"},
						{Name: "alpha-2"},
					},
				}, []agentmodel.NodeStatus{
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 12, "sys-alpha"),
				})

				if err := store.RegisterMember(context.Background(), MemberRegistration{
					NodeName:       "alpha-1",
					NodeRole:       cluster.NodeRoleData,
					APIAddress:     "10.0.0.10:8080",
					ControlAddress: "10.0.0.10:9090",
					RegisteredAt:   now,
				}); err != nil {
					t.Fatalf("register member: %v", err)
				}

				return store
			},
			target:  "alpha-1",
			reasons: []string{reasonMemberDoesNotRequireRejoin, reasonNodeStateNotObserved},
		},
		{
			name: "postgres not managed",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				unmanaged := rejoinFormerPrimaryStatus("alpha-1", now, 12, "sys-alpha")
				unmanaged.Postgres.Managed = false

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members: []cluster.MemberSpec{
						{Name: "alpha-1"},
						{Name: "alpha-2"},
					},
				}, []agentmodel.NodeStatus{
					unmanaged,
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 13, "sys-alpha"),
				})
			},
			target:  "alpha-1",
			reasons: []string{reasonPostgresNotManaged},
		},
		{
			name: "target is the current primary",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members: []cluster.MemberSpec{
						{Name: "alpha-1"},
						{Name: "alpha-2"},
					},
				}, []agentmodel.NodeStatus{
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 13, "sys-alpha"),
				})
			},
			target:  "alpha-2",
			reasons: []string{reasonCurrentPrimary, reasonMemberDoesNotRequireRejoin},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assessment, err := testCase.prepare(t).AssessRejoinMember(testCase.target)
			if err != nil {
				t.Fatalf("assess rejoin member: %v", err)
			}

			if assessment.Ready {
				t.Fatalf("expected blocked rejoin assessment, got %+v", assessment)
			}

			for _, reason := range testCase.reasons {
				if !containsString(assessment.Reasons, reason) {
					t.Fatalf("expected assessment reasons %v to contain %q", assessment.Reasons, reason)
				}
			}
		})
	}
}

func TestMemoryStateStoreDetectRejoinDivergenceRequirements(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 19, 0, 0, 0, time.UTC)

	testCases := []struct {
		name                string
		member              agentmodel.NodeStatus
		currentPrimary      agentmodel.NodeStatus
		wantCompared        bool
		wantDiverged        bool
		wantRequiresRewind  bool
		wantRequiresReclone bool
		wantReasons         []string
	}{
		{
			name:           "older timeline does not require rewind",
			member:         rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
			currentPrimary: rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantCompared:   true,
		},
		{
			name:           "matching timeline does not diverge",
			member:         rejoinFormerPrimaryStatus("alpha-1", now, 11, "sys-alpha"),
			currentPrimary: rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantCompared:   true,
		},
		{
			name:                "system identifier mismatch requires reclone",
			member:              rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-old"),
			currentPrimary:      rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-new"),
			wantCompared:        true,
			wantDiverged:        true,
			wantRequiresReclone: true,
			wantReasons:         []string{reasonSystemIdentifierMismatch},
		},
		{
			name:                "member timeline ahead requires reclone",
			member:              rejoinFormerPrimaryStatus("alpha-1", now, 12, "sys-alpha"),
			currentPrimary:      rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantCompared:        true,
			wantDiverged:        true,
			wantRequiresReclone: true,
			wantReasons:         []string{reasonTimelineAheadOfCurrentPrimary},
		},
		{
			name:           "missing member system identifier blocks comparison",
			member:         rejoinFormerPrimaryStatus("alpha-1", now, 10, ""),
			currentPrimary: rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantReasons:    []string{reasonMemberSystemIdentifierUnknown},
		},
		{
			name:   "unhealthy current primary blocks comparison",
			member: rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
			currentPrimary: func() agentmodel.NodeStatus {
				status := failoverNodeStatus("alpha-2", cluster.MemberRolePrimary, cluster.MemberStateFailed, now.Add(time.Second), false, 11, 0)
				status.Postgres.Details.SystemIdentifier = "sys-alpha"
				return status
			}(),
			wantReasons: []string{reasonCurrentPrimaryUnhealthy},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, cluster.ClusterSpec{
				ClusterName: "alpha",
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			}, []agentmodel.NodeStatus{testCase.member, testCase.currentPrimary})

			assessment, err := store.DetectRejoinDivergence("alpha-1")
			if err != nil {
				t.Fatalf("detect rejoin divergence: %v", err)
			}

			if assessment.State != cluster.RejoinStateDetectingDivergence || assessment.Member.Name != "alpha-1" || assessment.CurrentPrimary.Name != "alpha-2" {
				t.Fatalf("unexpected rejoin divergence metadata: %+v", assessment)
			}

			if assessment.Compared != testCase.wantCompared || assessment.Diverged != testCase.wantDiverged || assessment.RequiresRewind != testCase.wantRequiresRewind || assessment.RequiresReclone != testCase.wantRequiresReclone {
				t.Fatalf("unexpected rejoin divergence result: %+v", assessment)
			}

			for _, reason := range testCase.wantReasons {
				if !containsString(assessment.Reasons, reason) {
					t.Fatalf("expected divergence reasons %v to contain %q", assessment.Reasons, reason)
				}
			}
		})
	}
}

func TestMemoryStateStoreDecideRejoinStrategyChoosesRepairPath(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 19, 15, 0, 0, time.UTC)

	testCases := []struct {
		name             string
		member           agentmodel.NodeStatus
		currentPrimary   agentmodel.NodeStatus
		wantDecided      bool
		wantStrategy     cluster.RejoinStrategy
		wantDirectRejoin bool
		wantReasons      []string
	}{
		{
			name:             "direct rejoin possible for former primary behind current timeline",
			member:           rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
			currentPrimary:   rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantDirectRejoin: true,
		},
		{
			name:           "reclone selected for system identifier mismatch",
			member:         rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-old"),
			currentPrimary: rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-new"),
			wantDecided:    true,
			wantStrategy:   cluster.RejoinStrategyReclone,
			wantReasons:    []string{reasonSystemIdentifierMismatch},
		},
		{
			name:             "direct rejoin possible without repair path",
			member:           rejoinFormerPrimaryStatus("alpha-1", now, 11, "sys-alpha"),
			currentPrimary:   rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantDirectRejoin: true,
		},
		{
			name:           "blocked decision preserves divergence reasons",
			member:         rejoinFormerPrimaryStatus("alpha-1", now, 10, ""),
			currentPrimary: rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
			wantReasons:    []string{reasonMemberSystemIdentifierUnknown},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := seededFailoverStore(t, cluster.ClusterSpec{
				ClusterName: "alpha",
				Members: []cluster.MemberSpec{
					{Name: "alpha-1"},
					{Name: "alpha-2"},
				},
			}, []agentmodel.NodeStatus{testCase.member, testCase.currentPrimary})

			decision, err := store.DecideRejoinStrategy("alpha-1")
			if err != nil {
				t.Fatalf("decide rejoin strategy: %v", err)
			}

			if decision.State != cluster.RejoinStateSelectingStrategy || decision.Member.Name != "alpha-1" || decision.CurrentPrimary.Name != "alpha-2" {
				t.Fatalf("unexpected rejoin strategy decision metadata: %+v", decision)
			}

			if decision.Decided != testCase.wantDecided || decision.Strategy != testCase.wantStrategy || decision.DirectRejoinPossible != testCase.wantDirectRejoin {
				t.Fatalf("unexpected rejoin strategy decision: %+v", decision)
			}

			for _, reason := range testCase.wantReasons {
				if !containsString(decision.Reasons, reason) {
					t.Fatalf("expected strategy reasons %v to contain %q", decision.Reasons, reason)
				}
			}
		})
	}
}

func TestMemoryStateStoreExecuteRejoinRewindStartsRecoveringOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 20, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-1"},
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now.Add(-time.Minute), 10, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(-time.Minute+time.Second), 11, "sys-alpha"),
	})
	setTestNow(store, func() time.Time { return now })
	store.mu.Lock()
	store.clusterStatus.CurrentEpoch = 7
	store.mu.Unlock()

	rewinder := &recordingRewinder{}
	execution, err := store.ExecuteRejoinRewind(context.Background(), RejoinRequest{
		Member:      " alpha-1 ",
		RequestedBy: " operator ",
		Reason:      " repair former primary ",
	}, rewinder)
	if err != nil {
		t.Fatalf("execute rejoin rewind: %v", err)
	}

	if execution.State != cluster.RejoinStateRewinding || !execution.Rewound || execution.CurrentEpoch != 7 {
		t.Fatalf("unexpected rejoin execution result: %+v", execution)
	}

	if !execution.ExecutedAt.Equal(now) {
		t.Fatalf("unexpected rejoin execution metadata: %+v", execution)
	}

	if len(rewinder.requests) != 1 {
		t.Fatalf("expected one rewind request, got %+v", rewinder.requests)
	}

	request := rewinder.requests[0]
	if request.Operation.Kind != cluster.OperationKindRejoin || request.Operation.FromMember != "alpha-1" || request.Operation.ToMember != "alpha-2" {
		t.Fatalf("unexpected rewind operation payload: %+v", request.Operation)
	}

	if request.CurrentEpoch != 7 {
		t.Fatalf("unexpected rewind decision payload: %+v", request)
	}

	if request.SourceServer != "host=alpha-2-postgres port=5432" {
		t.Fatalf("unexpected rewind source server: got %q", request.SourceServer)
	}

	active, ok := store.ActiveOperation()
	if !ok {
		t.Fatal("expected active rejoin operation after successful rewind")
	}

	if active.Kind != cluster.OperationKindRejoin || active.State != cluster.OperationStateRunning || active.Result != cluster.OperationResultPending {
		t.Fatalf("unexpected active rejoin operation: %+v", active)
	}

	if active.RequestedBy != "operator" || active.Reason != "repair former primary" {
		t.Fatalf("unexpected normalized rejoin operation metadata: %+v", active)
	}

	status, ok := store.ClusterStatus()
	if !ok {
		t.Fatal("expected cluster status after rewind")
	}

	if status.Phase != cluster.ClusterPhaseRecovering {
		t.Fatalf("expected recovering phase during rejoin, got %+v", status)
	}

	member, ok := store.NodeStatus("alpha-1")
	if !ok {
		t.Fatal("expected former primary node status")
	}

	if member.State != cluster.MemberStateNeedsRejoin || !member.NeedsRejoin || member.Postgres.Up {
		t.Fatalf("expected rewound member to remain offline and require rejoin, got %+v", member)
	}

	if history := store.History(); len(history) != 0 {
		t.Fatalf("expected pg_rewind success to keep rejoin active without history, got %+v", history)
	}
}

func TestMemoryStateStoreExecuteRejoinRewindRejectsBlockedExecution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 20, 30, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		prepare  func(t *testing.T) *MemoryStateStore
		request  RejoinRequest
		rewinder RewindExecutor
		wantErr  error
	}{
		{
			name: "rewinder is required",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, []agentmodel.NodeStatus{
					rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
				})
			},
			request: RejoinRequest{Member: "alpha-1"},
			wantErr: ErrRejoinRewindExecutorRequired,
		},
		{
			name: "reclone path blocks rewind",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, []agentmodel.NodeStatus{
					rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-old"),
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-new"),
				})
			},
			request:  RejoinRequest{Member: "alpha-1"},
			rewinder: &recordingRewinder{},
			wantErr:  ErrRejoinRecloneRequired,
		},
		{
			name: "undetermined strategy blocks rewind",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				return seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, []agentmodel.NodeStatus{
					rejoinFormerPrimaryStatus("alpha-1", now, 10, ""),
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
				})
			},
			request:  RejoinRequest{Member: "alpha-1"},
			rewinder: &recordingRewinder{},
			wantErr:  ErrRejoinStrategyUndetermined,
		},
		{
			name: "active operation blocks rewind",
			prepare: func(t *testing.T) *MemoryStateStore {
				t.Helper()

				store := seededFailoverStore(t, cluster.ClusterSpec{
					ClusterName: "alpha",
					Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
				}, []agentmodel.NodeStatus{
					rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
					rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
				})

				if _, err := store.JournalOperation(context.Background(), cluster.Operation{
					ID:          "op-1",
					Kind:        cluster.OperationKindFailover,
					State:       cluster.OperationStateRunning,
					RequestedBy: "controller",
					RequestedAt: now,
				}); err != nil {
					t.Fatalf("journal active operation: %v", err)
				}

				return store
			},
			request:  RejoinRequest{Member: "alpha-1"},
			rewinder: &recordingRewinder{},
			wantErr:  ErrRejoinOperationInProgress,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := testCase.prepare(t).ExecuteRejoinRewind(context.Background(), testCase.request, testCase.rewinder)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected rejoin rewind error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestMemoryStateStoreExecuteRejoinRewindRecordsFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 21, 0, 0, 0, time.UTC)
	store := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members:     []cluster.MemberSpec{{Name: "alpha-1"}, {Name: "alpha-2"}},
	}, []agentmodel.NodeStatus{
		rejoinFormerPrimaryStatus("alpha-1", now, 10, "sys-alpha"),
		rejoinPrimaryStatus("alpha-2", now.Add(time.Second), 11, "sys-alpha"),
	})
	setTestNow(store, func() time.Time { return now })

	rewinder := &recordingRewinder{err: errors.New("pg_rewind failed")}
	_, err := store.ExecuteRejoinRewind(context.Background(), RejoinRequest{Member: "alpha-1"}, rewinder)
	if err == nil {
		t.Fatal("expected rejoin rewind failure")
	}

	if _, ok := store.ActiveOperation(); ok {
		t.Fatal("expected failed rewind to clear active operation")
	}

	history := store.History()
	if len(history) != 1 {
		t.Fatalf("expected one failed rejoin history entry, got %+v", history)
	}

	if history[0].Kind != cluster.OperationKindRejoin || history[0].FromMember != "alpha-1" || history[0].ToMember != "alpha-2" || history[0].Result != cluster.OperationResultFailed {
		t.Fatalf("unexpected failed rejoin history entry: %+v", history[0])
	}
}

func TestMemoryStateStoreRejoinAssessmentsRequireKnownTarget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 29, 19, 30, 0, 0, time.UTC)
	readyStore := seededFailoverStore(t, cluster.ClusterSpec{
		ClusterName: "alpha",
		Members: []cluster.MemberSpec{
			{Name: "alpha-2"},
		},
	}, []agentmodel.NodeStatus{
		rejoinPrimaryStatus("alpha-2", now, 14, "sys-alpha"),
	})

	testCases := []struct {
		name    string
		call    func() error
		wantErr error
	}{
		{
			name: "assess rejects empty target",
			call: func() error {
				_, err := readyStore.AssessRejoinMember(" ")
				return err
			},
			wantErr: ErrRejoinTargetRequired,
		},
		{
			name: "divergence rejects empty target",
			call: func() error {
				_, err := readyStore.DetectRejoinDivergence("")
				return err
			},
			wantErr: ErrRejoinTargetRequired,
		},
		{
			name: "assess rejects unknown target",
			call: func() error {
				_, err := readyStore.AssessRejoinMember("alpha-1")
				return err
			},
			wantErr: ErrRejoinTargetUnknown,
		},
		{
			name: "divergence rejects unknown target",
			call: func() error {
				_, err := readyStore.DetectRejoinDivergence("alpha-1")
				return err
			},
			wantErr: ErrRejoinTargetUnknown,
		},
		{
			name: "assess requires observed state",
			call: func() error {
				_, err := NewMemoryStateStore().AssessRejoinMember("alpha-1")
				return err
			},
			wantErr: ErrRejoinObservedStateRequired,
		},
		{
			name: "divergence requires observed state",
			call: func() error {
				_, err := NewMemoryStateStore().DetectRejoinDivergence("alpha-1")
				return err
			},
			wantErr: ErrRejoinObservedStateRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.call()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected error: got %v want %v", err, testCase.wantErr)
			}
		})
	}
}

func rejoinFormerPrimaryStatus(nodeName string, observedAt time.Time, timeline int64, systemIdentifier string) agentmodel.NodeStatus {
	status := failoverNodeStatus(nodeName, cluster.MemberRoleReplica, cluster.MemberStateNeedsRejoin, observedAt, false, timeline, 0)
	status.NeedsRejoin = true
	status.Postgres.Address = nodeName + "-postgres:5432"
	status.Postgres.Details.SystemIdentifier = systemIdentifier
	return status
}

func rejoinPrimaryStatus(nodeName string, observedAt time.Time, timeline int64, systemIdentifier string) agentmodel.NodeStatus {
	status := readyPrimaryStatus(nodeName, observedAt, timeline)
	status.Postgres.Address = nodeName + "-postgres:5432"
	status.Postgres.Details.SystemIdentifier = systemIdentifier
	return status
}

type recordingRewinder struct {
	requests []RewindRequest
	err      error
}

func (rewinder *recordingRewinder) Rewind(_ context.Context, request RewindRequest) error {
	rewinder.requests = append(rewinder.requests, request)
	return rewinder.err
}
