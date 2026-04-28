package cluster

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type domainStringValue interface {
	~string
	IsValid() bool
	String() string
}

func TestDomainModelRegistriesContainOnlyUniqueValidValues(t *testing.T) {
	t.Parallel()

	assertDomainRegistry(t, "member roles", MemberRoles())
	assertDomainRegistry(t, "node roles", NodeRoles())
	assertDomainRegistry(t, "member states", MemberStates())
	assertDomainRegistry(t, "cluster phases", ClusterPhases())
	assertDomainRegistry(t, "failover modes", FailoverModes())
	assertDomainRegistry(t, "synchronous modes", SynchronousModes())
	assertDomainRegistry(t, "operation kinds", OperationKinds())
	assertDomainRegistry(t, "operation states", OperationStates())
	assertDomainRegistry(t, "operation results", OperationResults())
	assertDomainRegistry(t, "failover states", FailoverStates())
	assertDomainRegistry(t, "switchover states", SwitchoverStates())
	assertDomainRegistry(t, "rejoin strategies", RejoinStrategies())
	assertDomainRegistry(t, "rejoin states", RejoinStates())
}

func TestDomainModelAggregateValidationWrapsMemberErrorsWithContext(t *testing.T) {
	t.Parallel()

	spec := ClusterSpec{
		ClusterName: "alpha",
		Members: []MemberSpec{
			{
				Name:     "alpha-1",
				Priority: -1,
			},
		},
	}
	specErr := spec.Validate()
	if !errors.Is(specErr, ErrMemberPriorityNegative) {
		t.Fatalf("expected member priority cause, got %v", specErr)
	}
	if !strings.Contains(specErr.Error(), `member "alpha-1" spec is invalid`) {
		t.Fatalf("expected member spec context, got %v", specErr)
	}

	status := ClusterStatus{
		ClusterName: "alpha",
		Phase:       ClusterPhaseHealthy,
		ObservedAt:  time.Now().UTC(),
		Members: []MemberStatus{
			{
				Name:       "alpha-2",
				Role:       MemberRoleReplica,
				State:      MemberStateStreaming,
				Priority:   -1,
				LastSeenAt: time.Now().UTC(),
			},
		},
	}
	statusErr := status.Validate()
	if !errors.Is(statusErr, ErrMemberPriorityNegative) {
		t.Fatalf("expected member priority cause, got %v", statusErr)
	}
	if !strings.Contains(statusErr.Error(), `member "alpha-2" status is invalid`) {
		t.Fatalf("expected member status context, got %v", statusErr)
	}
}

func TestDomainModelClonePreservesNilMutableFields(t *testing.T) {
	t.Parallel()

	spec := ClusterSpec{
		ClusterName: "alpha",
	}
	specClone := spec.Clone()
	if specClone.Postgres.Parameters != nil {
		t.Fatalf("expected nil postgres parameters to stay nil, got %+v", specClone.Postgres.Parameters)
	}
	if specClone.Members != nil {
		t.Fatalf("expected nil member specs to stay nil, got %+v", specClone.Members)
	}

	status := ClusterStatus{
		ClusterName: "alpha",
		Phase:       ClusterPhaseHealthy,
		ObservedAt:  time.Now().UTC(),
	}
	statusClone := status.Clone()
	if statusClone.ActiveOperation != nil {
		t.Fatalf("expected nil active operation to stay nil, got %+v", statusClone.ActiveOperation)
	}
	if statusClone.ScheduledSwitchover != nil {
		t.Fatalf("expected nil scheduled switchover to stay nil, got %+v", statusClone.ScheduledSwitchover)
	}
	if statusClone.Members != nil {
		t.Fatalf("expected nil member statuses to stay nil, got %+v", statusClone.Members)
	}

	memberSpec := MemberSpec{Name: "alpha-1"}
	if clone := memberSpec.Clone(); clone.Tags != nil {
		t.Fatalf("expected nil member spec tags to stay nil, got %+v", clone.Tags)
	}

	memberStatus := MemberStatus{
		Name:       "alpha-1",
		Role:       MemberRoleReplica,
		State:      MemberStateStreaming,
		LastSeenAt: time.Now().UTC(),
	}
	if clone := memberStatus.Clone(); clone.Tags != nil {
		t.Fatalf("expected nil member status tags to stay nil, got %+v", clone.Tags)
	}
}

func assertDomainRegistry[T domainStringValue](t *testing.T, name string, values []T) {
	t.Helper()

	if len(values) == 0 {
		t.Fatalf("%s registry is empty", name)
	}

	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		literal := string(value)
		if literal == "" {
			t.Fatalf("%s registry contains zero value", name)
		}
		if !value.IsValid() {
			t.Fatalf("%s registry contains invalid value %q", name, literal)
		}
		if got := value.String(); got != literal {
			t.Fatalf("%s registry value string mismatch: got %q, want %q", name, got, literal)
		}
		if _, ok := seen[literal]; ok {
			t.Fatalf("%s registry contains duplicate value %q", name, literal)
		}
		seen[literal] = struct{}{}
	}
}
