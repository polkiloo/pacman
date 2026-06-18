package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

func readTestFile(t *testing.T, path string) string {
	t.Helper()

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %q: %v", path, err)
	}

	return string(payload)
}

func writeTracingBinary(t *testing.T, binaryName, scriptTemplate string) (string, string) {
	t.Helper()

	binDir := t.TempDir()
	tracePath := filepath.Join(binDir, binaryName+".trace")
	scriptPath := filepath.Join(binDir, binaryName)
	tempPath := scriptPath + ".tmp"
	script := []byte(strings.TrimSpace(
		strings.ReplaceAll(
			strings.ReplaceAll(scriptTemplate, "%q", `"`+tracePath+`"`),
			"%%", "%",
		),
	) + "\n")

	if err := os.WriteFile(tempPath, script, 0o755); err != nil {
		t.Fatalf("write %s script: %v", binaryName, err)
	}
	if err := os.Rename(tempPath, scriptPath); err != nil {
		t.Fatalf("install %s script: %v", binaryName, err)
	}
	time.Sleep(10 * time.Millisecond)

	return binDir, tracePath
}

func assertTraceLines(t *testing.T, path string, want []string) {
	t.Helper()

	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read command trace %q: %v", path, err)
	}

	got := strings.Split(strings.TrimSpace(string(payload)), "\n")
	if len(got) != len(want) {
		t.Fatalf("unexpected traced command count: got %v, want %v", got, want)
	}

	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("trace %d: got %q, want %q", index, got[index], want[index])
		}
	}
}

type stubNodeStatusReader struct {
	status          agentmodel.NodeStatus
	ok              bool
	clusterStatus   cluster.ClusterStatus
	clusterStatusOK bool
}

func (reader stubNodeStatusReader) NodeStatus(string) (agentmodel.NodeStatus, bool) {
	if !reader.ok {
		return agentmodel.NodeStatus{}, false
	}

	return reader.status.Clone(), true
}

func (stubNodeStatusReader) NodeStatuses() []agentmodel.NodeStatus {
	return nil
}

func (stubNodeStatusReader) ClusterSpec() (cluster.ClusterSpec, bool) {
	return cluster.ClusterSpec{}, false
}

func (reader stubNodeStatusReader) ClusterStatus() (cluster.ClusterStatus, bool) {
	if !reader.clusterStatusOK {
		return cluster.ClusterStatus{}, false
	}

	return reader.clusterStatus.Clone(), true
}

func (stubNodeStatusReader) MaintenanceStatus() cluster.MaintenanceModeStatus {
	return cluster.MaintenanceModeStatus{}
}

func (stubNodeStatusReader) UpdateMaintenanceMode(context.Context, cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error) {
	return cluster.MaintenanceModeStatus{}, errors.New("unsupported")
}

func (stubNodeStatusReader) History() []cluster.HistoryEntry {
	return nil
}

func (stubNodeStatusReader) CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error) {
	return controlplane.SwitchoverIntent{}, errors.New("unsupported")
}

func (stubNodeStatusReader) CancelSwitchover(context.Context) (cluster.Operation, error) {
	return cluster.Operation{}, errors.New("unsupported")
}

func (stubNodeStatusReader) CreateFailoverIntent(context.Context, controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error) {
	return controlplane.FailoverIntent{}, errors.New("unsupported")
}

func (stubNodeStatusReader) CreateReinitIntent(context.Context, controlplane.ReinitRequest) (controlplane.ReinitIntent, error) {
	return controlplane.ReinitIntent{}, errors.New("unsupported")
}

func failoverTestClusterStatus(currentPrimary string, active *cluster.Operation, members ...cluster.MemberStatus) cluster.ClusterStatus {
	return cluster.ClusterStatus{
		ClusterName:    "alpha",
		Phase:          cluster.ClusterPhaseDegraded,
		CurrentPrimary: currentPrimary,
		ActiveOperation: func() *cluster.Operation {
			if active == nil {
				return nil
			}
			cloned := active.Clone()
			return &cloned
		}(),
		Members:    append([]cluster.MemberStatus(nil), members...),
		ObservedAt: clusterTime("2026-01-02T03:04:05Z"),
	}
}

func failoverTestMember(name string, role cluster.MemberRole, healthy bool, needsRejoin bool) cluster.MemberStatus {
	state := cluster.MemberStateFailed
	if healthy {
		state = cluster.MemberStateRunning
		if role == cluster.MemberRoleReplica {
			state = cluster.MemberStateStreaming
		}
	}
	if needsRejoin {
		state = cluster.MemberStateNeedsRejoin
	}

	return cluster.MemberStatus{
		Name:        name,
		Role:        role,
		State:       state,
		Healthy:     healthy,
		NeedsRejoin: needsRejoin,
		LastSeenAt:  clusterTime("2026-01-02T03:04:05Z"),
	}
}

func clusterTime(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}

	return parsed
}
