package nativeapi

import (
	"encoding/json"
	"testing"
	"time"
)

func TestClusterStatusResponseJSONShape(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(ClusterStatusResponse{
		ClusterName: "alpha",
		Phase:       "healthy",
		ObservedAt:  observedAt,
		Maintenance: MaintenanceModeStatus{Enabled: true},
		Members: []MemberStatus{
			{Name: "alpha-1", Role: "primary", State: "running", Healthy: true, LastSeenAt: observedAt},
		},
	})
	if err != nil {
		t.Fatalf("marshal cluster status response: %v", err)
	}

	if got, want := string(payload), `{"clusterName":"alpha","phase":"healthy","currentEpoch":0,"observedAt":"2026-04-05T12:00:00Z","maintenance":{"enabled":true},"members":[{"name":"alpha-1","role":"primary","state":"running","healthy":true,"lastSeenAt":"2026-04-05T12:00:00Z"}]}`; got != want {
		t.Fatalf("unexpected json payload: got %q want %q", got, want)
	}
}
