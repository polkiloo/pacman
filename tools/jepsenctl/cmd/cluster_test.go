package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateClusterStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  clusterStatus
		wantErr []string
	}{
		{
			name:   "accepts healthy three data node shape",
			status: validClusterStatus(),
		},
		{
			name: "rejects unhealthy phase",
			status: clusterStatus{
				Phase:          "degraded",
				CurrentPrimary: "alpha-1",
				Members:        validClusterStatus().Members,
			},
			wantErr: []string{`phase is "degraded", want healthy`},
		},
		{
			name: "rejects missing data node",
			status: clusterStatus{
				Phase:          "healthy",
				CurrentPrimary: "alpha-1",
				Members:        validClusterStatus().Members[:2],
			},
			wantErr: []string{
				"member count is 2, want 3",
				"members are [alpha-1 alpha-2], want [alpha-1 alpha-2 alpha-3]",
				"healthy replica count is 1, want 2",
				"streaming replica count is 1, want 2",
			},
		},
		{
			name: "rejects wrong member names",
			status: clusterStatus{
				Phase:          "healthy",
				CurrentPrimary: "alpha-1",
				Members: []clusterMember{
					{Name: "alpha-1", Role: "primary", State: "running", Healthy: true},
					{Name: "alpha-2", Role: "replica", State: "streaming", Healthy: true},
					{Name: "witness-1", Role: "replica", State: "streaming", Healthy: true},
				},
			},
			wantErr: []string{"members are [alpha-1 alpha-2 witness-1], want [alpha-1 alpha-2 alpha-3]"},
		},
		{
			name: "rejects unhealthy replica",
			status: clusterStatus{
				Phase:          "healthy",
				CurrentPrimary: "alpha-1",
				Members: []clusterMember{
					{Name: "alpha-1", Role: "primary", State: "running", Healthy: true},
					{Name: "alpha-2", Role: "replica", State: "streaming", Healthy: true},
					{Name: "alpha-3", Role: "replica", State: "streaming", Healthy: false},
				},
			},
			wantErr: []string{"healthy replica count is 1, want 2"},
		},
		{
			name: "rejects non-streaming replica",
			status: clusterStatus{
				Phase:          "healthy",
				CurrentPrimary: "alpha-1",
				Members: []clusterMember{
					{Name: "alpha-1", Role: "primary", State: "running", Healthy: true},
					{Name: "alpha-2", Role: "replica", State: "streaming", Healthy: true},
					{Name: "alpha-3", Role: "replica", State: "running", Healthy: true},
				},
			},
			wantErr: []string{"streaming replica count is 1, want 2"},
		},
		{
			name: "rejects mismatched current primary",
			status: clusterStatus{
				Phase:          "healthy",
				CurrentPrimary: "alpha-2",
				Members:        validClusterStatus().Members,
			},
			wantErr: []string{`currentPrimary is "alpha-2", but primary member is "alpha-1"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateClusterStatus(test.status)
			if len(test.wantErr) == 0 {
				if err != nil {
					t.Fatalf("validate cluster: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected validation error")
			}
			for _, fragment := range test.wantErr {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatalf("error missing %q in:\n%s", fragment, err.Error())
				}
			}
		})
	}
}

func TestClusterValidateCommand(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	clusterFile := filepath.Join(tempDir, "pacman-cluster-before.json")
	writeTestFile(t, clusterFile, validClusterStatusJSON())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{"cluster", "validate", clusterFile}, &stdout, &stderr)

	if status != 0 {
		t.Fatalf("status: got %d want 0; stderr:\n%s", status, stderr.String())
	}
	assertContainsAll(t, "stdout", stdout.String(), []string{
		"validated 1 PACMAN cluster snapshot(s)",
	})
}

func TestClusterValidateCommandDefaultsToClusterBeforeGlob(t *testing.T) {
	tempDir := t.TempDir()
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}

	writeTestFile(t, filepath.Join(tempDir, "pacman-cluster-before.json"), validClusterStatusJSON())
	writeTestFile(t, filepath.Join(tempDir, "pacman-cluster-before-append-smoke-none.json"), validClusterStatusJSON())

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{"cluster", "validate"}, &stdout, &stderr)

	if status != 0 {
		t.Fatalf("status: got %d want 0; stderr:\n%s", status, stderr.String())
	}
	assertContainsAll(t, "stdout", stdout.String(), []string{
		"validated 2 PACMAN cluster snapshot(s)",
	})
}

func TestClusterValidateCommandReportsInvalidShape(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	clusterFile := filepath.Join(tempDir, "pacman-cluster-before.json")
	writeTestFile(t, clusterFile, strings.Replace(validClusterStatusJSON(), `"phase": "healthy"`, `"phase": "degraded"`, 1))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{"cluster", "validate", clusterFile}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	assertContainsAll(t, "stderr", stderr.String(), []string{
		"invalid cluster shape",
		`phase is "degraded", want healthy`,
	})
}

func validClusterStatus() clusterStatus {
	return clusterStatus{
		Phase:          "healthy",
		CurrentPrimary: "alpha-1",
		Members: []clusterMember{
			{Name: "alpha-1", Role: "primary", State: "running", Healthy: true},
			{Name: "alpha-2", Role: "replica", State: "streaming", Healthy: true},
			{Name: "alpha-3", Role: "replica", State: "streaming", Healthy: true},
		},
	}
}

func validClusterStatusJSON() string {
	return `{
  "phase": "healthy",
  "currentPrimary": "alpha-1",
  "members": [
    {"name": "alpha-1", "role": "primary", "state": "running", "healthy": true},
    {"name": "alpha-2", "role": "replica", "state": "streaming", "healthy": true},
    {"name": "alpha-3", "role": "replica", "state": "streaming", "healthy": true}
  ]
}
`
}
