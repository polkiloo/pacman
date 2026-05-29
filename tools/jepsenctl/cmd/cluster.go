package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

var expectedDataMembers = []string{"alpha-1", "alpha-2", "alpha-3"}

type clusterStatus struct {
	Phase          string          `json:"phase"`
	CurrentPrimary string          `json:"currentPrimary"`
	Members        []clusterMember `json:"members"`
}

type clusterMember struct {
	Name    string `json:"name"`
	Role    string `json:"role"`
	State   string `json:"state"`
	Healthy bool   `json:"healthy"`
}

func newClusterCommand(stdout io.Writer) *cobra.Command {
	cluster := &cobra.Command{
		Use:   "cluster",
		Short: "validate Jepsen cluster artifacts",
	}

	cluster.AddCommand(newClusterValidateCommand(stdout))

	return cluster
}

func newClusterValidateCommand(stdout io.Writer) *cobra.Command {
	validate := &cobra.Command{
		Use:   "validate [pacman-cluster-before*.json ...]",
		Short: "validate captured PACMAN cluster shape",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			for _, arg := range args {
				if strings.TrimSpace(arg) == "" {
					return fmt.Errorf("cluster validate file paths must not be empty")
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			paths, err := resolveClusterStatusPaths(args)
			if err != nil {
				return err
			}

			for _, path := range paths {
				if err := validateClusterStatusFile(path); err != nil {
					return err
				}
			}

			fmt.Fprintf(stdout, "validated %d PACMAN cluster snapshot(s)\n", len(paths))
			return nil
		},
	}

	return validate
}

func resolveClusterStatusPaths(args []string) ([]string, error) {
	if len(args) > 0 {
		return args, nil
	}

	matches, err := filepath.Glob("pacman-cluster-before*.json")
	if err != nil {
		return nil, fmt.Errorf("resolve pacman-cluster-before*.json: %w", err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no pacman-cluster-before*.json files found")
	}
	sort.Strings(matches)
	return matches, nil
}

func validateClusterStatusFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read cluster status %s: %w", path, err)
	}

	clusterJSON := clusterStatusJSONObject(string(data))
	if clusterJSON == "" {
		return fmt.Errorf("parse cluster status %s: no PACMAN cluster JSON object found", path)
	}

	var status clusterStatus
	if err := json.Unmarshal([]byte(clusterJSON), &status); err != nil {
		return fmt.Errorf("parse cluster status %s: %w", path, err)
	}

	if err := validateClusterStatus(status); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func validateClusterStatus(status clusterStatus) error {
	var problems []string

	if status.Phase != "healthy" {
		problems = append(problems, fmt.Sprintf("phase is %q, want healthy", status.Phase))
	}
	if len(status.Members) != len(expectedDataMembers) {
		problems = append(problems, fmt.Sprintf("member count is %d, want %d", len(status.Members), len(expectedDataMembers)))
	}

	names := make([]string, 0, len(status.Members))
	primaryCount := 0
	healthyPrimaryCount := 0
	healthyReplicaCount := 0
	streamingReplicaCount := 0
	var primaryName string

	for _, member := range status.Members {
		names = append(names, member.Name)
		switch member.Role {
		case "primary":
			primaryCount++
			primaryName = member.Name
			if member.Healthy {
				healthyPrimaryCount++
			}
		case "replica":
			if member.Healthy {
				healthyReplicaCount++
			}
			if member.State == "streaming" {
				streamingReplicaCount++
			}
		}
	}

	sort.Strings(names)
	if !stringSlicesEqual(names, expectedDataMembers) {
		problems = append(problems, fmt.Sprintf("members are %v, want %v", names, expectedDataMembers))
	}
	if primaryCount != 1 {
		problems = append(problems, fmt.Sprintf("primary count is %d, want 1", primaryCount))
	}
	if healthyPrimaryCount != 1 {
		problems = append(problems, fmt.Sprintf("healthy primary count is %d, want 1", healthyPrimaryCount))
	}
	if healthyReplicaCount != 2 {
		problems = append(problems, fmt.Sprintf("healthy replica count is %d, want 2", healthyReplicaCount))
	}
	if streamingReplicaCount != 2 {
		problems = append(problems, fmt.Sprintf("streaming replica count is %d, want 2", streamingReplicaCount))
	}
	if status.CurrentPrimary == "" {
		problems = append(problems, "currentPrimary is empty")
	} else if primaryName != "" && status.CurrentPrimary != primaryName {
		problems = append(problems, fmt.Sprintf("currentPrimary is %q, but primary member is %q", status.CurrentPrimary, primaryName))
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid cluster shape: %s", strings.Join(problems, "; "))
	}
	return nil
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
