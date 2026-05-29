package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

func (lab *harnessLab) verifyThreeDataNodeCluster(ctx context.Context, outputFile string) error {
	output, err := lab.waitForThreeDataNodeCluster(ctx)
	if err != nil {
		return err
	}
	if outputFile != "" {
		if err := os.WriteFile(outputFile, []byte(output+"\n"), 0o644); err != nil {
			return err
		}
		return validateClusterStatusFile(outputFile)
	}
	var status clusterStatus
	if err := json.Unmarshal([]byte(output), &status); err != nil {
		return err
	}
	return validateClusterStatus(status)
}

func (lab *harnessLab) waitForThreeDataNodeCluster(ctx context.Context) (string, error) {
	deadline := time.Now().Add(lab.cfg.clusterVerifyTimeout)
	var lastJSON string
	var lastErr error

	for {
		for _, service := range []string{"pacman-primary", "pacman-replica", "pacman-replica-2"} {
			output, err := lab.pacmanClusterStatusJSON(ctx, service)
			if err != nil {
				lastErr = err
				continue
			}
			lastJSON = output
			var status clusterStatus
			if err := json.Unmarshal([]byte(output), &status); err != nil {
				lastErr = err
				continue
			}
			if err := validateClusterStatus(status); err != nil {
				lastErr = err
				continue
			}
			return output, nil
		}

		if time.Now().After(deadline) {
			if lastJSON != "" {
				return "", fmt.Errorf("timed out waiting for healthy three-data-node cluster; last status: %s; last error: %w", lastJSON, lastErr)
			}
			return "", fmt.Errorf("timed out waiting for healthy three-data-node cluster: %w", lastErr)
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(lab.cfg.clusterVerifyInterval):
		}
	}
}

func (lab *harnessLab) pacmanClusterStatusJSON(ctx context.Context, service string) (string, error) {
	output, status, err := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "status", "-o", "json")
	if err != nil || status != 0 {
		return "", fmt.Errorf("cluster status from %s failed: %s", service, strings.TrimSpace(output))
	}
	jsonText := clusterStatusJSONObject(output)
	if jsonText == "" {
		return "", fmt.Errorf("cluster status from %s did not contain JSON object: %s", service, output)
	}
	return jsonText, nil
}

func (lab *harnessLab) pacmanClusterStatusAny(ctx context.Context) (clusterStatus, string, error) {
	var lastErr error
	for _, service := range []string{"pacman-primary", "pacman-replica", "pacman-replica-2"} {
		text, err := lab.pacmanClusterStatusJSON(ctx, service)
		if err != nil {
			lastErr = err
			continue
		}
		var status clusterStatus
		if err := json.Unmarshal([]byte(text), &status); err != nil {
			lastErr = err
			continue
		}
		return status, service, nil
	}
	return clusterStatus{}, "", lastErr
}

func lastJSONObject(output string) string {
	scanner := bufio.NewScanner(strings.NewReader(output))
	var last string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
			last = line
		}
	}
	return last
}

func clusterStatusJSONObject(output string) string {
	for index, char := range output {
		if char != '{' {
			continue
		}
		decoder := json.NewDecoder(strings.NewReader(output[index:]))
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			continue
		}
		var status clusterStatus
		if err := json.Unmarshal(raw, &status); err != nil {
			continue
		}
		if status.Phase != "" || status.CurrentPrimary != "" || len(status.Members) > 0 {
			return string(bytes.TrimSpace(raw))
		}
	}
	return ""
}

func (lab *harnessLab) currentPrimaryName(ctx context.Context) string {
	status, _, err := lab.pacmanClusterStatusAny(ctx)
	if err != nil || status.CurrentPrimary == "" {
		return "unknown"
	}
	return status.CurrentPrimary
}

func (lab *harnessLab) switchoverCandidate(ctx context.Context) string {
	status, _, err := lab.pacmanClusterStatusAny(ctx)
	if err != nil {
		return ""
	}
	for _, member := range status.Members {
		if member.Name == status.CurrentPrimary || !member.Healthy {
			continue
		}
		if member.Role == "replica" && (member.State == "streaming" || member.State == "running") {
			return member.Name
		}
	}
	return ""
}

func serviceForMember(member string) string {
	switch member {
	case "alpha-1":
		return "pacman-primary"
	case "alpha-2":
		return "pacman-replica"
	case "alpha-3":
		return "pacman-replica-2"
	default:
		return ""
	}
}

func memberForService(service string) string {
	switch service {
	case "pacman-primary":
		return "alpha-1"
	case "pacman-replica":
		return "alpha-2"
	case "pacman-replica-2":
		return "alpha-3"
	default:
		return ""
	}
}

func peerServicesForMember(member string) []string {
	switch member {
	case "alpha-1":
		return []string{"pacman-replica", "pacman-replica-2"}
	case "alpha-2", "alpha-3":
		return []string{"pacman-primary"}
	default:
		return []string{"pacman-replica", "pacman-replica-2"}
	}
}

func dcsMemberForService(service string) string {
	switch service {
	case "pacman-dcs":
		return "alpha-dcs"
	case "pacman-dcs-2":
		return "alpha-dcs-2"
	case "pacman-dcs-3":
		return "alpha-dcs-3"
	default:
		return ""
	}
}

func serviceIP(service string) string {
	switch service {
	case "pacman-primary":
		return "172.28.0.11"
	case "pacman-replica":
		return "172.28.0.12"
	case "pacman-replica-2":
		return "172.28.0.13"
	case "pacman-dcs":
		return "172.28.0.10"
	case "pacman-dcs-2":
		return "172.28.0.14"
	case "pacman-dcs-3":
		return "172.28.0.15"
	default:
		return ""
	}
}

func sqlLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
