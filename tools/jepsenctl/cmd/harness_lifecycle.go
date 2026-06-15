package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (lab *harnessLab) bootstrapLab(ctx context.Context) error {
	if lab.options.target.supportsPatroniLab() {
		return lab.bootstrapPatroniLab(ctx)
	}
	if !lab.options.target.supportsPACMANLab() {
		return fmt.Errorf("Jepsen target %s has no lab bootstrap", lab.options.target.Name)
	}

	if envOrDefault("PACMAN_JEPSEN_RESET_LAB", "true") == "true" {
		if status, err := lab.runHost(ctx, filepath.Join(lab.options.repoRoot, "deploy", "lab", "scripts", "reset-state.sh")); err != nil || status != 0 {
			if err != nil {
				return err
			}
			return fmt.Errorf("reset lab exited with status %d", status)
		}
	}

	env := append(lab.options.env,
		"PACMAN_LAB_AUTO_PREPARE="+envOrDefault("PACMAN_LAB_AUTO_PREPARE", "false"),
		"PACMAN_LAB_WAIT_FOR_OBSERVABILITY=false",
	)
	status, err := lab.options.runner.Run(ctx, commandSpec{
		name:   filepath.Join(lab.options.repoRoot, "deploy", "lab", "scripts", "bootstrap-cluster.sh"),
		dir:    lab.options.repoRoot,
		env:    env,
		stdout: lab.options.stdout,
		stderr: lab.options.stderr,
	})
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("bootstrap lab exited with status %d", status)
	}
	return nil
}

func (lab *harnessLab) bootstrapPatroniLab(ctx context.Context) error {
	if envOrDefault("PACMAN_JEPSEN_RESET_LAB", "true") == "true" {
		if _, status, err := lab.compose(ctx, "down", "--volumes", "--remove-orphans"); err != nil || status != 0 {
			if err != nil {
				return err
			}
			return fmt.Errorf("reset Patroni lab exited with status %d", status)
		}
	}
	if _, status, err := lab.compose(ctx, "up", "-d", "--build"); err != nil || status != 0 {
		if err != nil {
			return err
		}
		return fmt.Errorf("bootstrap Patroni lab exited with status %d", status)
	}
	_, err := lab.waitForThreeDataNodeCluster(ctx)
	return err
}

func (lab *harnessLab) bootstrapLabWithRetries(ctx context.Context, label string) error {
	attempts := envInt("PACMAN_JEPSEN_BOOTSTRAP_ATTEMPTS", 3)
	delay := time.Duration(envInt("PACMAN_JEPSEN_BOOTSTRAP_RETRY_DELAY_SECONDS", 5)) * time.Second
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := lab.bootstrapLab(ctx); err != nil {
			lastErr = err
			fmt.Fprintf(lab.options.stderr, "bootstrap failed for %s on attempt %d/%d: %v\n", label, attempt, attempts, err)
			if attempt < attempts {
				if status, destroyErr := lab.destroyLab(ctx); destroyErr != nil || status != 0 {
					if destroyErr != nil {
						fmt.Fprintf(lab.options.stderr, "bootstrap retry cleanup for %s failed after attempt %d/%d: %v\n", label, attempt, attempts, destroyErr)
					} else {
						fmt.Fprintf(lab.options.stderr, "bootstrap retry cleanup for %s exited with status %d after attempt %d/%d\n", label, status, attempt, attempts)
					}
				}
				time.Sleep(delay)
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (lab *harnessLab) collectArtifacts(ctx context.Context, runDir string, valid bool) error {
	for _, dir := range []string{
		filepath.Join(runDir, "node-logs"),
		filepath.Join(runDir, "postgres-logs"),
		filepath.Join(runDir, "dcs-logs"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	lab.writeComposeOutput(ctx, filepath.Join(runDir, "docker-compose-ps.txt"), "ps")
	lab.writeComposeOutput(ctx, filepath.Join(runDir, "docker-compose.log"), "logs", "--no-color")

	for _, node := range lab.options.target.DataNodes {
		if lab.options.target.supportsPatroniLab() {
			lab.writeComposeOutput(ctx, filepath.Join(runDir, "node-logs", node.Name+"-patroni.log"), "logs", "--no-color", node.Service)
			lab.writeComposeOutput(ctx, filepath.Join(runDir, "postgres-logs", node.Name+"-postgres.log"), "logs", "--no-color", node.Service)
			continue
		}
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "node-logs", node.Name+"-pacmand.log"), node.Service, "/bin/sh", "-lc", "cat /var/log/pacman/pacmand.log 2>/dev/null || true")
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "postgres-logs", node.Name+"-postgres.log"), node.Service, "/bin/sh", "-lc", "if [ -d /var/lib/pgsql/17/data/log ]; then find /var/lib/pgsql/17/data/log -maxdepth 1 -type f -print -exec cat {} \\; 2>/dev/null; fi")
	}
	for _, node := range lab.options.target.DCSNodes {
		if lab.options.target.supportsPatroniLab() {
			lab.writeComposeOutput(ctx, filepath.Join(runDir, "dcs-logs", node.Name+"-etcd.log"), "logs", "--no-color", node.Service)
			continue
		}
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "dcs-logs", node.Name+"-etcd.log"), node.Service, "/bin/sh", "-lc", "cat /var/log/etcd.log 2>/dev/null || true")
	}

	if lab.options.target.supportsPatroniLab() {
		output, _ := lab.clusterStatusJSON(ctx, lab.options.target.firstDataService())
		_ = os.WriteFile(filepath.Join(runDir, "patroni-cluster-after.json"), []byte(output+"\n"), 0o644)
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "patroni-rest-cluster-after.json"), lab.options.target.firstDataService(), "curl", "-fsS", "http://127.0.0.1:8008/cluster")
	} else {
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "pacman-cluster-after.json"), "pacman-primary", "env",
			"PACMANCTL_API_URL=http://pacman-primary:8080",
			"PACMANCTL_API_TOKEN="+pacmanAPIToken,
			"pacmanctl", "cluster", "status", "-o", "json")
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "pacman-history.json"), "pacman-primary", "env",
			"PACMANCTL_API_URL=http://pacman-primary:8080",
			"PACMANCTL_API_TOKEN="+pacmanAPIToken,
			"pacmanctl", "history", "list", "-o", "json")
	}

	if err := lab.writeResultsFile(runDir, valid); err != nil {
		return err
	}
	if !valid {
		lab.writeFailureDiagnostics(runDir)
	}
	return lab.writeArtifactIndexHTML(runDir, valid)
}

func (lab *harnessLab) destroyLabAfterSuite(ctx context.Context, runDir, historyFile string) error {
	if envOrDefault("PACMAN_JEPSEN_DESTROY_LAB", "true") != "true" {
		_, err := writeEDNEvent(historyFile, "destroy", "ok", `"preserved-docker-lab"`)
		return err
	}

	if _, err := writeEDNEvent(historyFile, "destroy", "invoke", `"docker-lab"`); err != nil {
		return err
	}
	status, err := lab.destroyLab(ctx)
	destroyed := err == nil && status == 0 && lab.labDestroyed(ctx)
	if destroyed {
		_, err = writeEDNEvent(historyFile, "destroy", "ok", `"docker-lab"`)
	} else {
		_, err = writeEDNEvent(historyFile, "destroy", "fail", `"docker-lab"`)
	}
	lab.writeComposeOutput(ctx, filepath.Join(runDir, "docker-compose-after-destroy.txt"), "ps")
	if err != nil {
		return err
	}
	if !destroyed {
		return fmt.Errorf("destroy lab failed")
	}
	return nil
}

func (lab *harnessLab) destroyLab(ctx context.Context) (int, error) {
	if lab.options.target.supportsPatroniLab() {
		_, status, err := lab.compose(ctx, "down", "--volumes", "--remove-orphans")
		return status, err
	}
	return lab.runHost(ctx, filepath.Join(lab.options.repoRoot, "deploy", "lab", "scripts", "destroy-cluster.sh"))
}

func (lab *harnessLab) writeResultsFile(runDir string, valid bool) error {
	status := "false"
	if valid {
		status = "true"
	}
	value := fmt.Sprintf("{:valid? %s\n :campaign %q\n :target %q\n :target-store %q\n :checked-at %q}\n",
		status,
		envOrDefault("PACMAN_JEPSEN_CAMPAIGN", lab.options.campaign),
		lab.options.target.Name,
		lab.options.target.StoreName,
		time.Now().UTC().Format(time.RFC3339),
	)
	return os.WriteFile(filepath.Join(runDir, "results.edn"), []byte(value), 0o644)
}

func (lab *harnessLab) writeFailureDiagnostics(runDir string) {
	rootArtifacts := []string{
		"results.edn",
		"jepsen-history.edn",
		"nemesis-schedule.edn",
		"case-results.jsonl",
		"docker-compose-ps.txt",
		"docker-compose.log",
	}
	if lab.options.target.supportsPatroniLab() {
		rootArtifacts = append(rootArtifacts, "patroni-cluster-after.json", "patroni-rest-cluster-after.json")
	} else {
		rootArtifacts = append(rootArtifacts, "pacman-cluster-after.json", "pacman-history.json")
	}
	if _, err := os.Stat(filepath.Join(runDir, "nightly-failures.txt")); err == nil {
		rootArtifacts = append(rootArtifacts, "nightly-failures.txt")
	}

	diagnostics := map[string]any{
		"generatedAt":       time.Now().UTC().Format(time.RFC3339),
		"target":            lab.options.target.Name,
		"targetStore":       lab.options.target.StoreName,
		"campaign":          envOrDefault("PACMAN_JEPSEN_CAMPAIGN", lab.options.campaign),
		"requiredArtifacts": artifactPresence(runDir, rootArtifacts),
		"logDirectories": map[string]any{
			"node-logs":     directoryDiagnostics(filepath.Join(runDir, "node-logs")),
			"postgres-logs": directoryDiagnostics(filepath.Join(runDir, "postgres-logs")),
			"dcs-logs":      directoryDiagnostics(filepath.Join(runDir, "dcs-logs")),
		},
		"cases":    caseDiagnostics(filepath.Join(runDir, "cases")),
		"failures": collectFailureSummary(runDir),
	}
	writeJSON(filepath.Join(runDir, "failure-diagnostics.json"), diagnostics)
}

func artifactPresence(root string, relativePaths []string) []map[string]any {
	results := make([]map[string]any, 0, len(relativePaths))
	for _, relativePath := range relativePaths {
		path := filepath.Join(root, relativePath)
		info, err := os.Stat(path)
		results = append(results, map[string]any{
			"path":      relativePath,
			"present":   err == nil,
			"sizeBytes": fileSizeForInfo(info, err),
		})
	}
	return results
}

func directoryDiagnostics(path string) map[string]any {
	files := []string{}
	totalBytes := int64(0)
	_ = filepath.WalkDir(path, func(entryPath string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		relative, relErr := filepath.Rel(path, entryPath)
		if relErr != nil {
			relative = filepath.Base(entryPath)
		}
		files = append(files, relative)
		if info, statErr := entry.Info(); statErr == nil {
			totalBytes += info.Size()
		}
		return nil
	})
	sort.Strings(files)
	return map[string]any{
		"present":    dirExists(path),
		"fileCount":  len(files),
		"totalBytes": totalBytes,
		"files":      files,
	}
}

func caseDiagnostics(casesDir string) []map[string]any {
	entries, err := os.ReadDir(casesDir)
	if err != nil {
		return nil
	}
	required := []string{
		"history.edn",
		"nemesis.log",
		"nemesis-schedule.edn",
		"primary-observations.jsonl",
		"pacman-cluster-snapshots.jsonl",
		"checker.json",
	}
	var cases []map[string]any
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		caseDir := filepath.Join(casesDir, entry.Name())
		checkers := filesWithSuffix(caseDir, "checker.json")
		cases = append(cases, map[string]any{
			"name":              entry.Name(),
			"requiredArtifacts": artifactPresence(caseDir, required),
			"checkerArtifacts":  checkers,
		})
	}
	sort.Slice(cases, func(left, right int) bool {
		return cases[left]["name"].(string) < cases[right]["name"].(string)
	})
	return cases
}

func filesWithSuffix(root, suffix string) []string {
	var files []string
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(filepath.Base(path), suffix) {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			relative = filepath.Base(path)
		}
		files = append(files, relative)
		return nil
	})
	sort.Strings(files)
	return files
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func fileSizeForInfo(info os.FileInfo, err error) int64 {
	if err != nil || info == nil || info.IsDir() {
		return 0
	}
	return info.Size()
}

func (lab *harnessLab) writeArtifactIndexHTML(runDir string, valid bool) error {
	status := "false"
	if valid {
		status = "true"
	}
	campaign := envOrDefault("PACMAN_JEPSEN_CAMPAIGN", lab.options.campaign)
	clusterArtifacts := `<li><a href="pacman-cluster-after.json">pacman-cluster-after.json</a></li>
<li><a href="pacman-history.json">pacman-history.json</a></li>`
	if lab.options.target.supportsPatroniLab() {
		clusterArtifacts = `<li><a href="patroni-cluster-after.json">patroni-cluster-after.json</a></li>
<li><a href="patroni-rest-cluster-after.json">patroni-rest-cluster-after.json</a></li>`
	}
	html := fmt.Sprintf(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>Jepsen %s %s</title></head>
<body>
<h1>Jepsen %s %s</h1>
<p>Status: %s</p>
<ul>
<li><a href="results.edn">results.edn</a></li>
<li><a href="failure-diagnostics.json">failure-diagnostics.json</a></li>
<li><a href="case-results.jsonl">case-results.jsonl</a></li>
<li><a href="jepsen-history.edn">jepsen-history.edn</a></li>
<li><a href="nemesis-schedule.edn">nemesis-schedule.edn</a></li>
%s
<li>Per-case: primary-observations.jsonl, pacman-cluster-snapshots.jsonl, pg-stat-replication.json, pg-stat-wal-receiver.jsonl, single-primary-checker.json, acknowledged-write-checker.json, timeline-checker.json, old-primary-rejoin-checker.json, manual-switchover-checker.json, client-traffic-during-nemesis-checker.json, replication-traffic-during-nemesis-checker.json, dcs-traffic-during-nemesis-checker.json, dcs-quorum-checker.json, failover-chain-checker.json, open-transaction-checker.json, vip-routing-checker.json, synchronous-replication-config.json, synchronous-replication-checker.json, synchronous-standby-kill-checker.json, synchronous-standby-kill-probes.jsonl, strict-sync-no-standby-checker.json, strict-sync-write-probes.jsonl, maximum-lag-on-failover-config.json, maximum-lag-on-failover-checker.json, maximum-lag-on-failover-probes.jsonl, patroni-check-timeline-config.json, patroni-check-timeline-checker.json, and patroni-check-timeline-probes.jsonl</li>
</ul>
</body>
</html>
`, lab.options.target.Name, campaign, lab.options.target.Name, campaign, status, clusterArtifacts)
	return os.WriteFile(filepath.Join(runDir, "index.html"), []byte(html), 0o644)
}

func (lab *harnessLab) writeComposeOutput(ctx context.Context, path string, args ...string) {
	output, _, _ := lab.compose(ctx, args...)
	_ = os.WriteFile(path, []byte(output), 0o644)
}

func (lab *harnessLab) writeComposeExecOutput(ctx context.Context, path, service string, args ...string) {
	output, _, _ := lab.composeExec(ctx, service, args...)
	_ = os.WriteFile(path, []byte(output), 0o644)
}

func (lab *harnessLab) labDestroyed(ctx context.Context) bool {
	output, _, _ := lab.compose(ctx, "ps", "-q")
	return strings.TrimSpace(output) == ""
}
