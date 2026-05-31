package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (lab *harnessLab) bootstrapLab(ctx context.Context) error {
	if !lab.options.target.supportsPACMANLab() {
		return fmt.Errorf("Jepsen target %s is registered, but deploy/lab bootstrap currently supports %s only", lab.options.target.Name, defaultJepsenTarget)
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

func (lab *harnessLab) bootstrapLabWithRetries(ctx context.Context, label string) error {
	attempts := envInt("PACMAN_JEPSEN_BOOTSTRAP_ATTEMPTS", 3)
	delay := time.Duration(envInt("PACMAN_JEPSEN_BOOTSTRAP_RETRY_DELAY_SECONDS", 5)) * time.Second
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := lab.bootstrapLab(ctx); err != nil {
			lastErr = err
			fmt.Fprintf(lab.options.stderr, "bootstrap failed for %s on attempt %d/%d: %v\n", label, attempt, attempts, err)
			if attempt < attempts {
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

	for _, node := range []struct {
		service string
		member  string
	}{
		{"pacman-primary", "alpha-1"},
		{"pacman-replica", "alpha-2"},
		{"pacman-replica-2", "alpha-3"},
	} {
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "node-logs", node.member+"-pacmand.log"), node.service, "/bin/sh", "-lc", "cat /var/log/pacman/pacmand.log 2>/dev/null || true")
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "postgres-logs", node.member+"-postgres.log"), node.service, "/bin/sh", "-lc", "if [ -d /var/lib/pgsql/17/data/log ]; then find /var/lib/pgsql/17/data/log -maxdepth 1 -type f -print -exec cat {} \\; 2>/dev/null; fi")
	}
	for _, node := range []struct {
		service string
		member  string
	}{
		{"pacman-dcs", "alpha-dcs"},
		{"pacman-dcs-2", "alpha-dcs-2"},
		{"pacman-dcs-3", "alpha-dcs-3"},
	} {
		lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "dcs-logs", node.member+"-etcd.log"), node.service, "/bin/sh", "-lc", "cat /var/log/etcd.log 2>/dev/null || true")
	}

	lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "pacman-cluster-after.json"), "pacman-primary", "env",
		"PACMANCTL_API_URL=http://pacman-primary:8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "status", "-o", "json")
	lab.writeComposeExecOutput(ctx, filepath.Join(runDir, "pacman-history.json"), "pacman-primary", "env",
		"PACMANCTL_API_URL=http://pacman-primary:8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "history", "list", "-o", "json")

	if err := lab.writeResultsFile(runDir, valid); err != nil {
		return err
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
	status, err := lab.runHost(ctx, filepath.Join(lab.options.repoRoot, "deploy", "lab", "scripts", "destroy-cluster.sh"))
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

func (lab *harnessLab) writeArtifactIndexHTML(runDir string, valid bool) error {
	status := "false"
	if valid {
		status = "true"
	}
	campaign := envOrDefault("PACMAN_JEPSEN_CAMPAIGN", lab.options.campaign)
	html := fmt.Sprintf(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>PACMAN Jepsen %s</title></head>
<body>
<h1>PACMAN Jepsen %s</h1>
<p>Status: %s</p>
<ul>
<li><a href="results.edn">results.edn</a></li>
<li><a href="case-results.jsonl">case-results.jsonl</a></li>
<li><a href="jepsen-history.edn">jepsen-history.edn</a></li>
<li><a href="nemesis-schedule.edn">nemesis-schedule.edn</a></li>
<li><a href="pacman-cluster-after.json">pacman-cluster-after.json</a></li>
<li><a href="pacman-history.json">pacman-history.json</a></li>
<li>Per-case: primary-observations.jsonl, pacman-cluster-snapshots.jsonl, pg-stat-replication.json, pg-stat-wal-receiver.jsonl, single-primary-checker.json, acknowledged-write-checker.json, timeline-checker.json, old-primary-rejoin-checker.json, manual-switchover-checker.json, client-traffic-during-nemesis-checker.json, replication-traffic-during-nemesis-checker.json, dcs-traffic-during-nemesis-checker.json, dcs-quorum-checker.json, failover-chain-checker.json, open-transaction-checker.json, and vip-routing-checker.json</li>
</ul>
</body>
</html>
`, campaign, campaign, status)
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
