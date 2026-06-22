package cmd

import (
	"context"
	"fmt"
	"time"
)

func (lab *harnessLab) stopPostgres(ctx context.Context, service string) error {
	_, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", "runuser -u postgres -- /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/data -m immediate stop || true")
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("stop postgres status %d", status)
	}
	return nil
}

func (lab *harnessLab) startPostgres(ctx context.Context, service string) error {
	_, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", "runuser -u postgres -- /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/data -w start -l /var/lib/pgsql/17/data/log/jepsen-restart.log || true")
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("start postgres status %d", status)
	}
	return nil
}

func (lab *harnessLab) stopNodeRuntime(ctx context.Context, service string) error {
	if lab.options.target.supportsPatroniLab() {
		return lab.stopPatroniNodeRuntime(ctx, service)
	}
	return lab.stopPacmanNodeRuntime(ctx, service)
}

func (lab *harnessLab) startNodeRuntime(ctx context.Context, service string) error {
	if lab.options.target.supportsPatroniLab() {
		return lab.startPatroniNodeRuntime(ctx, service)
	}
	return lab.startPacmanNodeRuntime(ctx, service)
}

func (lab *harnessLab) stopPatroniNodeRuntime(ctx context.Context, service string) error {
	_, status, err := lab.compose(ctx, "stop", service)
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("stop Patroni runtime %s status=%d", service, status)
	}
	return nil
}

func (lab *harnessLab) startPatroniNodeRuntime(ctx context.Context, service string) error {
	_, status, err := lab.compose(ctx, "start", service)
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("start Patroni runtime %s status=%d", service, status)
	}
	return nil
}

func (lab *harnessLab) stopPacmanNodeRuntime(ctx context.Context, service string) error {
	commands := []string{
		"pids=$(pgrep -f '/usr/local/bin/[v]ip-manager --config /etc/pacman/vip-manager.yml' 2>/dev/null || true); if [ -n \"$pids\" ]; then kill $pids; fi",
		fmt.Sprintf("ip addr del '%s/24' dev '%s' 2>/dev/null || true", lab.cfg.pgHost, lab.cfg.vipInterface),
		"pkill -u postgres -f '/usr/bin/[p]acmand -config /etc/pacman/pacmand.yaml' 2>/dev/null || true",
		"runuser -u postgres -- /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/data -m immediate stop || true",
	}
	for _, command := range commands {
		if _, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", command); err != nil {
			return fmt.Errorf("stop runtime %s status=%d: %w", service, status, err)
		} else if status != 0 {
			return fmt.Errorf("stop runtime %s status=%d", service, status)
		}
	}
	return nil
}

func (lab *harnessLab) startPacmanNodeRuntime(ctx context.Context, service string) error {
	commands := []string{
		"mkdir -p /var/log/pacman; cd /var/lib/pacman && nohup runuser -u postgres -- /bin/bash -lc '. /etc/sysconfig/pacmand 2>/dev/null || true; export PACMAND_CONFIG PACMAND_EXTRA_ARGS PGPASSWORD; cd /var/lib/pacman && exec /usr/bin/pacmand -config \"${PACMAND_CONFIG:-/etc/pacman/pacmand.yaml}\" ${PACMAND_EXTRA_ARGS:-}' >>/var/log/pacman/pacmand.log 2>&1 &",
		"nohup /usr/local/bin/vip-manager --config /etc/pacman/vip-manager.yml </dev/null >>/var/log/pacman/vip-manager.log 2>&1 &",
	}
	for _, command := range commands {
		if _, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", command); err != nil {
			return fmt.Errorf("start runtime %s status=%d: %w", service, status, err)
		} else if status != 0 {
			return fmt.Errorf("start runtime %s status=%d", service, status)
		}
	}
	return nil
}

func (lab *harnessLab) requestManualSwitchover(ctx context.Context, candidate, service string) (string, int) {
	if candidate == "" {
		return "no healthy non-primary switchover candidate found", 2
	}
	output, status, _ := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "switchover",
		"-candidate", candidate,
		"-reason", "jepsen-manual-switchover",
		"-requested-by", "jepsen",
		"-force")
	return output, status
}

func (lab *harnessLab) waitForCurrentPrimaryNot(ctx context.Context, member string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current := lab.currentPrimaryName(ctx)
		if current != "" && current != "unknown" && current != member {
			return current
		}
		time.Sleep(time.Second)
	}
	return "unknown"
}

func (lab *harnessLab) waitForCurrentPrimary(ctx context.Context, member string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lab.currentPrimaryName(ctx) == member {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func (lab *harnessLab) clusterSwitchoverReady(ctx context.Context) bool {
	status, _, err := lab.pacmanClusterStatusAny(ctx)
	if err != nil || status.Phase != "healthy" {
		return false
	}
	for _, member := range status.Members {
		if !member.Healthy {
			return false
		}
	}
	return true
}

func (lab *harnessLab) waitForClusterSwitchoverReady(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lab.clusterSwitchoverReady(ctx) {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}
