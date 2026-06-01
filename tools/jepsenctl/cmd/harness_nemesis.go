package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (lab *harnessLab) runNemesisProfile(ctx context.Context, profile, caseDir, scheduleFile string, duration time.Duration) *nemesisRun {
	run := &nemesisRun{done: make(chan struct{})}
	if profile == "none" {
		writeNemesisScheduleEvent(scheduleFile, "none", "start", `:target "none"`)
		_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", profile, "", "")
		writeNemesisScheduleEvent(scheduleFile, "none", "stop", `:target "none" :result :ok`)
		_ = lab.captureClusterSnapshot(ctx, caseDir, "after-nemesis", profile, "", "")
		close(run.done)
		return run
	}
	go func() {
		defer close(run.done)
		time.Sleep(maxDuration(duration/3, time.Second))
		run.err = lab.applyNemesis(ctx, profile, caseDir, scheduleFile)
	}()
	return run
}

func (run *nemesisRun) wait() error {
	if run != nil && run.done != nil {
		<-run.done
	}
	if run == nil {
		return nil
	}
	return run.err
}

func (lab *harnessLab) applyNemesis(ctx context.Context, profile, caseDir, scheduleFile string) error {
	member := lab.currentPrimaryName(ctx)
	service := lab.serviceForMember(member)
	if service == "" {
		service = lab.options.target.firstDataService()
	}
	peers := lab.peerServicesForMember(member)
	logFile := filepath.Join(caseDir, "nemesis.log")
	log := func(format string, args ...any) { appendFile(logFile, fmt.Sprintf(format+"\n", args...)) }
	event := func(name, action, value string) { writeNemesisScheduleEvent(scheduleFile, name, action, value) }

	switch profile {
	case "kill":
		event("kill", "start", fmt.Sprintf(":target %q", member))
		promoted := "unknown"
		killErr := lab.stopNodeRuntime(ctx, service)
		if killErr == nil {
			promoted = lab.waitForCurrentPrimaryNot(ctx, member, 90*time.Second)
			if promoted == "unknown" {
				killErr = fmt.Errorf("timed out waiting for promotion after stopping %s", member)
			}
		}
		_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", profile, member, service)
		if killErr == nil {
			time.Sleep(lab.cfg.nemesisHold)
		}
		if restartErr := lab.startNodeRuntime(ctx, service); restartErr != nil {
			if killErr == nil {
				killErr = restartErr
			} else {
				killErr = fmt.Errorf("%w; restart failed: %w", killErr, restartErr)
			}
		}
		result := "ok"
		if killErr != nil {
			result = "fail"
		}
		event("kill", "stop", fmt.Sprintf(":target %q :promoted %q :result :%s", member, promoted, result))
		if killErr != nil {
			_ = lab.captureClusterSnapshot(ctx, caseDir, "after-nemesis", profile, member, service)
			return killErr
		}
	case "switchover":
		candidate := lab.switchoverCandidate(ctx)
		event("switchover", "start", fmt.Sprintf(":source %q :target %q", member, candidate))
		output, status := lab.requestManualSwitchover(ctx, candidate, service)
		writeJSON(filepath.Join(caseDir, "manual-switchover.json"), map[string]any{
			"requestedAt":      time.Now().UTC().Format(time.RFC3339),
			"source":           member,
			"sourceService":    service,
			"candidate":        candidate,
			"candidateService": lab.serviceForMember(candidate),
			"controlService":   service,
			"exitStatus":       status,
			"output":           output,
		})
		_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", profile, candidate, service)
		time.Sleep(lab.cfg.nemesisHold)
		event("switchover", "stop", fmt.Sprintf(":source %q :target %q :exit-status %d", member, candidate, status))
	case "packet":
		event("packet", "start", fmt.Sprintf(":target %q", member))
		if err := lab.iptablesPartition(ctx, service, peers); err != nil {
			lab.iptablesHeal(ctx, service, peers)
			event("packet", "stop", fmt.Sprintf(":target %q :result :fail :error %q", member, err))
			return err
		}
		_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", profile, member, service)
		time.Sleep(lab.cfg.nemesisHold)
		lab.iptablesHeal(ctx, service, peers)
		event("packet", "stop", fmt.Sprintf(":target %q :result :ok", member))
	case "packet,kill":
		event("packet-kill", "start", fmt.Sprintf(":target %q", member))
		if err := lab.iptablesPartition(ctx, service, peers); err != nil {
			lab.iptablesHeal(ctx, service, peers)
			event("packet-kill", "stop", fmt.Sprintf(":target %q :result :fail :error %q", member, err))
			return err
		}
		_ = lab.stopPostgres(ctx, service)
		_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", profile, member, service)
		time.Sleep(lab.cfg.nemesisHold)
		_ = lab.startPostgres(ctx, service)
		lab.iptablesHeal(ctx, service, peers)
		event("packet-kill", "stop", fmt.Sprintf(":target %q :result :ok", member))
	case "primary-dcs-partition":
		targets := []string{"pacman-dcs", "pacman-dcs-2", "pacman-dcs-3"}
		event("primary-dcs-partition", "start", fmt.Sprintf(":target %q :dcs %q", member, strings.Join(targets, " ")))
		if err := lab.iptablesPartition(ctx, service, targets); err != nil {
			lab.iptablesHeal(ctx, service, targets)
			event("primary-dcs-partition", "stop", fmt.Sprintf(":target %q :dcs %q :result :fail :error %q", member, strings.Join(targets, " "), err))
			return err
		}
		_ = lab.recordClientTrafficProbe(ctx, caseDir, profile, member+"-dcs-isolated")
		_ = lab.recordReplicationHealthProbe(ctx, service, caseDir, profile)
		time.Sleep(lab.cfg.nemesisHold)
		lab.iptablesHeal(ctx, service, targets)
		event("primary-dcs-partition", "stop", fmt.Sprintf(":target %q :dcs %q :result :ok", member, strings.Join(targets, " ")))
	case "primary-replication-partition":
		event("primary-replication-partition", "start", fmt.Sprintf(":target %q", member))
		if err := lab.iptablesReplicationPartition(ctx, service, peers); err != nil {
			lab.iptablesReplicationHeal(ctx, service, peers)
			event("primary-replication-partition", "stop", fmt.Sprintf(":target %q :result :fail :error %q", member, err))
			return err
		}
		_ = lab.recordDCSTrafficProbe(ctx, service, caseDir, profile)
		time.Sleep(lab.cfg.nemesisHold)
		lab.iptablesReplicationHeal(ctx, service, peers)
		event("primary-replication-partition", "stop", fmt.Sprintf(":target %q :result :ok", member))
	case "dcs-kill-one":
		lab.dcsKill(ctx, caseDir, scheduleFile, profile, []string{lab.cfg.dcsKillService})
	case "dcs-lose-majority":
		lab.dcsKill(ctx, caseDir, scheduleFile, profile, lab.cfg.dcsMajorityKillServices)
	case "primary-dcs-majority-partition":
		event("primary-dcs-majority-partition", "start", fmt.Sprintf(":target %q :dcs %q", member, strings.Join(lab.cfg.dcsMajorityPartitionServices, " ")))
		_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "before-primary-majority-partition", lab.cfg.dcsMajorityPartitionServices, service)
		if err := lab.iptablesPartition(ctx, service, lab.cfg.dcsMajorityPartitionServices); err != nil {
			lab.iptablesHeal(ctx, service, lab.cfg.dcsMajorityPartitionServices)
			event("primary-dcs-majority-partition", "stop", fmt.Sprintf(":target %q :dcs %q :result :fail :error %q", member, strings.Join(lab.cfg.dcsMajorityPartitionServices, " "), err))
			return err
		}
		_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "during-primary-majority-partition", lab.cfg.dcsMajorityPartitionServices, service)
		time.Sleep(lab.cfg.nemesisHold)
		lab.iptablesHeal(ctx, service, lab.cfg.dcsMajorityPartitionServices)
		_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "after-primary-majority-partition", lab.cfg.dcsMajorityPartitionServices, service)
		event("primary-dcs-majority-partition", "stop", fmt.Sprintf(":target %q :dcs %q :result :ok", member, strings.Join(lab.cfg.dcsMajorityPartitionServices, " ")))
	case "dcs-full-restart":
		lab.dcsFullRestart(ctx, caseDir, scheduleFile, profile, lab.cfg.dcsRestartServices)
	case "dcs-slow-network":
		lab.dcsSlowNetwork(ctx, caseDir, scheduleFile, profile, lab.cfg.dcsSlowServices)
	case "failover-chain":
		lab.failoverChain(ctx, caseDir, scheduleFile)
	case "slow-network":
		event("slow-network", "start", fmt.Sprintf(":target %q", member))
		_ = lab.slowNetworkOn(ctx, service)
		time.Sleep(lab.cfg.nemesisHold)
		_ = lab.slowNetworkOff(ctx, service)
		event("slow-network", "stop", fmt.Sprintf(":target %q :result :ok", member))
	case "repeated-failure":
		event("repeated-failure", "start", fmt.Sprintf(":target %q", member))
		_ = lab.slowNetworkOn(ctx, service)
		time.Sleep(3 * time.Second)
		_ = lab.slowNetworkOff(ctx, service)
		if err := lab.iptablesPartition(ctx, service, peers); err != nil {
			lab.iptablesHeal(ctx, service, peers)
			event("repeated-failure", "stop", fmt.Sprintf(":target %q :result :fail :error %q", member, err))
			return err
		}
		time.Sleep(3 * time.Second)
		lab.iptablesHeal(ctx, service, peers)
		_ = lab.stopPostgres(ctx, service)
		time.Sleep(lab.cfg.nemesisHold)
		_ = lab.startPostgres(ctx, service)
		event("repeated-failure", "stop", fmt.Sprintf(":target %q :result :ok", member))
	default:
		log("unsupported nemesis profile: %s", profile)
	}
	_ = lab.captureClusterSnapshot(ctx, caseDir, "after-nemesis", profile, member, service)
	return nil
}

func writeNemesisScheduleEvent(scheduleFile, name, action, value string) {
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :%s :action :%s %s}\n", time.Now().UTC().Format(time.RFC3339), name, action, value))
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (lab *harnessLab) iptablesPartition(ctx context.Context, service string, peers []string) error {
	for _, peer := range peers {
		ip := serviceIP(peer)
		if ip == "" {
			return fmt.Errorf("iptables partition %s from %s: unknown peer service", service, peer)
		}
		output, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", fmt.Sprintf("iptables_bin=$(command -v iptables || command -v /usr/sbin/iptables || true); if [ -z \"$iptables_bin\" ]; then echo 'iptables command not found' >&2; exit 127; fi; \"$iptables_bin\" -I INPUT -s %s -j DROP && \"$iptables_bin\" -I OUTPUT -d %s -j DROP", ip, ip))
		if err != nil {
			return fmt.Errorf("iptables partition %s from %s: %w", service, peer, err)
		}
		if status != 0 {
			return fmt.Errorf("iptables partition %s from %s failed with status %d: %s", service, peer, status, strings.TrimSpace(output))
		}
	}
	return nil
}

func (lab *harnessLab) iptablesHeal(ctx context.Context, service string, peers []string) {
	for _, peer := range peers {
		ip := serviceIP(peer)
		if ip == "" {
			continue
		}
		_, _, _ = lab.composeExec(ctx, service, "/bin/sh", "-lc", fmt.Sprintf("while iptables -D INPUT -s %s -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT -d %s -j DROP 2>/dev/null; do :; done", ip, ip))
	}
}

func (lab *harnessLab) iptablesReplicationPartition(ctx context.Context, service string, peers []string) error {
	for _, peer := range peers {
		ip := serviceIP(peer)
		if ip == "" {
			return fmt.Errorf("iptables replication partition %s from %s: unknown peer service", service, peer)
		}
		output, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", fmt.Sprintf("iptables_bin=$(command -v iptables || command -v /usr/sbin/iptables || true); if [ -z \"$iptables_bin\" ]; then echo 'iptables command not found' >&2; exit 127; fi; \"$iptables_bin\" -I INPUT -s %s -p tcp --dport 5432 -j DROP && \"$iptables_bin\" -I OUTPUT -d %s -p tcp --sport 5432 -j DROP", ip, ip))
		if err != nil {
			return fmt.Errorf("iptables replication partition %s from %s: %w", service, peer, err)
		}
		if status != 0 {
			return fmt.Errorf("iptables replication partition %s from %s failed with status %d: %s", service, peer, status, strings.TrimSpace(output))
		}
	}
	return nil
}

func (lab *harnessLab) iptablesReplicationHeal(ctx context.Context, service string, peers []string) {
	for _, peer := range peers {
		ip := serviceIP(peer)
		if ip == "" {
			continue
		}
		_, _, _ = lab.composeExec(ctx, service, "/bin/sh", "-lc", fmt.Sprintf("while iptables -D INPUT -s %s -p tcp --dport 5432 -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT -d %s -p tcp --sport 5432 -j DROP 2>/dev/null; do :; done", ip, ip))
	}
}

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

func (lab *harnessLab) slowNetworkOn(ctx context.Context, service string) error {
	_, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", "tc_bin=$(command -v tc || command -v /usr/sbin/tc || true); if [ -z \"$tc_bin\" ]; then echo 'tc command not found' >&2; exit 127; fi; \"$tc_bin\" qdisc replace dev eth0 root netem delay 250ms 50ms loss 2%")
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("slow network on %s failed", service)
	}
	return nil
}

func (lab *harnessLab) slowNetworkOff(ctx context.Context, service string) error {
	_, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", "tc_bin=$(command -v tc || command -v /usr/sbin/tc || true); if [ -n \"$tc_bin\" ]; then \"$tc_bin\" qdisc del dev eth0 root 2>/dev/null || true; fi")
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("slow network off %s failed", service)
	}
	return nil
}

func (lab *harnessLab) dcsKill(ctx context.Context, caseDir, scheduleFile, profile string, services []string) {
	var members []string
	for _, service := range services {
		members = append(members, dcsMemberForService(service))
	}
	beforePhase, duringPhase, afterPhase := dcsQuorumPhases(profile)
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :%s :action :start :targets %q :members %q}\n", time.Now().UTC().Format(time.RFC3339), strings.ReplaceAll(profile, ",", "-"), strings.Join(services, " "), strings.Join(members, " ")))
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, beforePhase, services, "pacman-primary")
	for _, service := range services {
		_ = lab.stopDCSMember(ctx, service)
	}
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, duringPhase, services, "pacman-primary")
	time.Sleep(lab.cfg.nemesisHold)
	for _, service := range services {
		_ = lab.startDCSMember(ctx, service)
	}
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, afterPhase, services, "pacman-primary")
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :%s :action :stop :targets %q :members %q :result :ok}\n", time.Now().UTC().Format(time.RFC3339), strings.ReplaceAll(profile, ",", "-"), strings.Join(services, " "), strings.Join(members, " ")))
}

func (lab *harnessLab) dcsFullRestart(ctx context.Context, caseDir, scheduleFile, profile string, services []string) {
	lab.dcsKill(ctx, caseDir, scheduleFile, profile, services)
}

func (lab *harnessLab) dcsSlowNetwork(ctx context.Context, caseDir, scheduleFile, profile string, services []string) {
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :dcs-slow-network :action :start :targets %q}\n", time.Now().UTC().Format(time.RFC3339), strings.Join(services, " ")))
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "before-dcs-slow-network", services, "pacman-primary")
	for _, service := range services {
		_ = lab.slowNetworkOn(ctx, service)
	}
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "during-dcs-slow-network", services, "pacman-primary")
	time.Sleep(lab.cfg.nemesisHold)
	for _, service := range services {
		_ = lab.slowNetworkOff(ctx, service)
	}
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "after-dcs-slow-network", services, "pacman-primary")
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :dcs-slow-network :action :stop :targets %q :result :ok}\n", time.Now().UTC().Format(time.RFC3339), strings.Join(services, " ")))
}

func (lab *harnessLab) stopDCSMember(ctx context.Context, service string) error {
	member := dcsMemberForService(service)
	_, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", fmt.Sprintf("pkill -TERM -f '/usr/bin/[e]tcd .*--name %s' 2>/dev/null || true", member))
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("stop dcs %s failed", service)
	}
	return nil
}

func (lab *harnessLab) startDCSMember(ctx context.Context, service string) error {
	member := dcsMemberForService(service)
	initialCluster := "alpha-dcs=http://pacman-dcs:2380,alpha-dcs-2=http://pacman-dcs-2:2380,alpha-dcs-3=http://pacman-dcs-3:2380"
	command := fmt.Sprintf("nohup /usr/bin/etcd --name %s --data-dir /var/lib/etcd/pacman --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://%s:2379 --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://%s:2380 --initial-cluster %s --initial-cluster-state existing --initial-cluster-token pacman-cluster >>/var/log/etcd.log 2>&1 &", member, service, service, initialCluster)
	_, status, err := lab.composeExec(ctx, service, "/bin/sh", "-lc", command)
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("start dcs %s failed", service)
	}
	return nil
}

func (lab *harnessLab) failoverChain(ctx context.Context, caseDir, scheduleFile string) {
	chainFile := filepath.Join(caseDir, "failover-chain.jsonl")
	_ = os.WriteFile(chainFile, nil, 0o644)
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :failover-chain :action :start :target %q}\n", time.Now().UTC().Format(time.RFC3339), lab.currentPrimaryName(ctx)))
	step := 0
	for _, target := range []string{"alpha-2", "alpha-3", "alpha-1"} {
		_ = lab.waitForClusterSwitchoverReady(ctx, 90*time.Second)
		source := lab.currentPrimaryName(ctx)
		if source == "" || source == "unknown" || source == target {
			continue
		}
		step++
		sourceService := serviceForMember(source)
		output, status := lab.requestManualSwitchover(ctx, target, sourceService)
		if status == 0 && !lab.waitForCurrentPrimary(ctx, target, 75*time.Second) {
			status = 1
		}
		appendJSONL(chainFile, map[string]any{
			"step":          step,
			"requestedAt":   time.Now().UTC().Format(time.RFC3339),
			"source":        source,
			"sourceService": sourceService,
			"target":        target,
			"targetService": serviceForMember(target),
			"exitStatus":    status,
			"output":        output,
		})
		appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :failover-chain :action :step :source %q :target %q :exit-status %d}\n", time.Now().UTC().Format(time.RFC3339), source, target, status))
		if status != 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	time.Sleep(lab.cfg.nemesisHold)
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :failover-chain :action :stop :target %q :result :ok}\n", time.Now().UTC().Format(time.RFC3339), lab.currentPrimaryName(ctx)))
}

func (lab *harnessLab) settleAfterNemesis(caseDir, profile string) {
	if profile == "none" || lab.cfg.postNemesisSettle <= 0 {
		return
	}
	appendFile(filepath.Join(caseDir, "nemesis.log"), fmt.Sprintf("settling for %s after %s nemesis healed\n", lab.cfg.postNemesisSettle, profile))
	time.Sleep(lab.cfg.postNemesisSettle)
}
