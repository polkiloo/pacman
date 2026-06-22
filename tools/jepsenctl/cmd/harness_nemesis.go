package cmd

import (
	"context"
	"fmt"
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
		promoted := "unknown"
		packetKillErr := lab.stopNodeRuntime(ctx, service)
		if packetKillErr == nil {
			promoted = lab.waitForCurrentPrimaryNot(ctx, member, 90*time.Second)
			if promoted == "unknown" {
				packetKillErr = fmt.Errorf("timed out waiting for promotion after partitioning and stopping %s", member)
			}
		}
		_ = lab.captureClusterSnapshot(ctx, caseDir, "during-nemesis", profile, member, service)
		if packetKillErr == nil {
			time.Sleep(lab.cfg.nemesisHold)
		}
		lab.iptablesHeal(ctx, service, peers)
		if restartErr := lab.startNodeRuntime(ctx, service); restartErr != nil {
			if packetKillErr == nil {
				packetKillErr = restartErr
			} else {
				packetKillErr = fmt.Errorf("%w; restart failed: %w", packetKillErr, restartErr)
			}
		}
		result := "ok"
		if packetKillErr != nil {
			result = "fail"
		}
		event("packet-kill", "stop", fmt.Sprintf(":target %q :promoted %q :result :%s", member, promoted, result))
		if packetKillErr != nil {
			_ = lab.captureClusterSnapshot(ctx, caseDir, "after-nemesis", profile, member, service)
			return packetKillErr
		}
	case "no-standby":
		return lab.strictSyncNoStandby(ctx, caseDir, scheduleFile, member, service, peers)
	case synchronousStandbyKillNemesisProfile:
		return lab.synchronousStandbyKill(ctx, caseDir, scheduleFile)
	case maximumLagOnFailoverNemesis:
		return lab.maximumLagOnFailover(ctx, caseDir, scheduleFile)
	case patroniCheckTimelineNemesis:
		return lab.patroniCheckTimelineFailover(ctx, caseDir, scheduleFile)
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
		lab.recordDCSQuorumRecoveryProbe(ctx, caseDir, profile, "after-primary-majority-partition", lab.cfg.dcsMajorityPartitionServices, service)
		event("primary-dcs-majority-partition", "stop", fmt.Sprintf(":target %q :dcs %q :result :ok", member, strings.Join(lab.cfg.dcsMajorityPartitionServices, " ")))
	case "dcs-full-restart":
		lab.dcsFullRestart(ctx, caseDir, scheduleFile, profile, lab.cfg.dcsRestartServices)
	case "dcs-slow-network":
		lab.dcsSlowNetwork(ctx, caseDir, scheduleFile, profile, lab.cfg.dcsSlowServices)
	case "failover-chain":
		lab.failoverChain(ctx, caseDir, scheduleFile)
	case "reinit-replica":
		return lab.reinitReplica(ctx, caseDir, scheduleFile)
	case "reinit-replica-kill-target":
		return lab.reinitReplicaKillTarget(ctx, caseDir, scheduleFile)
	case "reinit-replica-kill-source":
		return lab.reinitReplicaKillSource(ctx, caseDir, scheduleFile)
	case "reinit-replica-dcs-partition-target":
		return lab.reinitReplicaDCSPartitionTarget(ctx, caseDir, scheduleFile)
	case "reinit-replica-dcs-partition-primary":
		return lab.reinitReplicaDCSPartitionPrimary(ctx, caseDir, scheduleFile)
	case "reinit-replica-concurrent-request":
		return lab.reinitReplicaConcurrentRequest(ctx, caseDir, scheduleFile)
	case "reinit-replica-after-failover":
		return lab.reinitReplicaAfterFailover(ctx, caseDir, scheduleFile)
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
	if action == "stop" {
		appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :%s :action :heal %s}\n", time.Now().UTC().Format(time.RFC3339), name, value))
	}
	appendFile(scheduleFile, fmt.Sprintf("{:time %q :nemesis :%s :action :%s %s}\n", time.Now().UTC().Format(time.RFC3339), name, action, value))
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (lab *harnessLab) settleAfterNemesis(caseDir, profile string) {
	if profile == "none" || lab.cfg.postNemesisSettle <= 0 {
		return
	}
	appendFile(filepath.Join(caseDir, "nemesis.log"), fmt.Sprintf("settling for %s after %s nemesis healed\n", lab.cfg.postNemesisSettle, profile))
	time.Sleep(lab.cfg.postNemesisSettle)
}
