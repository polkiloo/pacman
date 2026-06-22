package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (lab *harnessLab) dcsKill(ctx context.Context, caseDir, scheduleFile, profile string, services []string) {
	var members []string
	for _, service := range services {
		members = append(members, dcsMemberForService(service))
	}
	beforePhase, duringPhase, afterPhase := dcsQuorumPhases(profile)
	scheduleName := strings.ReplaceAll(profile, ",", "-")
	targets := strings.Join(services, " ")
	writeNemesisScheduleEvent(scheduleFile, scheduleName, "start", fmt.Sprintf(":target %q :targets %q :members %q", targets, targets, strings.Join(members, " ")))
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, beforePhase, services, "pacman-primary")
	for _, service := range services {
		_ = lab.stopDCSMember(ctx, service)
	}
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, duringPhase, services, "pacman-primary")
	time.Sleep(lab.cfg.nemesisHold)
	for _, service := range services {
		_ = lab.startDCSMember(ctx, service)
	}
	lab.recordDCSQuorumRecoveryProbe(ctx, caseDir, profile, afterPhase, services, "pacman-primary")
	writeNemesisScheduleEvent(scheduleFile, scheduleName, "stop", fmt.Sprintf(":target %q :targets %q :members %q :result :ok", targets, targets, strings.Join(members, " ")))
}

func (lab *harnessLab) dcsFullRestart(ctx context.Context, caseDir, scheduleFile, profile string, services []string) {
	lab.dcsKill(ctx, caseDir, scheduleFile, profile, services)
}

func (lab *harnessLab) dcsSlowNetwork(ctx context.Context, caseDir, scheduleFile, profile string, services []string) {
	targets := strings.Join(services, " ")
	writeNemesisScheduleEvent(scheduleFile, "dcs-slow-network", "start", fmt.Sprintf(":target %q :targets %q", targets, targets))
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "before-dcs-slow-network", services, "pacman-primary")
	for _, service := range services {
		_ = lab.slowNetworkOn(ctx, service)
	}
	_ = lab.recordDCSQuorumProbe(ctx, caseDir, profile, "during-dcs-slow-network", services, "pacman-primary")
	time.Sleep(lab.cfg.nemesisHold)
	for _, service := range services {
		_ = lab.slowNetworkOff(ctx, service)
	}
	lab.recordDCSQuorumRecoveryProbe(ctx, caseDir, profile, "after-dcs-slow-network", services, "pacman-primary")
	writeNemesisScheduleEvent(scheduleFile, "dcs-slow-network", "stop", fmt.Sprintf(":target %q :targets %q :result :ok", targets, targets))
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
