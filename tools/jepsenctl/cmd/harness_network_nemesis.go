package cmd

import (
	"context"
	"fmt"
	"net"
	"strings"
)

func (lab *harnessLab) iptablesPartition(ctx context.Context, service string, peers []string) error {
	for _, peer := range peers {
		ip, err := lab.partitionServiceIP(ctx, service, peer)
		if err != nil {
			return fmt.Errorf("iptables partition %s from %s: %w", service, peer, err)
		}
		output, status, err := lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", fmt.Sprintf("iptables_bin=$(command -v iptables || command -v /usr/sbin/iptables || true); if [ -z \"$iptables_bin\" ]; then echo 'iptables command not found' >&2; exit 127; fi; \"$iptables_bin\" -I INPUT -s %s -j DROP && \"$iptables_bin\" -I OUTPUT -d %s -j DROP", ip, ip))
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
		ip, err := lab.partitionServiceIP(ctx, service, peer)
		if err != nil {
			continue
		}
		_, _, _ = lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", fmt.Sprintf("while iptables -D INPUT -s %s -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT -d %s -j DROP 2>/dev/null; do :; done", ip, ip))
	}
}

func (lab *harnessLab) iptablesReplicationPartition(ctx context.Context, service string, peers []string) error {
	for _, peer := range peers {
		ip, err := lab.partitionServiceIP(ctx, service, peer)
		if err != nil {
			return fmt.Errorf("iptables replication partition %s from %s: %w", service, peer, err)
		}
		output, status, err := lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", fmt.Sprintf("iptables_bin=$(command -v iptables || command -v /usr/sbin/iptables || true); if [ -z \"$iptables_bin\" ]; then echo 'iptables command not found' >&2; exit 127; fi; \"$iptables_bin\" -I INPUT -s %s -p tcp --dport 5432 -j DROP && \"$iptables_bin\" -I OUTPUT -d %s -p tcp --sport 5432 -j DROP", ip, ip))
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
		ip, err := lab.partitionServiceIP(ctx, service, peer)
		if err != nil {
			continue
		}
		_, _, _ = lab.composeExecAsUser(ctx, "root", service, "/bin/sh", "-lc", fmt.Sprintf("while iptables -D INPUT -s %s -p tcp --dport 5432 -j DROP 2>/dev/null; do :; done; while iptables -D OUTPUT -d %s -p tcp --sport 5432 -j DROP 2>/dev/null; do :; done", ip, ip))
	}
}

func (lab *harnessLab) partitionServiceIP(ctx context.Context, observer, peer string) (string, error) {
	if ip := serviceIP(peer); ip != "" {
		return ip, nil
	}
	if !lab.options.target.hasService(peer) {
		return "", fmt.Errorf("unknown peer service")
	}
	output, status, err := lab.composeExec(ctx, observer, "getent", "ahostsv4", peer)
	if err != nil {
		return "", err
	}
	if status != 0 {
		return "", fmt.Errorf("resolve service IP failed with status %d: %s", status, strings.TrimSpace(output))
	}
	fields := strings.Fields(output)
	if len(fields) == 0 || net.ParseIP(fields[0]).To4() == nil {
		return "", fmt.Errorf("resolve service IP returned no IPv4 address: %s", strings.TrimSpace(output))
	}
	return fields[0], nil
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
