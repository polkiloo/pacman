package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const defaultJepsenTarget = "pacman-3-data"

type jepsenTarget struct {
	Name        string
	StoreName   string
	Description string
	ComposeFile string
	PGClient    string
	PGHost      string
	PGPassword  string
	PSQLBinary  string
	DataNodes   []targetNode
	DCSNodes    []targetNode
}

type targetNode struct {
	Name    string
	Service string
	Role    string
}

func newTargetsCommand(stdout io.Writer) *cobra.Command {
	targets := &cobra.Command{
		Use:   "targets",
		Short: "work with Jepsen target registry",
	}

	targets.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "list supported Jepsen targets",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("targets list does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, target := range defaultJepsenTargets() {
				fmt.Fprintf(stdout, "%s %s %s data=%s dcs=%s\n",
					target.Name,
					target.StoreName,
					target.Description,
					formatTargetNodes(target.DataNodes),
					formatTargetNodes(target.DCSNodes),
				)
			}
			return nil
		},
	})

	return targets
}

func defaultJepsenTargets() []jepsenTarget {
	return []jepsenTarget{
		{
			Name:        "pacman-3-data",
			StoreName:   "pacman",
			Description: "PACMAN Docker lab with three PostgreSQL data nodes and three external etcd DCS nodes.",
			ComposeFile: "deploy/lab/compose.yml",
			PGClient:    "pacman-primary",
			PGHost:      "172.28.0.100",
			PGPassword:  "pacman-demo-password",
			PSQLBinary:  "/usr/pgsql-17/bin/psql",
			DataNodes: []targetNode{
				{Name: "alpha-1", Service: "pacman-primary", Role: "data"},
				{Name: "alpha-2", Service: "pacman-replica", Role: "data"},
				{Name: "alpha-3", Service: "pacman-replica-2", Role: "data"},
			},
			DCSNodes: []targetNode{
				{Name: "alpha-dcs", Service: "pacman-dcs", Role: "dcs"},
				{Name: "alpha-dcs-2", Service: "pacman-dcs-2", Role: "dcs"},
				{Name: "alpha-dcs-3", Service: "pacman-dcs-3", Role: "dcs"},
			},
		},
		{
			Name:        "patroni-3-data",
			StoreName:   "patroni",
			Description: "Patroni calibration baseline with three PostgreSQL data nodes and the same three-node etcd DCS shape.",
			ComposeFile: "deploy/patroni-lab/compose.yml",
			PGClient:    "patroni-primary",
			PGHost:      "127.0.0.1",
			PGPassword:  "patroni-demo-password",
			PSQLBinary:  "/usr/bin/psql",
			DataNodes: []targetNode{
				{Name: "patroni-1", Service: "patroni-primary", Role: "data"},
				{Name: "patroni-2", Service: "patroni-replica", Role: "data"},
				{Name: "patroni-3", Service: "patroni-replica-2", Role: "data"},
			},
			DCSNodes: []targetNode{
				{Name: "patroni-dcs", Service: "patroni-dcs", Role: "dcs"},
				{Name: "patroni-dcs-2", Service: "patroni-dcs-2", Role: "dcs"},
				{Name: "patroni-dcs-3", Service: "patroni-dcs-3", Role: "dcs"},
			},
		},
	}
}

func resolveJepsenTarget(name string) (jepsenTarget, error) {
	if name == "" {
		name = defaultJepsenTarget
	}
	for _, target := range defaultJepsenTargets() {
		if target.Name == name {
			return target, nil
		}
	}

	var supported []string
	for _, target := range defaultJepsenTargets() {
		supported = append(supported, target.Name)
	}
	return jepsenTarget{}, fmt.Errorf("unsupported Jepsen target %q; supported targets: %s", name, strings.Join(supported, ", "))
}

func formatTargetNodes(nodes []targetNode) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, node.Name+"@"+node.Service)
	}
	return strings.Join(parts, ",")
}

func (target jepsenTarget) supportsPACMANLab() bool {
	return target.Name == "pacman-3-data"
}

func (target jepsenTarget) supportsPatroniLab() bool {
	return target.Name == "patroni-3-data"
}

func (target jepsenTarget) serviceForMember(member string) string {
	for _, node := range target.DataNodes {
		if node.Name == member {
			return node.Service
		}
	}
	return ""
}

func (target jepsenTarget) memberForService(service string) string {
	for _, node := range target.DataNodes {
		if node.Service == service {
			return node.Name
		}
	}
	return ""
}

func (target jepsenTarget) firstDataService() string {
	if len(target.DataNodes) == 0 {
		return ""
	}
	return target.DataNodes[0].Service
}

func (target jepsenTarget) firstDataMember() string {
	if len(target.DataNodes) == 0 {
		return ""
	}
	return target.DataNodes[0].Name
}

func (target jepsenTarget) hasService(service string) bool {
	for _, nodes := range [][]targetNode{target.DataNodes, target.DCSNodes} {
		for _, node := range nodes {
			if node.Service == service {
				return true
			}
		}
	}
	return false
}

func (target jepsenTarget) supportsCase(workload, nemesis string) bool {
	if target.supportsPACMANLab() {
		return !isPatroniOnlyWorkload(workload)
	}
	if !target.supportsPatroniLab() {
		return false
	}
	return (workload == "append-smoke" && nemesis == "none") ||
		(workload == "append-failover" && nemesis == "kill") ||
		(workload == "single-key-register" && nemesis == "packet") ||
		(workload == "append-sync" && nemesis == "kill") ||
		(workload == "append-sync" && nemesis == "sync-standby-kill") ||
		(workload == "append-sync-two" && nemesis == "none") ||
		(workload == "append-strict-sync" && nemesis == "no-standby") ||
		(workload == "append-max-lag" && nemesis == maximumLagOnFailoverNemesis) ||
		(workload == "append-check-timeline" && nemesis == patroniCheckTimelineNemesis)
}

func isPatroniOnlyWorkload(workload string) bool {
	if _, ok := patroniSynchronousProfile(workload); ok {
		return true
	}
	if _, ok := resolvePatroniMaximumLagOnFailoverProfile(workload); ok {
		return true
	}
	_, ok := resolvePatroniCheckTimelineProfile(workload)
	return ok
}
