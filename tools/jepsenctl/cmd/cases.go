package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

type jepsenCase struct {
	Slug          string
	Spec          string
	Description   string
	PatroniOnly   bool
	NightlyUnsafe bool
}

type caseTargetValidation struct {
	CaseCount    int
	TargetCount  int
	MissingLines []string
}

type casesValidateOptions struct {
	casesFile string
	listCases string
	makefiles []string
}

func newCasesCommand(stdout, stderr io.Writer) *cobra.Command {
	cases := &cobra.Command{
		Use:   "cases",
		Short: "work with Jepsen case registry",
	}

	cases.AddCommand(newCasesListCommand(stdout))
	cases.AddCommand(newCasesValidateCommand(stdout, stderr))

	return cases
}

func newCasesListCommand(stdout io.Writer) *cobra.Command {
	list := &cobra.Command{
		Use:   "list",
		Short: "list supported Jepsen cases",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("cases list does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, testCase := range defaultJepsenCases() {
				fmt.Fprintf(stdout, "%s %s %s\n", testCase.Slug, testCase.Spec, testCase.Description)
			}
			return nil
		},
	}
	return list
}

func newCasesValidateCommand(stdout, stderr io.Writer) *cobra.Command {
	options := casesValidateOptions{}

	validate := &cobra.Command{
		Use:   "validate",
		Short: "validate Jepsen case registry targets",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("cases validate does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			caseOutput, err := loadCaseRegistry(options)
			if err != nil {
				return err
			}

			cases, err := parseListCasesOutput(caseOutput)
			if err != nil {
				return err
			}

			makefiles := options.makefiles
			if len(makefiles) == 0 {
				makefiles, err = discoverMakefiles(".")
				if err != nil {
					return err
				}
			}

			targets, err := parseMakeTargets(makefiles)
			if err != nil {
				return err
			}

			result := validateCaseTargets(cases, targets)
			for _, line := range result.MissingLines {
				fmt.Fprintln(stderr, line)
			}
			if len(result.MissingLines) > 0 {
				return fmt.Errorf("missing %d Jepsen case Make target(s)", len(result.MissingLines))
			}

			fmt.Fprintf(stdout, "validated %d Jepsen cases and %d required Make targets\n", result.CaseCount, result.TargetCount)
			return nil
		},
	}

	validate.Flags().StringVar(&options.casesFile, "cases-file", "", "path to captured case registry output")
	validate.Flags().StringVar(&options.listCases, "list-cases", options.listCases, "optional external case registry command")
	validate.Flags().StringArrayVar(&options.makefiles, "makefile", nil, "Makefile path to scan for Jepsen case targets; may be repeated")

	return validate
}

func loadCaseRegistry(options casesValidateOptions) ([]byte, error) {
	if options.casesFile != "" {
		return os.ReadFile(options.casesFile)
	}
	if options.listCases == "" {
		var output bytes.Buffer
		for _, testCase := range defaultJepsenCases() {
			fmt.Fprintf(&output, "%s %s %s\n", testCase.Slug, testCase.Spec, testCase.Description)
		}
		return output.Bytes(), nil
	}

	cmd := exec.Command(options.listCases)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", options.listCases, err)
	}
	return output, nil
}

func defaultJepsenCases() []jepsenCase {
	return []jepsenCase{
		{Slug: "append-smoke-none", Spec: "append-smoke:none", Description: "Smoke append workload without nemesis."},
		{Slug: "append-switchover-switchover", Spec: "append-switchover:switchover", Description: "Append workload while requesting a manual PACMAN switchover."},
		{Slug: "append-failover-kill", Spec: "append-failover:kill", Description: "Append workload while killing current primary PostgreSQL."},
		{Slug: "append-failover-packet", Spec: "append-failover:packet", Description: "Append workload while partitioning the current primary."},
		{Slug: "append-failover-packet-kill", Spec: "append-failover:packet,kill", Description: "Append workload while partitioning and killing the current primary."},
		{Slug: "append-failover-primary-dcs-partition", Spec: "append-failover:primary-dcs-partition", Description: "Append workload while isolating the current primary from DCS only.", NightlyUnsafe: true},
		{Slug: "append-failover-primary-replication-partition", Spec: "append-failover:primary-replication-partition", Description: "Append workload while blocking primary replication traffic only."},
		{Slug: "append-failover-failover-chain", Spec: "append-failover:failover-chain", Description: "Append workload while chaining manual failovers across all three data nodes."},
		{Slug: "append-reinit-reinit-replica", Spec: "append-reinit:reinit-replica", Description: "Append workload while reinitializing a replica through the full PACMAN WAL-G restore workflow."},
		{Slug: "append-reinit-reinit-replica-kill-target", Spec: "append-reinit:reinit-replica-kill-target", Description: "Append workload while killing the reinit target during the full replica restore workflow."},
		{Slug: "append-reinit-reinit-replica-kill-source", Spec: "append-reinit:reinit-replica-kill-source", Description: "Append workload while killing the reinit source primary during restore and requiring failed reinit reporting without unsafe promotion."},
		{Slug: "append-reinit-reinit-replica-dcs-partition-target", Spec: "append-reinit:reinit-replica-dcs-partition-target", Description: "Append workload while isolating the reinit target from DCS and rejecting misleading healthy target status."},
		{Slug: "append-reinit-reinit-replica-dcs-partition-primary", Spec: "append-reinit:reinit-replica-dcs-partition-primary", Description: "Append workload while isolating the source primary from a DCS majority during reinit and validating failover safety."},
		{Slug: "append-reinit-reinit-replica-repeated", Spec: "append-reinit:reinit-replica-repeated", Description: "Append workload while sequentially reinitializing replicas and validating history, cleanup, slots, and streaming health."},
		{Slug: "append-reinit-reinit-replica-with-lag", Spec: "append-reinit:reinit-replica-with-lag", Description: "Append workload while reinitializing a lagging replica and validating system identifier, timeline, slot, and streaming source."},
		{Slug: "append-reinit-reinit-replica-walg-fetch-failure", Spec: "append-reinit:reinit-replica-walg-fetch-failure", Description: "Append workload while injecting WAL-G backup-fetch failure and requiring safe failed reinit state."},
		{Slug: "append-reinit-reinit-replica-concurrent-request", Spec: "append-reinit:reinit-replica-concurrent-request", Description: "Append workload while a concurrent reinit request is rejected during an active replica reinit."},
		{Slug: "append-reinit-reinit-replica-after-failover", Spec: "append-reinit:reinit-replica-after-failover", Description: "Append workload while failing over first, then reinitializing a replica from the new primary."},
		{Slug: "open-transaction-failover-kill", Spec: "open-transaction-failover:kill", Description: "Hold a transaction open while killing the current primary."},
		{Slug: "vip-routing-switchover", Spec: "vip-routing:switchover", Description: "Verify vip-manager routes writes only to the current PACMAN primary during switchover."},
		{Slug: "append-dcs-quorum-dcs-kill-one", Spec: "append-dcs-quorum:dcs-kill-one", Description: "Append workload while killing one etcd DCS member."},
		{Slug: "append-dcs-quorum-dcs-lose-majority", Spec: "append-dcs-quorum:dcs-lose-majority", Description: "Append workload while killing two etcd DCS members."},
		{Slug: "append-dcs-quorum-primary-dcs-majority-partition", Spec: "append-dcs-quorum:primary-dcs-majority-partition", Description: "Append workload while isolating the current primary from a DCS majority."},
		{Slug: "append-dcs-quorum-dcs-full-restart", Spec: "append-dcs-quorum:dcs-full-restart", Description: "Append workload while restarting all etcd DCS members."},
		{Slug: "append-dcs-quorum-dcs-slow-network", Spec: "append-dcs-quorum:dcs-slow-network", Description: "Append workload while adding latency to all etcd DCS members."},
		{Slug: "single-key-register-packet", Spec: "single-key-register:packet", Description: "Register workload while partitioning the current primary."},
		{Slug: "read-committed-txn-slow-network", Spec: "read-committed-txn:slow-network", Description: "Read committed transaction workload under latency and loss."},
		{Slug: "serializable-txn-packet-kill", Spec: "serializable-txn:packet,kill", Description: "Serializable transaction workload under partition plus kill."},
		{Slug: "append-failover-repeated-failure", Spec: "append-failover:repeated-failure", Description: "Append workload under slow network, partition, and kill sequence."},
		{Slug: "append-sync-kill", Spec: "append-sync:kill", Description: "Synchronous append workload while killing the current primary.", PatroniOnly: true},
		{Slug: "append-sync-sync-standby-kill", Spec: "append-sync:sync-standby-kill", Description: "Synchronous append workload while killing an active synchronous standby.", PatroniOnly: true},
		{Slug: "append-sync-two-none", Spec: "append-sync-two:none", Description: "Synchronous append workload with synchronous_node_count=2.", PatroniOnly: true},
		{Slug: "append-strict-sync-no-standby", Spec: "append-strict-sync:no-standby", Description: "Strict synchronous append workload while stopping both standbys.", PatroniOnly: true},
		{Slug: "append-max-lag-lagging-replica-failover", Spec: "append-max-lag:lagging-replica-failover", Description: "Append workload while failing over with one replica above maximum_lag_on_failover.", PatroniOnly: true},
		{Slug: "append-check-timeline-stale-timeline-failover", Spec: "append-check-timeline:stale-timeline-failover", Description: "Append workload while check_timeline blocks promotion of a stale-timeline replica.", PatroniOnly: true},
	}
}

func parseListCasesOutput(output []byte) ([]jepsenCase, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	var cases []jepsenCase
	seen := make(map[string]struct{})

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("parse list-cases line %d: expected slug and workload:nemesis", lineNumber)
		}

		slug := fields[0]
		spec := fields[1]
		if !strings.Contains(spec, ":") {
			return nil, fmt.Errorf("parse list-cases line %d: spec %q must use workload:nemesis format", lineNumber, spec)
		}
		if _, ok := seen[slug]; ok {
			return nil, fmt.Errorf("parse list-cases line %d: duplicate case slug %q", lineNumber, slug)
		}
		seen[slug] = struct{}{}

		cases = append(cases, jepsenCase{
			Slug:        slug,
			Spec:        spec,
			Description: strings.Join(fields[2:], " "),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan list-cases output: %w", err)
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("list-cases output did not contain any cases")
	}

	return cases, nil
}

func discoverMakefiles(root string) ([]string, error) {
	makefiles := []string{filepath.Join(root, "Makefile")}

	matches, err := filepath.Glob(filepath.Join(root, "mk", "*.mk"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	makefiles = append(makefiles, matches...)

	return makefiles, nil
}

func parseMakeTargets(paths []string) (map[string]struct{}, error) {
	targets := make(map[string]struct{})

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read makefile %s: %w", path, err)
		}

		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" || strings.HasPrefix(line, "\t") || strings.HasPrefix(line, " ") {
				continue
			}

			colonIndex := strings.IndexByte(line, ':')
			if colonIndex < 0 {
				continue
			}

			assignmentIndex := strings.IndexByte(line, '=')
			if assignmentIndex >= 0 && assignmentIndex < colonIndex {
				continue
			}

			left := strings.TrimSpace(line[:colonIndex])
			if left == "" || strings.HasPrefix(left, ".") || strings.Contains(left, "$") {
				continue
			}

			for _, target := range strings.Fields(left) {
				targets[target] = struct{}{}
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("scan makefile %s: %w", path, err)
		}
	}

	return targets, nil
}

func validateCaseTargets(cases []jepsenCase, targets map[string]struct{}) caseTargetValidation {
	var missing []string

	for _, testCase := range cases {
		for _, target := range []string{
			"jepsen-case-" + testCase.Slug,
			"jepsen-docker-case-" + testCase.Slug,
		} {
			if _, ok := targets[target]; !ok {
				missing = append(missing, fmt.Sprintf("missing Make target for Jepsen case %s: %s", testCase.Slug, target))
			}
		}
	}

	return caseTargetValidation{
		CaseCount:    len(cases),
		TargetCount:  len(cases) * 2,
		MissingLines: missing,
	}
}
