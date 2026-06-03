package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	pacmanAPIToken = "lab-admin-token"
)

type harnessLab struct {
	options harnessOptions
	cfg     harnessConfig
}

type harnessConfig struct {
	composeFile                  string
	pgClientService              string
	pgHost                       string
	pgPort                       string
	pgUser                       string
	pgPassword                   string
	pgDatabase                   string
	psqlBinary                   string
	vipInterface                 string
	defaultOps                   int
	defaultDuration              time.Duration
	defaultClients               int
	defaultKeys                  int
	clusterVerifyTimeout         time.Duration
	clusterVerifyInterval        time.Duration
	nemesisHold                  time.Duration
	postNemesisSettle            time.Duration
	timelineConvergenceTimeout   time.Duration
	timelineConvergenceInterval  time.Duration
	workloadVisibilityTimeout    time.Duration
	workloadVisibilityInterval   time.Duration
	primarySampleInterval        time.Duration
	appendFailoverOpDelay        time.Duration
	appendSwitchoverOpDelay      time.Duration
	dcsKillService               string
	dcsMajorityKillServices      []string
	dcsMajorityPartitionServices []string
	dcsRestartServices           []string
	dcsSlowServices              []string
	dcsSlowMinLatencyMS          int
	dcsRecoveryTimeout           time.Duration
	dcsRecoveryInterval          time.Duration
	allowAsyncLoss               bool
	synchronousStandbyTimeout    time.Duration
	synchronousStandbyInterval   time.Duration
	strictSyncProbeTimeout       time.Duration
}

type harnessCommand struct {
	name string
	args []string
}

type nemesisRun struct {
	done chan struct{}
	err  error
}

type primarySampler struct {
	stopCh chan struct{}
	done   chan struct{}
}

func newHarnessLab(options harnessOptions) *harnessLab {
	if options.target.Name == "" {
		target, err := resolveJepsenTarget(defaultJepsenTarget)
		if err == nil {
			options.target = target
		}
	}
	cfg := harnessConfig{
		composeFile:                  filepath.Join(options.repoRoot, options.target.ComposeFile),
		pgClientService:              envOrDefault("PACMAN_JEPSEN_PG_CLIENT_SERVICE", options.target.PGClient),
		pgHost:                       envOrDefault("PACMAN_JEPSEN_PG_HOST", options.target.PGHost),
		pgPort:                       envOrDefault("PACMAN_JEPSEN_PG_PORT", "5432"),
		pgUser:                       envOrDefault("PACMAN_JEPSEN_PG_USER", "postgres"),
		pgPassword:                   envOrDefault("PACMAN_JEPSEN_PG_PASSWORD", options.target.PGPassword),
		pgDatabase:                   envOrDefault("PACMAN_JEPSEN_PG_DATABASE", "postgres"),
		psqlBinary:                   options.target.PSQLBinary,
		vipInterface:                 envOrDefault("PACMAN_JEPSEN_VIP_INTERFACE", "eth0"),
		defaultOps:                   envInt("PACMAN_JEPSEN_WORKLOAD_OPS", 12),
		defaultDuration:              time.Duration(envInt("PACMAN_JEPSEN_WORKLOAD_DURATION_SECONDS", 20)) * time.Second,
		defaultClients:               envInt("PACMAN_JEPSEN_WORKLOAD_CLIENTS", 3),
		defaultKeys:                  envInt("PACMAN_JEPSEN_WORKLOAD_KEYS", 3),
		clusterVerifyTimeout:         time.Duration(envInt("PACMAN_JEPSEN_CLUSTER_VERIFY_TIMEOUT_SECONDS", 120)) * time.Second,
		clusterVerifyInterval:        time.Duration(envInt("PACMAN_JEPSEN_CLUSTER_VERIFY_INTERVAL_SECONDS", 2)) * time.Second,
		nemesisHold:                  time.Duration(envInt("PACMAN_JEPSEN_NEMESIS_HOLD_SECONDS", 8)) * time.Second,
		postNemesisSettle:            time.Duration(envInt("PACMAN_JEPSEN_POST_NEMESIS_SETTLE_SECONDS", 10)) * time.Second,
		timelineConvergenceTimeout:   time.Duration(envInt("PACMAN_JEPSEN_TIMELINE_CONVERGENCE_TIMEOUT_SECONDS", 90)) * time.Second,
		timelineConvergenceInterval:  time.Duration(envInt("PACMAN_JEPSEN_TIMELINE_CONVERGENCE_INTERVAL_SECONDS", 2)) * time.Second,
		workloadVisibilityTimeout:    time.Duration(envInt("PACMAN_JEPSEN_WORKLOAD_VISIBILITY_TIMEOUT_SECONDS", 60)) * time.Second,
		workloadVisibilityInterval:   time.Duration(envInt("PACMAN_JEPSEN_WORKLOAD_VISIBILITY_INTERVAL_SECONDS", 2)) * time.Second,
		primarySampleInterval:        time.Duration(envInt("PACMAN_JEPSEN_PRIMARY_SAMPLE_INTERVAL_SECONDS", 1)) * time.Second,
		appendFailoverOpDelay:        time.Duration(envInt("PACMAN_JEPSEN_APPEND_FAILOVER_OP_DELAY_SECONDS", 1)) * time.Second,
		appendSwitchoverOpDelay:      time.Duration(envInt("PACMAN_JEPSEN_APPEND_SWITCHOVER_OP_DELAY_SECONDS", 1)) * time.Second,
		dcsKillService:               envOrDefault("PACMAN_JEPSEN_DCS_KILL_SERVICE", "pacman-dcs-2"),
		dcsMajorityKillServices:      strings.Fields(envOrDefault("PACMAN_JEPSEN_DCS_MAJORITY_KILL_SERVICES", "pacman-dcs-2 pacman-dcs-3")),
		dcsMajorityPartitionServices: strings.Fields(envOrDefault("PACMAN_JEPSEN_DCS_MAJORITY_PARTITION_SERVICES", "pacman-dcs-2 pacman-dcs-3")),
		dcsRestartServices:           strings.Fields(envOrDefault("PACMAN_JEPSEN_DCS_RESTART_SERVICES", "pacman-dcs pacman-dcs-2 pacman-dcs-3")),
		dcsSlowServices:              strings.Fields(envOrDefault("PACMAN_JEPSEN_DCS_SLOW_SERVICES", "pacman-dcs pacman-dcs-2 pacman-dcs-3")),
		dcsSlowMinLatencyMS:          envInt("PACMAN_JEPSEN_DCS_SLOW_MIN_LATENCY_MS", 100),
		dcsRecoveryTimeout:           time.Duration(envInt("PACMAN_JEPSEN_DCS_RECOVERY_TIMEOUT_SECONDS", 10)) * time.Second,
		dcsRecoveryInterval:          time.Duration(envInt("PACMAN_JEPSEN_DCS_RECOVERY_INTERVAL_SECONDS", 1)) * time.Second,
		allowAsyncLoss:               envOrDefault("PACMAN_JEPSEN_ALLOW_ASYNC_LOSS", "false") == "true",
		synchronousStandbyTimeout:    time.Duration(envInt("PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_TIMEOUT_SECONDS", 60)) * time.Second,
		synchronousStandbyInterval:   time.Duration(envInt("PACMAN_JEPSEN_SYNCHRONOUS_STANDBY_INTERVAL_SECONDS", 1)) * time.Second,
		strictSyncProbeTimeout:       time.Duration(envInt("PACMAN_JEPSEN_STRICT_SYNC_PROBE_TIMEOUT_SECONDS", 3)) * time.Second,
	}
	return &harnessLab{options: options, cfg: cfg}
}

func (lab *harnessLab) dispatch(ctx context.Context, body string) (int, error) {
	command, err := parseHarnessCommand(body)
	if err != nil {
		return 1, err
	}

	switch command.name {
	case "bootstrap_lab":
		return statusError(lab.bootstrapLab(ctx))
	case "bootstrap_lab_with_retries":
		label := argOr(command.args, 0, "docker-lab")
		return statusError(lab.bootstrapLabWithRetries(ctx, label))
	case "verify_three_data_node_cluster":
		return statusError(lab.verifyThreeDataNodeCluster(ctx, argOr(command.args, 0, "")))
	case "verify_lab":
		if lab.options.target.supportsPatroniLab() {
			return statusError(lab.verifyThreeDataNodeCluster(ctx, ""))
		}
		return lab.runHost(ctx, filepath.Join(lab.options.repoRoot, "deploy", "lab", "scripts", "demo.sh"), "verify")
	case "run_jepsen_cases":
		if len(command.args) != 4 {
			return 1, fmt.Errorf("run_jepsen_cases expects 4 args, got %d", len(command.args))
		}
		return statusError(lab.runCases(ctx, strings.Fields(command.args[0]), command.args[1], command.args[2], command.args[3]))
	case "ensure_workload_schema":
		return statusError(lab.ensureWorkloadSchema(ctx))
	case "run_jepsen_case":
		if len(command.args) != 6 {
			return 1, fmt.Errorf("run_jepsen_case expects 6 args, got %d", len(command.args))
		}
		return statusError(lab.runCase(ctx, command.args[0], command.args[1], command.args[2], command.args[3], command.args[4], command.args[5]))
	case "collect_artifacts":
		if len(command.args) != 2 {
			return 1, fmt.Errorf("collect_artifacts expects 2 args, got %d", len(command.args))
		}
		return statusError(lab.collectArtifacts(ctx, command.args[0], command.args[1] == "true"))
	case "destroy_lab_after_suite":
		if len(command.args) != 2 {
			return 1, fmt.Errorf("destroy_lab_after_suite expects 2 args, got %d", len(command.args))
		}
		return statusError(lab.destroyLabAfterSuite(ctx, command.args[0], command.args[1]))
	case "write_results_file":
		if len(command.args) != 2 {
			return 1, fmt.Errorf("write_results_file expects 2 args, got %d", len(command.args))
		}
		return statusError(lab.writeResultsFile(command.args[0], command.args[1] == "true"))
	default:
		return 1, fmt.Errorf("unsupported Go harness command %q", command.name)
	}
}

func parseHarnessCommand(body string) (harnessCommand, error) {
	fields, err := shellFields(body)
	if err != nil {
		return harnessCommand{}, err
	}
	if len(fields) == 0 {
		return harnessCommand{}, fmt.Errorf("empty harness command")
	}
	return harnessCommand{name: fields[0], args: fields[1:]}, nil
}

func shellFields(input string) ([]string, error) {
	var fields []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	escaped := false
	flush := func() {
		if current.Len() > 0 {
			fields = append(fields, current.String())
			current.Reset()
		}
	}
	for _, r := range input {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\' && !inSingle:
			escaped = true
		case r == '\'' && !inDouble:
			inSingle = !inSingle
		case r == '"' && !inSingle:
			inDouble = !inDouble
		case (r == ' ' || r == '\t' || r == '\n') && !inSingle && !inDouble:
			flush()
		default:
			current.WriteRune(r)
		}
	}
	if escaped || inSingle || inDouble {
		return nil, fmt.Errorf("unterminated quoted harness command %q", input)
	}
	flush()
	return fields, nil
}

func argOr(args []string, index int, fallback string) string {
	if index < len(args) && args[index] != "" {
		return args[index]
	}
	return fallback
}

func statusError(err error) (int, error) {
	if err != nil {
		return 1, err
	}
	return 0, nil
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
