package pacmanctl

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

func parseOutputFormat(command string, args []string, stderr io.Writer) (string, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)

	format := defaultOutputFormat
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return "", err
	}

	if len(fs.Args()) > 0 {
		return "", fmt.Errorf("unexpected arguments for %s: %s", command, strings.Join(fs.Args(), " "))
	}

	return validateOutputFormat(format)
}

func normalizeOutputFormat(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return defaultOutputFormat
	}

	return trimmed
}

type switchoverCommandOptions struct {
	format      string
	candidate   string
	scheduledAt *time.Time
	reason      string
	requestedBy string
	force       bool
}

type failoverCommandOptions struct {
	format      string
	candidate   string
	reason      string
	requestedBy string
}

type reinitCommandOptions struct {
	format      string
	member      string
	reason      string
	requestedBy string
}

type maintenanceCommandOptions struct {
	format      string
	reason      string
	requestedBy string
}

type nodeStatusOptions struct {
	format   string
	nodeName string
}

type diagnosticsOptions struct {
	format         string
	includeMembers bool
}

func parseSwitchoverCommandOptions(args []string, stderr io.Writer) (switchoverCommandOptions, error) {
	fs := flag.NewFlagSet("cluster switchover", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var candidate string
	var scheduledAt string
	var reason string
	var requestedBy string
	var force bool
	format := defaultOutputFormat

	fs.StringVar(&candidate, "candidate", "", "switchover target member")
	fs.StringVar(&scheduledAt, "scheduled-at", "", "RFC3339 schedule time")
	fs.StringVar(&reason, "reason", "", "operator reason for the switchover")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	fs.BoolVar(&force, "force", false, "cancel any pending operation and proceed")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return switchoverCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return switchoverCommandOptions{}, fmt.Errorf("unexpected arguments for cluster switchover: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return switchoverCommandOptions{}, err
	}

	trimmedCandidate := strings.TrimSpace(candidate)
	if trimmedCandidate == "" {
		return switchoverCommandOptions{}, errCandidateRequired
	}

	options := switchoverCommandOptions{
		format:      normalizedFormat,
		candidate:   trimmedCandidate,
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
		force:       force,
	}

	if strings.TrimSpace(scheduledAt) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(scheduledAt))
		if err != nil {
			return switchoverCommandOptions{}, fmt.Errorf("invalid -scheduled-at value %q: %w", scheduledAt, err)
		}
		parsed = parsed.UTC()
		options.scheduledAt = &parsed
	}

	return options, nil
}

func parseFailoverCommandOptions(args []string, stderr io.Writer) (failoverCommandOptions, error) {
	fs := flag.NewFlagSet("cluster failover", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var candidate string
	var reason string
	var requestedBy string
	format := defaultOutputFormat

	fs.StringVar(&candidate, "candidate", "", "preferred failover target member")
	fs.StringVar(&reason, "reason", "", "operator reason for the failover")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return failoverCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return failoverCommandOptions{}, fmt.Errorf("unexpected arguments for cluster failover: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return failoverCommandOptions{}, err
	}

	return failoverCommandOptions{
		format:      normalizedFormat,
		candidate:   strings.TrimSpace(candidate),
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
	}, nil
}

func parseReinitCommandOptions(args []string, stderr io.Writer) (reinitCommandOptions, error) {
	fs := flag.NewFlagSet("cluster reinit", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var member string
	var reason string
	var requestedBy string
	format := defaultOutputFormat

	fs.StringVar(&member, "member", "", "replica member to reinitialize")
	fs.StringVar(&reason, "reason", "", "operator reason for the reinit")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return reinitCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return reinitCommandOptions{}, fmt.Errorf("unexpected arguments for cluster reinit: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return reinitCommandOptions{}, err
	}

	trimmedMember := strings.TrimSpace(member)
	if trimmedMember == "" {
		return reinitCommandOptions{}, errReinitMemberRequired
	}

	return reinitCommandOptions{
		format:      normalizedFormat,
		member:      trimmedMember,
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
	}, nil
}

func parseMaintenanceCommandOptions(command string, args []string, stderr io.Writer) (maintenanceCommandOptions, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)

	var reason string
	var requestedBy string
	format := defaultOutputFormat

	fs.StringVar(&reason, "reason", "", "maintenance reason")
	fs.StringVar(&requestedBy, "requested-by", "", "operator identity")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return maintenanceCommandOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return maintenanceCommandOptions{}, fmt.Errorf("unexpected arguments for %s: %s", command, strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return maintenanceCommandOptions{}, err
	}

	return maintenanceCommandOptions{
		format:      normalizedFormat,
		reason:      strings.TrimSpace(reason),
		requestedBy: strings.TrimSpace(requestedBy),
	}, nil
}

func parseNodeStatusOptions(args []string, stderr io.Writer) (nodeStatusOptions, error) {
	fs := flag.NewFlagSet("node status", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var nodeName string
	format := defaultOutputFormat

	fs.StringVar(&nodeName, "node", "", "PACMAN node name")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return nodeStatusOptions{}, err
	}

	trimmedName := strings.TrimSpace(nodeName)
	remaining := fs.Args()
	switch {
	case trimmedName != "" && len(remaining) > 0:
		return nodeStatusOptions{}, fmt.Errorf("unexpected arguments for node status: %s", strings.Join(remaining, " "))
	case trimmedName == "" && len(remaining) == 1:
		trimmedName = strings.TrimSpace(remaining[0])
	case trimmedName == "" && len(remaining) == 0:
		return nodeStatusOptions{}, errNodeNameRequired
	case trimmedName == "" && len(remaining) > 1:
		return nodeStatusOptions{}, fmt.Errorf("unexpected arguments for node status: %s", strings.Join(remaining, " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return nodeStatusOptions{}, err
	}

	return nodeStatusOptions{
		format:   normalizedFormat,
		nodeName: trimmedName,
	}, nil
}

func parseDiagnosticsOptions(args []string, stderr io.Writer) (diagnosticsOptions, error) {
	fs := flag.NewFlagSet("diagnostics show", flag.ContinueOnError)
	fs.SetOutput(stderr)

	includeMembers := true
	format := defaultOutputFormat

	fs.BoolVar(&includeMembers, "include-members", true, "include per-member diagnostics")
	addOutputFormatFlags(fs, &format)

	if err := fs.Parse(args); err != nil {
		return diagnosticsOptions{}, err
	}
	if len(fs.Args()) > 0 {
		return diagnosticsOptions{}, fmt.Errorf("unexpected arguments for diagnostics show: %s", strings.Join(fs.Args(), " "))
	}

	normalizedFormat, err := validateOutputFormat(format)
	if err != nil {
		return diagnosticsOptions{}, err
	}

	return diagnosticsOptions{
		format:         normalizedFormat,
		includeMembers: includeMembers,
	}, nil
}

func addOutputFormatFlags(fs *flag.FlagSet, format *string) {
	fs.StringVar(format, "format", defaultOutputFormat, "output format: text|pretty|json|yaml")
	fs.StringVar(format, "o", defaultOutputFormat, "output format: text|pretty|json|yaml")
}

func validateOutputFormat(format string) (string, error) {
	normalized := normalizeOutputFormat(format)
	switch normalized {
	case outputFormatText, outputFormatPretty, outputFormatJSON, outputFormatYAML:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: %s", errUnsupportedOutputFormat, format)
	}
}
