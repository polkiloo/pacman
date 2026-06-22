package cmd

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
)

type nemesisScheduleOptions struct {
	workload     string
	nemesis      string
	schedulePath string
}

type nemesisScheduleEntry struct {
	Line   int
	Fields map[string]string
}

func newNemesisCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "nemesis",
		Short: "validate Jepsen nemesis artifacts",
	}
	command.AddCommand(newNemesisValidateScheduleCommand())
	return command
}

func newNemesisValidateScheduleCommand() *cobra.Command {
	var options nemesisScheduleOptions

	command := &cobra.Command{
		Use:   "validate-schedule",
		Short: "validate nemesis schedule entries for a workload profile",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return fmt.Errorf("nemesis validate-schedule does not accept arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			entries, err := readNemesisSchedule(options.schedulePath)
			if err != nil {
				return err
			}
			problems := validateNemesisSchedule(options.workload, options.nemesis, entries)
			if len(problems) > 0 {
				return fmt.Errorf("invalid nemesis schedule for %s:%s: %s", options.workload, options.nemesis, strings.Join(problems, "; "))
			}
			return nil
		},
	}

	command.Flags().StringVar(&options.workload, "workload", "", "selected Jepsen workload")
	command.Flags().StringVar(&options.nemesis, "nemesis", "", "selected Jepsen nemesis")
	command.Flags().StringVar(&options.schedulePath, "schedule-file", "", "nemesis schedule EDN path")
	for _, flag := range []string{"workload", "nemesis", "schedule-file"} {
		if err := command.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}
	return command
}

func readNemesisSchedule(path string) ([]nemesisScheduleEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read nemesis schedule %s: %w", path, err)
	}
	defer file.Close()

	var entries []nemesisScheduleEntry
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields, err := parseNemesisScheduleLine(line)
		if err != nil {
			return nil, fmt.Errorf("parse nemesis schedule %s:%d: %w", path, lineNumber, err)
		}
		entries = append(entries, nemesisScheduleEntry{Line: lineNumber, Fields: fields})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan nemesis schedule %s: %w", path, err)
	}
	return entries, nil
}

func validateNemesisSchedule(workload, nemesis string, entries []nemesisScheduleEntry) []string {
	var problems []string
	if !nemesisAllowedForWorkload(workload, nemesis) {
		problems = append(problems, fmt.Sprintf("profile %s:%s is not registered", workload, nemesis))
	}
	if len(entries) == 0 {
		return append(problems, "schedule has no entries")
	}

	expectedScheduleNemesis := scheduleNemesisName(nemesis)
	actions := make(map[string]int)
	for _, entry := range entries {
		entryNemesis := entry.Fields["nemesis"]
		if entryNemesis != expectedScheduleNemesis {
			problems = append(problems, fmt.Sprintf("line %d nemesis is %q, want %q", entry.Line, entryNemesis, expectedScheduleNemesis))
		}

		action := entry.Fields["action"]
		if action == "" {
			problems = append(problems, fmt.Sprintf("line %d missing action", entry.Line))
			continue
		}
		actions[action]++

		if actionRequiresTarget(action) && !hasNemesisTarget(entry.Fields) {
			problems = append(problems, fmt.Sprintf("line %d action %q missing target", entry.Line, action))
		}
		if actionCompletesCommand(action) && !hasNemesisCommandResult(entry.Fields) {
			problems = append(problems, fmt.Sprintf("line %d action %q missing command result", entry.Line, action))
		}
	}

	if actions["start"] == 0 {
		problems = append(problems, "missing start action")
	}
	if actions["stop"] == 0 {
		problems = append(problems, "missing stop action")
	}
	if actions["heal"] == 0 {
		problems = append(problems, "missing heal action")
	}
	if nemesis == "failover-chain" && actions["step"] == 0 {
		problems = append(problems, "failover-chain schedule missing step action")
	}
	if actions["stop"] > 1 && nemesis != "failover-chain" {
		problems = append(problems, fmt.Sprintf("stop action count is %d, want 1", actions["stop"]))
	}

	sort.Strings(problems)
	return problems
}

func parseNemesisScheduleLine(line string) (map[string]string, error) {
	parser := ednMapParser{input: line}
	return parser.parse()
}

type ednMapParser struct {
	input string
	pos   int
}

func (p *ednMapParser) parse() (map[string]string, error) {
	p.skipSpace()
	if !p.consume('{') {
		return nil, fmt.Errorf("expected map opening")
	}

	fields := make(map[string]string)
	for {
		p.skipSpace()
		if p.consume('}') {
			break
		}

		key, err := p.readKeyword()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		value, err := p.readValue()
		if err != nil {
			return nil, fmt.Errorf("read value for %s: %w", key, err)
		}
		fields[key] = value
	}

	p.skipSpace()
	if p.pos != len(p.input) {
		return nil, fmt.Errorf("unexpected trailing data")
	}
	return fields, nil
}

func (p *ednMapParser) skipSpace() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *ednMapParser) consume(want byte) bool {
	if p.pos >= len(p.input) || p.input[p.pos] != want {
		return false
	}
	p.pos++
	return true
}

func (p *ednMapParser) readKeyword() (string, error) {
	if !p.consume(':') {
		return "", fmt.Errorf("expected keyword")
	}
	start := p.pos
	for p.pos < len(p.input) && isEDNSymbolByte(p.input[p.pos]) {
		p.pos++
	}
	if start == p.pos {
		return "", fmt.Errorf("empty keyword")
	}
	return p.input[start:p.pos], nil
}

func (p *ednMapParser) readValue() (string, error) {
	if p.pos >= len(p.input) {
		return "", fmt.Errorf("missing value")
	}
	if p.input[p.pos] == '"' {
		return p.readString()
	}
	if p.input[p.pos] == ':' {
		return p.readKeyword()
	}

	start := p.pos
	for p.pos < len(p.input) && isEDNSymbolByte(p.input[p.pos]) {
		p.pos++
	}
	if start == p.pos {
		return "", fmt.Errorf("invalid value")
	}
	return p.input[start:p.pos], nil
}

func (p *ednMapParser) readString() (string, error) {
	if !p.consume('"') {
		return "", fmt.Errorf("expected string")
	}
	var builder strings.Builder
	for p.pos < len(p.input) {
		ch := p.input[p.pos]
		p.pos++
		if ch == '"' {
			return builder.String(), nil
		}
		if ch == '\\' {
			if p.pos >= len(p.input) {
				return "", fmt.Errorf("unterminated escape")
			}
			escaped := p.input[p.pos]
			p.pos++
			switch escaped {
			case '"', '\\':
				builder.WriteByte(escaped)
			case 'n':
				builder.WriteByte('\n')
			case 't':
				builder.WriteByte('\t')
			default:
				builder.WriteByte(escaped)
			}
			continue
		}
		builder.WriteByte(ch)
	}
	return "", fmt.Errorf("unterminated string")
}

func isEDNSymbolByte(ch byte) bool {
	return ch == '-' || ch == '_' || ch == ',' || ch == '/' || ch == '.' || unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch))
}

func scheduleNemesisName(nemesis string) string {
	if nemesis == "packet,kill" {
		return "packet-kill"
	}
	return nemesis
}

func actionRequiresTarget(action string) bool {
	switch action {
	case "start", "heal", "stop", "step":
		return true
	default:
		return false
	}
}

func actionCompletesCommand(action string) bool {
	switch action {
	case "heal", "stop", "step":
		return true
	default:
		return false
	}
}

func hasNemesisTarget(fields map[string]string) bool {
	for _, key := range []string{"target", "targets", "source", "dcs", "member", "members"} {
		if value := fields[key]; value != "" && value != "unknown" {
			return true
		}
	}
	return false
}

func hasNemesisCommandResult(fields map[string]string) bool {
	if value := fields["result"]; value != "" {
		return true
	}
	if value := fields["exit-status"]; value != "" {
		_, err := strconv.Atoi(value)
		return err == nil
	}
	return false
}

func nemesisAllowedForWorkload(workload, nemesis string) bool {
	allowed := map[string]map[string]struct{}{
		"append-smoke":              {"none": {}},
		"append-switchover":         {"switchover": {}},
		"append-failover":           {"kill": {}, "packet": {}, "packet,kill": {}, "primary-dcs-partition": {}, "primary-replication-partition": {}, "failover-chain": {}, "repeated-failure": {}},
		"append-reinit":             {"reinit-replica": {}, "reinit-replica-kill-target": {}, "reinit-replica-kill-source": {}, "reinit-replica-dcs-partition-target": {}, "reinit-replica-concurrent-request": {}, "reinit-replica-after-failover": {}},
		"append-sync":               {"kill": {}, "sync-standby-kill": {}},
		"append-sync-two":           {"none": {}},
		"append-strict-sync":        {"no-standby": {}},
		"append-max-lag":            {maximumLagOnFailoverNemesis: {}},
		"append-check-timeline":     {patroniCheckTimelineNemesis: {}},
		"open-transaction-failover": {"kill": {}},
		"vip-routing":               {"switchover": {}},
		"append-dcs-quorum":         {"dcs-kill-one": {}, "dcs-lose-majority": {}, "primary-dcs-majority-partition": {}, "dcs-full-restart": {}, "dcs-slow-network": {}},
		"single-key-register":       {"packet": {}},
		"read-committed-txn":        {"slow-network": {}},
		"serializable-txn":          {"packet,kill": {}},
	}
	nemeses, ok := allowed[workload]
	if !ok {
		return false
	}
	_, ok = nemeses[nemesis]
	return ok
}
