package pacmanctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// parsePatronictlListOptions
// ---------------------------------------------------------------------------

func TestParsePatronictlListOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlListOptions(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatPretty {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatPretty)
	}
	if opts.extended || opts.timestamp || opts.scope != "" {
		t.Fatalf("unexpected default options: %+v", opts)
	}
}

func TestParsePatronictlListOptionsExtendedAndTimestamp(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlListOptions([]string{"-e", "--timestamp"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !opts.extended {
		t.Fatal("expected extended=true")
	}
	if !opts.timestamp {
		t.Fatal("expected timestamp=true")
	}
}

func TestParsePatronictlListOptionsFormatFlag(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"-f", "json"},
		{"--format", "json"},
		{"-f=json"},
		{"--format=json"},
	} {
		opts, err := parsePatronictlListOptions(args)
		if err != nil {
			t.Fatalf("%v: unexpected error: %v", args, err)
		}
		if opts.format != outputFormatJSON {
			t.Fatalf("%v: format: got %q, want %q", args, opts.format, outputFormatJSON)
		}
	}
}

func TestParsePatronictlListOptionsTSVFormat(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlListOptions([]string{"-f", "tsv"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatTSV {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatTSV)
	}
}

func TestParsePatronictlListOptionsScopeArg(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlListOptions([]string{"mycluster"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.scope != "mycluster" {
		t.Fatalf("scope: got %q, want %q", opts.scope, "mycluster")
	}
}

func TestParsePatronictlListOptionsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlListOptions([]string{"--unknown"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePatronictlListOptionsExtraArgs(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlListOptions([]string{"cluster1", "cluster2"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

func TestParsePatronictlListOptionsInvalidFormat(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlListOptions([]string{"-f", "xml"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !errors.Is(err, errUnsupportedOutputFormat) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePatronictlListOptionsMissingFlagValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlListOptions([]string{"-f"})
	if err == nil {
		t.Fatal("expected error for missing flag value")
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlHistoryOptions
// ---------------------------------------------------------------------------

func TestParsePatronictlHistoryOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlHistoryOptions(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatPretty {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatPretty)
	}
}

func TestParsePatronictlHistoryOptionsTSV(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlHistoryOptions([]string{"-f", "tsv"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatTSV {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatTSV)
	}
}

func TestParsePatronictlHistoryOptionsScope(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlHistoryOptions([]string{"alpha"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.scope != "alpha" {
		t.Fatalf("scope: got %q, want %q", opts.scope, "alpha")
	}
}

func TestParsePatronictlHistoryOptionsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlHistoryOptions([]string{"--no-such"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParsePatronictlHistoryOptionsExtraArgs(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlHistoryOptions([]string{"a", "b"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

func TestParsePatronictlHistoryOptionsFormatInline(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlHistoryOptions([]string{"--format=yaml"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatYAML {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatYAML)
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlShowConfigOptions
// ---------------------------------------------------------------------------

func TestParsePatronictlShowConfigOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlShowConfigOptions(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatYAML {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatYAML)
	}
}

func TestParsePatronictlShowConfigOptionsJSONFormat(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlShowConfigOptions([]string{"-f", "json"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatJSON {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatJSON)
	}
}

func TestParsePatronictlShowConfigOptionsScope(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlShowConfigOptions([]string{"myscope"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.scope != "myscope" {
		t.Fatalf("scope: got %q, want %q", opts.scope, "myscope")
	}
}

func TestParsePatronictlShowConfigOptionsInvalidFormat(t *testing.T) {
	t.Parallel()

	// tsv is not valid for show-config
	_, err := parsePatronictlShowConfigOptions([]string{"-f", "tsv"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestParsePatronictlShowConfigOptionsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlShowConfigOptions([]string{"--nope"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParsePatronictlShowConfigOptionsExtraArgs(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlShowConfigOptions([]string{"a", "b"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlMaintenanceOptions
// ---------------------------------------------------------------------------

func TestParsePatronictlMaintenanceOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatPretty {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatPretty)
	}
	if opts.wait {
		t.Fatal("expected wait=false")
	}
}

func TestParsePatronictlMaintenanceOptionsWaitFlag(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--wait"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !opts.wait {
		t.Fatal("expected wait=true")
	}
}

func TestParsePatronictlMaintenanceOptionsWaitInline(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--wait=true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !opts.wait {
		t.Fatal("expected wait=true")
	}
}

func TestParsePatronictlMaintenanceOptionsWaitFalse(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--wait=false"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.wait {
		t.Fatal("expected wait=false")
	}
}

func TestParsePatronictlMaintenanceOptionsReason(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--reason", "rolling upgrade"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.reason != "rolling upgrade" {
		t.Fatalf("reason: got %q, want %q", opts.reason, "rolling upgrade")
	}
}

func TestParsePatronictlMaintenanceOptionsReasonInline(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--reason=maintenance"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.reason != "maintenance" {
		t.Fatalf("reason: got %q, want %q", opts.reason, "maintenance")
	}
}

func TestParsePatronictlMaintenanceOptionsRequestedBy(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--requested-by", "ops-bot"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.requestedBy != "ops-bot" {
		t.Fatalf("requestedBy: got %q, want %q", opts.requestedBy, "ops-bot")
	}
}

func TestParsePatronictlMaintenanceOptionsRequestedByInline(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"--requested-by=admin"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.requestedBy != "admin" {
		t.Fatalf("requestedBy: got %q, want %q", opts.requestedBy, "admin")
	}
}

func TestParsePatronictlMaintenanceOptionsFormatInline(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlMaintenanceOptions([]string{"-f=json"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.format != outputFormatJSON {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatJSON)
	}
}

func TestParsePatronictlMaintenanceOptionsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlMaintenanceOptions([]string{"--unknown"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParsePatronictlMaintenanceOptionsExtraArgs(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlMaintenanceOptions([]string{"a", "b"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

func TestParsePatronictlMaintenanceOptionsMissingReasonValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlMaintenanceOptions([]string{"--reason"})
	if err == nil {
		t.Fatal("expected error for missing reason value")
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlSwitchoverOptions
// ---------------------------------------------------------------------------

func TestParsePatronictlSwitchoverOptionsBasic(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlSwitchoverOptions([]string{"--candidate", "alpha-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.candidate != "alpha-2" {
		t.Fatalf("candidate: got %q, want %q", opts.candidate, "alpha-2")
	}
}

func TestParsePatronictlSwitchoverOptionsAllFlags(t *testing.T) {
	t.Parallel()

	schedTime := "2026-05-01T10:00:00Z"
	opts, err := parsePatronictlSwitchoverOptions([]string{
		"--leader", "alpha-1",
		"--candidate", "alpha-2",
		"--scheduled", schedTime,
		"--reason", "planned",
		"--requested-by", "ops",
		"--force",
		"-f", "json",
		"mycluster",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.leader != "alpha-1" {
		t.Fatalf("leader: got %q, want %q", opts.leader, "alpha-1")
	}
	if opts.candidate != "alpha-2" {
		t.Fatalf("candidate: got %q, want %q", opts.candidate, "alpha-2")
	}
	if opts.reason != "planned" {
		t.Fatalf("reason: got %q, want %q", opts.reason, "planned")
	}
	if opts.requestedBy != "ops" {
		t.Fatalf("requestedBy: got %q, want %q", opts.requestedBy, "ops")
	}
	if !opts.force {
		t.Fatal("expected force=true")
	}
	if opts.format != outputFormatJSON {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatJSON)
	}
	if opts.scope != "mycluster" {
		t.Fatalf("scope: got %q, want %q", opts.scope, "mycluster")
	}
	if opts.scheduledAt == nil {
		t.Fatal("expected scheduledAt to be set")
	}
}

func TestParsePatronictlSwitchoverOptionsInlineFlags(t *testing.T) {
	t.Parallel()

	schedTime := "2026-05-01T10:00:00Z"
	opts, err := parsePatronictlSwitchoverOptions([]string{
		"--leader=alpha-1",
		"--candidate=alpha-2",
		"--scheduled=" + schedTime,
		"--reason=deploy",
		"--requested-by=ci",
		"--format=yaml",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.candidate != "alpha-2" {
		t.Fatalf("candidate: got %q, want %q", opts.candidate, "alpha-2")
	}
	if opts.scheduledAt == nil {
		t.Fatal("expected scheduledAt to be set")
	}
}

func TestParsePatronictlSwitchoverOptionsPrimaryAlias(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlSwitchoverOptions([]string{"--primary=alpha-1", "--candidate", "alpha-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.leader != "alpha-1" {
		t.Fatalf("leader: got %q, want %q", opts.leader, "alpha-1")
	}
}

func TestParsePatronictlSwitchoverOptionsCandidateRequired(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions(nil)
	if err == nil {
		t.Fatal("expected error for missing candidate")
	}
	if !strings.Contains(err.Error(), "--candidate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePatronictlSwitchoverOptionsInvalidScheduled(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--candidate", "alpha-2", "--scheduled", "not-a-time"})
	if err == nil {
		t.Fatal("expected error for invalid time")
	}
}

func TestParsePatronictlSwitchoverOptionsInvalidScheduledInline(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--candidate", "alpha-2", "--scheduled=not-a-time"})
	if err == nil {
		t.Fatal("expected error for invalid inline time")
	}
}

func TestParsePatronictlSwitchoverOptionsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--unknown"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParsePatronictlSwitchoverOptionsExtraArgs(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--candidate", "alpha-2", "cluster1", "extra"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

func TestParsePatronictlSwitchoverOptionsMissingLeaderValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--leader"})
	if err == nil {
		t.Fatal("expected error for missing leader value")
	}
}

func TestParsePatronictlSwitchoverOptionsMissingScheduledValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--scheduled"})
	if err == nil {
		t.Fatal("expected error for missing scheduled value")
	}
}

func TestParsePatronictlSwitchoverOptionsMissingReasonValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--reason"})
	if err == nil {
		t.Fatal("expected error for missing reason value")
	}
}

func TestParsePatronictlSwitchoverOptionsMissingRequestedByValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--requested-by"})
	if err == nil {
		t.Fatal("expected error for missing requested-by value")
	}
}

func TestParsePatronictlSwitchoverOptionsInvalidFormat(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--candidate", "alpha-2", "-f", "tsv"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestParsePatronictlSwitchoverOptionsMissingCandidateValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlSwitchoverOptions([]string{"--candidate"})
	if err == nil {
		t.Fatal("expected error for missing candidate value")
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlFailoverOptions
// ---------------------------------------------------------------------------

func TestParsePatronictlFailoverOptionsBasic(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlFailoverOptions([]string{"--candidate", "alpha-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.candidate != "alpha-2" {
		t.Fatalf("candidate: got %q, want %q", opts.candidate, "alpha-2")
	}
}

func TestParsePatronictlFailoverOptionsAllFlags(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlFailoverOptions([]string{
		"--leader", "alpha-1",
		"--candidate", "alpha-2",
		"--reason", "lost",
		"--requested-by", "ops",
		"--force",
		"--format", "json",
		"mycluster",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.leader != "alpha-1" {
		t.Fatalf("leader: got %q, want %q", opts.leader, "alpha-1")
	}
	if opts.candidate != "alpha-2" {
		t.Fatalf("candidate: got %q, want %q", opts.candidate, "alpha-2")
	}
	if !opts.force {
		t.Fatal("expected force=true")
	}
	if opts.format != outputFormatJSON {
		t.Fatalf("format: got %q, want %q", opts.format, outputFormatJSON)
	}
}

func TestParsePatronictlFailoverOptionsInlineFlags(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlFailoverOptions([]string{
		"--leader=alpha-1",
		"--candidate=alpha-2",
		"--reason=lost",
		"--requested-by=admin",
		"--format=yaml",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.candidate != "alpha-2" {
		t.Fatalf("candidate: got %q, want %q", opts.candidate, "alpha-2")
	}
}

func TestParsePatronictlFailoverOptionsPrimaryAlias(t *testing.T) {
	t.Parallel()

	opts, err := parsePatronictlFailoverOptions([]string{"--primary=alpha-1", "--candidate", "alpha-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.leader != "alpha-1" {
		t.Fatalf("leader: got %q, want %q", opts.leader, "alpha-1")
	}
}

func TestParsePatronictlFailoverOptionsCandidateRequired(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions(nil)
	if err == nil {
		t.Fatal("expected error for missing candidate")
	}
}

func TestParsePatronictlFailoverOptionsUnknownFlag(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--unknown"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParsePatronictlFailoverOptionsExtraArgs(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--candidate", "alpha-2", "cluster1", "extra"})
	if err == nil {
		t.Fatal("expected error for extra args")
	}
}

func TestParsePatronictlFailoverOptionsMissingLeaderValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--leader"})
	if err == nil {
		t.Fatal("expected error for missing leader value")
	}
}

func TestParsePatronictlFailoverOptionsMissingReasonValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--reason"})
	if err == nil {
		t.Fatal("expected error for missing reason value")
	}
}

func TestParsePatronictlFailoverOptionsMissingRequestedByValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--requested-by"})
	if err == nil {
		t.Fatal("expected error for missing requested-by value")
	}
}

func TestParsePatronictlFailoverOptionsMissingCandidateValue(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--candidate"})
	if err == nil {
		t.Fatal("expected error for missing candidate value")
	}
}

func TestParsePatronictlFailoverOptionsInvalidFormat(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlFailoverOptions([]string{"--candidate", "alpha-2", "-f=tsv"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlValueFlag / parsePatronictlInlineValue
// ---------------------------------------------------------------------------

func TestParsePatronictlValueFlag(t *testing.T) {
	t.Parallel()

	args := []string{"--format", "json"}
	value, next, err := parsePatronictlValueFlag(args, 0, "--format")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != "json" {
		t.Fatalf("value: got %q, want %q", value, "json")
	}
	if next != 1 {
		t.Fatalf("next: got %d, want 1", next)
	}
}

func TestParsePatronictlValueFlagMissingValue(t *testing.T) {
	t.Parallel()

	_, _, err := parsePatronictlValueFlag([]string{"--format"}, 0, "--format")
	if err == nil {
		t.Fatal("expected error for missing value")
	}
}

func TestParsePatronictlInlineValue(t *testing.T) {
	t.Parallel()

	value, err := parsePatronictlInlineValue("--format=json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != "json" {
		t.Fatalf("value: got %q, want %q", value, "json")
	}
}

func TestParsePatronictlInlineValueInvalid(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlInlineValue("nodash")
	if err == nil {
		t.Fatal("expected error for invalid syntax")
	}
}

// ---------------------------------------------------------------------------
// parsePatronictlTime
// ---------------------------------------------------------------------------

func TestParsePatronictlTimeValid(t *testing.T) {
	t.Parallel()

	result, err := parsePatronictlTime("2026-05-01T10:00:00Z", "--scheduled")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil time")
	}
	want := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)
	if !result.Equal(want) {
		t.Fatalf("time: got %v, want %v", result, want)
	}
}

func TestParsePatronictlTimeInvalid(t *testing.T) {
	t.Parallel()

	_, err := parsePatronictlTime("not-a-time", "--scheduled")
	if err == nil {
		t.Fatal("expected error for invalid time")
	}
	if !strings.Contains(err.Error(), "--scheduled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// validatePatronictlFormat
// ---------------------------------------------------------------------------

func TestValidatePatronictlFormatAllowed(t *testing.T) {
	t.Parallel()

	for _, format := range []string{outputFormatPretty, outputFormatJSON, outputFormatYAML} {
		result, err := validatePatronictlFormat(format, outputFormatPretty, outputFormatJSON, outputFormatYAML)
		if err != nil {
			t.Fatalf("%q: unexpected error: %v", format, err)
		}
		if result != format {
			t.Fatalf("%q: result: got %q, want %q", format, result, format)
		}
	}
}

func TestValidatePatronictlFormatDisallowed(t *testing.T) {
	t.Parallel()

	_, err := validatePatronictlFormat("tsv", outputFormatPretty, outputFormatJSON)
	if err == nil {
		t.Fatal("expected error for disallowed format")
	}
	if !errors.Is(err, errUnsupportedOutputFormat) {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ensurePatronictlScope
// ---------------------------------------------------------------------------

func TestEnsurePatronictlScopeEmpty(t *testing.T) {
	t.Parallel()

	if err := ensurePatronictlScope("", "alpha"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsurePatronictlScopeMatch(t *testing.T) {
	t.Parallel()

	if err := ensurePatronictlScope("alpha", "alpha"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsurePatronictlScopeMismatch(t *testing.T) {
	t.Parallel()

	err := ensurePatronictlScope("alpha", "beta")
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsurePatronictlScopeRequestedButActualEmpty(t *testing.T) {
	t.Parallel()

	err := ensurePatronictlScope("alpha", "")
	if err == nil {
		t.Fatal("expected error when requested but API has no cluster name")
	}
}

// ---------------------------------------------------------------------------
// ensurePatronictlLeader
// ---------------------------------------------------------------------------

func TestEnsurePatronictlLeaderEmpty(t *testing.T) {
	t.Parallel()

	if err := ensurePatronictlLeader("", "alpha-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsurePatronictlLeaderMatch(t *testing.T) {
	t.Parallel()

	if err := ensurePatronictlLeader("alpha-1", "alpha-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsurePatronictlLeaderMismatch(t *testing.T) {
	t.Parallel()

	err := ensurePatronictlLeader("alpha-1", "alpha-2")
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// patronictlLagMBValue / formatPatronictlLagMB
// ---------------------------------------------------------------------------

func TestPatronictlLagMBValueZeroAndNegative(t *testing.T) {
	t.Parallel()

	if got := patronictlLagMBValue(0); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
	if got := patronictlLagMBValue(-1); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}

func TestPatronictlLagMBValuePositive(t *testing.T) {
	t.Parallel()

	// 1 MB = 1048576 bytes
	got := patronictlLagMBValue(1048576)
	if got != 1.0 {
		t.Fatalf("got %v, want 1.0", got)
	}
}

func TestFormatPatronictlLagMBZero(t *testing.T) {
	t.Parallel()

	if got := formatPatronictlLagMB(0); got != "0" {
		t.Fatalf("got %q, want %q", got, "0")
	}
}

func TestFormatPatronictlLagMBInteger(t *testing.T) {
	t.Parallel()

	if got := formatPatronictlLagMB(1048576); got != "1" {
		t.Fatalf("got %q, want %q", got, "1")
	}
}

func TestFormatPatronictlLagMBFractional(t *testing.T) {
	t.Parallel()

	// 1.5 MB = 1572864 bytes
	if got := formatPatronictlLagMB(1572864); got != "1.5" {
		t.Fatalf("got %q, want %q", got, "1.5")
	}
}

// ---------------------------------------------------------------------------
// formatPatronictlHost
// ---------------------------------------------------------------------------

func TestFormatPatronictlHostWithPortAndHost(t *testing.T) {
	t.Parallel()

	member := memberStatusJSON{Host: "db.internal", Port: 5432}
	if got := formatPatronictlHost(member); got != "db.internal:5432" {
		t.Fatalf("got %q, want %q", got, "db.internal:5432")
	}
}

func TestFormatPatronictlHostWithHostOnly(t *testing.T) {
	t.Parallel()

	member := memberStatusJSON{Host: "db.internal"}
	if got := formatPatronictlHost(member); got != "db.internal" {
		t.Fatalf("got %q, want %q", got, "db.internal")
	}
}

func TestFormatPatronictlHostWithAPIURL(t *testing.T) {
	t.Parallel()

	member := memberStatusJSON{APIURL: "http://db.internal:8080"}
	if got := formatPatronictlHost(member); got != "http://db.internal:8080" {
		t.Fatalf("got %q, want %q", got, "http://db.internal:8080")
	}
}

func TestFormatPatronictlHostEmpty(t *testing.T) {
	t.Parallel()

	member := memberStatusJSON{}
	if got := formatPatronictlHost(member); got != "-" {
		t.Fatalf("got %q, want %q", got, "-")
	}
}

// ---------------------------------------------------------------------------
// errorsForPatroniCandidate
// ---------------------------------------------------------------------------

func TestErrorsForPatroniCandidate(t *testing.T) {
	t.Parallel()

	err := errorsForPatroniCandidate("--candidate")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "--candidate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// buildPatronictlListDocument
// ---------------------------------------------------------------------------

func TestBuildPatronictlListDocument(t *testing.T) {
	t.Parallel()

	lastSeen := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)
	status := clusterStatusResponse{
		ClusterName: "alpha",
		Members: []memberStatusJSON{
			{
				Name:        "alpha-1",
				Host:        "db1",
				Port:        5432,
				Role:        "primary",
				State:       "running",
				Timeline:    1,
				LagBytes:    0,
				LastSeenAt:  lastSeen,
				NeedsRejoin: false,
				APIURL:      "http://db1:8080",
				Tags:        map[string]any{"zone": "a"},
			},
			{
				Name:     "alpha-2",
				Role:     "replica",
				State:    "streaming",
				LagBytes: 1048576,
			},
		},
	}

	doc := buildPatronictlListDocument(status)

	if doc.Cluster != "alpha" {
		t.Fatalf("cluster: got %q, want %q", doc.Cluster, "alpha")
	}
	if len(doc.Members) != 2 {
		t.Fatalf("members: got %d, want 2", len(doc.Members))
	}
	if doc.Members[0].Member != "alpha-1" {
		t.Fatalf("member[0].Member: got %q", doc.Members[0].Member)
	}
	if doc.Members[0].LastSeenAt == nil {
		t.Fatal("expected LastSeenAt to be set for alpha-1")
	}
	if doc.Members[1].LastSeenAt != nil {
		t.Fatal("expected LastSeenAt to be nil for alpha-2 (zero time)")
	}
	// 1048576 bytes = 1MB
	if doc.Members[1].LagMB != 1.0 {
		t.Fatalf("LagMB: got %v, want 1.0", doc.Members[1].LagMB)
	}
}

// ---------------------------------------------------------------------------
// buildPatronictlDynamicConfig
// ---------------------------------------------------------------------------

func TestBuildPatronictlDynamicConfigDisabledSync(t *testing.T) {
	t.Parallel()

	spec := clusterSpecResponse{
		ClusterName: "alpha",
		Postgres:    postgresPolicyJSON{SynchronousMode: "disabled"},
	}
	config := buildPatronictlDynamicConfig(spec)

	if config["synchronous_mode"] != false {
		t.Fatalf("synchronous_mode: got %v, want false", config["synchronous_mode"])
	}
	if config["synchronous_mode_strict"] != false {
		t.Fatalf("synchronous_mode_strict: got %v, want false", config["synchronous_mode_strict"])
	}
}

func TestBuildPatronictlDynamicConfigQuorumSync(t *testing.T) {
	t.Parallel()

	spec := clusterSpecResponse{
		Postgres: postgresPolicyJSON{SynchronousMode: "quorum"},
	}
	config := buildPatronictlDynamicConfig(spec)

	if config["synchronous_mode"] != true {
		t.Fatalf("synchronous_mode: got %v, want true", config["synchronous_mode"])
	}
	if config["synchronous_mode_strict"] != false {
		t.Fatalf("synchronous_mode_strict: got %v, want false", config["synchronous_mode_strict"])
	}
}

func TestBuildPatronictlDynamicConfigStrictSync(t *testing.T) {
	t.Parallel()

	spec := clusterSpecResponse{
		Postgres: postgresPolicyJSON{SynchronousMode: "strict"},
	}
	config := buildPatronictlDynamicConfig(spec)

	if config["synchronous_mode"] != true {
		t.Fatalf("synchronous_mode: got %v, want true", config["synchronous_mode"])
	}
	if config["synchronous_mode_strict"] != true {
		t.Fatalf("synchronous_mode_strict: got %v, want true", config["synchronous_mode_strict"])
	}
}

func TestBuildPatronictlDynamicConfigEmptySyncMode(t *testing.T) {
	t.Parallel()

	spec := clusterSpecResponse{
		Postgres: postgresPolicyJSON{SynchronousMode: ""},
	}
	config := buildPatronictlDynamicConfig(spec)

	if config["synchronous_mode"] != false {
		t.Fatalf("synchronous_mode: got %v, want false", config["synchronous_mode"])
	}
}

func TestBuildPatronictlDynamicConfigUnknownSyncMode(t *testing.T) {
	t.Parallel()

	spec := clusterSpecResponse{
		Postgres: postgresPolicyJSON{SynchronousMode: "custom"},
	}
	config := buildPatronictlDynamicConfig(spec)

	if config["synchronous_mode"] != "custom" {
		t.Fatalf("synchronous_mode: got %v, want %q", config["synchronous_mode"], "custom")
	}
}

func TestBuildPatronictlDynamicConfigPauseAndMaxLag(t *testing.T) {
	t.Parallel()

	spec := clusterSpecResponse{
		Maintenance: maintenanceDesiredJSON{Enabled: true, DefaultReason: "ops"},
		Failover:    failoverPolicyJSON{MaximumLagBytes: 2097152},
		Postgres:    postgresPolicyJSON{SynchronousMode: "disabled"},
	}
	config := buildPatronictlDynamicConfig(spec)

	if config["pause"] != true {
		t.Fatalf("pause: got %v, want true", config["pause"])
	}
	if config["maximum_lag_on_failover"] != int64(2097152) {
		t.Fatalf("maximum_lag_on_failover: got %v, want 2097152", config["maximum_lag_on_failover"])
	}
}

// ---------------------------------------------------------------------------
// renderPatronictlListPretty
// ---------------------------------------------------------------------------

func TestRenderPatronictlListPrettyBasic(t *testing.T) {
	t.Parallel()

	status := clusterStatusResponse{
		ClusterName: "alpha",
		Members: []memberStatusJSON{
			{Name: "alpha-1", Role: "primary", State: "running", Timeline: 2},
			{Name: "alpha-2", Role: "replica", State: "streaming", LagBytes: 512},
		},
	}

	var buf bytes.Buffer
	if err := renderPatronictlListPretty(&buf, status, patronictlListOptions{format: outputFormatPretty}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	assertContains(t, output, "Cluster")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "alpha-2")
	assertContains(t, output, "primary")
}

func TestRenderPatronictlListPrettyWithTimestampAndExtended(t *testing.T) {
	t.Parallel()

	lastSeen := time.Date(2026, time.April, 1, 10, 0, 0, 0, time.UTC)
	status := clusterStatusResponse{
		ClusterName: "alpha",
		Members: []memberStatusJSON{
			{
				Name:        "alpha-1",
				Role:        "primary",
				State:       "running",
				LastSeenAt:  lastSeen,
				APIURL:      "http://alpha-1:8080",
				NeedsRejoin: true,
				Tags:        map[string]any{"zone": "a"},
			},
		},
	}

	var buf bytes.Buffer
	if err := renderPatronictlListPretty(&buf, status, patronictlListOptions{
		format:    outputFormatPretty,
		timestamp: true,
		extended:  true,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	assertContains(t, output, "Last Seen")
	assertContains(t, output, "API URL")
	assertContains(t, output, "Needs Rejoin")
}

// ---------------------------------------------------------------------------
// renderPatronictlListTSV
// ---------------------------------------------------------------------------

func TestRenderPatronictlListTSV(t *testing.T) {
	t.Parallel()

	lastSeen := time.Date(2026, time.April, 1, 10, 0, 0, 0, time.UTC)
	status := clusterStatusResponse{
		ClusterName: "alpha",
		Members: []memberStatusJSON{
			{
				Name:       "alpha-1",
				Role:       "primary",
				State:      "running",
				Timeline:   1,
				LastSeenAt: lastSeen,
			},
		},
	}

	var buf bytes.Buffer
	if err := renderPatronictlListTSV(&buf, status, patronictlListOptions{
		format:    outputFormatTSV,
		timestamp: true,
		extended:  true,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	assertContains(t, output, "Last Seen")
	assertContains(t, output, "alpha-1")
}

// ---------------------------------------------------------------------------
// renderPatronictlHistoryTSV
// ---------------------------------------------------------------------------

func TestRenderPatronictlHistoryTSV(t *testing.T) {
	t.Parallel()

	finishedAt := time.Date(2026, time.April, 2, 8, 0, 0, 0, time.UTC)
	response := historyResponse{
		Items: []historyEntryJSON{
			{
				OperationID: "op-1",
				Kind:        "switchover",
				Result:      "succeeded",
				Timeline:    3,
				WALLSN:      "0/3000000",
				FromMember:  "alpha-1",
				ToMember:    "alpha-2",
				FinishedAt:  finishedAt,
				Reason:      "planned",
			},
		},
	}

	var buf bytes.Buffer
	if err := renderPatronictlHistoryTSV(&buf, response); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := buf.String()
	assertContains(t, output, "Operation ID")
	assertContains(t, output, "op-1")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "planned")
}

// ---------------------------------------------------------------------------
// patronictlListFields
// ---------------------------------------------------------------------------

func TestPatronictlListFieldsBasic(t *testing.T) {
	t.Parallel()

	member := memberStatusJSON{
		Name:     "alpha-1",
		Host:     "db",
		Port:     5432,
		Role:     "primary",
		State:    "running",
		Timeline: 1,
		LagBytes: 0,
	}

	fields := patronictlListFields("alpha", member, patronictlListOptions{})
	if len(fields) != 7 {
		t.Fatalf("expected 7 fields, got %d", len(fields))
	}
}

func TestPatronictlListFieldsWithTimestampAndExtended(t *testing.T) {
	t.Parallel()

	lastSeen := time.Date(2026, time.April, 1, 10, 0, 0, 0, time.UTC)
	member := memberStatusJSON{
		Name:        "alpha-1",
		Role:        "primary",
		State:       "running",
		LastSeenAt:  lastSeen,
		APIURL:      "http://alpha-1:8080",
		NeedsRejoin: false,
		Tags:        map[string]any{"zone": "b"},
	}

	fields := patronictlListFields("alpha", member, patronictlListOptions{timestamp: true, extended: true})
	// 7 base + 1 timestamp + 3 extended
	if len(fields) != 11 {
		t.Fatalf("expected 11 fields, got %d: %v", len(fields), fields)
	}
}

// ---------------------------------------------------------------------------
// End-to-end patronictl command tests via Run()
// ---------------------------------------------------------------------------

func TestRunPatronictlList(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/cluster" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName:    "alpha",
			CurrentPrimary: "alpha-1",
			Members: []memberStatusJSON{
				{Name: "alpha-1", Role: "primary", State: "running", Timeline: 1},
				{Name: "alpha-2", Role: "replica", State: "streaming"},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "list"}); err != nil {
		t.Fatalf("run list: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "Cluster")
	assertContains(t, output, "alpha-1")
	assertContains(t, output, "primary")
}

func TestRunPatronictlListTSV(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "alpha",
			Members: []memberStatusJSON{
				{Name: "alpha-1", Role: "primary", State: "running"},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "list", "-f", "tsv"}); err != nil {
		t.Fatalf("run list tsv: %v", err)
	}

	assertContains(t, stdout.String(), "alpha-1")
}

func TestRunPatronictlListJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "beta",
			Members: []memberStatusJSON{
				{Name: "beta-1", Role: "primary", State: "running"},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "list", "-f", "json"}); err != nil {
		t.Fatalf("run list json: %v", err)
	}

	var doc patronictlListDocument
	if err := json.Unmarshal(stdout.Bytes(), &doc); err != nil {
		t.Fatalf("decode list json: %v", err)
	}
	if doc.Cluster != "beta" {
		t.Fatalf("cluster: got %q, want %q", doc.Cluster, "beta")
	}
}

func TestRunPatronictlListWithScope(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "alpha",
			Members:     []memberStatusJSON{{Name: "alpha-1", Role: "primary"}},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "list", "alpha"}); err != nil {
		t.Fatalf("run list with scope: %v", err)
	}
}

func TestRunPatronictlListScopeMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{ClusterName: "alpha"}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"-api-url", server.URL, "list", "wrong-scope"})
	if err == nil {
		t.Fatal("expected scope mismatch error")
	}
}

func TestRunTopologyAlias(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "alpha",
			Members:     []memberStatusJSON{{Name: "alpha-1", Role: "primary", State: "running"}},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "topology"}); err != nil {
		t.Fatalf("run topology: %v", err)
	}
	assertContains(t, stdout.String(), "alpha-1")
}

func TestRunPatronictlHistoryCommand(t *testing.T) {
	t.Parallel()

	finishedAt := time.Date(2026, time.April, 3, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(historyResponse{
			Items: []historyEntryJSON{
				{
					OperationID: "op-patronictl-1",
					Kind:        "switchover",
					Result:      "succeeded",
					FinishedAt:  finishedAt,
				},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	// history without "list" subcommand routes to patronictl history
	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "history"}); err != nil {
		t.Fatalf("run patronictl history: %v", err)
	}

	assertContains(t, stdout.String(), "op-patronictl-1")
}

func TestRunPatronictlHistoryTSV(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(historyResponse{
			Items: []historyEntryJSON{
				{OperationID: "op-2", Kind: "failover", Result: "succeeded"},
			},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "history", "-f", "tsv"}); err != nil {
		t.Fatalf("run patronictl history tsv: %v", err)
	}

	assertContains(t, stdout.String(), "op-2")
}

func TestRunPatronictlHistoryWithScope(t *testing.T) {
	t.Parallel()

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		callCount++
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/history":
			if err := json.NewEncoder(writer).Encode(historyResponse{
				Items: []historyEntryJSON{{OperationID: "op-scoped", Kind: "switchover"}},
			}); err != nil {
				t.Fatalf("encode history: %v", err)
			}
		case "/api/v1/cluster":
			if err := json.NewEncoder(writer).Encode(clusterStatusResponse{ClusterName: "alpha"}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "history", "alpha"}); err != nil {
		t.Fatalf("run patronictl history with scope: %v", err)
	}
	assertContains(t, stdout.String(), "op-scoped")
}

func TestRunPatronictlShowConfig(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/cluster/spec" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
			Maintenance: maintenanceDesiredJSON{Enabled: false},
			Failover:    failoverPolicyJSON{MaximumLagBytes: 1048576},
			Postgres:    postgresPolicyJSON{SynchronousMode: "quorum", UsePgRewind: true},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "show-config"}); err != nil {
		t.Fatalf("run show-config: %v", err)
	}

	output := stdout.String()
	assertContains(t, output, "synchronous_mode")
}

func TestRunPatronictlShowConfigJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
			Postgres:    postgresPolicyJSON{SynchronousMode: "disabled"},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "show-config", "-f", "json"}); err != nil {
		t.Fatalf("run show-config json: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &doc); err != nil {
		t.Fatalf("decode show-config json: %v", err)
	}
}

func TestRunPatronictlPause(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, time.April, 4, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v1/maintenance":
			if request.Method == http.MethodPut {
				var body maintenanceModeUpdateRequestJSON
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Fatalf("decode body: %v", err)
				}
				if !body.Enabled {
					t.Fatal("expected enabled=true for pause")
				}
				writer.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{
					Enabled:   true,
					Reason:    body.Reason,
					UpdatedAt: &updatedAt,
				}); err != nil {
					t.Fatalf("encode response: %v", err)
				}
			}
		default:
			t.Fatalf("unexpected path/method: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "pause", "--reason", "upgrade"}); err != nil {
		t.Fatalf("run pause: %v", err)
	}

	assertContains(t, stdout.String(), "Enabled:")
	assertContains(t, stdout.String(), "true")
}

func TestRunPatronictlResume(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/v1/maintenance" && request.Method == http.MethodPut {
			var body maintenanceModeUpdateRequestJSON
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body.Enabled {
				t.Fatal("expected enabled=false for resume")
			}
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: false}); err != nil {
				t.Fatalf("encode response: %v", err)
			}
			return
		}
		t.Fatalf("unexpected request: %s %s", request.Method, request.URL.Path)
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "resume"}); err != nil {
		t.Fatalf("run resume: %v", err)
	}

	assertContains(t, stdout.String(), "Enabled:")
}

func TestRunPatronictlSwitchover(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/api/v1/cluster" && request.Method == http.MethodGet:
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
				ClusterName:    "alpha",
				CurrentPrimary: "alpha-1",
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case request.URL.Path == "/api/v1/operations/switchover" && request.Method == http.MethodPost:
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusAccepted)
			if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
				Message: "switchover accepted",
				Operation: operationJSON{
					ID:       "sw-patronictl-1",
					Kind:     "switchover",
					State:    "pending",
					ToMember: "alpha-2",
				},
			}); err != nil {
				t.Fatalf("encode switchover: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{
		"-api-url", server.URL,
		"switchover",
		"--leader", "alpha-1",
		"--candidate", "alpha-2",
	}); err != nil {
		t.Fatalf("run patronictl switchover: %v", err)
	}

	assertContains(t, stdout.String(), "sw-patronictl-1")
}

func TestRunPatronictlSwitchoverLeaderMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName:    "alpha",
			CurrentPrimary: "alpha-1",
		}); err != nil {
			t.Fatalf("encode cluster: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{
		"-api-url", server.URL,
		"switchover",
		"--leader", "wrong-leader",
		"--candidate", "alpha-2",
	})
	if err == nil {
		t.Fatal("expected leader mismatch error")
	}
}

func TestRunPatronictlFailoverCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/api/v1/cluster" && request.Method == http.MethodGet:
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
				ClusterName:    "alpha",
				CurrentPrimary: "alpha-1",
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case request.URL.Path == "/api/v1/operations/failover" && request.Method == http.MethodPost:
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusAccepted)
			if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
				Operation: operationJSON{
					ID:   "fo-patronictl-1",
					Kind: "failover",
				},
			}); err != nil {
				t.Fatalf("encode failover: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{
		"-api-url", server.URL,
		"failover",
		"--candidate", "alpha-2",
	}); err != nil {
		t.Fatalf("run patronictl failover: %v", err)
	}

	assertContains(t, stdout.String(), "fo-patronictl-1")
}

func TestRunPatronictlFailoverWithLeaderUsesSwitchover(t *testing.T) {
	t.Parallel()

	switchoverCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/api/v1/cluster":
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
				ClusterName:    "alpha",
				CurrentPrimary: "alpha-1",
			}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case request.URL.Path == "/api/v1/operations/switchover":
			switchoverCalled = true
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusAccepted)
			if err := json.NewEncoder(writer).Encode(operationAcceptedResponse{
				Operation: operationJSON{ID: "sw-via-failover", Kind: "switchover"},
			}); err != nil {
				t.Fatalf("encode response: %v", err)
			}
		default:
			t.Fatalf("unexpected: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{
		"-api-url", server.URL,
		"failover",
		"--leader", "alpha-1",
		"--candidate", "alpha-2",
	}); err != nil {
		t.Fatalf("run patronictl failover with leader: %v", err)
	}

	if !switchoverCalled {
		t.Fatal("expected switchover endpoint to be called when leader is specified")
	}
}

func TestRunPatronictlSwitchoverInvalidOptions(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"switchover"})
	if err == nil {
		t.Fatal("expected error for missing candidate")
	}
}

func TestRunPatronictlFailoverInvalidOptions(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"failover"})
	if err == nil {
		t.Fatal("expected error for missing candidate")
	}
}

func TestRunPatronictlPauseInvalidOptions(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"pause", "--unknown-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestRunPatronictlResumeInvalidOptions(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"resume", "--unknown-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestRunPatronictlShowConfigInvalidOptions(t *testing.T) {
	t.Parallel()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"show-config", "--unknown-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestRunPatronictlPauseWithWait(t *testing.T) {
	t.Parallel()

	// Return enabled=true immediately so the wait loop converges on first poll.
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPut && request.URL.Path == "/api/v1/maintenance":
			if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: false}); err != nil {
				t.Fatalf("encode: %v", err)
			}
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/maintenance":
			// waitForPatronictlMaintenanceState polls this; return enabled=true to converge.
			if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: true}); err != nil {
				t.Fatalf("encode: %v", err)
			}
		default:
			t.Fatalf("unexpected: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "pause", "--wait"}); err != nil {
		t.Fatalf("run pause --wait: %v", err)
	}
	assertContains(t, stdout.String(), "Enabled:")
}

func TestRunPatronictlResumeWithWait(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPut && request.URL.Path == "/api/v1/maintenance":
			if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: true}); err != nil {
				t.Fatalf("encode: %v", err)
			}
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/maintenance":
			if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: false}); err != nil {
				t.Fatalf("encode: %v", err)
			}
		default:
			t.Fatalf("unexpected: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "resume", "--wait"}); err != nil {
		t.Fatalf("run resume --wait: %v", err)
	}
	assertContains(t, stdout.String(), "Enabled:")
}

func TestRunPatronictlPauseWithScope(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/cluster":
			if err := json.NewEncoder(writer).Encode(clusterStatusResponse{ClusterName: "alpha"}); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case request.Method == http.MethodPut && request.URL.Path == "/api/v1/maintenance":
			if err := json.NewEncoder(writer).Encode(maintenanceModeStatusJSON{Enabled: true}); err != nil {
				t.Fatalf("encode maintenance: %v", err)
			}
		default:
			t.Fatalf("unexpected: %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "pause", "alpha"}); err != nil {
		t.Fatalf("run pause with scope: %v", err)
	}
}

func TestRunPatronictlPauseWithScopeMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{ClusterName: "alpha"}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"-api-url", server.URL, "pause", "wrong-scope"})
	if err == nil {
		t.Fatal("expected scope mismatch error")
	}
}

func TestRunPatronictlShowConfigPrettyFormat(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
			Postgres:    postgresPolicyJSON{SynchronousMode: "strict"},
		}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	// pretty format should render as YAML for show-config
	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "show-config", "-f", "pretty"}); err != nil {
		t.Fatalf("run show-config pretty: %v", err)
	}
	assertContains(t, stdout.String(), "synchronous_mode")
}

func TestRenderPatronictlHistoryTSVEmpty(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if err := renderPatronictlHistoryTSV(&buf, historyResponse{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertContains(t, buf.String(), "Operation ID")
}

func TestRunPatronictlListYAML(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterStatusResponse{
			ClusterName: "alpha",
			Members:     []memberStatusJSON{{Name: "alpha-1", Role: "primary"}},
		}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "list", "-f", "yaml"}); err != nil {
		t.Fatalf("run list yaml: %v", err)
	}
	assertContains(t, stdout.String(), "alpha-1")
}

func TestRunPatronictlShowConfigWithScope(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{
			ClusterName: "alpha",
			Postgres:    postgresPolicyJSON{SynchronousMode: "disabled"},
		}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	app := New(Params{Stdout: &stdout, Stderr: &bytes.Buffer{}})

	if err := app.Run(context.Background(), []string{"-api-url", server.URL, "show-config", "alpha"}); err != nil {
		t.Fatalf("run show-config with scope: %v", err)
	}
}

func TestRunPatronictlShowConfigScopeMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(clusterSpecResponse{ClusterName: "alpha"}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}))
	defer server.Close()

	app := New(Params{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	err := app.Run(context.Background(), []string{"-api-url", server.URL, "show-config", "wrong"})
	if err == nil {
		t.Fatal("expected scope mismatch error")
	}
}
