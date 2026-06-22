package cmd

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateNemesisSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		workload string
		nemesis  string
		schedule string
		wantErr  []string
	}{
		{
			name:     "accepts none start and stop",
			workload: "append-smoke",
			nemesis:  "none",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :none :action :start :target "none"}
{:time "2026-05-01T00:00:02Z" :nemesis :none :action :heal :target "none" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :none :action :stop :target "none" :result :ok}
`,
		},
		{
			name:     "accepts kill start and stop",
			workload: "append-failover",
			nemesis:  "kill",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :kill :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :kill :action :heal :target "alpha-1" :promoted "alpha-2" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :kill :action :stop :target "alpha-1" :promoted "alpha-2" :result :ok}
`,
		},
		{
			name:     "accepts packet kill encoded schedule name",
			workload: "append-failover",
			nemesis:  "packet,kill",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :packet-kill :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :packet-kill :action :heal :target "alpha-1" :promoted "alpha-2" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :packet-kill :action :stop :target "alpha-1" :promoted "alpha-2" :result :ok}
`,
		},
		{
			name:     "accepts switchover exit status as result",
			workload: "append-switchover",
			nemesis:  "switchover",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :switchover :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :switchover :action :heal :source "alpha-1" :target "alpha-2" :exit-status 0}
{:time "2026-05-01T00:00:02Z" :nemesis :switchover :action :stop :source "alpha-1" :target "alpha-2" :exit-status 0}
`,
		},
		{
			name:     "accepts strict sync no standby",
			workload: "append-strict-sync",
			nemesis:  "no-standby",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :no-standby :action :start :target "patroni-1" :standbys "patroni-replica patroni-replica-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :no-standby :action :heal :target "patroni-1" :standbys "patroni-replica patroni-replica-2" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :no-standby :action :stop :target "patroni-1" :standbys "patroni-replica patroni-replica-2" :result :ok}
`,
		},
		{
			name:     "accepts synchronous standby kill",
			workload: "append-sync",
			nemesis:  "sync-standby-kill",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :sync-standby-kill :action :start :target "patroni-2" :service "patroni-replica"}
{:time "2026-05-01T00:00:02Z" :nemesis :sync-standby-kill :action :heal :target "patroni-2" :service "patroni-replica" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :sync-standby-kill :action :stop :target "patroni-2" :service "patroni-replica" :result :ok}
`,
		},
		{
			name:     "accepts maximum lag failover",
			workload: "append-max-lag",
			nemesis:  maximumLagOnFailoverNemesis,
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :lagging-replica-failover :action :start :target "patroni-1" :lagging-replica "patroni-2" :eligible-replica "patroni-3"}
{:time "2026-05-01T00:00:02Z" :nemesis :lagging-replica-failover :action :heal :target "patroni-1" :lagging-replica "patroni-2" :eligible-replica "patroni-3" :promoted "patroni-3" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :lagging-replica-failover :action :stop :target "patroni-1" :lagging-replica "patroni-2" :eligible-replica "patroni-3" :promoted "patroni-3" :result :ok}
`,
		},
		{
			name:     "accepts Patroni check timeline failover",
			workload: "append-check-timeline",
			nemesis:  patroniCheckTimelineNemesis,
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :stale-timeline-failover :action :start :target "patroni-1" :stale-replica "patroni-2" :eligible-replica "patroni-3"}
{:time "2026-05-01T00:00:02Z" :nemesis :stale-timeline-failover :action :heal :target "patroni-1" :stale-replica "patroni-2" :eligible-replica "patroni-3" :promoted "patroni-3" :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :stale-timeline-failover :action :stop :target "patroni-1" :stale-replica "patroni-2" :eligible-replica "patroni-3" :promoted "patroni-3" :result :ok}
`,
		},
		{
			name:     "accepts failover chain step result",
			workload: "append-failover",
			nemesis:  "failover-chain",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :failover-chain :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :failover-chain :action :step :source "alpha-1" :target "alpha-2" :exit-status 0}
{:time "2026-05-01T00:00:03Z" :nemesis :failover-chain :action :heal :target "alpha-2" :result :ok}
{:time "2026-05-01T00:00:03Z" :nemesis :failover-chain :action :stop :target "alpha-2" :result :ok}
`,
		},
		{
			name:     "accepts replica reinit result",
			workload: "append-reinit",
			nemesis:  "reinit-replica",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica :action :heal :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica :action :stop :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "accepts replica reinit kill target result",
			workload: "append-reinit",
			nemesis:  "reinit-replica-kill-target",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica-kill-target :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-kill-target :action :heal :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-kill-target :action :stop :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "accepts replica reinit kill source result",
			workload: "append-reinit",
			nemesis:  "reinit-replica-kill-source",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica-kill-source :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-kill-source :action :heal :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-kill-source :action :stop :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "accepts replica reinit DCS partition target result",
			workload: "append-reinit",
			nemesis:  "reinit-replica-dcs-partition-target",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica-dcs-partition-target :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-dcs-partition-target :action :heal :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-dcs-partition-target :action :stop :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "accepts replica reinit DCS partition primary result",
			workload: "append-reinit",
			nemesis:  "reinit-replica-dcs-partition-primary",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica-dcs-partition-primary :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-dcs-partition-primary :action :heal :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-dcs-partition-primary :action :stop :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "accepts replica reinit concurrent request result",
			workload: "append-reinit",
			nemesis:  "reinit-replica-concurrent-request",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica-concurrent-request :action :start :source "alpha-1" :target "alpha-2"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-concurrent-request :action :heal :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-concurrent-request :action :stop :source "alpha-1" :target "alpha-2" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "accepts replica reinit after failover result",
			workload: "append-reinit",
			nemesis:  "reinit-replica-after-failover",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :reinit-replica-after-failover :action :start :source "alpha-2" :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-after-failover :action :heal :source "alpha-2" :target "alpha-1" :operation-id "reinit-1" :exit-status 0 :result :ok}
{:time "2026-05-01T00:00:02Z" :nemesis :reinit-replica-after-failover :action :stop :source "alpha-2" :target "alpha-1" :operation-id "reinit-1" :exit-status 0 :result :ok}
`,
		},
		{
			name:     "rejects missing stop result",
			workload: "append-failover",
			nemesis:  "packet",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :packet :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :packet :action :stop :target "alpha-1"}
`,
			wantErr: []string{`action "stop" missing command result`},
		},
		{
			name:     "rejects missing heal",
			workload: "append-failover",
			nemesis:  "packet",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :packet :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :packet :action :stop :target "alpha-1" :result :ok}
`,
			wantErr: []string{`missing heal action`},
		},
		{
			name:     "rejects workload mismatch",
			workload: "append-smoke",
			nemesis:  "kill",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :kill :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :kill :action :stop :target "alpha-1" :result :ok}
`,
			wantErr: []string{`profile append-smoke:kill is not registered`},
		},
		{
			name:     "rejects schedule nemesis mismatch",
			workload: "append-failover",
			nemesis:  "packet",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :kill :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :kill :action :stop :target "alpha-1" :result :ok}
`,
			wantErr: []string{`nemesis is "kill", want "packet"`},
		},
		{
			name:     "rejects missing target",
			workload: "append-failover",
			nemesis:  "packet",
			schedule: `
{:time "2026-05-01T00:00:01Z" :nemesis :packet :action :start}
{:time "2026-05-01T00:00:02Z" :nemesis :packet :action :stop :result :ok}
`,
			wantErr: []string{`action "start" missing target`, `action "stop" missing target`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			entries, err := parseNemesisSchedule(test.schedule)
			if err != nil {
				t.Fatalf("parse schedule: %v", err)
			}
			problems := validateNemesisSchedule(test.workload, test.nemesis, entries)
			if len(test.wantErr) == 0 && len(problems) > 0 {
				t.Fatalf("problems: got %v want none", problems)
			}
			for _, want := range test.wantErr {
				if !strings.Contains(strings.Join(problems, "\n"), want) {
					t.Fatalf("problems: got %v want substring %q", problems, want)
				}
			}
		})
	}
}

func TestNemesisValidateScheduleCommandReportsFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	schedulePath := filepath.Join(dir, "nemesis-schedule.edn")
	writeTestFile(t, schedulePath, `
{:time "2026-05-01T00:00:01Z" :nemesis :packet :action :start :target "alpha-1"}
{:time "2026-05-01T00:00:02Z" :nemesis :packet :action :stop :target "alpha-1"}
`)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := Run([]string{
		"nemesis", "validate-schedule",
		"--workload", "append-failover",
		"--nemesis", "packet",
		"--schedule-file", schedulePath,
	}, &stdout, &stderr)

	if status != 2 {
		t.Fatalf("status: got %d want 2", status)
	}
	if !strings.Contains(stderr.String(), "missing command result") {
		t.Fatalf("stderr: got %q want missing command result", stderr.String())
	}
}

func parseNemesisSchedule(schedule string) ([]nemesisScheduleEntry, error) {
	var entries []nemesisScheduleEntry
	for index, line := range strings.Split(strings.TrimSpace(schedule), "\n") {
		fields, err := parseNemesisScheduleLine(strings.TrimSpace(line))
		if err != nil {
			return nil, err
		}
		entries = append(entries, nemesisScheduleEntry{Line: index + 1, Fields: fields})
	}
	return entries, nil
}
