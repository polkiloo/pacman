package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (lab *harnessLab) startPrimarySampler(ctx context.Context, caseDir string) *primarySampler {
	sampler := &primarySampler{stopCh: make(chan struct{}), done: make(chan struct{})}
	observationFile := filepath.Join(caseDir, "primary-observations.jsonl")
	go func() {
		defer close(sampler.done)
		sampleID := 1
		ticker := time.NewTicker(lab.cfg.primarySampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sampler.stopCh:
				return
			default:
				_ = lab.samplePrimaryState(ctx, sampleID, observationFile)
				sampleID++
			}
			select {
			case <-sampler.stopCh:
				return
			case <-ticker.C:
			}
		}
	}()
	return sampler
}

func (sampler *primarySampler) stop() {
	if sampler == nil {
		return
	}
	close(sampler.stopCh)
	<-sampler.done
}

func (lab *harnessLab) samplePrimaryState(ctx context.Context, sampleID int, observationFile string) error {
	observations := lab.samplePrimaryRound(ctx, sampleID, 0)
	for _, observation := range observations {
		appendJSONL(observationFile, observation.record)
	}
	if writableProbeCount(observations) <= 1 {
		return nil
	}

	for _, observation := range lab.samplePrimaryRound(ctx, sampleID, 1) {
		appendJSONL(observationFile, observation.record)
	}
	return nil
}

type primaryProbe struct {
	record   map[string]any
	writable bool
}

func (lab *harnessLab) samplePrimaryRound(ctx context.Context, sampleID, probeRound int) []primaryProbe {
	probes := make([]primaryProbe, len(lab.options.target.DataNodes))
	var waitGroup sync.WaitGroup
	for index, node := range lab.options.target.DataNodes {
		waitGroup.Add(1)
		go func(index int, node targetNode) {
			defer waitGroup.Done()
			probes[index] = lab.probePrimaryNode(ctx, sampleID, probeRound, node)
		}(index, node)
	}
	waitGroup.Wait()
	return probes
}

func (lab *harnessLab) probePrimaryNode(ctx context.Context, sampleID, probeRound int, node targetNode) primaryProbe {
	startedAt := time.Now().UTC()
	output, err := lab.psqlService(ctx, node.Service, `
with local as (
  select
    pg_is_in_recovery() as in_recovery,
    case when pg_is_in_recovery() then null else pg_current_wal_lsn() end as write_lsn,
    pg_last_wal_replay_lsn() as replay_lsn,
    (select received_tli from pg_stat_wal_receiver where status = 'streaming' limit 1) as received_tli
),
observed as (
  select in_recovery, coalesce(write_lsn, replay_lsn) as lsn, received_tli from local
)
select
  in_recovery,
  case
    when in_recovery then coalesce(received_tli, 0)
    when lsn is null then 0
    else ('x' || substr(pg_walfile_name(lsn), 1, 8))::bit(32)::int
  end as timeline,
  coalesce(lsn::text, '')
from observed;`)
	finishedAt := time.Now().UTC()
	record := map[string]any{
		"sampleId":        sampleID,
		"probeRound":      probeRound,
		"probeStartedAt":  startedAt.Format(time.RFC3339Nano),
		"probeFinishedAt": finishedAt.Format(time.RFC3339Nano),
		"observedAt":      finishedAt.Format(time.RFC3339Nano),
		"member":          node.Name,
		"service":         node.Service,
		"reachable":       err == nil,
		"writable":        false,
		"inRecovery":      nil,
		"timeline":        nil,
		"lsn":             "",
		"error":           "",
	}
	if err != nil {
		record["error"] = err.Error()
		return primaryProbe{record: record}
	}

	parts := strings.Split(lastNonEmptyLine(output), "\t")
	inRecovery := len(parts) > 0 && parts[0] == "t"
	timeline := 0
	if len(parts) > 1 {
		timeline, _ = strconv.Atoi(parts[1])
	}
	lsn := ""
	if len(parts) > 2 {
		lsn = parts[2]
	}
	record["writable"] = !inRecovery
	record["inRecovery"] = inRecovery
	record["timeline"] = timeline
	record["lsn"] = lsn
	return primaryProbe{record: record, writable: !inRecovery}
}

func writableProbeCount(probes []primaryProbe) int {
	count := 0
	for _, probe := range probes {
		if probe.writable {
			count++
		}
	}
	return count
}

func (lab *harnessLab) captureClusterSnapshot(ctx context.Context, caseDir, phase, nemesis, target, service string) error {
	if service == "" {
		service = lab.options.target.firstDataService()
	}
	snapshotFile := filepath.Join(caseDir, "pacman-cluster-snapshots.jsonl")
	output, err := lab.clusterStatusJSON(ctx, service)
	if fallback := lab.options.target.firstDataService(); err != nil && service != fallback {
		output, err = lab.clusterStatusJSON(ctx, fallback)
		service = fallback
	}
	if err != nil {
		appendJSONL(snapshotFile, map[string]any{
			"observedAt": time.Now().UTC().Format(time.RFC3339),
			"phase":      phase,
			"nemesis":    nemesis,
			"target":     target,
			"service":    service,
			"ok":         false,
			"cluster":    nil,
			"error":      err.Error(),
		})
		return err
	}
	var cluster any
	_ = json.Unmarshal([]byte(output), &cluster)
	appendJSONL(snapshotFile, map[string]any{
		"observedAt": time.Now().UTC().Format(time.RFC3339),
		"phase":      phase,
		"nemesis":    nemesis,
		"target":     target,
		"service":    service,
		"ok":         true,
		"cluster":    cluster,
		"error":      "",
	})
	return nil
}

func (lab *harnessLab) capturePGStatReplication(ctx context.Context, caseDir, phase string) error {
	primary := lab.currentPrimaryName(ctx)
	service := lab.serviceForMember(primary)
	if service == "" {
		service = lab.options.target.firstDataService()
	}
	output, err := lab.psqlService(ctx, service, `
SELECT coalesce(json_agg(json_build_object(
  'applicationName', application_name,
  'clientAddr', client_addr::text,
  'state', state,
  'syncState', sync_state,
  'writeLsn', write_lsn::text,
  'flushLsn', flush_lsn::text,
  'replayLsn', replay_lsn::text
) ORDER BY application_name), '[]'::json)
FROM pg_stat_replication;`)
	rows := []any{}
	ok := err == nil && json.Unmarshal([]byte(lastNonEmptyLine(output)), &rows) == nil
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	writeJSON(filepath.Join(caseDir, "pg-stat-replication.json"), map[string]any{
		"observedAt":     time.Now().UTC().Format(time.RFC3339),
		"phase":          phase,
		"currentPrimary": primary,
		"service":        service,
		"ok":             ok,
		"rows":           rows,
		"error":          errText,
	})
	return err
}

func (lab *harnessLab) capturePGStatWalReceiver(ctx context.Context, caseDir, phase string) error {
	path := filepath.Join(caseDir, "pg-stat-wal-receiver.jsonl")
	_ = os.WriteFile(path, nil, 0o644)
	for _, node := range lab.options.target.DataNodes {
		member := node.Name
		service := node.Service
		output, err := lab.psqlService(ctx, service, `SELECT coalesce(json_agg(json_build_object('status', status, 'receivedTli', received_tli, 'latestEndLsn', latest_end_lsn::text)), '[]'::json) FROM pg_stat_wal_receiver;`)
		rows := []any{}
		ok := err == nil && json.Unmarshal([]byte(lastNonEmptyLine(output)), &rows) == nil
		errText := ""
		if err != nil {
			errText = err.Error()
		}
		appendJSONL(path, map[string]any{
			"observedAt": time.Now().UTC().Format(time.RFC3339),
			"phase":      phase,
			"member":     member,
			"service":    service,
			"ok":         ok,
			"rows":       rows,
			"error":      errText,
		})
	}
	return nil
}

func (lab *harnessLab) recordClientTrafficProbe(ctx context.Context, caseDir, nemesis, probeID string) error {
	path := filepath.Join(caseDir, "client-traffic-during-nemesis.jsonl")
	_, err := lab.psqlVIP(ctx, fmt.Sprintf(`INSERT INTO jepsen.client_availability_probes(probe_id, nemesis) VALUES (%s, %s) ON CONFLICT (probe_id) DO NOTHING;`, sqlLiteral(probeID), sqlLiteral(nemesis)))
	appendJSONL(path, map[string]any{
		"observedAt": time.Now().UTC().Format(time.RFC3339),
		"nemesis":    nemesis,
		"probeId":    probeID,
		"ok":         err == nil,
		"error":      errorString(err),
	})
	return err
}

func (lab *harnessLab) recordReplicationHealthProbe(ctx context.Context, service, caseDir, nemesis string) error {
	path := filepath.Join(caseDir, "replication-traffic-during-nemesis.jsonl")
	output, err := lab.psqlService(ctx, service, `SELECT coalesce(json_agg(json_build_object('applicationName', application_name, 'state', state, 'syncState', sync_state)), '[]'::json) FROM pg_stat_replication;`)
	var rows []map[string]any
	ok := err == nil && json.Unmarshal([]byte(lastNonEmptyLine(output)), &rows) == nil
	streaming := 0
	for _, row := range rows {
		if row["state"] == "streaming" {
			streaming++
		}
	}
	appendJSONL(path, map[string]any{
		"observedAt":        time.Now().UTC().Format(time.RFC3339),
		"nemesis":           nemesis,
		"service":           service,
		"ok":                ok,
		"streamingReplicas": streaming,
		"rows":              rows,
		"error":             errorString(err),
	})
	return err
}

func (lab *harnessLab) recordDCSTrafficProbe(ctx context.Context, service, caseDir, nemesis string) error {
	path := filepath.Join(caseDir, "dcs-traffic-during-nemesis.jsonl")
	health, err := lab.dcsHealth(ctx, service)
	appendJSONL(path, map[string]any{
		"observedAt": time.Now().UTC().Format(time.RFC3339),
		"nemesis":    nemesis,
		"service":    service,
		"ok":         err == nil,
		"output":     health,
		"endpoints":  strings.Join(dcsEndpoints(), ","),
		"error":      errorString(err),
	})
	return err
}

func (lab *harnessLab) recordDCSQuorumProbe(ctx context.Context, caseDir, nemesis, phase string, targets []string, observer string) error {
	path := filepath.Join(caseDir, "dcs-quorum-during-nemesis.jsonl")
	health, err := lab.dcsHealth(ctx, observer)
	targetMembers := make([]string, 0, len(targets))
	for _, target := range targets {
		targetMembers = append(targetMembers, dcsMemberForService(target))
	}
	runningTargets, targetRunning := dcsQuorumTargetState(nemesis, targets, health)
	appendJSONL(path, map[string]any{
		"observedAt":               time.Now().UTC().Format(time.RFC3339),
		"nemesis":                  nemesis,
		"phase":                    phase,
		"ok":                       health.TotalEndpoints == len(dcsEndpoints()),
		"targetServices":           strings.Join(targets, " "),
		"targetMembers":            strings.Join(targetMembers, " "),
		"targetRunning":            targetRunning,
		"runningTargets":           runningTargets,
		"targetCount":              len(targets),
		"totalEndpoints":           health.TotalEndpoints,
		"healthyEndpoints":         health.HealthyEndpoints,
		"failedEndpoints":          health.FailedEndpoints,
		"totalElapsedMillis":       health.TotalElapsedMillis,
		"maxEndpointLatencyMillis": health.MaxEndpointLatencyMillis,
		"health":                   health,
		"error":                    errorString(err),
	})
	return err
}

func (lab *harnessLab) recordDCSQuorumRecoveryProbe(ctx context.Context, caseDir, nemesis, phase string, targets []string, observer string) {
	deadline := time.Now().Add(lab.cfg.dcsRecoveryTimeout)
	interval := lab.cfg.dcsRecoveryInterval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := lab.recordDCSQuorumProbe(ctx, caseDir, nemesis, phase, targets, observer); err == nil {
			return
		}
		if lab.cfg.dcsRecoveryTimeout <= 0 || !time.Now().Before(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func dcsQuorumTargetState(nemesis string, targets []string, health dcsHealthResult) (int, bool) {
	switch nemesis {
	case "dcs-kill-one", "dcs-lose-majority", "dcs-full-restart":
		running := 0
		for _, target := range targets {
			if dcsEndpointHealthy("http://"+target+":2379", health) {
				running++
			}
		}
		return running, len(targets) > 0 && running == len(targets)
	default:
		return len(targets), len(targets) > 0
	}
}

func dcsEndpointHealthy(endpoint string, health dcsHealthResult) bool {
	for _, info := range health.Endpoints {
		if info.Endpoint == endpoint {
			return info.OK
		}
	}
	return false
}

type dcsHealthResult struct {
	TotalEndpoints           int               `json:"totalEndpoints"`
	HealthyEndpoints         int               `json:"healthyEndpoints"`
	FailedEndpoints          int               `json:"failedEndpoints"`
	TotalElapsedMillis       int64             `json:"totalElapsedMillis"`
	MaxEndpointLatencyMillis int64             `json:"maxEndpointLatencyMillis"`
	Endpoints                []dcsEndpointInfo `json:"endpoints"`
}

type dcsEndpointInfo struct {
	Endpoint      string `json:"endpoint"`
	OK            bool   `json:"ok"`
	Body          string `json:"body,omitempty"`
	Error         string `json:"error,omitempty"`
	ElapsedMillis int64  `json:"elapsedMillis"`
}

func dcsEndpoints() []string {
	return []string{"http://pacman-dcs:2379", "http://pacman-dcs-2:2379", "http://pacman-dcs-3:2379"}
}

func (lab *harnessLab) dcsHealth(ctx context.Context, observer string) (dcsHealthResult, error) {
	result := dcsHealthResult{TotalEndpoints: len(dcsEndpoints())}
	started := time.Now()
	var problems []string
	for _, endpoint := range dcsEndpoints() {
		endpointStart := time.Now()
		output, status, _ := lab.composeExec(ctx, observer, "python3", "-c", "import sys, urllib.request; print(urllib.request.urlopen(sys.argv[1] + '/health', timeout=3).read().decode())", endpoint)
		info := dcsEndpointInfo{Endpoint: endpoint, ElapsedMillis: time.Since(endpointStart).Milliseconds()}
		if status == 0 {
			info.OK = true
			info.Body = strings.TrimSpace(output)
			result.HealthyEndpoints++
		} else {
			info.Error = strings.TrimSpace(output)
			problems = append(problems, endpoint)
		}
		if info.ElapsedMillis > result.MaxEndpointLatencyMillis {
			result.MaxEndpointLatencyMillis = info.ElapsedMillis
		}
		result.Endpoints = append(result.Endpoints, info)
	}
	result.TotalElapsedMillis = time.Since(started).Milliseconds()
	result.FailedEndpoints = result.TotalEndpoints - result.HealthyEndpoints
	if len(problems) > 0 {
		return result, fmt.Errorf("unhealthy DCS endpoints: %s", strings.Join(problems, ", "))
	}
	return result, nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
