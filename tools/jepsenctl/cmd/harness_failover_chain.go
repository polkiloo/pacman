package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (lab *harnessLab) failoverChain(ctx context.Context, caseDir, scheduleFile string) {
	chainFile := filepath.Join(caseDir, "failover-chain.jsonl")
	_ = os.WriteFile(chainFile, nil, 0o644)
	writeNemesisScheduleEvent(scheduleFile, "failover-chain", "start", fmt.Sprintf(":target %q", lab.currentPrimaryName(ctx)))
	step := 0
	for _, target := range []string{"alpha-2", "alpha-3", "alpha-1"} {
		_ = lab.waitForClusterSwitchoverReady(ctx, 90*time.Second)
		source := lab.currentPrimaryName(ctx)
		if source == "" || source == "unknown" || source == target {
			continue
		}
		step++
		sourceService := serviceForMember(source)
		output, status := lab.requestManualSwitchover(ctx, target, sourceService)
		if status == 0 && !lab.waitForCurrentPrimary(ctx, target, 75*time.Second) {
			status = 1
		}
		appendJSONL(chainFile, map[string]any{
			"step":          step,
			"requestedAt":   time.Now().UTC().Format(time.RFC3339),
			"source":        source,
			"sourceService": sourceService,
			"target":        target,
			"targetService": serviceForMember(target),
			"exitStatus":    status,
			"output":        output,
		})
		writeNemesisScheduleEvent(scheduleFile, "failover-chain", "step", fmt.Sprintf(":source %q :target %q :exit-status %d", source, target, status))
		if status != 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	time.Sleep(lab.cfg.nemesisHold)
	writeNemesisScheduleEvent(scheduleFile, "failover-chain", "stop", fmt.Sprintf(":target %q :result :ok", lab.currentPrimaryName(ctx)))
}
