package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	reinitArtifactFile = "reinit.json"
	reinitCheckerFile  = "reinit-checker.json"
)

type reinitObservation struct {
	ObservedAt      string           `json:"observedAt"`
	Service         string           `json:"service"`
	ClusterPhase    string           `json:"clusterPhase"`
	CurrentPrimary  string           `json:"currentPrimary"`
	ActiveOperation *operationStatus `json:"activeOperation,omitempty"`
	ClusterReinit   *reinitStatus    `json:"clusterReinit,omitempty"`
	TargetMember    clusterMember    `json:"targetMember"`
	TargetStreaming bool             `json:"targetStreaming"`
	TargetHealthy   bool             `json:"targetHealthy"`
	Error           string           `json:"error,omitempty"`
}

type reinitWaitResult struct {
	Completed    bool                `json:"completed"`
	Valid        bool                `json:"valid"`
	Outcome      string              `json:"outcome,omitempty"`
	OperationID  string              `json:"operationId"`
	Target       string              `json:"target"`
	Source       string              `json:"source"`
	Observations []reinitObservation `json:"observations"`
	FinalStatus  *clusterStatus      `json:"finalStatus,omitempty"`
	Error        string              `json:"error,omitempty"`
}

type reinitRunOptions struct {
	Nemesis        string
	Reason         string
	InitialDetails map[string]any
	BeforeRequest  func(context.Context, reinitRunContext) reinitVariantResult
	AfterAccepted  func(context.Context, reinitRunContext) reinitVariantResult
	AfterWait      func(context.Context, reinitRunContext, reinitWaitResult) reinitVariantResult
	WaitForResult  func(context.Context, string, string, string) reinitWaitResult
}

type reinitRunContext struct {
	Target         string
	Source         string
	ControlService string
	TargetService  string
	SourceService  string
	OperationID    string
	CaseDir        string
	ScheduleFile   string
}

type reinitVariantResult struct {
	Valid   bool
	Error   string
	Details map[string]any
}

type reinitDCSPartitionTargetProbe struct {
	Valid                         bool                `json:"valid"`
	DcsServices                   []string            `json:"dcsServices"`
	Observations                  []reinitObservation `json:"observations"`
	MisleadingHealthyObservations []reinitObservation `json:"misleadingHealthyObservations,omitempty"`
	Error                         string              `json:"error,omitempty"`
}

type reinitDCSPartitionPrimaryProbe struct {
	Valid                             bool                `json:"valid"`
	SourceService                     string              `json:"sourceService"`
	DcsServices                       []string            `json:"dcsServices"`
	ObserverServices                  []string            `json:"observerServices"`
	Observations                      []reinitObservation `json:"observations"`
	SafeFailoverObserved              bool                `json:"safeFailoverObserved"`
	UnsafeTargetPrimaryObservations   []reinitObservation `json:"unsafeTargetPrimaryObservations,omitempty"`
	UnavailableQuorumSideObservations int                 `json:"unavailableQuorumSideObservations,omitempty"`
	Error                             string              `json:"error,omitempty"`
}

type operationStatus struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	State  string `json:"state"`
	Result string `json:"result,omitempty"`
}

type reinitRepeatedStep struct {
	Index          int              `json:"index"`
	RequestedAt    string           `json:"requestedAt"`
	Target         string           `json:"target"`
	Source         string           `json:"source"`
	ControlService string           `json:"controlService"`
	TargetService  string           `json:"targetService"`
	ExitStatus     int              `json:"exitStatus"`
	Output         string           `json:"output"`
	OperationID    string           `json:"operationId"`
	Wait           reinitWaitResult `json:"wait"`
	Valid          bool             `json:"valid"`
	Error          string           `json:"error,omitempty"`
}

type reinitRepeatedResult struct {
	Valid       bool                       `json:"valid"`
	Nemesis     string                     `json:"nemesis"`
	Steps       []reinitRepeatedStep       `json:"steps"`
	History     reinitRepeatedHistoryCheck `json:"history"`
	Slots       reinitRepeatedSlotCheck    `json:"slots"`
	FinalHealth reinitRepeatedFinalHealth  `json:"finalHealth"`
	FinalStatus *clusterStatus             `json:"finalStatus,omitempty"`
	Error       string                     `json:"error,omitempty"`
}

type reinitRepeatedHistoryCheck struct {
	Valid   bool                 `json:"valid"`
	Entries []reinitHistoryEntry `json:"entries"`
	Error   string               `json:"error,omitempty"`
}

type reinitHistoryResponse struct {
	Items []reinitHistoryEntry `json:"items"`
}

type reinitHistoryEntry struct {
	OperationID string `json:"operationId"`
	Kind        string `json:"kind"`
	FromMember  string `json:"fromMember,omitempty"`
	ToMember    string `json:"toMember,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Result      string `json:"result"`
	FinishedAt  string `json:"finishedAt"`
}

type reinitRepeatedSlotCheck struct {
	Valid          bool               `json:"valid"`
	Primary        string             `json:"primary"`
	PrimaryService string             `json:"primaryService"`
	ExpectedSlots  []string           `json:"expectedSlots"`
	Slots          []reinitSlotStatus `json:"slots"`
	Error          string             `json:"error,omitempty"`
}

type reinitSlotStatus struct {
	SlotName   string `json:"slotName"`
	Active     bool   `json:"active"`
	RestartLSN string `json:"restartLsn,omitempty"`
}

type reinitLagState struct {
	Member       string `json:"member"`
	Service      string `json:"service"`
	ReplayPaused bool   `json:"replayPaused"`
	LagBytes     int64  `json:"lagBytes"`
	ReceiveLSN   string `json:"receiveLsn,omitempty"`
	ReplayLSN    string `json:"replayLsn,omitempty"`
}

type reinitLagVerification struct {
	Valid                   bool   `json:"valid"`
	Target                  string `json:"target"`
	Source                  string `json:"source"`
	TargetService           string `json:"targetService"`
	SourceService           string `json:"sourceService"`
	PrimarySystemIdentifier string `json:"primarySystemIdentifier,omitempty"`
	TargetSystemIdentifier  string `json:"targetSystemIdentifier,omitempty"`
	PrimaryTimeline         int64  `json:"primaryTimeline,omitempty"`
	TargetTimeline          int64  `json:"targetTimeline,omitempty"`
	ExpectedSlot            string `json:"expectedSlot,omitempty"`
	SlotActive              bool   `json:"slotActive"`
	SlotRestartLSN          string `json:"slotRestartLsn,omitempty"`
	TargetInRecovery        bool   `json:"targetInRecovery"`
	WALReceiverStatus       string `json:"walReceiverStatus,omitempty"`
	WALReceiverConninfo     string `json:"walReceiverConninfo,omitempty"`
	StreamingSource         string `json:"streamingSource,omitempty"`
	Error                   string `json:"error,omitempty"`
}

type reinitRepeatedFinalHealth struct {
	Valid   bool     `json:"valid"`
	Targets []string `json:"targets"`
	Error   string   `json:"error,omitempty"`
}

func (lab *harnessLab) runReinitReplica(ctx context.Context, caseDir, scheduleFile string, options reinitRunOptions) error {
	if options.WaitForResult == nil {
		options.WaitForResult = lab.waitForReinitCompletion
	}
	target, source, controlService, err := lab.reinitTarget(ctx)
	if err != nil {
		lab.writeReinitFailure(caseDir, "", "", "", err)
		return err
	}

	writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "start", fmt.Sprintf(":source %q :target %q", source, target))
	requestedAt := time.Now().UTC()
	result := map[string]any{
		"requestedAt":    requestedAt.Format(time.RFC3339),
		"nemesis":        options.Nemesis,
		"source":         source,
		"target":         target,
		"targetService":  lab.serviceForMember(target),
		"controlService": controlService,
	}
	for key, value := range options.InitialDetails {
		result[key] = value
	}

	run := reinitRunContext{
		Target:         target,
		Source:         source,
		ControlService: controlService,
		TargetService:  lab.serviceForMember(target),
		SourceService:  lab.serviceForMember(source),
		CaseDir:        caseDir,
		ScheduleFile:   scheduleFile,
	}
	var setup reinitVariantResult
	if options.BeforeRequest != nil {
		setup = options.BeforeRequest(ctx, run)
		result["setup"] = setup.Details
		if setup.Error != "" {
			result["setupError"] = setup.Error
		}
		if !setup.Valid {
			result["valid"] = false
			writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
			lab.writeReinitChecker(caseDir, false, target, source, "", setup.Error, nil)
			writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", source, target, "", 1))
			return fmt.Errorf("reinit setup for %s failed: %s", target, setup.Error)
		}
	} else {
		setup.Valid = true
	}

	output, status := lab.requestReinit(ctx, target, controlService, options.Reason)
	operationID := reinitOperationIDFromOutput(output)
	result["exitStatus"] = status
	result["output"] = output
	result["operationId"] = operationID

	if status != 0 {
		result["valid"] = false
		writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
		lab.writeReinitChecker(caseDir, false, target, source, operationID, "reinit request failed", nil)
		writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :fail", source, target, operationID, status))
		return fmt.Errorf("reinit request for %s failed with status %d: %s", target, status, strings.TrimSpace(output))
	}

	var variant reinitVariantResult
	if options.AfterAccepted != nil {
		run.OperationID = operationID
		variant = options.AfterAccepted(ctx, run)
		result["variant"] = variant.Details
		if variant.Error != "" {
			result["variantError"] = variant.Error
		}
	} else {
		variant.Valid = true
	}

	wait := options.WaitForResult(ctx, target, source, operationID)
	run.OperationID = operationID
	var afterWait reinitVariantResult
	if options.AfterWait != nil {
		afterWait = options.AfterWait(ctx, run, wait)
		result["postCheck"] = afterWait.Details
		if afterWait.Error != "" {
			result["postCheckError"] = afterWait.Error
		}
	} else {
		afterWait.Valid = true
	}
	result["valid"] = setup.Valid && wait.Valid && variant.Valid && afterWait.Valid
	result["completed"] = wait.Completed
	result["wait"] = wait
	writeJSON(filepath.Join(caseDir, reinitArtifactFile), result)
	checkerError := wait.Error
	if checkerError == "" {
		checkerError = variant.Error
	}
	if checkerError == "" {
		checkerError = afterWait.Error
	}
	lab.writeReinitChecker(caseDir, setup.Valid && wait.Valid && variant.Valid && afterWait.Valid, target, source, operationID, checkerError, &wait)

	statusLabel := "ok"
	if !setup.Valid || !wait.Valid || !variant.Valid || !afterWait.Valid {
		statusLabel = "fail"
	}
	writeNemesisScheduleEvent(scheduleFile, options.Nemesis, "stop", fmt.Sprintf(":source %q :target %q :operation-id %q :exit-status %d :result :%s", source, target, operationID, status, statusLabel))
	if !setup.Valid || !wait.Valid || !variant.Valid || !afterWait.Valid {
		return fmt.Errorf("reinit did not satisfy %s for %s: %s", options.Nemesis, target, checkerError)
	}
	return nil
}

func (lab *harnessLab) reinitTarget(ctx context.Context) (target, source, controlService string, err error) {
	return lab.reinitTargetExcluding(ctx, nil)
}

func (lab *harnessLab) reinitTargetExcluding(ctx context.Context, excluded map[string]struct{}) (target, source, controlService string, err error) {
	status, service, err := lab.pacmanClusterStatusAny(ctx)
	if err != nil {
		return "", "", "", err
	}
	source = status.CurrentPrimary
	for _, member := range status.Members {
		if member.Name == source || !member.Healthy || member.Role != "replica" {
			continue
		}
		if _, skip := excluded[member.Name]; skip {
			continue
		}
		if member.State == "streaming" || member.State == "running" {
			return member.Name, source, service, nil
		}
	}
	return "", source, service, fmt.Errorf("no healthy reinit replica target found")
}

func (lab *harnessLab) requestReinit(ctx context.Context, target, service, reason string) (string, int) {
	if target == "" {
		return "no healthy non-primary reinit target found", 2
	}
	output, status, _ := lab.composeExec(ctx, service, "env",
		"PACMANCTL_API_URL=http://"+service+":8080",
		"PACMANCTL_API_TOKEN="+pacmanAPIToken,
		"pacmanctl", "cluster", "reinit",
		"-member", target,
		"-reason", reason,
		"-requested-by", "jepsen",
		"-o", "json")
	return output, status
}
