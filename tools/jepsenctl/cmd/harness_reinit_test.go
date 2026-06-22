package cmd

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestReinitCompletionRequiresCompletedClusterAndStreamingTarget(t *testing.T) {
	t.Parallel()

	status := clusterStatusJSONWithPrimary("alpha-1")
	var healthy clusterStatus
	if err := json.Unmarshal([]byte(status), &healthy); err != nil {
		t.Fatalf("decode cluster: %v", err)
	}
	healthy.Reinit = &reinitStatus{
		OperationID: "reinit-1",
		State:       "completed",
		LastResult:  "succeeded",
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
	}
	for index := range healthy.Members {
		if healthy.Members[index].Name == "alpha-2" {
			healthy.Members[index].Reinit = healthy.Reinit
		}
	}

	if !reinitComplete(healthy, "alpha-2", "alpha-1", "reinit-1") {
		t.Fatalf("expected healthy completed reinit status to pass")
	}

	wrongOperation := healthy
	wrongOperation.Reinit = &reinitStatus{
		OperationID: "reinit-other",
		State:       "completed",
		LastResult:  "succeeded",
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
	}
	if reinitComplete(wrongOperation, "alpha-2", "alpha-1", "reinit-1") {
		t.Fatalf("expected mismatched operation id to fail")
	}

	notStreaming := healthy
	for index := range notStreaming.Members {
		if notStreaming.Members[index].Name == "alpha-2" {
			notStreaming.Members[index].State = "stopping"
		}
	}
	if reinitComplete(notStreaming, "alpha-2", "alpha-1", "reinit-1") {
		t.Fatalf("expected non-streaming target to fail")
	}
}

func TestReinitTerminalFailureRejectsUnsafeTargetPromotion(t *testing.T) {
	t.Parallel()

	status := clusterStatusJSONWithPrimary("alpha-1")
	var failed clusterStatus
	if err := json.Unmarshal([]byte(status), &failed); err != nil {
		t.Fatalf("decode cluster: %v", err)
	}
	failed.Reinit = &reinitStatus{
		OperationID: "reinit-1",
		State:       "failed",
		LastResult:  "failed",
		FromMember:  "alpha-1",
		ToMember:    "alpha-2",
	}
	for index := range failed.Members {
		if failed.Members[index].Name == "alpha-2" {
			failed.Members[index].Reinit = failed.Reinit
		}
	}

	if !reinitTerminalFailure(failed, "alpha-2", "alpha-1", "reinit-1") {
		t.Fatalf("expected terminal failed reinit to pass kill-target outcome")
	}

	unsafe := failed
	unsafe.CurrentPrimary = "alpha-2"
	for index := range unsafe.Members {
		if unsafe.Members[index].Name == "alpha-2" {
			unsafe.Members[index].Role = "primary"
			unsafe.Members[index].State = "running"
		}
	}
	if reinitTerminalFailure(unsafe, "alpha-2", "alpha-1", "reinit-1") {
		t.Fatalf("expected promoted reinit target to fail terminal-failure outcome")
	}
}

func TestCheckReinitProcedureWritesNotApplicableAndRejectsInvalidArtifact(t *testing.T) {
	t.Parallel()

	lab := &harnessLab{}
	caseDir := t.TempDir()

	if err := lab.checkReinitProcedure("kill", caseDir); err != nil {
		t.Fatalf("non-reinit checker: %v", err)
	}
	var checker map[string]any
	readJSONTestFile(t, filepath.Join(caseDir, reinitCheckerFile), &checker)
	if checker["applicable"] != false || checker["valid"] != true {
		t.Fatalf("not applicable checker: %#v", checker)
	}

	writeTestFile(t, filepath.Join(caseDir, reinitCheckerFile), `{"checker":"full-replica-reinit","valid":false}`+"\n")
	for _, nemesis := range []string{"reinit-replica", "reinit-replica-kill-target", "reinit-replica-concurrent-request", "reinit-replica-after-failover"} {
		if err := lab.checkReinitProcedure(nemesis, caseDir); err == nil || !strings.Contains(err.Error(), "reinit checker failed") {
			t.Fatalf("invalid reinit checker error for %s: %v", nemesis, err)
		}
	}
}

func TestReinitOperationIDFromPrettyJSONOutput(t *testing.T) {
	t.Parallel()

	output := `log line
{
  "message": "reinit accepted",
  "operation": {
    "id": "reinit-20260621T120000Z",
    "kind": "reinit"
  }
}
`

	if got := reinitOperationIDFromOutput(output); got != "reinit-20260621T120000Z" {
		t.Fatalf("operation id: got %q", got)
	}
}
