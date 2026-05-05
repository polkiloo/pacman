//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	testcontainers "github.com/testcontainers/testcontainers-go"

	"github.com/polkiloo/pacman/test/testenv"
)

const postgresSubprocessNegativeBinaryPath = "/usr/local/bin/postgres-subprocess-negative.test"

func TestPostgresSubprocessNegativeCasesInRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed subprocess negative tests in short mode")
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	binaryPath := buildPostgresSubprocessNegativeBinary(t)
	runner := env.StartRunner(t, testenv.RunnerConfig{
		Name: "postgres-subprocess-negative",
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      binaryPath,
				ContainerFilePath: postgresSubprocessNegativeBinaryPath,
				FileMode:          0o755,
			},
		},
	})

	output := runner.RequireExec(
		t,
		"/bin/sh",
		"-lc",
		postgresSubprocessNegativeBinaryPath+" -test.run '^TestPatroniInspired(ExecuteCommandMissingBinary|ExecutePassthroughCommandMissingBinary|ExecuteCommandPreCanceledContext|ExecutePassthroughCommandPreCanceledContext|PGCtlStartCancelsLongRunningPgCtl|PGCtlStopCancelsLongRunningPgCtl|PGRewindCancelsLongRunningRewind)$' -test.v",
	)
	for _, name := range []string{
		"TestPatroniInspiredExecuteCommandMissingBinary",
		"TestPatroniInspiredExecutePassthroughCommandMissingBinary",
		"TestPatroniInspiredExecuteCommandPreCanceledContext",
		"TestPatroniInspiredExecutePassthroughCommandPreCanceledContext",
		"TestPatroniInspiredPGCtlStartCancelsLongRunningPgCtl",
		"TestPatroniInspiredPGCtlStopCancelsLongRunningPgCtl",
		"TestPatroniInspiredPGRewindCancelsLongRunningRewind",
	} {
		if !strings.Contains(output, "--- PASS: "+name) {
			t.Fatalf("expected container test output to include %q pass, got:\n%s", name, output)
		}
	}
}

func buildPostgresSubprocessNegativeBinary(t *testing.T) string {
	t.Helper()

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	repoRoot := filepath.Clean(filepath.Join(workingDir, "..", ".."))
	outputPath := filepath.Join(t.TempDir(), "postgres-subprocess-negative.test")

	cmd := exec.Command("go", "test", "-c", "-o", outputPath, "./internal/postgres")
	cmd.Dir = repoRoot
	cmd.Env = append(
		os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runnerGOARCH(t),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build postgres subprocess negative test binary: %v\n%s", err, output)
	}

	return outputPath
}
