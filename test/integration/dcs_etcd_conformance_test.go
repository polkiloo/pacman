//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/test/testenv"
)

const (
	testEtcdImage             = "gcr.io/etcd-development/etcd:v3.5.15"
	etcdConformanceBinaryPath = "/usr/local/bin/dcs-etcd-conformance"
)

func TestEtcdDCSConformanceInRunner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed etcd conformance test in short mode")
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         "etcd",
		Image:        testEtcdImage,
		Aliases:      []string{"etcd"},
		Entrypoint:   []string{"etcd"},
		ExposedPorts: []string{"2379/tcp"},
		Cmd: []string{
			"--name=default",
			"--data-dir=/etcd-data",
			"--listen-client-urls=http://0.0.0.0:2379",
			"--advertise-client-urls=http://etcd:2379",
			"--listen-peer-urls=http://0.0.0.0:2380",
			"--initial-advertise-peer-urls=http://etcd:2380",
			"--initial-cluster=default=http://etcd:2380",
		},
		WaitStrategy: wait.ForHTTP("/health").
			WithPort("2379/tcp").
			WithStartupTimeout(60 * time.Second),
	})

	_ = service

	binaryPath := buildEtcdConformanceBinary(t)
	runner := env.StartRunner(t, testenv.RunnerConfig{
		Name: "dcs-etcd-conformance",
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      binaryPath,
				ContainerFilePath: etcdConformanceBinaryPath,
				FileMode:          0o755,
			},
		},
	})

	output := runner.RequireExec(
		t,
		"/bin/sh",
		"-lc",
		"PACMAN_DCS_ETCD_ENDPOINTS=http://etcd:2379 "+etcdConformanceBinaryPath+" -test.run '^TestBackendConformance$' -test.v",
	)
	if !strings.Contains(output, "--- PASS: TestBackendConformance") {
		t.Fatalf("unexpected conformance output: %s", output)
	}
}

func buildEtcdConformanceBinary(t *testing.T) string {
	t.Helper()

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	repoRoot := filepath.Clean(filepath.Join(workingDir, "..", ".."))
	outputPath := filepath.Join(t.TempDir(), "dcs-etcd-conformance.test")

	cmd := exec.Command("go", "test", "-c", "-tags=integration", "-o", outputPath, "./internal/dcs/etcd")
	cmd.Dir = repoRoot
	cmd.Env = append(
		os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runnerGOARCH(t),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build etcd conformance test binary: %v\n%s", err, output)
	}

	return outputPath
}

func runnerGOARCH(t *testing.T) string {
	t.Helper()

	command := exec.Command("docker", "image", "inspect", pacmanTestImage(), "--format", "{{.Architecture}}")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect runner image architecture: %v\n%s", err, output)
	}

	arch := strings.TrimSpace(string(output))
	if arch == "" {
		t.Fatal("runner image architecture is empty")
	}

	return arch
}
