package testenv

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	dockernetwork "github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	testcontainers "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ServiceConfig configures an arbitrary long-lived service container on the shared test network.
type ServiceConfig struct {
	Name         string
	Image        string
	Aliases      []string
	IPv4Address  string
	CapAdd       []string
	Env          map[string]string
	Files        []testcontainers.ContainerFile
	Entrypoint   []string
	Cmd          []string
	ExposedPorts []string
	WaitStrategy wait.Strategy
}

// Service is a long-lived arbitrary service container used during integration tests.
type Service struct {
	ctx       context.Context
	name      string
	container testcontainers.Container
}

// StartService starts an arbitrary service container on the shared network.
func (e *Environment) StartService(t *testing.T, cfg ServiceConfig) *Service {
	t.Helper()

	if strings.TrimSpace(cfg.Name) == "" {
		t.Fatal("service name must be provided")
	}

	if strings.TrimSpace(cfg.Image) == "" {
		t.Fatal("service image must be provided")
	}

	aliases := cfg.Aliases
	if len(aliases) == 0 {
		aliases = []string{cfg.Name}
	}

	options := []testcontainers.ContainerCustomizer{
		testcontainers.WithName(fmt.Sprintf("%s-%s-%d", e.namePrefix, sanitizeName(cfg.Name), time.Now().UnixNano())),
		testcontainers.WithEnv(cfg.Env),
		testcontainers.WithFiles(cfg.Files...),
		network.WithNetwork(aliases, e.network),
		testcontainers.WithLogger(tclog.TestLogger(t)),
		testcontainers.WithLogConsumerConfig(&testcontainers.LogConsumerConfig{
			Consumers: []testcontainers.LogConsumer{&testLogConsumer{t: t, name: cfg.Name}},
		}),
	}

	if strings.TrimSpace(cfg.IPv4Address) != "" {
		options = append(options, testcontainers.WithEndpointSettingsModifier(func(settings map[string]*dockernetwork.EndpointSettings) {
			endpoint := settings[e.network.Name]
			if endpoint == nil {
				endpoint = &dockernetwork.EndpointSettings{}
				settings[e.network.Name] = endpoint
			}
			if endpoint.IPAMConfig == nil {
				endpoint.IPAMConfig = &dockernetwork.EndpointIPAMConfig{}
			}
			endpoint.IPAMConfig.IPv4Address = cfg.IPv4Address
		}))
	}

	if len(cfg.CapAdd) > 0 {
		capAdd := append([]string(nil), cfg.CapAdd...)
		options = append(options, testcontainers.WithHostConfigModifier(func(hostConfig *dockercontainer.HostConfig) {
			hostConfig.CapAdd = append(hostConfig.CapAdd, capAdd...)
		}))
	}

	if len(cfg.ExposedPorts) > 0 {
		options = append(options, testcontainers.WithExposedPorts(cfg.ExposedPorts...))
	}

	if cfg.WaitStrategy != nil {
		options = append(options, testcontainers.WithWaitStrategy(cfg.WaitStrategy))
	}

	if len(cfg.Entrypoint) > 0 {
		options = append(options, testcontainers.WithEntrypoint(cfg.Entrypoint...))
	}

	if len(cfg.Cmd) > 0 {
		options = append(options, testcontainers.WithCmd(cfg.Cmd...))
	}

	runCtx, cancel := context.WithTimeout(e.ctx, 2*dockerOperationTimeout)
	defer cancel()

	container, err := testcontainers.Run(runCtx, cfg.Image, options...)
	if err != nil {
		t.Fatalf("start service %q: %v", cfg.Name, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), dockerOperationTimeout)
		defer cleanupCancel()

		if err := testcontainers.TerminateContainer(container, testcontainers.StopContext(cleanupCtx)); err != nil {
			t.Logf("terminate service %q: %v", cfg.Name, err)
		}
	})

	return &Service{
		ctx:       e.ctx,
		name:      cfg.Name,
		container: container,
	}
}

// Name returns the logical service name used by the test scenario.
func (s *Service) Name() string {
	return s.name
}

// Host returns the host-reachable address used for direct connections from the Go test process.
func (s *Service) Host(t *testing.T) string {
	t.Helper()

	host, err := s.container.Host(s.ctx)
	if err != nil {
		t.Fatalf("load host for service %q: %v", s.name, err)
	}

	return host
}

// Port returns the host-reachable TCP port for the given container port.
func (s *Service) Port(t *testing.T, port string) int {
	t.Helper()

	return s.mappedPort(t, port).Int()
}

// Address returns the host-reachable host:port pair for the given container port.
func (s *Service) Address(t *testing.T, port string) string {
	t.Helper()

	return net.JoinHostPort(s.Host(t), s.mappedPort(t, port).Port())
}

// Exec executes a command in the service and returns its exit status and output.
func (s *Service) Exec(t *testing.T, cmd ...string) ExecResult {
	t.Helper()

	exitCode, reader, err := s.container.Exec(s.ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec %q in service %q: %v", strings.Join(cmd, " "), s.name, err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read exec output for %q in service %q: %v", strings.Join(cmd, " "), s.name, err)
	}

	return ExecResult{
		ExitCode: exitCode,
		Output:   string(output),
	}
}

// RequireExec executes a command in the service and fails the test if execution fails.
func (s *Service) RequireExec(t *testing.T, cmd ...string) string {
	t.Helper()

	result := s.Exec(t, cmd...)
	if result.ExitCode != 0 {
		t.Fatalf("exec %q in service %q returned %d: %s", strings.Join(cmd, " "), s.name, result.ExitCode, result.Output)
	}

	return result.Output
}

// Logs returns the current container log stream collected for the service.
func (s *Service) Logs(t *testing.T) string {
	t.Helper()

	return readContainerLogs(t, s.ctx, s.container, s.name)
}

// Stop stops the service container.
func (s *Service) Stop(t *testing.T) {
	t.Helper()

	stopCtx, cancel := context.WithTimeout(context.Background(), dockerOperationTimeout)
	defer cancel()

	timeout := dockerOperationTimeout
	if err := s.container.Stop(stopCtx, &timeout); err != nil {
		t.Fatalf("stop service %q: %v", s.name, err)
	}
}

func (s *Service) mappedPort(t *testing.T, port string) nat.Port {
	t.Helper()

	natPort, err := nat.NewPort("tcp", port)
	if err != nil {
		t.Fatalf("construct service port %q for %q: %v", port, s.name, err)
	}

	mapped, err := s.container.MappedPort(s.ctx, natPort)
	if err != nil {
		t.Fatalf("load mapped port %q for service %q: %v", port, s.name, err)
	}

	return mapped
}
