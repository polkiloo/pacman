//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	testcontainers "github.com/testcontainers/testcontainers-go"

	"github.com/polkiloo/pacman/test/testenv"
)

func TestPacmandDaemonStartupMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)

	testCases := []struct {
		name         string
		args         []string
		configBody   string
		wantExitCode int
		wantContains []string
	}{
		{
			name: "positive data node minimal config starts",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
`,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"loaded node configuration"`,
				`"msg":"started local agent daemon"`,
				`"node":"alpha-1"`,
				`"manages_postgres":true`,
			},
		},
		{
			name: "positive data node with explicit sections starts",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-2
  role: data
  apiAddress: 10.0.0.12:8081
  controlAddress: 10.0.0.12:9091
tls:
  enabled: true
  certFile: /etc/pacman/tls/server.crt
  keyFile: /etc/pacman/tls/server.key
  serverName: pacmand.internal
postgres:
  dataDir: /srv/postgres
  binDir: /usr/lib/postgresql/17/bin
  listenAddress: 127.0.0.1
  port: 5433
  parameters:
    max_connections: "200"
bootstrap:
  clusterName: alpha
  initialPrimary: alpha-2
  seedAddresses:
    - 10.0.0.12:9091
  expectedMembers:
    - alpha-2
    - alpha-3
`,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"alpha-2"`,
				`"role":"data"`,
				`"api_address":"10.0.0.12:8081"`,
				`"control_address":"10.0.0.12:9091"`,
			},
		},
		{
			name: "positive witness node without postgres starts",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: witness-1
  role: witness
`,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"witness-1"`,
				`"role":"witness"`,
				`"manages_postgres":false`,
			},
		},
		{
			name: "positive witness node with tls starts",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: witness-2
  role: witness
  apiAddress: 0.0.0.0:8181
  controlAddress: 0.0.0.0:9191
tls:
  enabled: true
  certFile: /etc/pacman/tls/witness.crt
  keyFile: /etc/pacman/tls/witness.key
`,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"witness-2"`,
				`"api_address":"0.0.0.0:8181"`,
				`"control_address":"0.0.0.0:9191"`,
			},
		},
		{
			name: "positive data node with defaults and safe params starts",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-3
  role: data
postgres:
  dataDir: /data/postgres
  parameters:
    max_connections: "150"
    shared_buffers: 256MB
`,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"alpha-3"`,
				`"api_address":"0.0.0.0:8080"`,
				`"control_address":"0.0.0.0:9090"`,
			},
		},
		{
			name:         "negative missing config path fails",
			args:         []string{"pacmand"},
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`pacmand config path is required`,
			},
		},
		{
			name:         "negative missing config file fails",
			args:         []string{"pacmand", "-config", daemonConfigPath},
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`open config file`,
			},
		},
		{
			name: "negative data node without postgres section fails",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: broken-data
  role: data
`,
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`construct local agent daemon`,
				`agent postgres config is required for data nodes`,
			},
		},
		{
			name: "negative tls config missing key fails",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: broken-tls
  role: witness
tls:
  enabled: true
  certFile: /etc/pacman/tls/server.crt
`,
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config tls keyFile is required`,
			},
		},
		{
			name: "negative unsafe postgres override fails",
			args: []string{"pacmand", "-config", daemonConfigPath},
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: broken-override
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
  parameters:
    primary_conninfo: host=alpha-1
`,
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config postgres parameters contain unsafe local override`,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			runner := startDaemonRunner(t, env, testCase.name, testCase.configBody)

			result := runner.Exec(t, testCase.args...)
			if result.ExitCode != testCase.wantExitCode {
				t.Fatalf("unexpected exit code: got %d, want %d, output=%q", result.ExitCode, testCase.wantExitCode, result.Output)
			}

			for _, want := range testCase.wantContains {
				if !strings.Contains(result.Output, want) {
					t.Fatalf("expected output %q to contain %q", result.Output, want)
				}
			}
		})
	}
}

const daemonConfigPath = "/tmp/pacmand.yaml"

func startDaemonRunner(t *testing.T, env *testenv.Environment, name, configBody string) *testenv.Runner {
	t.Helper()

	cfg := testenv.RunnerConfig{
		Name:    name,
		Aliases: []string{sanitizeIntegrationName(name)},
		Env: map[string]string{
			"PACMAN_TEST_ROLE": "pacmand",
		},
	}

	if strings.TrimSpace(configBody) != "" {
		cfg.Files = []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)}
	}

	return env.StartRunner(t, cfg)
}

func writeDaemonConfigFile(t *testing.T, body string) testcontainers.ContainerFile {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatalf("write daemon config: %v", err)
	}

	return testcontainers.ContainerFile{
		HostFilePath:      path,
		ContainerFilePath: daemonConfigPath,
		FileMode:          0o600,
	}
}

func sanitizeIntegrationName(value string) string {
	replacer := strings.NewReplacer("/", "-", "_", "-", " ", "-", ":", "-")
	return strings.ToLower(replacer.Replace(value))
}
