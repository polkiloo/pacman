//go:build integration

package integration_test

import (
	"fmt"
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
		configBody   string
		runAsDaemon  bool
		withPostgres bool
		wantExitCode int
		extraEnv     map[string]string
		wantContains []string
	}{
		{
			name: "positive data node starts heartbeat with unavailable postgres",
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
`,
			runAsDaemon:  true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"loaded node configuration"`,
				`"msg":"started local agent daemon"`,
				`"node":"alpha-1"`,
				`"msg":"observed PostgreSQL unavailability"`,
				`"postgres_up":false`,
			},
		},
		{
			name: "positive data node reports reachable postgres",
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
  listenAddress: {{postgres_host}}
  port: 5432
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
			runAsDaemon:  true,
			withPostgres: true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"alpha-2"`,
				`"role":"data"`,
				`"api_address":"10.0.0.12:8081"`,
				`"control_address":"10.0.0.12:9091"`,
				`"msg":"observed PostgreSQL availability"`,
				`"postgres_up":true`,
				`"member_role":"primary"`,
				`"in_recovery":false`,
			},
		},
		{
			name: "positive witness node without postgres starts",
			configBody: `
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: witness-1
  role: witness
`,
			runAsDaemon:  true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"witness-1"`,
				`"role":"witness"`,
				`"manages_postgres":false`,
				`"msg":"observed heartbeat without local PostgreSQL"`,
				`"postgres_managed":false`,
			},
		},
		{
			name: "positive witness node with tls starts",
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
			runAsDaemon:  true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"witness-2"`,
				`"api_address":"0.0.0.0:8181"`,
				`"control_address":"0.0.0.0:9191"`,
				`"msg":"observed heartbeat without local PostgreSQL"`,
			},
		},
		{
			name: "positive data node with defaults and safe params starts",
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
			runAsDaemon:  true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"alpha-3"`,
				`"msg":"observed PostgreSQL unavailability"`,
				`"postgres_address":"127.0.0.1:5432"`,
			},
		},
		{
			name:         "negative missing config path fails",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`pacmand config path is required`,
			},
		},
		{
			name:         "negative missing config file fails",
			configBody:   "",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`open config file`,
			},
		},
		{
			name: "negative data node without postgres section fails",
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
			configBody := testCase.configBody
			if testCase.withPostgres {
				postgresAlias := sanitizeIntegrationName(testCase.name) + "-postgres"
				postgresFixture := env.StartPostgres(t, testCase.name, postgresAlias)
				configBody = strings.ReplaceAll(configBody, "{{postgres_host}}", postgresFixture.Alias())
				testCase.extraEnv = map[string]string{
					"PGDATABASE": postgresFixture.Database(),
					"PGUSER":     postgresFixture.Username(),
					"PGPASSWORD": postgresFixture.Password(),
				}
			}

			runner := startDaemonRunner(t, env, testCase.name, configBody, testCase.extraEnv)

			var result testenv.ExecResult
			switch {
			case testCase.runAsDaemon:
				result = runPacmandUntilTerminated(t, runner)
			case strings.TrimSpace(configBody) == "":
				result = runner.Exec(t, "pacmand", "-config", daemonConfigPath)
			default:
				result = runner.Exec(t, "pacmand", "-config", daemonConfigPath)
			}

			if testCase.name == "negative missing config path fails" {
				result = runner.Exec(t, "pacmand")
			}

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

func startDaemonRunner(t *testing.T, env *testenv.Environment, name, configBody string, extraEnv map[string]string) *testenv.Runner {
	t.Helper()

	runnerEnv := map[string]string{
		"PACMAN_TEST_ROLE": "pacmand",
	}

	for key, value := range extraEnv {
		runnerEnv[key] = value
	}

	cfg := testenv.RunnerConfig{
		Name:    name,
		Aliases: []string{sanitizeIntegrationName(name)},
		Env:     runnerEnv,
	}

	if strings.TrimSpace(configBody) != "" {
		cfg.Files = []testcontainers.ContainerFile{writeDaemonConfigFile(t, configBody)}
	}

	return env.StartRunner(t, cfg)
}

func runPacmandUntilTerminated(t *testing.T, runner *testenv.Runner) testenv.ExecResult {
	t.Helper()

	script := fmt.Sprintf(
		"pacmand -config %s >/tmp/pacmand.log 2>&1 & pid=$!; sleep 2; kill -TERM $pid; wait $pid; status=$?; cat /tmp/pacmand.log; exit $status",
		daemonConfigPath,
	)

	return runner.Exec(t, "/bin/sh", "-lc", script)
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
