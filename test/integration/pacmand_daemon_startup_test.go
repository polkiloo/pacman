//go:build integration

package integration_test

import (
	"crypto/sha1"
	"encoding/hex"
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
		configFile   string
		configMode   os.FileMode
		runAsDaemon  bool
		withPostgres bool
		wantExitCode int
		extraEnv     map[string]string
		prepareFiles func(*testing.T) []testcontainers.ContainerFile
		wantContains []string
	}{
		{
			name:         "positive data node starts heartbeat with unavailable postgres",
			configFile:   "positive-data-node-unavailable-postgres.yaml",
			runAsDaemon:  true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"loaded node configuration"`,
				`"msg":"started local agent daemon"`,
				`"node":"alpha-1"`,
				`"msg":"observed PostgreSQL unavailability"`,
				`"msg":"published local state to control plane"`,
				`"cluster_reachable":true`,
				`"postgres_up":false`,
			},
		},
		{
			name:         "positive data node reports reachable postgres",
			configFile:   "positive-data-node-reachable-postgres.yaml",
			runAsDaemon:  true,
			withPostgres: true,
			prepareFiles: func(t *testing.T) []testcontainers.ContainerFile {
				return writeIntegrationTLSFixture(t).containerFiles("/etc/pacman/tls/server.crt", "/etc/pacman/tls/server.key")
			},
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"alpha-2"`,
				`"node_role":"data"`,
				`"api_tls_enabled":true`,
				`"api_address":"0.0.0.0:8081"`,
				`"control_address":"0.0.0.0:9091"`,
				`"msg":"observed PostgreSQL availability"`,
				`"msg":"published local state to control plane"`,
				`"cluster_reachable":true`,
				`"postgres_up":true`,
				`"member_role":"primary"`,
				`"in_recovery":false`,
				`"system_identifier":"`,
				`"timeline":1`,
				`"write_lsn":"`,
				`"flush_lsn":"`,
				`"postmaster_start_time":"`,
				`"replication_lag_bytes":0`,
			},
		},
		{
			name:         "positive witness node without postgres starts",
			configFile:   "positive-witness-without-postgres.yaml",
			runAsDaemon:  true,
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"witness-1"`,
				`"node_role":"witness"`,
				`"manages_postgres":false`,
				`"msg":"observed heartbeat without local PostgreSQL"`,
				`"msg":"published local state to control plane"`,
				`"postgres_managed":false`,
			},
		},
		{
			name:        "positive witness node with tls starts",
			configFile:  "positive-witness-with-tls.yaml",
			runAsDaemon: true,
			prepareFiles: func(t *testing.T) []testcontainers.ContainerFile {
				return writeIntegrationTLSFixture(t).containerFiles("/etc/pacman/tls/witness.crt", "/etc/pacman/tls/witness.key")
			},
			wantExitCode: 0,
			wantContains: []string{
				`"msg":"started local agent daemon"`,
				`"node":"witness-2"`,
				`"api_tls_enabled":true`,
				`"api_address":"0.0.0.0:8181"`,
				`"control_address":"0.0.0.0:9191"`,
				`"msg":"observed heartbeat without local PostgreSQL"`,
			},
		},
		{
			name:         "positive data node with defaults and safe params starts",
			configFile:   "positive-data-node-defaults-safe-params.yaml",
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
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`open config file`,
			},
		},
		{
			name:         "negative data node without postgres section fails",
			configFile:   "negative-data-node-without-postgres.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`construct local agent daemon`,
				`agent postgres config is required for data nodes`,
			},
		},
		{
			name:         "negative tls config missing key fails",
			configFile:   "negative-tls-missing-key.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config tls keyFile is required`,
			},
		},
		{
			name:         "negative unsafe postgres override fails",
			configFile:   "negative-unsafe-postgres-override.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config postgres parameters contain unsafe local override`,
			},
		},
		{
			name:         "negative unsupported api version fails",
			configFile:   "negative-unsupported-api-version.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config apiVersion is unsupported`,
			},
		},
		{
			name:         "negative unexpected kind fails",
			configFile:   "negative-unexpected-kind.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config kind is invalid`,
			},
		},
		{
			name:         "negative missing node name fails",
			configFile:   "negative-missing-node-name.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config node name is required`,
			},
		},
		{
			name:         "negative invalid node role fails",
			configFile:   "negative-invalid-node-role.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config node role is invalid`,
			},
		},
		{
			name:         "negative invalid api address fails",
			configFile:   "negative-invalid-api-address.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config node apiAddress is invalid`,
			},
		},
		{
			name:         "negative postgres port out of range fails",
			configFile:   "negative-postgres-port-out-of-range.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config postgres port must be between 1 and 65535`,
			},
		},
		{
			name:         "negative security section without token fails",
			configFile:   "negative-security-without-token.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config security adminBearerToken or adminBearerTokenFile is required`,
			},
		},
		{
			name:         "negative malformed yaml fails",
			configFile:   "negative-malformed-yaml.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`decode config document`,
			},
		},
		{
			name:         "negative unknown config field fails",
			configFile:   "negative-unknown-config-field.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`field unknownField not found`,
			},
		},
		{
			name:         "negative dcs bootstrap cluster mismatch fails",
			configFile:   "negative-dcs-bootstrap-cluster-mismatch.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config dcs clusterName must match bootstrap clusterName`,
			},
		},
		{
			name:         "negative bootstrap seed address invalid fails",
			configFile:   "negative-bootstrap-seed-address-invalid.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate config document`,
				`config bootstrap seedAddresses contain an invalid address`,
			},
		},
		{
			name:         "negative permissive inline admin token fails",
			configFile:   "negative-permissive-inline-admin-token.yaml",
			configMode:   0o644,
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`validate sensitive config file`,
				`config file containing inline secrets must not be readable by group or others`,
			},
		},
		{
			name:         "negative patroni multiple dcs backends fails",
			configFile:   "negative-patroni-multiple-dcs-backends.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`Patroni config declares multiple DCS backends`,
			},
		},
		{
			name:         "negative patroni invalid etcd hosts type fails",
			configFile:   "negative-patroni-invalid-etcd-hosts-type.yaml",
			wantExitCode: 1,
			wantContains: []string{
				`"msg":"app run failed"`,
				`decode Patroni hosts`,
				`expected string or list`,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			configBody := readDaemonStartupConfig(t, testCase.configFile)
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

			var extraFiles []testcontainers.ContainerFile
			if testCase.prepareFiles != nil {
				extraFiles = testCase.prepareFiles(t)
			}

			runner := startDaemonRunnerWithConfigMode(
				t,
				env,
				testCase.name,
				configBody,
				testCase.configMode,
				extraFiles,
				testCase.extraEnv,
			)

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

const (
	daemonConfigPath             = "/tmp/pacmand.yaml"
	daemonStartupConfigDirectory = "testdata/pacmand_daemon_startup"
)

func readDaemonStartupConfig(t *testing.T, name string) string {
	t.Helper()

	if strings.TrimSpace(name) == "" {
		return ""
	}

	path := filepath.Join(daemonStartupConfigDirectory, name)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read daemon startup config fixture %q: %v", path, err)
	}

	return string(body)
}

func startDaemonRunner(t *testing.T, env *testenv.Environment, name, configBody string, extraFiles []testcontainers.ContainerFile, extraEnv map[string]string) *testenv.Runner {
	t.Helper()

	return startDaemonRunnerWithConfigMode(t, env, name, configBody, 0o600, extraFiles, extraEnv)
}

func startDaemonRunnerWithConfigMode(t *testing.T, env *testenv.Environment, name, configBody string, configMode os.FileMode, extraFiles []testcontainers.ContainerFile, extraEnv map[string]string) *testenv.Runner {
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
		cfg.Files = append(cfg.Files, writeDaemonConfigFileWithMode(t, configBody, configMode))
	}
	cfg.Files = append(cfg.Files, extraFiles...)

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

	return writeDaemonConfigFileWithMode(t, body, 0o600)
}

func writeDaemonConfigFileWithMode(t *testing.T, body string, mode os.FileMode) testcontainers.ContainerFile {
	t.Helper()

	if mode == 0 {
		mode = 0o600
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "pacmand.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), mode); err != nil {
		t.Fatalf("write daemon config: %v", err)
	}

	return testcontainers.ContainerFile{
		HostFilePath:      path,
		ContainerFilePath: daemonConfigPath,
		FileMode:          int64(mode),
	}
}

func sanitizeIntegrationName(value string) string {
	replacer := strings.NewReplacer("/", "-", "_", "-", " ", "-", ":", "-")
	sanitized := strings.Trim(strings.ToLower(replacer.Replace(value)), "-")
	if sanitized == "" {
		return "integration"
	}

	const maxLen = 40
	if len(sanitized) <= maxLen {
		return sanitized
	}

	sum := sha1.Sum([]byte(sanitized))
	suffix := hex.EncodeToString(sum[:4])
	prefixLen := maxLen - len(suffix) - 1
	if prefixLen < 1 {
		return suffix
	}

	return sanitized[:prefixLen] + "-" + suffix
}
