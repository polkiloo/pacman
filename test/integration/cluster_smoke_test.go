//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/test/testenv"
)

func TestPACMANClusterEnvironment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	nodes := []*testenv.Node{
		env.StartNode(t, "pacmand-1"),
		env.StartNode(t, "pacmand-2"),
		env.StartNode(t, "pacmand-3"),
	}

	cli := env.StartPacmanctl(t, "pacmanctl-1")
	daemonNodeName := "alpha-cli"
	daemonAlias := "pacmand-cli-api"
	daemonConfig := fmt.Sprintf(daemonNodeConfig, daemonNodeName, nodes[0].Postgres.Alias(), daemonNodeName, daemonNodeName)

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         "pacmand-cli-service",
		Image:        pacmanTestImage(),
		Aliases:      []string{daemonAlias},
		Env:          postgresConnectionEnv(nodes[0].Postgres),
		Files:        []testcontainers.ContainerFile{writeDaemonConfigFile(t, daemonConfig)},
		ExposedPorts: []string{"8080/tcp"},
		Cmd: []string{
			"/bin/sh",
			"-lc",
			fmt.Sprintf("pacmand -config %s", daemonConfigPath),
		},
		WaitStrategy: wait.ForListeningPort("8080/tcp").WithStartupTimeout(pacmandStartupTimeout),
	})
	daemonBase := "http://" + service.Address(t, "8080")
	daemonClient := &http.Client{Timeout: 2 * time.Second}
	waitForProbeStatus(t, daemonClient, daemonBase+"/health", http.StatusOK, pacmandStartupTimeout)
	waitForProbeStatus(t, daemonClient, daemonBase+"/api/v1/members", http.StatusOK, pacmandStartupTimeout)

	for _, node := range nodes {
		version := node.Pacmand.RequireExec(t, "pacmand", "-version")
		if !strings.Contains(version, "commit=") {
			t.Fatalf("expected version output from %q, got %q", node.Name, version)
		}

		pacmandAliases := node.Pacmand.NetworkAliases(t)
		if !contains(pacmandAliases[env.NetworkName()], node.Name) {
			t.Fatalf("expected pacmand alias %q, got %v", node.Name, pacmandAliases[env.NetworkName()])
		}

		if !contains(node.Pacmand.Networks(t), env.NetworkName()) {
			t.Fatalf("expected pacmand runner %q to be attached to network %q", node.Name, env.NetworkName())
		}

		postgresAliases := node.Postgres.NetworkAliases(t)
		if !contains(postgresAliases[env.NetworkName()], node.Postgres.Alias()) {
			t.Fatalf("expected postgres alias %q, got %v", node.Postgres.Alias(), postgresAliases[env.NetworkName()])
		}

		if !contains(node.Postgres.Networks(t), env.NetworkName()) {
			t.Fatalf("expected postgres fixture %q to be attached to network %q", node.Postgres.Name(), env.NetworkName())
		}

		readiness := node.Pacmand.RequireExec(
			t,
			"/bin/sh",
			"-lc",
			"PGPASSWORD=$PACMAN_TEST_POSTGRES_PASSWORD pg_isready -h \"$PACMAN_TEST_POSTGRES_HOST\" -p \"$PACMAN_TEST_POSTGRES_PORT\" -U \"$PACMAN_TEST_POSTGRES_USERNAME\" -d \"$PACMAN_TEST_POSTGRES_DATABASE\"",
		)
		if !strings.Contains(readiness, "accepting connections") {
			t.Fatalf("expected pg_isready success for %q, got %q", node.Name, readiness)
		}

		serverVersion := node.Pacmand.RequireExec(
			t,
			"/bin/sh",
			"-lc",
			"PGPASSWORD=$PACMAN_TEST_POSTGRES_PASSWORD psql -h \"$PACMAN_TEST_POSTGRES_HOST\" -p \"$PACMAN_TEST_POSTGRES_PORT\" -U \"$PACMAN_TEST_POSTGRES_USERNAME\" -d \"$PACMAN_TEST_POSTGRES_DATABASE\" -tAc 'show server_version'",
		)
		if !strings.HasPrefix(strings.TrimSpace(serverVersion), "17.") {
			t.Fatalf("expected PostgreSQL 17 for %q, got %q", node.Name, serverVersion)
		}
	}

	cliVersion := cli.RequireExec(t, "pacmanctl", "-version")
	if !strings.Contains(cliVersion, "commit=") {
		t.Fatalf("expected version output from pacmanctl runner, got %q", cliVersion)
	}

	statusOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl cluster status", daemonAlias))
	if !strings.Contains(statusOutput, "Cluster Name:") || !strings.Contains(statusOutput, "alpha") || !strings.Contains(statusOutput, daemonNodeName) {
		t.Fatalf("unexpected pacmanctl cluster status output: %q", statusOutput)
	}

	specOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl cluster spec show", daemonAlias))
	if !strings.Contains(specOutput, "Cluster Name:") || !strings.Contains(specOutput, "alpha") || !strings.Contains(specOutput, daemonNodeName) {
		t.Fatalf("unexpected pacmanctl cluster spec output: %q", specOutput)
	}

	membersOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl members list", daemonAlias))
	if !strings.Contains(membersOutput, "NAME") || !strings.Contains(membersOutput, daemonNodeName) || !strings.Contains(membersOutput, "primary") {
		t.Fatalf("unexpected pacmanctl members list output: %q", membersOutput)
	}

	historyOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl history list", daemonAlias))
	if !strings.Contains(historyOutput, "No history.") {
		t.Fatalf("unexpected pacmanctl history output: %q", historyOutput)
	}

	patroniHistoryOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl history", daemonAlias))
	if !strings.Contains(patroniHistoryOutput, "No history.") {
		t.Fatalf("unexpected patronictl-compatible history output: %q", patroniHistoryOutput)
	}

	nodeOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl node status %s", daemonAlias, daemonNodeName))
	if !strings.Contains(nodeOutput, "Node Name:") || !strings.Contains(nodeOutput, daemonNodeName) || !strings.Contains(nodeOutput, "Cluster Reachable:") {
		t.Fatalf("unexpected pacmanctl node status output: %q", nodeOutput)
	}

	diagnosticsOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl diagnostics show", daemonAlias))
	if !strings.Contains(diagnosticsOutput, "Cluster Name:") || !strings.Contains(diagnosticsOutput, "alpha") || !strings.Contains(diagnosticsOutput, "Members:") {
		t.Fatalf("unexpected pacmanctl diagnostics output: %q", diagnosticsOutput)
	}

	listOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl list", daemonAlias))
	if !strings.Contains(listOutput, "Cluster") || !strings.Contains(listOutput, daemonNodeName) || !strings.Contains(listOutput, "primary") {
		t.Fatalf("unexpected patronictl-compatible list output: %q", listOutput)
	}

	topologyOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl topology", daemonAlias))
	if !strings.Contains(topologyOutput, "Cluster") || !strings.Contains(topologyOutput, daemonNodeName) {
		t.Fatalf("unexpected patronictl-compatible topology output: %q", topologyOutput)
	}

	listJSONOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl list -f json 2>/dev/null", daemonAlias))
	var listJSON struct {
		Cluster string `json:"cluster"`
		Members []struct {
			Member string `json:"member"`
			Role   string `json:"role"`
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(listJSONOutput), &listJSON); err != nil {
		t.Fatalf("decode patronictl-compatible list json: %v\noutput=%q", err, listJSONOutput)
	}
	foundDaemonMember := false
	for _, member := range listJSON.Members {
		if member.Member == daemonNodeName && member.Role == "primary" {
			foundDaemonMember = true
			break
		}
	}
	if listJSON.Cluster != "alpha" || !foundDaemonMember {
		t.Fatalf("unexpected patronictl-compatible list json payload: %+v", listJSON)
	}

	showConfigOutput := cli.RequireExec(t, "/bin/sh", "-lc", fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl show-config", daemonAlias))
	if !strings.Contains(showConfigOutput, "pause: false") || !strings.Contains(showConfigOutput, "cluster_name: alpha") {
		t.Fatalf("unexpected patronictl-compatible show-config output: %q", showConfigOutput)
	}

	positiveCases := []struct {
		name         string
		command      string
		wantContains []string
	}{
		{
			name:    "positive cluster status json presentation",
			command: "pacmanctl cluster status -o json 2>/dev/null",
			wantContains: []string{
				`"clusterName": "alpha"`,
				daemonNodeName,
			},
		},
		{
			name:    "positive cluster spec json presentation",
			command: "pacmanctl cluster spec show -o json 2>/dev/null",
			wantContains: []string{
				`"clusterName": "alpha"`,
				daemonNodeName,
			},
		},
		{
			name:    "positive members yaml presentation",
			command: "pacmanctl members list -o yaml 2>/dev/null",
			wantContains: []string{
				daemonNodeName,
				"primary",
			},
		},
		{
			name:    "positive node status json presentation",
			command: "pacmanctl node status -node " + daemonNodeName + " -o json 2>/dev/null",
			wantContains: []string{
				`"nodeName": "` + daemonNodeName + `"`,
				`"role": "primary"`,
			},
		},
		{
			name:    "positive diagnostics json presentation",
			command: "pacmanctl diagnostics show -o json 2>/dev/null",
			wantContains: []string{
				`"clusterName": "alpha"`,
				daemonNodeName,
			},
		},
		{
			name:    "positive patronictl list extended timestamp tsv",
			command: "pacmanctl list alpha --extended --timestamp -f tsv 2>/dev/null",
			wantContains: []string{
				"Last Seen",
				"Needs Rejoin",
				daemonNodeName,
			},
		},
		{
			name:    "positive patronictl show-config scoped json",
			command: "pacmanctl show-config alpha -f json 2>/dev/null",
			wantContains: []string{
				`"pause": false`,
				`"cluster_name": "alpha"`,
			},
		},
	}
	for _, testCase := range positiveCases {
		t.Run(testCase.name, func(t *testing.T) {
			assertPacmanctlPositive(t, cli, daemonAlias, testCase.command, testCase.wantContains...)
		})
	}

	negativeCases := []struct {
		name         string
		command      string
		wantContains []string
	}{
		{
			name:    "negative unsupported members output format",
			command: "pacmanctl members list -o xml",
			wantContains: []string{
				"unsupported output format",
				"xml",
			},
		},
		{
			name:    "negative node status requires node name",
			command: "pacmanctl node status",
			wantContains: []string{
				"node name is required",
			},
		},
		{
			name:    "negative patronictl list rejects wrong scope",
			command: "pacmanctl list wrong-scope",
			wantContains: []string{
				"cluster name mismatch",
				"wrong-scope",
				"alpha",
			},
		},
		{
			name:    "negative patronictl show-config rejects wrong scope",
			command: "pacmanctl show-config wrong-scope",
			wantContains: []string{
				"cluster name mismatch",
				"wrong-scope",
				"alpha",
			},
		},
		{
			name:    "negative patronictl pause rejects wrong scope",
			command: "pacmanctl pause wrong-scope",
			wantContains: []string{
				"cluster name mismatch",
				"wrong-scope",
				"alpha",
			},
		},
		{
			name:    "negative patronictl switchover requires candidate",
			command: "pacmanctl switchover",
			wantContains: []string{
				"--candidate is required",
			},
		},
		{
			name:    "negative patronictl switchover rejects wrong leader",
			command: "pacmanctl switchover --leader wrong-leader --candidate alpha-2",
			wantContains: []string{
				"leader mismatch",
				"wrong-leader",
				daemonNodeName,
			},
		},
	}
	for _, testCase := range negativeCases {
		t.Run(testCase.name, func(t *testing.T) {
			assertPacmanctlNegative(t, cli, daemonAlias, testCase.command, testCase.wantContains...)
		})
	}

	// Ensure maintenance is always disabled on exit, even if assertions below fail.
	t.Cleanup(func() {
		cli.Exec(t, "/bin/sh", "-lc",
			fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl cluster maintenance disable", daemonAlias))
	})

	maintenanceEnableOutput := cli.RequireExec(
		t,
		"/bin/sh",
		"-lc",
		fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl cluster maintenance enable -reason cli-smoke", daemonAlias),
	)
	if !strings.Contains(maintenanceEnableOutput, "Enabled:") || !strings.Contains(maintenanceEnableOutput, "true") || !strings.Contains(maintenanceEnableOutput, "cli-smoke") {
		t.Fatalf("unexpected pacmanctl maintenance enable output: %q", maintenanceEnableOutput)
	}

	maintenanceDisableOutput := cli.RequireExec(
		t,
		"/bin/sh",
		"-lc",
		fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl cluster maintenance disable -reason cli-smoke-complete", daemonAlias),
	)
	if !strings.Contains(maintenanceDisableOutput, "Enabled:") || !strings.Contains(maintenanceDisableOutput, "false") {
		t.Fatalf("unexpected pacmanctl maintenance disable output: %q", maintenanceDisableOutput)
	}

	pauseOutput := cli.RequireExec(
		t,
		"/bin/sh",
		"-lc",
		fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl pause --reason patroni-cli-smoke", daemonAlias),
	)
	if !strings.Contains(pauseOutput, "Enabled:") || !strings.Contains(pauseOutput, "true") || !strings.Contains(pauseOutput, "patroni-cli-smoke") {
		t.Fatalf("unexpected patronictl-compatible pause output: %q", pauseOutput)
	}

	resumeOutput := cli.RequireExec(
		t,
		"/bin/sh",
		"-lc",
		fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 pacmanctl resume --reason patroni-cli-smoke-complete", daemonAlias),
	)
	if !strings.Contains(resumeOutput, "Enabled:") || !strings.Contains(resumeOutput, "false") {
		t.Fatalf("unexpected patronictl-compatible resume output: %q", resumeOutput)
	}

	if !contains(cli.Networks(t), env.NetworkName()) {
		t.Fatalf("expected pacmanctl runner to be attached to network %q", env.NetworkName())
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}

	return false
}

func assertPacmanctlNegative(t *testing.T, cli *testenv.Runner, daemonAlias, command string, wantContains ...string) {
	t.Helper()

	result := cli.Exec(
		t,
		"/bin/sh",
		"-lc",
		fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 %s", daemonAlias, command),
	)
	if result.ExitCode == 0 {
		t.Fatalf("expected %q to fail, output=%q", command, result.Output)
	}

	for _, want := range wantContains {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected failed command %q output %q to contain %q", command, result.Output, want)
		}
	}
}

func assertPacmanctlPositive(t *testing.T, cli *testenv.Runner, daemonAlias, command string, wantContains ...string) {
	t.Helper()

	result := cli.Exec(
		t,
		"/bin/sh",
		"-lc",
		fmt.Sprintf("PACMANCTL_API_URL=http://%s:8080 %s", daemonAlias, command),
	)
	if result.ExitCode != 0 {
		t.Fatalf("expected %q to succeed, exit=%d output=%q", command, result.ExitCode, result.Output)
	}

	for _, want := range wantContains {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected command %q output %q to contain %q", command, result.Output, want)
		}
	}
}
