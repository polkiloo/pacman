//go:build integration

package integration_test

import (
	"strings"
	"testing"

	"github.com/polkiloo/pacman/test/testenv"
)

func TestPACMANClusterEnvironment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed integration test in short mode")
	}

	env := testenv.New(t)

	nodes := []*testenv.Node{
		env.StartNode(t, "pacmand-1"),
		env.StartNode(t, "pacmand-2"),
		env.StartNode(t, "pacmand-3"),
	}

	cli := env.StartPacmanctl(t, "pacmanctl-1")

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

	statusOutput := cli.RequireExec(t, "pacmanctl", "cluster", "status")
	if !strings.Contains(statusOutput, "pacmanctl scaffold: command support is not implemented yet (cluster status)") {
		t.Fatalf("unexpected pacmanctl scaffold output: %q", statusOutput)
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
