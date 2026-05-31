package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestResolveJepsenTarget(t *testing.T) {
	t.Parallel()

	pacman, err := resolveJepsenTarget("")
	if err != nil {
		t.Fatalf("resolve default target: %v", err)
	}
	if pacman.Name != "pacman-3-data" || pacman.StoreName != "pacman" || len(pacman.DataNodes) != 3 {
		t.Fatalf("default target: %#v", pacman)
	}
	if !pacman.supportsPACMANLab() {
		t.Fatalf("pacman target should support deploy/lab")
	}

	patroni, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve patroni target: %v", err)
	}
	if patroni.StoreName != "patroni" || len(patroni.DataNodes) != 3 || len(patroni.DCSNodes) != 3 {
		t.Fatalf("patroni target: %#v", patroni)
	}
	if patroni.supportsPACMANLab() {
		t.Fatalf("patroni target should not use deploy/lab bootstrap")
	}
	if !patroni.supportsPatroniLab() {
		t.Fatalf("patroni target should support Patroni lab bootstrap")
	}
	if patroni.ComposeFile != "deploy/patroni-lab/compose.yml" || patroni.PSQLBinary != "/usr/bin/psql" {
		t.Fatalf("patroni runtime config: %#v", patroni)
	}
	if got := patroni.serviceForMember("patroni-2"); got != "patroni-replica" {
		t.Fatalf("patroni service: got %q", got)
	}
	if got := patroni.memberForService("patroni-replica-2"); got != "patroni-3" {
		t.Fatalf("patroni member: got %q", got)
	}
	if !patroni.supportsCase("append-smoke", "none") {
		t.Fatalf("patroni target should support append-smoke:none")
	}
	if patroni.supportsCase("append-failover", "kill") {
		t.Fatalf("patroni target should not support append-failover:kill yet")
	}

	if _, err := resolveJepsenTarget("unknown"); err == nil || !strings.Contains(err.Error(), "supported targets") {
		t.Fatalf("unknown target error: %v", err)
	}
}

func TestTargetsListCommandIncludesPatroniBaseline(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	status := Run([]string{"targets", "list"}, &stdout, &bytes.Buffer{})
	if status != 0 {
		t.Fatalf("status: got %d want 0", status)
	}
	assertContainsAll(t, "targets list", stdout.String(), []string{
		"pacman-3-data pacman",
		"patroni-3-data patroni",
		"patroni-1@patroni-primary,patroni-2@patroni-replica,patroni-3@patroni-replica-2",
	})
}

func TestRunOptionsSelectTargetFromEnvironment(t *testing.T) {
	t.Setenv("PACMAN_JEPSEN_TARGET", "patroni-3-data")

	options, err := parseRunOptions([]string{"smoke"})
	if err != nil {
		t.Fatalf("parse run options: %v", err)
	}
	if options.target.Name != "patroni-3-data" || options.target.StoreName != "patroni" {
		t.Fatalf("target: %#v", options.target)
	}
}

func TestDockerCampaignEnvPassesSelectedTarget(t *testing.T) {
	t.Setenv("PACMAN_JEPSEN_TARGET", "patroni-3-data")

	env := dockerCampaignEnv("/repo", "smoke", "")
	if env["PACMAN_JEPSEN_TARGET"] != "patroni-3-data" {
		t.Fatalf("docker env target: got %q", env["PACMAN_JEPSEN_TARGET"])
	}

	args := strings.Join(dockerEnvArgs(env), " ")
	if !strings.Contains(args, "PACMAN_JEPSEN_TARGET=patroni-3-data") {
		t.Fatalf("docker env args missing target: %s", args)
	}
}

func TestRunDirUsesTargetStoreNamespace(t *testing.T) {
	t.Setenv("PACMAN_JEPSEN_RUN_ID", "run-1")

	target, err := resolveJepsenTarget("patroni-3-data")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	got := runDirFor("/store", "smoke", target)
	if got != "/store/patroni/smoke/run-1" {
		t.Fatalf("run dir: got %q", got)
	}
}
