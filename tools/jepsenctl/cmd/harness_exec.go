package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

func (lab *harnessLab) compose(ctx context.Context, args ...string) (string, int, error) {
	fullArgs := append([]string{"compose", "-f", lab.cfg.composeFile}, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status, err := lab.options.runner.Run(ctx, commandSpec{
		name:   "docker",
		args:   fullArgs,
		dir:    lab.options.repoRoot,
		env:    lab.options.env,
		stdout: &stdout,
		stderr: &stderr,
	})
	output := stdout.String()
	if stderr.Len() > 0 {
		output += stderr.String()
	}
	return output, status, err
}

func (lab *harnessLab) composeExec(ctx context.Context, service string, args ...string) (string, int, error) {
	fullArgs := append([]string{"exec", "-T", service}, args...)
	return lab.compose(ctx, fullArgs...)
}

func (lab *harnessLab) composeExecInput(ctx context.Context, service, input string, args ...string) (string, int, error) {
	fullArgs := append([]string{"compose", "-f", lab.cfg.composeFile, "exec", "-T", service}, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", fullArgs...)
	cmd.Dir = lab.options.repoRoot
	cmd.Env = lab.options.env
	cmd.Stdin = strings.NewReader(input)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	status := 0
	if err != nil {
		status = 1
	}
	output := stdout.String()
	if stderr.Len() > 0 {
		output += stderr.String()
	}
	return output, status, err
}

func (lab *harnessLab) runHost(ctx context.Context, name string, args ...string) (int, error) {
	return lab.options.runner.Run(ctx, commandSpec{
		name:   name,
		args:   args,
		dir:    lab.options.repoRoot,
		env:    lab.options.env,
		stdout: lab.options.stdout,
		stderr: lab.options.stderr,
	})
}

func (lab *harnessLab) psqlVIP(ctx context.Context, sql string) (string, error) {
	if lab.options.target.supportsPatroniLab() {
		service := lab.serviceForMember(lab.currentPrimaryName(ctx))
		if service == "" {
			return "", fmt.Errorf("Patroni primary is unavailable")
		}
		return lab.psqlService(ctx, service, sql)
	}
	return lab.psql(ctx, lab.cfg.pgClientService, lab.cfg.pgHost, lab.cfg.pgPort, sql)
}

func (lab *harnessLab) psqlService(ctx context.Context, service, sql string) (string, error) {
	return lab.psql(ctx, service, "127.0.0.1", "5432", sql)
}

func (lab *harnessLab) psql(ctx context.Context, service, host, port, sql string) (string, error) {
	output, status, err := lab.composeExecInput(ctx, service, sql, "env", "PGPASSWORD="+lab.cfg.pgPassword,
		lab.cfg.psqlBinary,
		"-v", "ON_ERROR_STOP=1",
		"-h", host,
		"-p", port,
		"-U", lab.cfg.pgUser,
		"-d", lab.cfg.pgDatabase,
		"-F", "\t",
		"-Atq")
	if err != nil || status != 0 {
		return output, fmt.Errorf("psql in %s failed: %s", service, strings.TrimSpace(output))
	}
	return strings.TrimSpace(output), nil
}
