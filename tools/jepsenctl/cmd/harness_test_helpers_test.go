package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type scriptedRunner struct {
	outputs  []string
	statuses []int
	calls    int
	specs    []commandSpec
}

func (runner *scriptedRunner) Run(_ context.Context, spec commandSpec) (int, error) {
	output := ""
	if runner.calls < len(runner.outputs) {
		output = runner.outputs[runner.calls]
	} else if len(runner.outputs) > 0 {
		output = runner.outputs[len(runner.outputs)-1]
	}
	runner.calls++
	runner.specs = append(runner.specs, spec)
	if spec.stdout != nil {
		fmt.Fprint(spec.stdout, output)
	}
	status := 0
	if runner.calls <= len(runner.statuses) {
		status = runner.statuses[runner.calls-1]
	}
	return status, nil
}

type clusterStatusRunner struct {
	initialPrimary   string
	promotedPrimary  string
	statusAfterCalls int
	statusCalls      int
	failoverStatus   int
	specs            []commandSpec
}

func (runner *clusterStatusRunner) Run(_ context.Context, spec commandSpec) (int, error) {
	runner.specs = append(runner.specs, spec)
	if spec.stdout != nil && strings.Contains(strings.Join(spec.args, " "), "pacmanctl cluster status") {
		runner.statusCalls++
		primary := runner.initialPrimary
		if runner.statusCalls > runner.statusAfterCalls {
			primary = runner.promotedPrimary
		}
		fmt.Fprint(spec.stdout, clusterStatusJSONWithPrimary(primary))
	}
	if strings.Contains(strings.Join(spec.args, " "), "pacmanctl cluster failover") {
		return runner.failoverStatus, nil
	}
	return 0, nil
}

func (runner *clusterStatusRunner) commands() []string {
	commands := make([]string, 0, len(runner.specs))
	for _, spec := range runner.specs {
		commands = append(commands, strings.Join(append([]string{spec.name}, spec.args...), " "))
	}
	return commands
}

func (runner *clusterStatusRunner) firstCommandIndex(needle string) int {
	for index, command := range runner.commands() {
		if strings.Contains(command, needle) {
			return index
		}
	}
	return -1
}

func clusterStatusJSONWithPrimary(primary string) string {
	members := []string{"alpha-1", "alpha-2", "alpha-3"}
	status := clusterStatus{
		Phase:          "healthy",
		CurrentPrimary: primary,
	}
	for _, member := range members {
		role := "replica"
		state := "streaming"
		if member == primary {
			role = "primary"
			state = "running"
		}
		status.Members = append(status.Members, clusterMember{
			Name:    member,
			Role:    role,
			State:   state,
			Healthy: true,
		})
	}
	data, _ := json.Marshal(status)
	return string(data)
}
