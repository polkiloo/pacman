//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/test/testenv"
)

type e2eClusterBootstrapStatus struct {
	ClusterName    string `json:"clusterName"`
	Phase          string `json:"phase"`
	CurrentPrimary string `json:"currentPrimary"`
	CurrentEpoch   int64  `json:"currentEpoch"`
	Maintenance    struct {
		Enabled bool `json:"enabled"`
	} `json:"maintenance"`
	Members []e2eClusterBootstrapMember `json:"members"`
}

type e2eClusterBootstrapMember struct {
	Name    string `json:"name"`
	Role    string `json:"role"`
	State   string `json:"state"`
	Healthy bool   `json:"healthy"`
}

type e2eClusterBootstrapSpec struct {
	ClusterName string `json:"clusterName"`
	Members     []struct {
		Name string `json:"name"`
	} `json:"members"`
}

type e2eClusterBootstrapMembersResponse struct {
	Items []e2eClusterBootstrapMember `json:"items"`
}

// TestEndToEndThreeNodeClusterBootstrap starts three pacmand data nodes against
// one shared etcd backend and verifies that each node converges on the same
// desired cluster spec and healthy observed membership.
func TestEndToEndThreeNodeClusterBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	const (
		clusterName = "alpha"
		etcdAlias   = "etcd-e2e-bootstrap"
	)

	memberNames := []string{"alpha-1", "alpha-2", "alpha-3"}
	env := testenv.New(t)
	startTopologyEtcd(t, env, etcdAlias)

	nodes := make(map[string]clusterTopologyNode, len(memberNames))
	for _, memberName := range memberNames {
		serviceName := memberName + "-e2e"
		cfg := fmt.Sprintf(daemonEtcdThreeNodeConfig,
			memberName,
			serviceName+topologyPGPostgresSuffix,
			etcdAlias,
			memberNames[0],
			memberNames[0],
			memberNames[1],
			memberNames[2],
		)
		nodes[memberName] = startEtcdBackedDaemonNode(t, env, serviceName, cfg)
	}

	statuses := make(map[string]e2eClusterBootstrapStatus, len(memberNames))
	specs := make(map[string]e2eClusterBootstrapSpec, len(memberNames))
	membersByNode := make(map[string]e2eClusterBootstrapMembersResponse, len(memberNames))
	for _, memberName := range memberNames {
		node := nodes[memberName]
		waitForTopologyMemberCount(t, node.Client, node.Base, len(memberNames))

		statuses[memberName] = waitForEndToEndBootstrapStatus(t, memberName, node, clusterName, memberNames)
		var spec e2eClusterBootstrapSpec
		clusterJSON(t, node.Client, node.Base+topologyClusterSpecAPI, &spec)
		specs[memberName] = spec

		var members e2eClusterBootstrapMembersResponse
		clusterJSON(t, node.Client, node.Base+topologyMembersAPI, &members)
		membersByNode[memberName] = members
	}

	positiveCases := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "positive all nodes report healthy cluster status",
			run: func(t *testing.T) {
				for _, memberName := range memberNames {
					status := statuses[memberName]
					if status.ClusterName != clusterName || status.Phase != "healthy" {
						t.Fatalf("%s status: got cluster=%q phase=%q, want cluster=%q phase=healthy",
							memberName, status.ClusterName, status.Phase, clusterName)
					}
					if !e2eBootstrapSameStringSet(e2eBootstrapObservedMemberNames(status.Members), memberNames) {
						t.Fatalf("%s status members: got %v, want %v",
							memberName, e2eBootstrapObservedMemberNames(status.Members), memberNames)
					}
				}
			},
		},
		{
			name: "positive desired spec is identical on every node",
			run: func(t *testing.T) {
				for _, memberName := range memberNames {
					spec := specs[memberName]
					if spec.ClusterName != clusterName {
						t.Fatalf("%s cluster spec name: got %q, want %q", memberName, spec.ClusterName, clusterName)
					}
					if !e2eBootstrapSameStringSet(e2eBootstrapSpecMemberNames(spec), memberNames) {
						t.Fatalf("%s cluster spec members: got %v, want %v",
							memberName, e2eBootstrapSpecMemberNames(spec), memberNames)
					}
				}
			},
		},
		{
			name: "positive members endpoint exposes all bootstrapped nodes",
			run: func(t *testing.T) {
				for _, memberName := range memberNames {
					members := membersByNode[memberName]
					if !e2eBootstrapSameStringSet(e2eBootstrapObservedMemberNames(members.Items), memberNames) {
						t.Fatalf("%s members response: got %v, want %v",
							memberName, e2eBootstrapObservedMemberNames(members.Items), memberNames)
					}
				}
			},
		},
		{
			name: "positive current primary is one of the configured members",
			run: func(t *testing.T) {
				for _, memberName := range memberNames {
					status := statuses[memberName]
					if status.CurrentPrimary == "" || !e2eBootstrapContainsName(memberNames, status.CurrentPrimary) {
						t.Fatalf("%s reported unexpected currentPrimary %q", memberName, status.CurrentPrimary)
					}
				}
			},
		},
		{
			name: "positive maintenance mode remains disabled after bootstrap",
			run: func(t *testing.T) {
				for _, memberName := range memberNames {
					if statuses[memberName].Maintenance.Enabled {
						t.Fatalf("%s unexpectedly reported maintenance mode enabled", memberName)
					}
				}
			},
		},
	}

	for _, testCase := range positiveCases {
		t.Run(testCase.name, testCase.run)
	}
}

func TestEndToEndThreeNodeClusterBootstrapNegativeCases(t *testing.T) {
	if testing.Short() {
		t.Skip(skipShortMode)
	}

	env := testenv.New(t)
	testCases := []struct {
		name         string
		bootstrap    string
		wantContains []string
	}{
		{
			name: "negative bootstrap cluster name is required",
			bootstrap: `
  initialPrimary: alpha-1
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-1
    - alpha-2
    - alpha-3
`,
			wantContains: []string{
				topologyValidateConfig,
				"config bootstrap clusterName is required",
			},
		},
		{
			name: "negative dcs cluster name must match bootstrap cluster name",
			bootstrap: `
  clusterName: beta
  initialPrimary: alpha-1
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-1
    - alpha-2
    - alpha-3
`,
			wantContains: []string{
				topologyValidateConfig,
				"config dcs clusterName must match bootstrap clusterName",
			},
		},
		{
			name: "negative initial primary must be an expected member",
			bootstrap: `
  clusterName: alpha
  initialPrimary: alpha-4
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-1
    - alpha-2
    - alpha-3
`,
			wantContains: []string{
				topologyValidateConfig,
				"config bootstrap initialPrimary must be listed in expectedMembers",
			},
		},
		{
			name: "negative expected member names cannot be empty",
			bootstrap: `
  clusterName: alpha
  initialPrimary: alpha-1
  seedAddresses:
    - 0.0.0.0:9090
  expectedMembers:
    - alpha-1
    - " "
    - alpha-3
`,
			wantContains: []string{
				topologyValidateConfig,
				"config bootstrap expectedMembers contain an empty member name",
			},
		},
		{
			name: "negative seed address must be host port",
			bootstrap: `
  clusterName: alpha
  initialPrimary: alpha-1
  seedAddresses:
    - not-a-host-port
  expectedMembers:
    - alpha-1
    - alpha-2
    - alpha-3
`,
			wantContains: []string{
				topologyValidateConfig,
				"config bootstrap seedAddresses contain an invalid address",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			runner := startDaemonRunner(t, env, testCase.name, e2eBootstrapInvalidConfig(testCase.bootstrap), nil, nil)
			result := runner.Exec(t, "pacmand", "-config", daemonConfigPath)
			if result.ExitCode == 0 {
				t.Fatalf("expected bootstrap config to fail validation, got exit code 0 with output:\n%s", result.Output)
			}
			for _, want := range testCase.wantContains {
				if !strings.Contains(result.Output, want) {
					t.Fatalf("expected output to contain %q\n--- output ---\n%s", want, result.Output)
				}
			}
		})
	}
}

func e2eBootstrapInvalidConfig(bootstrapBlock string) string {
	return fmt.Sprintf(`
apiVersion: pacman.io/v1alpha1
kind: NodeConfig
node:
  name: alpha-1
  role: data
postgres:
  dataDir: /var/lib/postgresql/data
  listenAddress: 127.0.0.1
  port: 5432
dcs:
  backend: etcd
  clusterName: alpha
  etcd:
    endpoints:
      - http://etcd-invalid-bootstrap:2379
bootstrap:
%s
`, strings.TrimRight(bootstrapBlock, "\n"))
}

func waitForEndToEndBootstrapStatus(
	t *testing.T,
	memberName string,
	node clusterTopologyNode,
	wantClusterName string,
	wantMembers []string,
) e2eClusterBootstrapStatus {
	t.Helper()

	deadline := time.Now().Add(topologyStartupTimeout)
	var (
		lastStatus int
		lastBody   string
		lastErr    error
	)

	for time.Now().Before(deadline) {
		status, code, body, err := fetchEndToEndBootstrapStatus(t, node.Client, node.Base+topologyClusterAPI)
		lastStatus = code
		lastBody = body
		lastErr = err

		if err == nil && code == http.StatusOK && e2eBootstrapStatusReady(status, wantClusterName, wantMembers) {
			return status
		}

		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf(
		"%s did not report healthy 3-node bootstrap before deadline; lastStatus=%d lastErr=%v body=%s",
		memberName,
		lastStatus,
		lastErr,
		lastBody,
	)
	return e2eClusterBootstrapStatus{}
}

func fetchEndToEndBootstrapStatus(
	t *testing.T,
	client *http.Client,
	url string,
) (e2eClusterBootstrapStatus, int, string, error) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		return e2eClusterBootstrapStatus{}, 0, "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return e2eClusterBootstrapStatus{}, resp.StatusCode, "", err
	}
	body := string(bodyBytes)

	var status e2eClusterBootstrapStatus
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(bodyBytes, &status); err != nil {
			return e2eClusterBootstrapStatus{}, resp.StatusCode, body, err
		}
	}

	return status, resp.StatusCode, body, nil
}

func e2eBootstrapStatusReady(status e2eClusterBootstrapStatus, wantClusterName string, wantMembers []string) bool {
	if status.ClusterName != wantClusterName || status.Phase != "healthy" {
		return false
	}
	if !e2eBootstrapSameStringSet(e2eBootstrapObservedMemberNames(status.Members), wantMembers) {
		return false
	}

	for _, member := range status.Members {
		if !member.Healthy || member.Role == "" || member.State == "" {
			return false
		}
	}

	return true
}

func e2eBootstrapSpecMemberNames(spec e2eClusterBootstrapSpec) []string {
	names := make([]string, 0, len(spec.Members))
	for _, member := range spec.Members {
		names = append(names, member.Name)
	}
	return names
}

func e2eBootstrapObservedMemberNames(members []e2eClusterBootstrapMember) []string {
	names := make([]string, 0, len(members))
	for _, member := range members {
		names = append(names, member.Name)
	}
	return names
}

func e2eBootstrapSameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}

	got = slices.Clone(got)
	want = slices.Clone(want)
	slices.Sort(got)
	slices.Sort(want)

	return slices.Equal(got, want)
}

func e2eBootstrapContainsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}
