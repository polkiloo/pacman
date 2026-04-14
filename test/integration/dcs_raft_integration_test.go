//go:build integration

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/test/testenv"
)

const (
	raftHelperBinaryPath = "/usr/local/bin/raftd"
	raftHTTPPort         = "8080"
	raftTransportPort    = "7100"
)

var errRemoteNotLeader = errors.New("remote raft helper: not leader")

func TestRaftThreeNodeReplicationAndWatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed raft integration test in short mode")
	}

	cluster := startRaftCluster(t)
	leader := waitForRaftLeader(t, cluster.nodes, 30*time.Second)
	follower := firstFollower(t, cluster.nodes, leader)

	lease, held, err := leader.Campaign(t.Context(), "alpha-1")
	if err != nil {
		t.Fatalf("campaign logical leader: %v", err)
	}

	if !held {
		t.Fatal("expected consensus leader to acquire logical leader lease")
	}

	if lease.Leader != "alpha-1" {
		t.Fatalf("unexpected logical leader: got %q, want %q", lease.Leader, "alpha-1")
	}

	for _, node := range cluster.nodes {
		node := node
		t.Run("logical leader visible on "+node.name, func(t *testing.T) {
			current := waitForLogicalLeader(t, node, "alpha-1", 10*time.Second)
			if current.Leader != "alpha-1" {
				t.Fatalf("unexpected logical leader: got %q, want %q", current.Leader, "alpha-1")
			}
		})
	}

	watchPrefix := "/pacman/alpha/status/"
	watch := follower.Watch(t, watchPrefix)
	defer watch.Close()

	key := "/pacman/alpha/status/alpha-1"
	if err := leader.Set(t.Context(), key, []byte("up"), 0); err != nil {
		t.Fatalf("set replicated key: %v", err)
	}

	putEvent := watch.Next(t, 10*time.Second)
	if putEvent.Type != dcs.EventPut || putEvent.Key != key || string(putEvent.Value) != "up" {
		t.Fatalf("unexpected replicated put event: %+v", putEvent)
	}

	replicatedRevision := waitForReplicatedValue(t, cluster.nodes, key, "up", 10*time.Second)

	for _, node := range cluster.nodes {
		entry, err := node.Get(t.Context(), key)
		if err != nil {
			t.Fatalf("read replicated key from %s: %v", node.name, err)
		}
		if entry.Revision != replicatedRevision {
			t.Fatalf("unexpected revision on %s: got %d, want %d", node.name, entry.Revision, replicatedRevision)
		}
	}

	ttlKey := "/pacman/alpha/status/alpha-2"
	if err := leader.Set(t.Context(), ttlKey, []byte("ttl"), 400*time.Millisecond); err != nil {
		t.Fatalf("set ttl key: %v", err)
	}

	ttlPutEvent := watch.Next(t, 10*time.Second)
	if ttlPutEvent.Type != dcs.EventPut || ttlPutEvent.Key != ttlKey {
		t.Fatalf("unexpected ttl put event: %+v", ttlPutEvent)
	}

	expiredEvent := watch.Next(t, 10*time.Second)
	if expiredEvent.Type != dcs.EventExpired || expiredEvent.Key != ttlKey {
		t.Fatalf("unexpected ttl expired event: %+v", expiredEvent)
	}

	if err := leader.Delete(t.Context(), key); err != nil {
		t.Fatalf("delete replicated key: %v", err)
	}

	deleteEvent := watch.Next(t, 10*time.Second)
	if deleteEvent.Type != dcs.EventDelete || deleteEvent.Key != key {
		t.Fatalf("unexpected replicated delete event: %+v", deleteEvent)
	}
}

func TestRaftThreeNodeLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed raft integration test in short mode")
	}

	cluster := startRaftCluster(t)
	leader := waitForRaftLeader(t, cluster.nodes, 30*time.Second)

	key := "/pacman/alpha/config/failover"
	if err := leader.Set(t.Context(), key, []byte("before"), 0); err != nil {
		t.Fatalf("seed failover key: %v", err)
	}

	waitForReplicatedValue(t, cluster.nodes, key, "before", 10*time.Second)

	leader.Stop(t)

	survivors := filterStopped(cluster.nodes, leader.name)
	newLeader := waitForRaftLeader(t, survivors, 30*time.Second)
	if newLeader.name == leader.name {
		t.Fatalf("leader did not fail over: still %s", newLeader.name)
	}

	waitForReplicatedValue(t, survivors, key, "before", 10*time.Second)

	if err := newLeader.Set(t.Context(), key, []byte("after"), 0); err != nil {
		t.Fatalf("write after failover: %v", err)
	}

	waitForReplicatedValue(t, survivors, key, "after", 10*time.Second)
}

type raftCluster struct {
	nodes []*raftIntegrationNode
}

type raftIntegrationNode struct {
	name    string
	baseURL string
	service *testenv.Service
	client  *http.Client
}

type raftWatch struct {
	cancel context.CancelFunc
	events chan dcs.WatchEvent
}

type helperAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type helperLeaderResponse struct {
	OK    bool            `json:"ok"`
	Lease dcs.LeaderLease `json:"lease"`
}

type helperCampaignResponse struct {
	Lease dcs.LeaderLease `json:"lease"`
	Held  bool            `json:"held"`
}

func startRaftCluster(t *testing.T) raftCluster {
	t.Helper()

	env := testenv.New(t)
	testenv.RequireLocalImage(t, pacmanTestImage())

	binaryPath := buildRaftHelperBinary(t)
	clusterName := "raft-int-" + sanitizeRaftIntegrationName(t.Name())
	peerAddresses := []string{
		"raft-1:" + raftTransportPort,
		"raft-2:" + raftTransportPort,
		"raft-3:" + raftTransportPort,
	}

	nodes := make([]*raftIntegrationNode, 0, len(peerAddresses))
	for index, peerAddress := range peerAddresses {
		name := fmt.Sprintf("raft-%d", index+1)
		service := env.StartService(t, testenv.ServiceConfig{
			Name:         name,
			Image:        pacmanTestImage(),
			Aliases:      []string{name},
			ExposedPorts: []string{raftHTTPPort + "/tcp"},
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      binaryPath,
					ContainerFilePath: raftHelperBinaryPath,
					FileMode:          0o755,
				},
			},
			Env: map[string]string{
				"PACMAN_DCS_RAFT_HTTP_ADDRESS":         ":" + raftHTTPPort,
				"PACMAN_DCS_RAFT_CLUSTER_NAME":         clusterName,
				"PACMAN_DCS_RAFT_TTL":                  "750ms",
				"PACMAN_DCS_RAFT_RETRY_TIMEOUT":        "5s",
				"PACMAN_DCS_RAFT_DATA_DIR":             "/var/lib/pacman/raft",
				"PACMAN_DCS_RAFT_BIND_ADDRESS":         peerAddress,
				"PACMAN_DCS_RAFT_PEERS":                strings.Join(peerAddresses, ","),
				"PACMAN_DCS_RAFT_BOOTSTRAP":            strconv.FormatBool(index == 0),
				"PACMAN_DCS_RAFT_APPLY_TIMEOUT":        "5s",
				"PACMAN_DCS_RAFT_TRANSPORT_TIMEOUT":    "5s",
				"PACMAN_DCS_RAFT_HEARTBEAT_TIMEOUT":    "200ms",
				"PACMAN_DCS_RAFT_ELECTION_TIMEOUT":     "200ms",
				"PACMAN_DCS_RAFT_LEADER_LEASE_TIMEOUT": "200ms",
				"PACMAN_DCS_RAFT_EXPIRY_INTERVAL":      "50ms",
			},
			Entrypoint: []string{raftHelperBinaryPath},
			WaitStrategy: wait.ForHTTP("/health").
				WithPort(raftHTTPPort + "/tcp").
				WithStartupTimeout(60 * time.Second),
		})

		nodes = append(nodes, &raftIntegrationNode{
			name:    name,
			baseURL: "http://" + service.Address(t, raftHTTPPort),
			service: service,
			client: &http.Client{
				Timeout: 3 * time.Second,
			},
		})
	}

	return raftCluster{nodes: nodes}
}

func buildRaftHelperBinary(t *testing.T) string {
	t.Helper()

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	repoRoot := filepath.Clean(filepath.Join(workingDir, "..", ".."))
	outputPath := filepath.Join(t.TempDir(), "raftd")

	cmd := exec.Command("go", "build", "-o", outputPath, "./test/cmd/raftd")
	cmd.Dir = repoRoot
	cmd.Env = append(
		os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runnerGOARCH(t),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build raft helper binary: %v\n%s", err, output)
	}

	return outputPath
}

func waitForRaftLeader(t *testing.T, nodes []*raftIntegrationNode, timeout time.Duration) *raftIntegrationNode {
	t.Helper()

	deadline := time.Now().Add(timeout)
	probeKey := fmt.Sprintf("/_integration/raft-leader/%d", time.Now().UnixNano())
	var lastErr error

	for time.Now().Before(deadline) {
		var leader *raftIntegrationNode
		successes := 0

		for _, node := range nodes {
			err := node.Set(context.Background(), probeKey, []byte(node.name), time.Second)
			switch {
			case err == nil:
				leader = node
				successes++
			case errors.Is(err, errRemoteNotLeader):
			default:
				lastErr = err
			}
		}

		if successes == 1 {
			if err := leader.Delete(context.Background(), probeKey); err != nil && !errors.Is(err, dcs.ErrKeyNotFound) && !errors.Is(err, errRemoteNotLeader) {
				t.Fatalf("cleanup leader probe key: %v", err)
			}
			return leader
		}

		time.Sleep(200 * time.Millisecond)
	}

	if lastErr != nil {
		t.Fatalf("timed out waiting for raft leader: last error: %v", lastErr)
	}

	t.Fatal("timed out waiting for raft leader")
	return nil
}

func firstFollower(t *testing.T, nodes []*raftIntegrationNode, leader *raftIntegrationNode) *raftIntegrationNode {
	t.Helper()

	for _, node := range nodes {
		if node.name != leader.name {
			return node
		}
	}

	t.Fatal("expected at least one raft follower")
	return nil
}

func filterStopped(nodes []*raftIntegrationNode, stoppedName string) []*raftIntegrationNode {
	filtered := make([]*raftIntegrationNode, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.name != stoppedName {
			filtered = append(filtered, node)
		}
	}

	return filtered
}

func waitForReplicatedValue(t *testing.T, nodes []*raftIntegrationNode, key, want string, timeout time.Duration) int64 {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var (
			revision int64
			ready    = true
		)

		for index, node := range nodes {
			entry, err := node.Get(context.Background(), key)
			if err != nil {
				ready = false
				break
			}

			if string(entry.Value) != want {
				ready = false
				break
			}

			if index == 0 {
				revision = entry.Revision
				continue
			}

			if entry.Revision != revision {
				ready = false
				break
			}
		}

		if ready {
			return revision
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for replicated value %q at key %q", want, key)
	return 0
}

func waitForLogicalLeader(t *testing.T, node *raftIntegrationNode, want string, timeout time.Duration) dcs.LeaderLease {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lease, ok, err := node.Leader(context.Background())
		if err == nil && ok && lease.Leader == want {
			return lease
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for logical leader %q on %s", want, node.name)
	return dcs.LeaderLease{}
}

func (node *raftIntegrationNode) Stop(t *testing.T) {
	t.Helper()
	node.service.Stop(t)
}

func (node *raftIntegrationNode) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	requestURL := node.baseURL + "/v1/key?key=" + url.QueryEscape(key)
	if ttl > 0 {
		requestURL += "&ttl_ms=" + strconv.FormatInt(ttl.Milliseconds(), 10)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewReader(value))
	if err != nil {
		return err
	}

	response, err := node.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	return decodeNoContentError(response)
}

func (node *raftIntegrationNode) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, node.baseURL+"/v1/key?key="+url.QueryEscape(key), nil)
	if err != nil {
		return dcs.KeyValue{}, err
	}

	response, err := node.client.Do(request)
	if err != nil {
		return dcs.KeyValue{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return dcs.KeyValue{}, decodeAPIError(response)
	}

	var entry dcs.KeyValue
	if err := json.NewDecoder(response.Body).Decode(&entry); err != nil {
		return dcs.KeyValue{}, err
	}

	return entry, nil
}

func (node *raftIntegrationNode) Delete(ctx context.Context, key string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, node.baseURL+"/v1/key?key="+url.QueryEscape(key), nil)
	if err != nil {
		return err
	}

	response, err := node.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	return decodeNoContentError(response)
}

func (node *raftIntegrationNode) Campaign(ctx context.Context, candidate string) (dcs.LeaderLease, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, node.baseURL+"/v1/campaign?candidate="+url.QueryEscape(candidate), nil)
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}

	response, err := node.client.Do(request)
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return dcs.LeaderLease{}, false, decodeAPIError(response)
	}

	var payload helperCampaignResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	return payload.Lease, payload.Held, nil
}

func (node *raftIntegrationNode) Leader(ctx context.Context) (dcs.LeaderLease, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, node.baseURL+"/v1/leader", nil)
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}

	response, err := node.client.Do(request)
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return dcs.LeaderLease{}, false, decodeAPIError(response)
	}

	var payload helperLeaderResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	return payload.Lease, payload.OK, nil
}

func (node *raftIntegrationNode) Watch(t *testing.T, prefix string) *raftWatch {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, node.baseURL+"/v1/watch?prefix="+url.QueryEscape(prefix), nil)
	if err != nil {
		cancel()
		t.Fatalf("construct watch request: %v", err)
	}

	// Watch is a long-lived streaming response; use node.client's transport but
	// no per-request timeout — the context handles cancellation instead.
	streamClient := &http.Client{Transport: node.client.Transport}
	response, err := streamClient.Do(request)
	if err != nil {
		cancel()
		t.Fatalf("open watch stream: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		cancel()
		t.Fatalf("watch stream status %d: %v", response.StatusCode, decodeAPIError(response))
	}

	watch := &raftWatch{
		cancel: cancel,
		events: make(chan dcs.WatchEvent, 16),
	}

	go func() {
		defer close(watch.events)
		defer response.Body.Close()

		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			var event dcs.WatchEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				return
			}

			select {
			case watch.events <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return watch
}

func (watch *raftWatch) Next(t *testing.T, timeout time.Duration) dcs.WatchEvent {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case event, ok := <-watch.events:
		if !ok {
			t.Fatal("watch channel closed before expected event")
		}
		return event
	case <-timer.C:
		t.Fatalf("timed out waiting for watch event after %s", timeout)
		return dcs.WatchEvent{}
	}
}

func (watch *raftWatch) Close() {
	watch.cancel()
}

func decodeNoContentError(response *http.Response) error {
	if response.StatusCode == http.StatusNoContent {
		return nil
	}

	return decodeAPIError(response)
}

func decodeAPIError(response *http.Response) error {
	body, _ := io.ReadAll(response.Body)
	var payload helperAPIError
	if len(body) > 0 {
		_ = json.Unmarshal(body, &payload)
	}

	switch payload.Code {
	case "not_leader":
		return errRemoteNotLeader
	case "key_not_found":
		return dcs.ErrKeyNotFound
	case "revision_mismatch":
		return dcs.ErrRevisionMismatch
	case "backend_unavailable":
		return dcs.ErrBackendUnavailable
	}

	if payload.Message != "" {
		return errors.New(payload.Message)
	}

	return fmt.Errorf("unexpected status %d", response.StatusCode)
}

func sanitizeRaftIntegrationName(name string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-", "_", "-", "(", "-", ")", "-", ":", "-")
	return strings.Trim(replacer.Replace(strings.ToLower(name)), "-")
}
