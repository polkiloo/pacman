//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobymount "github.com/moby/moby/api/types/mount"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gopkg.in/yaml.v3"

	"github.com/polkiloo/pacman/test/testenv"
)

const (
	testConsulImage    = "hashicorp/consul:1.20"
	testZooKeeperImage = "zookeeper:3.9.3"
	testK3sImage       = "rancher/k3s:v1.27.1-k3s1"
	testPythonImage    = "python:3.12-alpine"
)

// TestPatroniDCSBackendContractsInTestcontainers covers the Patroni DCS
// backend matrix against live testcontainer fixtures. PACMAN currently runs
// etcd/etcd3 and Raft migrations, while the remaining Patroni DCS backends
// must fail with an explicit unsupported-backend error.
func TestPatroniDCSBackendContractsInTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed Patroni DCS backend contract tests in short mode")
	}

	t.Run("etcd", func(t *testing.T) {
		env := testenv.New(t)
		etcd := startTopologyEtcd(t, env, "patroni-dcs-etcd")
		serviceName := "patroni-dcs-etcd-node"

		node := startEtcdBackedDaemonNode(t, env, serviceName, patroniDCSContractEtcdConfig(
			"etcd",
			etcd.Alias,
			serviceName+topologyPGPostgresSuffix,
		))
		waitForTopologyCurrentPrimary(t, node.Client, node.Base, "postgresql0")
		assertPatroniDCSContractCluster(t, node)
	})

	t.Run("etcd3", func(t *testing.T) {
		env := testenv.New(t)
		etcd := startTopologyEtcd(t, env, "patroni-dcs-etcd3")
		serviceName := "patroni-dcs-etcd3-node"

		node := startEtcdBackedDaemonNode(t, env, serviceName, patroniDCSContractEtcdConfig(
			"etcd3",
			etcd.Alias,
			serviceName+topologyPGPostgresSuffix,
		))
		waitForTopologyCurrentPrimary(t, node.Client, node.Base, "postgresql0")
		assertPatroniDCSContractCluster(t, node)
	})

	t.Run("raft", func(t *testing.T) {
		env := testenv.New(t)
		serviceName := "patroni-dcs-raft-node"

		node := startEtcdBackedDaemonNode(t, env, serviceName, patroniDCSContractRaftConfig(
			serviceName+topologyPGPostgresSuffix,
		))
		waitForTopologyCurrentPrimary(t, node.Client, node.Base, "postgresql0")
		assertPatroniDCSContractCluster(t, node)
	})

	t.Run("consul", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniConsulFixture(t, env, "patroni-dcs-consul")

		assertPatroniUnsupportedDCSBackend(t, env, "consul", `
consul:
  host: patroni-dcs-consul:8500
  register_service: true
`)
	})

	t.Run("zookeeper", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniZooKeeperFixture(t, env, "patroni-dcs-zookeeper")

		assertPatroniUnsupportedDCSBackend(t, env, "zookeeper", `
zookeeper:
  hosts:
    - patroni-dcs-zookeeper:2181
`)
	})

	t.Run("exhibitor", func(t *testing.T) {
		env := testenv.New(t)
		zookeeperAlias := "patroni-dcs-exhibitor-zookeeper"
		startPatroniZooKeeperFixture(t, env, zookeeperAlias)
		exhibitor := startPatroniHTTPFixtureWithEnv(t, env, "patroni-dcs-exhibitor", patroniExhibitorFixtureSource, map[string]string{
			"PATRONI_EXHIBITOR_ZOOKEEPER_HOST": zookeeperAlias,
			"PATRONI_EXHIBITOR_ZOOKEEPER_PORT": "2181",
		})
		assertPatroniExhibitorDiscoversZooKeeper(t, exhibitor, zookeeperAlias)

		assertPatroniUnsupportedDCSBackend(t, env, "exhibitor", `
exhibitor:
  hosts:
    - patroni-dcs-exhibitor
  port: 8000
`)
	})

	t.Run("kubernetes", func(t *testing.T) {
		env := testenv.New(t)
		startPatroniKubernetesAPIFixture(t)

		assertPatroniUnsupportedDCSBackend(t, env, "kubernetes", `
kubernetes:
  namespace: default
  use_endpoints: true
`)
	})
}

func patroniDCSContractEtcdConfig(backend, etcdAlias, postgresAlias string) string {
	return fmt.Sprintf(`
scope: patroni-dcs-contract
name: postgresql0
restapi:
  listen: 0.0.0.0:8080
%s:
  host: %s:2379
bootstrap:
  dcs:
    ttl: 5
    retry_timeout: 5
postgresql:
  listen: %s:5432
  data_dir: /var/lib/postgresql/data
`, backend, etcdAlias, postgresAlias)
}

func patroniDCSContractRaftConfig(postgresAlias string) string {
	return fmt.Sprintf(`
scope: patroni-dcs-contract
name: postgresql0
restapi:
  listen: 0.0.0.0:8080
raft:
  data_dir: /var/lib/pacman/raft
  self_addr: 0.0.0.0:7100
bootstrap:
  dcs:
    ttl: 5
    retry_timeout: 5
postgresql:
  listen: %s:5432
  data_dir: /var/lib/postgresql/data
`, postgresAlias)
}

func assertPatroniDCSContractCluster(t *testing.T, node clusterTopologyNode) {
	t.Helper()

	var cluster struct {
		ClusterName    string `json:"clusterName"`
		CurrentPrimary string `json:"currentPrimary"`
	}
	clusterJSON(t, node.Client, node.Base+topologyClusterAPI, &cluster)

	if cluster.ClusterName != "patroni-dcs-contract" {
		t.Fatalf("unexpected clusterName: got %q, want %q", cluster.ClusterName, "patroni-dcs-contract")
	}
	if cluster.CurrentPrimary != "postgresql0" {
		t.Fatalf("unexpected currentPrimary: got %q, want %q", cluster.CurrentPrimary, "postgresql0")
	}
}

func assertPatroniUnsupportedDCSBackend(t *testing.T, env *testenv.Environment, backend, dcsBlock string) {
	t.Helper()

	testenv.RequireLocalImage(t, pacmanTestImage())

	runner := startDaemonRunner(
		t,
		env,
		"patroni-dcs-"+backend+"-runner",
		patroniUnsupportedDCSConfig(dcsBlock),
		nil,
		nil,
	)
	result := runPacmandUntilTerminated(t, runner)

	if result.ExitCode == 0 {
		t.Fatalf("expected non-zero exit for unsupported Patroni DCS backend %q, got 0", backend)
	}
	if !strings.Contains(result.Output, "Patroni config DCS backend is unsupported") ||
		!strings.Contains(result.Output, backend) {
		t.Fatalf("expected unsupported %s backend error in output, got:\n%s", backend, result.Output)
	}
}

func patroniUnsupportedDCSConfig(dcsBlock string) string {
	return fmt.Sprintf(`
scope: patroni-dcs-contract
name: postgresql0
restapi:
  listen: 127.0.0.1:8008
%s
postgresql:
  listen: 127.0.0.1:5432
  data_dir: data/postgresql0
`, strings.TrimSpace(dcsBlock))
}

func startPatroniConsulFixture(t *testing.T, env *testenv.Environment, alias string) {
	t.Helper()

	env.StartService(t, testenv.ServiceConfig{
		Name:         alias,
		Image:        testConsulImage,
		Aliases:      []string{alias},
		Entrypoint:   []string{"consul"},
		Cmd:          []string{"agent", "-dev", "-client=0.0.0.0", "-bind=0.0.0.0"},
		ExposedPorts: []string{"8500/tcp"},
		WaitStrategy: wait.ForHTTP("/v1/status/leader").
			WithPort("8500/tcp").
			WithStartupTimeout(60 * time.Second),
	})
}

func startPatroniZooKeeperFixture(t *testing.T, env *testenv.Environment, alias string) {
	t.Helper()

	env.StartService(t, testenv.ServiceConfig{
		Name:    alias,
		Image:   testZooKeeperImage,
		Aliases: []string{alias},
		Env: map[string]string{
			"ZOO_4LW_COMMANDS_WHITELIST": "ruok",
		},
		ExposedPorts: []string{"2181/tcp"},
		WaitStrategy: wait.ForListeningPort("2181/tcp").
			WithStartupTimeout(60 * time.Second),
	})
}

func startPatroniHTTPFixtureWithEnv(t *testing.T, env *testenv.Environment, alias, source string, environment map[string]string) *testenv.Service {
	t.Helper()

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         alias,
		Image:        testPythonImage,
		Aliases:      []string{alias},
		Env:          environment,
		ExposedPorts: []string{"8000/tcp"},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(source),
				ContainerFilePath: "/fixture/server.py",
				FileMode:          0o755,
			},
		},
		Cmd: []string{"python", "-u", "/fixture/server.py"},
		WaitStrategy: wait.ForHTTP("/health").
			WithPort("8000/tcp").
			WithStartupTimeout(60 * time.Second),
	})

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + service.Address(t, "8000") + "/health")
	if err != nil {
		t.Fatalf("GET %s fixture health: %v", alias, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s fixture health returned %d, want 200", alias, resp.StatusCode)
	}

	return service
}

func startPatroniKubernetesAPIFixture(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	container, err := testcontainers.Run(
		ctx,
		testK3sImage,
		testcontainers.WithExposedPorts("6443/tcp", "8443/tcp"),
		testcontainers.WithHostConfigModifier(func(hostConfig *mobycontainer.HostConfig) {
			hostConfig.Privileged = true
			hostConfig.CgroupnsMode = "host"
			hostConfig.Tmpfs = map[string]string{
				"/run":     "",
				"/var/run": "",
			}
			hostConfig.Mounts = []mobymount.Mount{}
		}),
		testcontainers.WithCmd(
			"server",
			"--disable=servicelb",
			"--disable=traefik",
			"--tls-san=localhost",
			"--tls-san=127.0.0.1",
		),
		testcontainers.WithEnv(map[string]string{
			"K3S_KUBECONFIG_MODE": "644",
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Node controller sync successful").
			WithStartupTimeout(3*time.Minute)),
	)
	if err != nil {
		t.Fatalf("start Kubernetes API fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate Kubernetes API fixture: %v", err)
		}
	})

	kubeConfig, err := k3sKubeConfig(t, ctx, container)
	if err != nil {
		t.Fatalf("read Kubernetes API fixture kubeconfig: %v", err)
	}
	assertPatroniKubernetesAPIServer(t, kubeConfig)
}

type patroniKubeConfig struct {
	Clusters []struct {
		Cluster struct {
			CertificateAuthorityData string `yaml:"certificate-authority-data"`
			Server                   string `yaml:"server"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Users []struct {
		User struct {
			ClientCertificateData string `yaml:"client-certificate-data"`
			ClientKeyData         string `yaml:"client-key-data"`
			Token                 string `yaml:"token"`
		} `yaml:"user"`
	} `yaml:"users"`
}

func k3sKubeConfig(t *testing.T, ctx context.Context, container testcontainers.Container) (patroniKubeConfig, error) {
	t.Helper()

	reader, err := container.CopyFileFromContainer(ctx, "/etc/rancher/k3s/k3s.yaml")
	if err != nil {
		return patroniKubeConfig{}, fmt.Errorf("copy kubeconfig: %w", err)
	}
	defer reader.Close()

	payload, err := io.ReadAll(reader)
	if err != nil {
		return patroniKubeConfig{}, fmt.Errorf("read kubeconfig: %w", err)
	}

	var kubeConfig patroniKubeConfig
	if err := yaml.Unmarshal(payload, &kubeConfig); err != nil {
		return patroniKubeConfig{}, fmt.Errorf("decode kubeconfig: %w", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "6443/tcp", "https")
	if err != nil {
		return patroniKubeConfig{}, fmt.Errorf("resolve Kubernetes API endpoint: %w", err)
	}
	if len(kubeConfig.Clusters) == 0 {
		return patroniKubeConfig{}, fmt.Errorf("kubeconfig has no clusters")
	}
	kubeConfig.Clusters[0].Cluster.Server = endpoint

	return kubeConfig, nil
}

func assertPatroniKubernetesAPIServer(t *testing.T, kubeConfig patroniKubeConfig) {
	t.Helper()

	client := patroniKubernetesHTTPClient(t, kubeConfig)
	baseURL := kubeConfig.Clusters[0].Cluster.Server

	var version struct {
		GitVersion string `json:"gitVersion"`
		Major      string `json:"major"`
		Minor      string `json:"minor"`
	}
	patroniKubernetesGET(t, client, kubeConfig, baseURL+"/version", &version)
	if strings.TrimSpace(version.GitVersion) == "" || strings.TrimSpace(version.Major) == "" {
		t.Fatalf("unexpected Kubernetes version response: %+v", version)
	}

	var namespace struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	patroniKubernetesGET(t, client, kubeConfig, baseURL+"/api/v1/namespaces/default", &namespace)
	if namespace.Kind != "Namespace" || namespace.Metadata.Name != "default" {
		t.Fatalf("unexpected Kubernetes namespace response: %+v", namespace)
	}
}

func patroniKubernetesHTTPClient(t *testing.T, kubeConfig patroniKubeConfig) *http.Client {
	t.Helper()

	if len(kubeConfig.Clusters) == 0 {
		t.Fatal("kubeconfig has no clusters")
	}
	if len(kubeConfig.Users) == 0 {
		t.Fatal("kubeconfig has no users")
	}

	caPEM, err := base64.StdEncoding.DecodeString(kubeConfig.Clusters[0].Cluster.CertificateAuthorityData)
	if err != nil {
		t.Fatalf("decode Kubernetes CA data: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("parse Kubernetes CA data")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
	}

	user := kubeConfig.Users[0].User
	if user.ClientCertificateData != "" || user.ClientKeyData != "" {
		certPEM, err := base64.StdEncoding.DecodeString(user.ClientCertificateData)
		if err != nil {
			t.Fatalf("decode Kubernetes client certificate data: %v", err)
		}
		keyPEM, err := base64.StdEncoding.DecodeString(user.ClientKeyData)
		if err != nil {
			t.Fatalf("decode Kubernetes client key data: %v", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatalf("parse Kubernetes client key pair: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}

func patroniKubernetesGET(t *testing.T, client *http.Client, kubeConfig patroniKubeConfig, rawURL string, target any) {
	t.Helper()

	request, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("build Kubernetes API request: %v", err)
	}
	if token := strings.TrimSpace(kubeConfig.Users[0].User.Token); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Kubernetes API response from %s: %v", rawURL, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d, want 200: %s", rawURL, response.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, target); err != nil {
		t.Fatalf("decode Kubernetes API response from %s: %v", rawURL, err)
	}
}

func assertPatroniExhibitorDiscoversZooKeeper(t *testing.T, exhibitor *testenv.Service, zookeeperAlias string) {
	t.Helper()

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + exhibitor.Address(t, "8000") + "/exhibitor/v1/cluster/list")
	if err != nil {
		t.Fatalf("GET exhibitor cluster list: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read exhibitor cluster list: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("exhibitor cluster list returned %d, want 200: %s", resp.StatusCode, string(body))
	}

	var payload struct {
		Servers []string `json:"servers"`
		Port    int      `json:"port"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode exhibitor cluster list: %v", err)
	}
	if len(payload.Servers) != 1 || payload.Servers[0] != zookeeperAlias || payload.Port != 2181 {
		t.Fatalf("unexpected exhibitor cluster list: %+v", payload)
	}
}

const patroniExhibitorFixtureSource = `
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import socket

ZOOKEEPER_HOST = os.environ.get("PATRONI_EXHIBITOR_ZOOKEEPER_HOST", "patroni-dcs-zookeeper")
ZOOKEEPER_PORT = int(os.environ.get("PATRONI_EXHIBITOR_ZOOKEEPER_PORT", "2181"))

def zookeeper_ready():
    try:
        with socket.create_connection((ZOOKEEPER_HOST, ZOOKEEPER_PORT), timeout=2) as conn:
            conn.sendall(b"ruok")
            return conn.recv(4) == b"imok"
    except OSError:
        return False

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200 if zookeeper_ready() else 503)
            self.end_headers()
            self.wfile.write(b"ok" if zookeeper_ready() else b"zookeeper unavailable")
            return
        if self.path == "/exhibitor/v1/cluster/list":
            if not zookeeper_ready():
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b"zookeeper unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"servers": [ZOOKEEPER_HOST], "port": ZOOKEEPER_PORT}).encode())
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_):
        return

ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
`
