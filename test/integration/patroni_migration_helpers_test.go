//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	pacmanconfig "github.com/polkiloo/pacman/internal/config"
	"gopkg.in/yaml.v3"
)

const (
	// patroniCompatClusterName is the cluster name from the Patroni upstream
	// examples (scope: batman) used across all migration tests.
	patroniCompatClusterName = "batman"

	// patroniCompatNode0/1/2Name mirror the name fields in the three upstream
	// Patroni example files (postgres0.yml, postgres1.yml, postgres2.yml).
	patroniCompatNode0Name = "postgresql0"
	patroniCompatNode1Name = "postgresql1"
	patroniCompatNode2Name = "postgresql2"

	patroniCompatNode0File = "pacman-compat-node0.yaml"
	patroniCompatNode1File = "pacman-compat-node1.yaml"
	patroniCompatNode2File = "pacman-compat-node2.yaml"
)

type patroniCompatRuntimeOptions struct {
	etcdAlias        string
	postgresAlias    string
	apiAddress       string
	controlAddress   string
	postgresDataDir  string
	postgresPort     int
	adminBearerToken string
}

func renderPatroniCompatConfig(t *testing.T, fileName string, options patroniCompatRuntimeOptions) string {
	t.Helper()

	cfg := loadPatroniCompatConfig(t, fileName)

	if strings.TrimSpace(options.apiAddress) == "" {
		options.apiAddress = "0.0.0.0:8080"
	}
	if strings.TrimSpace(options.controlAddress) == "" {
		options.controlAddress = "0.0.0.0:9090"
	}
	if strings.TrimSpace(options.postgresDataDir) == "" {
		options.postgresDataDir = "/var/lib/postgresql/data"
	}
	if options.postgresPort == 0 {
		options.postgresPort = 5432
	}

	cfg.Node.APIAddress = options.apiAddress
	cfg.Node.ControlAddress = options.controlAddress

	if cfg.DCS != nil && cfg.DCS.Etcd != nil && strings.TrimSpace(options.etcdAlias) != "" {
		cfg.DCS.Etcd.Endpoints = []string{"http://" + options.etcdAlias + ":2379"}
	}

	if cfg.Postgres != nil {
		cfg.Postgres.DataDir = options.postgresDataDir
		if strings.TrimSpace(options.postgresAlias) != "" {
			cfg.Postgres.ListenAddress = options.postgresAlias
		}
		cfg.Postgres.Port = options.postgresPort
	}

	if cfg.Bootstrap != nil {
		cfg.Bootstrap.SeedAddresses = []string{options.controlAddress}
	}

	if cfg.Security != nil && strings.TrimSpace(options.adminBearerToken) != "" {
		cfg.Security.AdminBearerToken = options.adminBearerToken
		cfg.Security.AdminBearerTokenFile = ""
	}

	rendered, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal patroni compatibility config %q: %v", fileName, err)
	}

	return string(rendered)
}

func loadPatroniCompatConfig(t *testing.T, fileName string) pacmanconfig.Config {
	t.Helper()

	path := patroniCompatExamplePath(t, fileName)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read patroni compatibility example %q: %v", path, err)
	}

	cfg, err := pacmanconfig.Decode(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("decode patroni compatibility example %q: %v", path, err)
	}

	return cfg
}

func patroniCompatExamplePath(t *testing.T, fileName string) string {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve patroni migration helper path: runtime.Caller failed")
	}

	return filepath.Join(filepath.Dir(currentFile), "..", "..", "docs", "examples", fileName)
}
