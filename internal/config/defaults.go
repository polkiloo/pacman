package config

import (
	"strings"

	"github.com/polkiloo/pacman/internal/cluster"
)

const (
	DefaultAPIAddress            = "0.0.0.0:8080"
	DefaultControlAddress        = "0.0.0.0:9090"
	DefaultPostgresListenAddress = "127.0.0.1"
	DefaultPostgresPort          = 5432
)

// WithDefaults returns a copy of the config with omitted fields filled using
// PACMAN's process-local defaults.
func (config Config) WithDefaults() Config {
	defaulted := config

	if defaulted.APIVersion == "" {
		defaulted.APIVersion = APIVersionV1Alpha1
	}

	if defaulted.Kind == "" {
		defaulted.Kind = KindNodeConfig
	}

	defaulted.Node = defaulted.Node.WithDefaults()

	if defaulted.DCS != nil {
		dcsConfig := defaulted.DCS.WithDefaults()
		if strings.TrimSpace(dcsConfig.ClusterName) == "" && defaulted.Bootstrap != nil {
			dcsConfig.ClusterName = defaulted.Bootstrap.ClusterName
		}
		defaulted.DCS = &dcsConfig
	}

	if defaulted.TLS != nil {
		tls := defaulted.TLS.WithDefaults()
		defaulted.TLS = &tls
	}

	if defaulted.Security != nil {
		security := defaulted.Security.WithDefaults()
		defaulted.Security = &security
	}

	if defaulted.Postgres != nil {
		postgres := defaulted.Postgres.WithDefaults()
		defaulted.Postgres = &postgres
	}

	if defaulted.Bootstrap != nil {
		bootstrap := defaulted.Bootstrap.WithDefaults(defaulted.Node.Name, defaulted.Node.ControlAddress)
		defaulted.Bootstrap = &bootstrap
	}

	return defaulted
}

// WithDefaults returns a copy of the node config with omitted fields filled
// using PACMAN's local-node defaults.
func (node NodeConfig) WithDefaults() NodeConfig {
	defaulted := node

	if defaulted.Role == "" {
		defaulted.Role = cluster.NodeRoleData
	}

	if defaulted.APIAddress == "" {
		defaulted.APIAddress = DefaultAPIAddress
	}

	if defaulted.ControlAddress == "" {
		defaulted.ControlAddress = DefaultControlAddress
	}

	return defaulted
}

// WithDefaults returns a copy of the tls config with omitted fields filled
// using PACMAN defaults.
func (tls TLSConfig) WithDefaults() TLSConfig {
	return tls
}

// WithDefaults returns a copy of the security config with omitted fields filled
// using PACMAN defaults.
func (security SecurityConfig) WithDefaults() SecurityConfig {
	return security
}

// WithDefaults returns a copy of the postgres local config with omitted fields
// filled using PACMAN defaults.
func (postgres PostgresLocalConfig) WithDefaults() PostgresLocalConfig {
	defaulted := postgres

	if defaulted.ListenAddress == "" {
		defaulted.ListenAddress = DefaultPostgresListenAddress
	}

	if defaulted.Port == 0 {
		defaulted.Port = DefaultPostgresPort
	}

	return defaulted
}

// WithDefaults returns a copy of the bootstrap config with omitted fields
// filled using local-node defaults.
func (bootstrap ClusterBootstrapConfig) WithDefaults(nodeName, controlAddress string) ClusterBootstrapConfig {
	defaulted := bootstrap

	if defaulted.InitialPrimary == "" {
		defaulted.InitialPrimary = nodeName
	}

	if len(defaulted.SeedAddresses) == 0 && controlAddress != "" {
		defaulted.SeedAddresses = []string{controlAddress}
	}

	if len(defaulted.ExpectedMembers) == 0 && nodeName != "" {
		defaulted.ExpectedMembers = []string{nodeName}
	}

	return defaulted
}
