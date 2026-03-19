package config

import "github.com/polkiloo/pacman/internal/cluster"

const (
	DefaultAPIAddress     = "0.0.0.0:8080"
	DefaultControlAddress = "0.0.0.0:9090"
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
