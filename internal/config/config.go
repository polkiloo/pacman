package config

import "github.com/polkiloo/pacman/internal/cluster"

const (
	// APIVersionV1Alpha1 is the initial PACMAN node-config schema version.
	APIVersionV1Alpha1 = "pacman.io/v1alpha1"
	// KindNodeConfig identifies the pacmand bootstrap node configuration
	// document.
	KindNodeConfig = "NodeConfig"
)

// Config describes the pacmand bootstrap node configuration document. It
// intentionally starts small: node identity and listen/advertise endpoints land
// here first, while cluster/bootstrap, TLS, and PostgreSQL-specific sections
// are added in later tasks.
type Config struct {
	APIVersion string     `yaml:"apiVersion" json:"apiVersion"`
	Kind       string     `yaml:"kind" json:"kind"`
	Node       NodeConfig `yaml:"node" json:"node"`
}

// NodeConfig captures the local node identity and addresses pacmand needs
// before cluster-wide truth exists.
type NodeConfig struct {
	Name           string           `yaml:"name" json:"name"`
	Role           cluster.NodeRole `yaml:"role,omitempty" json:"role,omitempty"`
	APIAddress     string           `yaml:"apiAddress,omitempty" json:"apiAddress,omitempty"`
	ControlAddress string           `yaml:"controlAddress,omitempty" json:"controlAddress,omitempty"`
}
