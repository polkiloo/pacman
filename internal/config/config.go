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
	APIVersion string                  `yaml:"apiVersion" json:"apiVersion"`
	Kind       string                  `yaml:"kind" json:"kind"`
	Node       NodeConfig              `yaml:"node" json:"node"`
	TLS        *TLSConfig              `yaml:"tls,omitempty" json:"tls,omitempty"`
	Postgres   *PostgresLocalConfig    `yaml:"postgres,omitempty" json:"postgres,omitempty"`
	Bootstrap  *ClusterBootstrapConfig `yaml:"bootstrap,omitempty" json:"bootstrap,omitempty"`
}

// NodeConfig captures the local node identity and addresses pacmand needs
// before cluster-wide truth exists.
type NodeConfig struct {
	Name           string           `yaml:"name" json:"name"`
	Role           cluster.NodeRole `yaml:"role,omitempty" json:"role,omitempty"`
	APIAddress     string           `yaml:"apiAddress,omitempty" json:"apiAddress,omitempty"`
	ControlAddress string           `yaml:"controlAddress,omitempty" json:"controlAddress,omitempty"`
}

// TLSConfig captures node-local TLS material and verification settings for
// pacmand endpoints and peer traffic.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	CAFile             string `yaml:"caFile,omitempty" json:"caFile,omitempty"`
	CertFile           string `yaml:"certFile,omitempty" json:"certFile,omitempty"`
	KeyFile            string `yaml:"keyFile,omitempty" json:"keyFile,omitempty"`
	ServerName         string `yaml:"serverName,omitempty" json:"serverName,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify,omitempty" json:"insecureSkipVerify,omitempty"`
}

// PostgresLocalConfig captures node-local PostgreSQL process settings that are
// safe for pacmand to source from local config.
type PostgresLocalConfig struct {
	DataDir       string            `yaml:"dataDir,omitempty" json:"dataDir,omitempty"`
	BinDir        string            `yaml:"binDir,omitempty" json:"binDir,omitempty"`
	ListenAddress string            `yaml:"listenAddress,omitempty" json:"listenAddress,omitempty"`
	Port          int               `yaml:"port,omitempty" json:"port,omitempty"`
	Parameters    map[string]string `yaml:"parameters,omitempty" json:"parameters,omitempty"`
}

// ClusterBootstrapConfig captures the initial cluster formation intent before a
// replicated source of truth exists.
type ClusterBootstrapConfig struct {
	ClusterName     string   `yaml:"clusterName,omitempty" json:"clusterName,omitempty"`
	InitialPrimary  string   `yaml:"initialPrimary,omitempty" json:"initialPrimary,omitempty"`
	SeedAddresses   []string `yaml:"seedAddresses,omitempty" json:"seedAddresses,omitempty"`
	ExpectedMembers []string `yaml:"expectedMembers,omitempty" json:"expectedMembers,omitempty"`
}
