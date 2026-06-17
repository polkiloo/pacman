package config

import (
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/dcs"
)

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
	DCS        *dcs.Config             `yaml:"dcs,omitempty" json:"dcs,omitempty"`
	TLS        *TLSConfig              `yaml:"tls,omitempty" json:"tls,omitempty"`
	Security   *SecurityConfig         `yaml:"security,omitempty" json:"security,omitempty"`
	Postgres   *PostgresLocalConfig    `yaml:"postgres,omitempty" json:"postgres,omitempty"`
	Reinit     *ReinitConfig           `yaml:"reinit,omitempty" json:"reinit,omitempty"`
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

// SecurityConfig captures node-local API authentication controls for
// administrative endpoints.
type SecurityConfig struct {
	AdminBearerToken     string `yaml:"adminBearerToken,omitempty" json:"adminBearerToken,omitempty"`
	AdminBearerTokenFile string `yaml:"adminBearerTokenFile,omitempty" json:"adminBearerTokenFile,omitempty"`
	MemberMTLSEnabled    bool   `yaml:"memberMTLSEnabled,omitempty" json:"memberMTLSEnabled,omitempty"`
}

// PostgresLocalConfig captures node-local PostgreSQL process settings and
// rejoin credentials that pacmand sources only from local node config.
type PostgresLocalConfig struct {
	DataDir             string            `yaml:"dataDir,omitempty" json:"dataDir,omitempty"`
	BinDir              string            `yaml:"binDir,omitempty" json:"binDir,omitempty"`
	ListenAddress       string            `yaml:"listenAddress,omitempty" json:"listenAddress,omitempty"`
	Port                int               `yaml:"port,omitempty" json:"port,omitempty"`
	Parameters          map[string]string `yaml:"parameters,omitempty" json:"parameters,omitempty"`
	ReplicationUser     string            `yaml:"replicationUser,omitempty" json:"replicationUser,omitempty"`
	ReplicationPassword string            `yaml:"replicationPassword,omitempty" json:"replicationPassword,omitempty"`
}

// ReinitConfig captures node-local settings for destructive replica
// reinitialization workflows.
type ReinitConfig struct {
	WALG *WALGConfig `yaml:"walg,omitempty" json:"walg,omitempty"`
}

// WALGConfig captures WAL-G settings used by reinit restore workflows.
type WALGConfig struct {
	Binary      string                `yaml:"binary,omitempty" json:"binary,omitempty"`
	Repository  WALGRepositoryConfig  `yaml:"repository,omitempty" json:"repository,omitempty"`
	Restore     WALGRestoreConfig     `yaml:"restore,omitempty" json:"restore,omitempty"`
	Credentials WALGCredentialsConfig `yaml:"credentials,omitempty" json:"credentials,omitempty"`
}

// WALGRepositoryProvider identifies the WAL-G storage backend.
type WALGRepositoryProvider string

const (
	WALGRepositoryProviderS3         WALGRepositoryProvider = "s3"
	WALGRepositoryProviderGCS        WALGRepositoryProvider = "gcs"
	WALGRepositoryProviderAzure      WALGRepositoryProvider = "azure"
	WALGRepositoryProviderFilesystem WALGRepositoryProvider = "filesystem"
)

// WALGRepositoryConfig describes the WAL-G backup repository location.
type WALGRepositoryConfig struct {
	Provider WALGRepositoryProvider `yaml:"provider,omitempty" json:"provider,omitempty"`
	Prefix   string                 `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	Endpoint string                 `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Region   string                 `yaml:"region,omitempty" json:"region,omitempty"`
}

// WALGRestoreConfig selects the base backup used by WAL-G restore workflows.
type WALGRestoreConfig struct {
	BackupName string `yaml:"backupName,omitempty" json:"backupName,omitempty"`
}

// WALGCredentialsConfig describes how pacmand sources environment variables
// that are passed to WAL-G. Inline environment values are treated as secrets;
// environmentFiles values name files whose trimmed contents are used as the
// corresponding environment variable values.
type WALGCredentialsConfig struct {
	InheritEnvironment []string          `yaml:"inheritEnvironment,omitempty" json:"inheritEnvironment,omitempty"`
	Environment        map[string]string `yaml:"environment,omitempty" json:"environment,omitempty"`
	EnvironmentFiles   map[string]string `yaml:"environmentFiles,omitempty" json:"environmentFiles,omitempty"`
}

// ClusterBootstrapConfig captures the initial cluster formation intent before a
// replicated source of truth exists.
type ClusterBootstrapConfig struct {
	ClusterName     string   `yaml:"clusterName,omitempty" json:"clusterName,omitempty"`
	InitialPrimary  string   `yaml:"initialPrimary,omitempty" json:"initialPrimary,omitempty"`
	SeedAddresses   []string `yaml:"seedAddresses,omitempty" json:"seedAddresses,omitempty"`
	ExpectedMembers []string `yaml:"expectedMembers,omitempty" json:"expectedMembers,omitempty"`
}
