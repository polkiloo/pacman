package dcs

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultTTL                   = 30 * time.Second
	DefaultRetryTimeout          = 10 * time.Second
	DefaultRaftSnapshotInterval  = 120 * time.Second
	DefaultRaftSnapshotThreshold = uint64(8192)
	DefaultRaftTrailingLogs      = uint64(10240)

	redactedValue = "<redacted>"
)

// Backend identifies the configured DCS implementation.
type Backend string

const (
	BackendRaft Backend = "raft"
	BackendEtcd Backend = "etcd"
)

// Config captures the backend-neutral DCS configuration section.
type Config struct {
	Backend      Backend       `yaml:"backend,omitempty" json:"backend,omitempty"`
	ClusterName  string        `yaml:"clusterName,omitempty" json:"clusterName,omitempty"`
	TTL          time.Duration `yaml:"ttl,omitempty" json:"ttl,omitempty"`
	RetryTimeout time.Duration `yaml:"retryTimeout,omitempty" json:"retryTimeout,omitempty"`
	Raft         *RaftConfig   `yaml:"raft,omitempty" json:"raft,omitempty"`
	Etcd         *EtcdConfig   `yaml:"etcd,omitempty" json:"etcd,omitempty"`
}

// RaftConfig captures embedded Raft backend configuration.
type RaftConfig struct {
	DataDir           string        `yaml:"dataDir,omitempty" json:"dataDir,omitempty"`
	BindAddress       string        `yaml:"bindAddress,omitempty" json:"bindAddress,omitempty"`
	Peers             []string      `yaml:"peers,omitempty" json:"peers,omitempty"`
	SnapshotInterval  time.Duration `yaml:"snapshotInterval,omitempty" json:"snapshotInterval,omitempty"`
	SnapshotThreshold uint64        `yaml:"snapshotThreshold,omitempty" json:"snapshotThreshold,omitempty"`
	TrailingLogs      uint64        `yaml:"trailingLogs,omitempty" json:"trailingLogs,omitempty"`
}

// EtcdConfig captures etcd backend configuration.
type EtcdConfig struct {
	Endpoints []string `yaml:"endpoints,omitempty" json:"endpoints,omitempty"`
	Username  string   `yaml:"username,omitempty" json:"username,omitempty"`
	Password  string   `yaml:"password,omitempty" json:"password,omitempty"`
}

// WithDefaults returns a copy of the DCS config with omitted defaults filled.
func (config Config) WithDefaults() Config {
	defaulted := config

	if defaulted.TTL == 0 {
		defaulted.TTL = DefaultTTL
	}

	if defaulted.RetryTimeout == 0 {
		defaulted.RetryTimeout = DefaultRetryTimeout
	}

	if defaulted.Raft != nil {
		raft := defaulted.Raft.WithDefaults()
		defaulted.Raft = &raft
	}

	return defaulted
}

// Validate reports whether the DCS configuration is coherent enough to select
// and initialize a backend.
func (config Config) Validate() error {
	if !config.Backend.IsValid() {
		if strings.TrimSpace(string(config.Backend)) == "" {
			return ErrBackendRequired
		}

		return ErrBackendInvalid
	}

	if strings.TrimSpace(config.ClusterName) == "" {
		return ErrClusterNameRequired
	}

	if config.TTL <= 0 {
		return ErrTTLRequired
	}

	if config.RetryTimeout <= 0 {
		return ErrRetryTimeoutRequired
	}

	switch config.Backend {
	case BackendRaft:
		if config.Raft == nil {
			return ErrRaftConfigRequired
		}

		if config.Etcd != nil {
			return ErrEtcdConfigUnexpected
		}

		return config.Raft.Validate()
	case BackendEtcd:
		if config.Etcd == nil {
			return ErrEtcdConfigRequired
		}

		if config.Raft != nil {
			return ErrRaftConfigUnexpected
		}

		return config.Etcd.Validate()
	default:
		return ErrBackendInvalid
	}
}

// Redacted returns a copy of the config with inline secret material masked.
func (config Config) Redacted() Config {
	redacted := config

	if redacted.Etcd != nil {
		etcd := redacted.Etcd.Redacted()
		redacted.Etcd = &etcd
	}

	return redacted
}

// HasInlineSecrets reports whether the config embeds secret material directly.
func (config Config) HasInlineSecrets() bool {
	return config.Etcd != nil && config.Etcd.HasInlineSecrets()
}

// WithDefaults returns a copy of the raft config with omitted defaults filled.
func (config RaftConfig) WithDefaults() RaftConfig {
	defaulted := config

	if defaulted.SnapshotInterval == 0 {
		defaulted.SnapshotInterval = DefaultRaftSnapshotInterval
	}

	if defaulted.SnapshotThreshold == 0 {
		defaulted.SnapshotThreshold = DefaultRaftSnapshotThreshold
	}

	if defaulted.TrailingLogs == 0 {
		defaulted.TrailingLogs = DefaultRaftTrailingLogs
	}

	return defaulted
}

// Validate reports whether the raft backend configuration is coherent.
func (config RaftConfig) Validate() error {
	if strings.TrimSpace(config.DataDir) == "" {
		return ErrRaftDataDirRequired
	}

	if strings.TrimSpace(config.BindAddress) == "" {
		return ErrRaftBindAddressRequired
	}

	if !isValidHostPort(config.BindAddress) {
		return ErrRaftBindAddressInvalid
	}

	if len(config.Peers) == 0 {
		return ErrRaftPeersRequired
	}

	for _, peer := range config.Peers {
		if !isValidHostPort(peer) {
			return ErrRaftPeerInvalid
		}
	}

	return nil
}

// Validate reports whether the etcd backend configuration is coherent.
func (config EtcdConfig) Validate() error {
	if len(config.Endpoints) == 0 {
		return ErrEtcdEndpointsRequired
	}

	for _, endpoint := range config.Endpoints {
		if !isValidEndpoint(endpoint) {
			return ErrEtcdEndpointInvalid
		}
	}

	return nil
}

// Redacted returns a copy of the etcd config with inline secret material
// masked.
func (config EtcdConfig) Redacted() EtcdConfig {
	redacted := config

	if strings.TrimSpace(redacted.Password) != "" {
		redacted.Password = redactedValue
	}

	return redacted
}

// HasInlineSecrets reports whether the etcd config embeds secret material.
func (config EtcdConfig) HasInlineSecrets() bool {
	return strings.TrimSpace(config.Password) != ""
}

// IsValid reports whether the backend value identifies a supported backend.
func (backend Backend) IsValid() bool {
	switch backend {
	case BackendRaft, BackendEtcd:
		return true
	default:
		return false
	}
}

// isValidHostPort reports whether address is a valid host:port pair with a
// non-empty host and a port in the range [1, 65535].
func isValidHostPort(address string) bool {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}

	if strings.TrimSpace(host) == "" {
		return false
	}

	value, err := strconv.Atoi(port)
	if err != nil {
		return false
	}

	return value >= 1 && value <= 65535
}

func isValidEndpoint(endpoint string) bool {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return false
	}

	return parsed.Scheme != "" && parsed.Host != ""
}
