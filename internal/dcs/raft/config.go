package raft

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
)

const (
	DefaultApplyTimeout       = 5 * time.Second
	DefaultTransportTimeout   = 5 * time.Second
	DefaultExpiryInterval     = 50 * time.Millisecond
	DefaultHeartbeatTimeout   = 150 * time.Millisecond
	DefaultElectionTimeout    = 150 * time.Millisecond
	DefaultLeaderLeaseTimeout = 150 * time.Millisecond
	DefaultSnapshotRetention  = 2

	defaultTransportMaxPool = 4
	minExpiryInterval       = 10 * time.Millisecond
	maxExpiryInterval       = 250 * time.Millisecond
)

// Config captures the embedded Raft backend settings needed to construct a DCS
// instance.
type Config struct {
	ClusterName        string
	TTL                time.Duration
	RetryTimeout       time.Duration
	DataDir            string
	BindAddress        string
	Peers              []string
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64
	Bootstrap          bool
	ExpiryInterval     time.Duration
	ApplyTimeout       time.Duration
	TransportTimeout   time.Duration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotRetention  int
	ServerTLSConfig    *tls.Config
	ClientTLSConfig    *tls.Config
	Logger             *slog.Logger
	Now                func() time.Time
}

// ConfigFromDCS converts the backend-neutral DCS config into the embedded Raft
// backend configuration.
func ConfigFromDCS(config dcs.Config) (Config, error) {
	defaulted := config.WithDefaults()
	if err := defaulted.Validate(); err != nil {
		return Config{}, err
	}

	if defaulted.Backend != dcs.BackendRaft || defaulted.Raft == nil {
		return Config{}, dcs.ErrRaftConfigRequired
	}

	resolved := Config{
		ClusterName:       defaulted.ClusterName,
		TTL:               defaulted.TTL,
		RetryTimeout:      defaulted.RetryTimeout,
		DataDir:           defaulted.Raft.DataDir,
		BindAddress:       defaulted.Raft.BindAddress,
		Peers:             append([]string(nil), defaulted.Raft.Peers...),
		SnapshotInterval:  defaulted.Raft.SnapshotInterval,
		SnapshotThreshold: defaulted.Raft.SnapshotThreshold,
		TrailingLogs:      defaulted.Raft.TrailingLogs,
	}

	return resolved.WithDefaults(), nil
}

// WithDefaults returns a copy of the config with omitted defaults filled.
func (config Config) WithDefaults() Config {
	defaulted := config

	if defaulted.TTL <= 0 {
		defaulted.TTL = dcs.DefaultTTL
	}

	if defaulted.RetryTimeout <= 0 {
		defaulted.RetryTimeout = dcs.DefaultRetryTimeout
	}

	if defaulted.SnapshotInterval <= 0 {
		defaulted.SnapshotInterval = dcs.DefaultRaftSnapshotInterval
	}

	if defaulted.SnapshotThreshold == 0 {
		defaulted.SnapshotThreshold = dcs.DefaultRaftSnapshotThreshold
	}

	if defaulted.TrailingLogs == 0 {
		defaulted.TrailingLogs = dcs.DefaultRaftTrailingLogs
	}

	if defaulted.ApplyTimeout <= 0 {
		defaulted.ApplyTimeout = defaulted.RetryTimeout
	}

	if defaulted.TransportTimeout <= 0 {
		defaulted.TransportTimeout = DefaultTransportTimeout
	}

	if defaulted.HeartbeatTimeout <= 0 {
		defaulted.HeartbeatTimeout = DefaultHeartbeatTimeout
	}

	if defaulted.ElectionTimeout <= 0 {
		defaulted.ElectionTimeout = DefaultElectionTimeout
	}

	if defaulted.LeaderLeaseTimeout <= 0 {
		defaulted.LeaderLeaseTimeout = DefaultLeaderLeaseTimeout
	}

	if defaulted.ExpiryInterval <= 0 {
		defaulted.ExpiryInterval = clampDuration(defaulted.TTL/4, minExpiryInterval, maxExpiryInterval)
	}

	if defaulted.SnapshotRetention <= 0 {
		defaulted.SnapshotRetention = DefaultSnapshotRetention
	}

	if defaulted.Now == nil {
		defaulted.Now = time.Now
	}

	if !defaulted.Bootstrap {
		self := strings.TrimSpace(defaulted.BindAddress)
		defaulted.Bootstrap = len(defaulted.Peers) == 1 && strings.TrimSpace(defaulted.Peers[0]) == self
	}

	return defaulted
}

// Validate reports whether the backend config is coherent enough to construct
// and initialize an embedded Raft store.
func (config Config) Validate() error {
	if _, err := dcs.NewKeySpace(config.ClusterName); err != nil {
		return err
	}

	if config.TTL <= 0 {
		return dcs.ErrTTLRequired
	}

	if config.RetryTimeout <= 0 {
		return dcs.ErrRetryTimeoutRequired
	}

	raftConfig := dcs.RaftConfig{
		DataDir:           config.DataDir,
		BindAddress:       config.BindAddress,
		Peers:             append([]string(nil), config.Peers...),
		SnapshotInterval:  config.SnapshotInterval,
		SnapshotThreshold: config.SnapshotThreshold,
		TrailingLogs:      config.TrailingLogs,
	}
	if err := raftConfig.Validate(); err != nil {
		return err
	}

	if config.ApplyTimeout <= 0 {
		return fmt.Errorf("dcs/raft: apply timeout must be greater than zero")
	}

	if config.TransportTimeout <= 0 {
		return fmt.Errorf("dcs/raft: transport timeout must be greater than zero")
	}

	if config.HeartbeatTimeout <= 0 {
		return fmt.Errorf("dcs/raft: heartbeat timeout must be greater than zero")
	}

	if config.ElectionTimeout <= 0 {
		return fmt.Errorf("dcs/raft: election timeout must be greater than zero")
	}

	if config.LeaderLeaseTimeout <= 0 {
		return fmt.Errorf("dcs/raft: leader lease timeout must be greater than zero")
	}

	if config.ExpiryInterval <= 0 {
		return fmt.Errorf("dcs/raft: expiry interval must be greater than zero")
	}

	if config.SnapshotRetention <= 0 {
		return fmt.Errorf("dcs/raft: snapshot retention must be greater than zero")
	}

	self := strings.TrimSpace(config.BindAddress)
	for _, peer := range config.Peers {
		if strings.TrimSpace(peer) == self {
			return nil
		}
	}

	return fmt.Errorf("dcs/raft: peers must include bind address %q", self)
}

func (config Config) nowUTC() time.Time {
	return config.Now().UTC()
}

func clampDuration(value, min, max time.Duration) time.Duration {
	if value < min {
		return min
	}

	if value > max {
		return max
	}

	return value
}
