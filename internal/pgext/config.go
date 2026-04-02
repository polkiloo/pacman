package pgext

import (
	"errors"
	"strings"

	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/config"
)

var ErrPostgresManagedNodeRequired = errors.New("postgres background worker mode requires a postgres-managing node role")

// Settings is the Go-side view of the PostgreSQL GUC bridge exposed by the
// pacman_agent extension.
type Settings struct {
	NodeName              string
	NodeRole              cluster.NodeRole
	APIAddress            string
	ControlAddress        string
	PostgresDataDir       string
	PostgresBinDir        string
	PostgresListenAddress string
	PostgresPort          int
	ClusterName           string
	InitialPrimary        string
	SeedAddresses         []string
	ExpectedMembers       []string
}

// Snapshot is the raw GUC-value view exposed by the PostgreSQL extension.
// List-valued settings keep the comma-separated string form used by
// postgresql.conf.
type Snapshot struct {
	NodeName              string
	NodeRole              string
	APIAddress            string
	ControlAddress        string
	PostgresDataDir       string
	PostgresBinDir        string
	PostgresListenAddress string
	PostgresPort          int
	ClusterName           string
	InitialPrimary        string
	SeedAddresses         string
	ExpectedMembers       string
}

// Settings converts a raw GUC snapshot into the normalized typed settings
// consumed by the PACMAN runtime bridge.
func (snapshot Snapshot) Settings() Settings {
	return Settings{
		NodeName:              snapshot.NodeName,
		NodeRole:              cluster.NodeRole(strings.TrimSpace(snapshot.NodeRole)),
		APIAddress:            snapshot.APIAddress,
		ControlAddress:        snapshot.ControlAddress,
		PostgresDataDir:       snapshot.PostgresDataDir,
		PostgresBinDir:        snapshot.PostgresBinDir,
		PostgresListenAddress: snapshot.PostgresListenAddress,
		PostgresPort:          snapshot.PostgresPort,
		ClusterName:           snapshot.ClusterName,
		InitialPrimary:        snapshot.InitialPrimary,
		SeedAddresses:         ParseListSetting(snapshot.SeedAddresses),
		ExpectedMembers:       ParseListSetting(snapshot.ExpectedMembers),
	}
}

// RuntimeConfig converts a raw GUC snapshot directly into the validated
// PACMAN node-runtime config.
func (snapshot Snapshot) RuntimeConfig() (config.Config, error) {
	return snapshot.Settings().RuntimeConfig()
}

// RuntimeConfig converts extension settings into the validated PACMAN
// node-runtime config consumed by the shared local-agent core.
func (settings Settings) RuntimeConfig() (config.Config, error) {
	normalized := settings.normalized()
	if !normalized.NodeRole.HasLocalPostgres() {
		return config.Config{}, ErrPostgresManagedNodeRequired
	}

	cfg := config.Config{
		Node: config.NodeConfig{
			Name:           normalized.NodeName,
			Role:           normalized.NodeRole,
			APIAddress:     normalized.APIAddress,
			ControlAddress: normalized.ControlAddress,
		},
		Postgres: &config.PostgresLocalConfig{
			DataDir:       normalized.PostgresDataDir,
			BinDir:        normalized.PostgresBinDir,
			ListenAddress: normalized.PostgresListenAddress,
			Port:          normalized.PostgresPort,
		},
	}

	if bootstrap := normalized.bootstrapConfig(); bootstrap != nil {
		cfg.Bootstrap = bootstrap
	}

	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return config.Config{}, err
	}

	return cfg, nil
}

func (settings Settings) normalized() Settings {
	normalized := settings
	normalized.NodeName = strings.TrimSpace(normalized.NodeName)
	normalized.NodeRole = normalizeNodeRole(normalized.NodeRole)
	normalized.APIAddress = strings.TrimSpace(normalized.APIAddress)
	normalized.ControlAddress = strings.TrimSpace(normalized.ControlAddress)
	normalized.PostgresDataDir = strings.TrimSpace(normalized.PostgresDataDir)
	normalized.PostgresBinDir = strings.TrimSpace(normalized.PostgresBinDir)
	normalized.PostgresListenAddress = strings.TrimSpace(normalized.PostgresListenAddress)
	normalized.ClusterName = strings.TrimSpace(normalized.ClusterName)
	normalized.InitialPrimary = strings.TrimSpace(normalized.InitialPrimary)
	normalized.SeedAddresses = normalizeList(normalized.SeedAddresses)
	normalized.ExpectedMembers = normalizeList(normalized.ExpectedMembers)
	return normalized
}

func (settings Settings) bootstrapConfig() *config.ClusterBootstrapConfig {
	if settings.ClusterName == "" &&
		settings.InitialPrimary == "" &&
		len(settings.SeedAddresses) == 0 &&
		len(settings.ExpectedMembers) == 0 {
		return nil
	}

	return &config.ClusterBootstrapConfig{
		ClusterName:     settings.ClusterName,
		InitialPrimary:  settings.InitialPrimary,
		SeedAddresses:   append([]string(nil), settings.SeedAddresses...),
		ExpectedMembers: append([]string(nil), settings.ExpectedMembers...),
	}
}

// normalizeNodeRole maps an empty role to NodeRoleData, mirroring the
// default value of the pacman.node_role GUC defined in pacman_agent.c.
func normalizeNodeRole(role cluster.NodeRole) cluster.NodeRole {
	if role == "" {
		return cluster.NodeRoleData
	}

	return role
}

func normalizeList(items []string) []string {
	if len(items) == 0 {
		return nil
	}

	normalized := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		normalized = append(normalized, trimmed)
	}

	if len(normalized) == 0 {
		return nil
	}

	return normalized
}

// ParseListSetting splits a comma-separated PostgreSQL GUC value into trimmed
// PACMAN list items, dropping empty segments.
func ParseListSetting(value string) []string {
	return normalizeList(strings.Split(value, ","))
}
