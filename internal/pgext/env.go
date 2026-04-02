package pgext

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	EnvNodeName              = "PACMAN_PGEXT_NODE_NAME"
	EnvNodeRole              = "PACMAN_PGEXT_NODE_ROLE"
	EnvAPIAddress            = "PACMAN_PGEXT_API_ADDRESS"
	EnvControlAddress        = "PACMAN_PGEXT_CONTROL_ADDRESS"
	EnvPostgresDataDir       = "PACMAN_PGEXT_POSTGRES_DATA_DIR"
	EnvPostgresBinDir        = "PACMAN_PGEXT_POSTGRES_BIN_DIR"
	EnvPostgresListenAddress = "PACMAN_PGEXT_POSTGRES_LISTEN_ADDRESS"
	EnvPostgresPort          = "PACMAN_PGEXT_POSTGRES_PORT"
	EnvClusterName           = "PACMAN_PGEXT_CLUSTER_NAME"
	EnvInitialPrimary        = "PACMAN_PGEXT_INITIAL_PRIMARY"
	EnvSeedAddresses         = "PACMAN_PGEXT_SEED_ADDRESSES"
	EnvExpectedMembers       = "PACMAN_PGEXT_EXPECTED_MEMBERS"
)

// Environment returns the process environment expected by `pacmand -pgext-env`.
func (snapshot Snapshot) Environment() map[string]string {
	return map[string]string{
		EnvNodeName:              snapshot.NodeName,
		EnvNodeRole:              snapshot.NodeRole,
		EnvAPIAddress:            snapshot.APIAddress,
		EnvControlAddress:        snapshot.ControlAddress,
		EnvPostgresDataDir:       snapshot.PostgresDataDir,
		EnvPostgresBinDir:        snapshot.PostgresBinDir,
		EnvPostgresListenAddress: snapshot.PostgresListenAddress,
		EnvPostgresPort:          formatOptionalInt(snapshot.PostgresPort),
		EnvClusterName:           snapshot.ClusterName,
		EnvInitialPrimary:        snapshot.InitialPrimary,
		EnvSeedAddresses:         snapshot.SeedAddresses,
		EnvExpectedMembers:       snapshot.ExpectedMembers,
	}
}

// LoadSnapshotFromEnv reads the raw PostgreSQL-extension environment exported
// by the PACMAN background worker bootstrap.
func LoadSnapshotFromEnv(lookup func(string) (string, bool)) (Snapshot, error) {
	if lookup == nil {
		lookup = emptyEnvLookup
	}

	port, err := parseOptionalIntEnv(lookup, EnvPostgresPort)
	if err != nil {
		return Snapshot{}, err
	}

	return Snapshot{
		NodeName:              lookupTrimmed(lookup, EnvNodeName),
		NodeRole:              lookupTrimmed(lookup, EnvNodeRole),
		APIAddress:            lookupTrimmed(lookup, EnvAPIAddress),
		ControlAddress:        lookupTrimmed(lookup, EnvControlAddress),
		PostgresDataDir:       lookupTrimmed(lookup, EnvPostgresDataDir),
		PostgresBinDir:        lookupTrimmed(lookup, EnvPostgresBinDir),
		PostgresListenAddress: lookupTrimmed(lookup, EnvPostgresListenAddress),
		PostgresPort:          port,
		ClusterName:           lookupTrimmed(lookup, EnvClusterName),
		InitialPrimary:        lookupTrimmed(lookup, EnvInitialPrimary),
		SeedAddresses:         lookupTrimmed(lookup, EnvSeedAddresses),
		ExpectedMembers:       lookupTrimmed(lookup, EnvExpectedMembers),
	}, nil
}

func emptyEnvLookup(string) (string, bool) {
	return "", false
}

func lookupTrimmed(lookup func(string) (string, bool), key string) string {
	value, _ := lookup(key)
	return strings.TrimSpace(value)
}

func parseOptionalIntEnv(lookup func(string) (string, bool), key string) (int, error) {
	value, ok := lookup(key)
	if !ok {
		return 0, nil
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}

	return parsed, nil
}

func formatOptionalInt(value int) string {
	if value == 0 {
		return ""
	}

	return strconv.Itoa(value)
}
