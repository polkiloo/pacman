package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

var unsafeLocalPostgresParameters = map[string]struct{}{
	"archive_cleanup_command":   {},
	"cluster_name":              {},
	"hot_standby":               {},
	"listen_addresses":          {},
	"port":                      {},
	"primary_conninfo":          {},
	"primary_slot_name":         {},
	"promote_trigger_file":      {},
	"recovery_target_timeline":  {},
	"restore_command":           {},
	"synchronous_standby_names": {},
	"trigger_file":              {},
	"wal_level":                 {},
}

// Validate reports whether the node configuration document is coherent enough
// to start pacmand with.
func (config Config) Validate() error {
	if strings.TrimSpace(config.APIVersion) != APIVersionV1Alpha1 {
		return ErrUnsupportedAPIVersion
	}

	if strings.TrimSpace(config.Kind) != KindNodeConfig {
		return ErrUnexpectedKind
	}

	if err := config.Node.Validate(); err != nil {
		return err
	}

	if config.TLS != nil {
		if err := config.TLS.Validate(); err != nil {
			return err
		}
	}

	if config.Postgres != nil {
		if err := config.Postgres.Validate(); err != nil {
			return err
		}
	}

	if config.Bootstrap != nil {
		if err := config.Bootstrap.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate reports whether the local node configuration is coherent enough to
// bootstrap pacmand.
func (node NodeConfig) Validate() error {
	if strings.TrimSpace(node.Name) == "" {
		return ErrNodeNameRequired
	}

	if node.Role == "" {
		return ErrNodeRoleRequired
	}

	if !node.Role.IsValid() {
		return ErrNodeRoleInvalid
	}

	if strings.TrimSpace(node.APIAddress) == "" {
		return ErrNodeAPIAddressRequired
	}

	if !isValidListenAddress(node.APIAddress) {
		return ErrNodeAPIAddressInvalid
	}

	if strings.TrimSpace(node.ControlAddress) == "" {
		return ErrNodeControlAddressRequired
	}

	if !isValidListenAddress(node.ControlAddress) {
		return ErrNodeControlAddressInvalid
	}

	return nil
}

// Validate reports whether the node-local TLS configuration is coherent enough
// for pacmand to consume.
func (tls TLSConfig) Validate() error {
	if strings.TrimSpace(tls.CertFile) == "" && strings.TrimSpace(tls.KeyFile) != "" {
		return ErrTLSCertFileRequired
	}

	if strings.TrimSpace(tls.KeyFile) == "" && strings.TrimSpace(tls.CertFile) != "" {
		return ErrTLSKeyFileRequired
	}

	if tls.Enabled && strings.TrimSpace(tls.CertFile) == "" {
		return ErrTLSCertFileRequired
	}

	if tls.Enabled && strings.TrimSpace(tls.KeyFile) == "" {
		return ErrTLSKeyFileRequired
	}

	return nil
}

// Validate reports whether the local PostgreSQL configuration is coherent
// enough for pacmand to consume safely.
func (postgres PostgresLocalConfig) Validate() error {
	if strings.TrimSpace(postgres.DataDir) == "" {
		return ErrPostgresDataDirRequired
	}

	if strings.TrimSpace(postgres.ListenAddress) == "" {
		return ErrPostgresListenAddressRequired
	}

	if !isValidHost(postgres.ListenAddress) {
		return ErrPostgresListenAddressInvalid
	}

	if postgres.Port < 1 || postgres.Port > 65535 {
		return ErrPostgresPortOutOfRange
	}

	for key := range postgres.Parameters {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if _, unsafe := unsafeLocalPostgresParameters[normalized]; unsafe {
			return fmt.Errorf("%w: %s", ErrUnsafeClusterParameterOverride, normalized)
		}
	}

	return nil
}

// Validate reports whether the cluster bootstrap configuration is coherent
// enough to form an initial PACMAN cluster.
func (bootstrap ClusterBootstrapConfig) Validate() error {
	if strings.TrimSpace(bootstrap.ClusterName) == "" {
		return ErrBootstrapClusterNameRequired
	}

	if strings.TrimSpace(bootstrap.InitialPrimary) == "" {
		return ErrBootstrapInitialPrimaryRequired
	}

	if len(bootstrap.SeedAddresses) == 0 {
		return ErrBootstrapSeedAddressRequired
	}

	for _, address := range bootstrap.SeedAddresses {
		if !isValidListenAddress(address) {
			return ErrBootstrapSeedAddressInvalid
		}
	}

	if len(bootstrap.ExpectedMembers) == 0 {
		return ErrBootstrapExpectedMembersRequired
	}

	foundInitialPrimary := false
	for _, member := range bootstrap.ExpectedMembers {
		if strings.TrimSpace(member) == "" {
			return ErrBootstrapExpectedMemberInvalid
		}

		if member == bootstrap.InitialPrimary {
			foundInitialPrimary = true
		}
	}

	if !foundInitialPrimary {
		return ErrBootstrapInitialPrimaryUnknown
	}

	return nil
}

func isValidListenAddress(address string) bool {
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

func isValidHost(host string) bool {
	trimmed := strings.TrimSpace(host)
	return trimmed != "" && !strings.Contains(trimmed, ":")
}
