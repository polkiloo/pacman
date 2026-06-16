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

	if config.DCS != nil {
		if err := config.DCS.Validate(); err != nil {
			return err
		}

		if config.Bootstrap != nil &&
			strings.TrimSpace(config.Bootstrap.ClusterName) != "" &&
			strings.TrimSpace(config.DCS.ClusterName) != strings.TrimSpace(config.Bootstrap.ClusterName) {
			return ErrDCSClusterNameMismatch
		}
	}

	if config.TLS != nil {
		if err := config.TLS.Validate(); err != nil {
			return err
		}
	}

	if config.Security != nil {
		if err := config.Security.Validate(); err != nil {
			return err
		}

		if config.Security.PeerMTLSEnabled() {
			if config.TLS == nil || !config.TLS.Enabled {
				return ErrSecurityMemberMTLSRequiresTLS
			}

			if strings.TrimSpace(config.TLS.CAFile) == "" {
				return ErrSecurityMemberMTLSCAFileRequired
			}

			if config.Bootstrap == nil || len(config.Bootstrap.ExpectedMembers) == 0 {
				return ErrSecurityMemberMTLSBootstrapRequired
			}

			nodeListed := false
			for _, member := range config.Bootstrap.ExpectedMembers {
				if strings.TrimSpace(member) == strings.TrimSpace(config.Node.Name) {
					nodeListed = true
					break
				}
			}

			if !nodeListed {
				return ErrSecurityMemberMTLSNodeUnknown
			}
		}
	}

	if config.Postgres != nil {
		if err := config.Postgres.Validate(); err != nil {
			return err
		}
	}

	if config.Reinit != nil {
		if err := config.Reinit.Validate(); err != nil {
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

// Validate reports whether the node-local security configuration is coherent
// enough for pacmand to consume safely.
func (security SecurityConfig) Validate() error {
	hasInlineToken := strings.TrimSpace(security.AdminBearerToken) != ""
	hasTokenFile := strings.TrimSpace(security.AdminBearerTokenFile) != ""

	if hasInlineToken && hasTokenFile {
		return ErrSecurityAdminBearerTokenConflict
	}

	if !hasInlineToken && !hasTokenFile && !security.MemberMTLSEnabled {
		return ErrSecurityAdminBearerTokenRequired
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

	if strings.TrimSpace(postgres.ReplicationPassword) != "" && strings.TrimSpace(postgres.ReplicationUser) == "" {
		return ErrPostgresReplicationUserRequired
	}

	for key := range postgres.Parameters {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if _, unsafe := unsafeLocalPostgresParameters[normalized]; unsafe {
			return fmt.Errorf("%w: %s", ErrUnsafeClusterParameterOverride, normalized)
		}
	}

	return nil
}

// Validate reports whether the local reinit configuration is coherent enough
// for pacmand to use when executing destructive replica reinitialization.
func (reinit ReinitConfig) Validate() error {
	if reinit.WALG == nil {
		return ErrReinitWALGConfigRequired
	}

	return reinit.WALG.Validate()
}

// Validate reports whether the WAL-G reinit configuration is coherent enough
// to plan a restore workflow.
func (walg WALGConfig) Validate() error {
	if strings.TrimSpace(walg.Binary) == "" {
		return ErrReinitWALGBinaryRequired
	}

	if err := walg.Repository.Validate(); err != nil {
		return err
	}

	return walg.Credentials.Validate()
}

// Validate reports whether the WAL-G repository settings identify a supported
// backup repository.
func (repository WALGRepositoryConfig) Validate() error {
	if strings.TrimSpace(string(repository.Provider)) == "" {
		return ErrReinitWALGRepositoryProviderRequired
	}

	if !repository.Provider.IsValid() {
		return ErrReinitWALGRepositoryProviderInvalid
	}

	if strings.TrimSpace(repository.Prefix) == "" {
		return ErrReinitWALGRepositoryPrefixRequired
	}

	return nil
}

// IsValid reports whether the repository provider is supported.
func (provider WALGRepositoryProvider) IsValid() bool {
	switch provider {
	case WALGRepositoryProviderS3,
		WALGRepositoryProviderGCS,
		WALGRepositoryProviderAzure,
		WALGRepositoryProviderFilesystem:
		return true
	default:
		return false
	}
}

// Validate reports whether the WAL-G credential source configuration is
// unambiguous and uses valid environment variable names.
func (credentials WALGCredentialsConfig) Validate() error {
	seen := make(map[string]struct{})

	for _, name := range credentials.InheritEnvironment {
		normalized, err := normalizeEnvironmentName(name)
		if err != nil {
			return err
		}
		if _, ok := seen[normalized]; ok {
			return ErrReinitWALGCredentialSourceConflict
		}
		seen[normalized] = struct{}{}
	}

	for name, value := range credentials.Environment {
		normalized, err := normalizeEnvironmentName(name)
		if err != nil {
			return err
		}
		if strings.TrimSpace(value) == "" {
			return ErrReinitWALGCredentialValueRequired
		}
		if _, ok := seen[normalized]; ok {
			return ErrReinitWALGCredentialSourceConflict
		}
		seen[normalized] = struct{}{}
	}

	for name, path := range credentials.EnvironmentFiles {
		normalized, err := normalizeEnvironmentName(name)
		if err != nil {
			return err
		}
		if strings.TrimSpace(path) == "" {
			return ErrReinitWALGCredentialFileRequired
		}
		if _, ok := seen[normalized]; ok {
			return ErrReinitWALGCredentialSourceConflict
		}
		seen[normalized] = struct{}{}
	}

	return nil
}

func normalizeEnvironmentName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || !isValidEnvironmentName(trimmed) {
		return "", ErrReinitWALGCredentialNameInvalid
	}

	return trimmed, nil
}

func isValidEnvironmentName(name string) bool {
	for index, r := range name {
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' && index > 0 {
			continue
		}
		if r == '_' {
			continue
		}
		return false
	}

	return true
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
