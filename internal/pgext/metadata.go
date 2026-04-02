package pgext

const (
	// ExtensionName is the PostgreSQL extension and shared_preload_libraries
	// identifier for the PACMAN background worker.
	ExtensionName = "pacman_agent"
	// ExtensionVersion is the current scaffolded SQL/control version.
	ExtensionVersion = "0.1.0"
	// WorkerName is the display name registered with PostgreSQL.
	WorkerName = "PACMAN local agent"
	// RepositoryLayout points to the in-repo PGXS extension sources.
	RepositoryLayout = "postgresql/pacman_agent"
	// SupportedPostgresMajorMin is the lowest PostgreSQL major version the
	// scaffold currently targets.
	SupportedPostgresMajorMin = 17
	// SupportedPostgresMajorMax is the highest PostgreSQL major version the
	// scaffold currently targets.
	SupportedPostgresMajorMax = 17
)

const (
	GUCNodeName              = "pacman.node_name"
	GUCNodeRole              = "pacman.node_role"
	GUCAPIAddress            = "pacman.api_address"
	GUCControlAddress        = "pacman.control_address"
	GUCHelperPath            = "pacman.helper_path"
	GUCPostgresDataDir       = "pacman.postgres_data_dir"
	GUCPostgresBinDir        = "pacman.postgres_bin_dir"
	GUCPostgresListenAddress = "pacman.postgres_listen_address"
	GUCPostgresPort          = "pacman.postgres_port"
	GUCClusterName           = "pacman.cluster_name"
	GUCInitialPrimary        = "pacman.initial_primary"
	GUCSeedAddresses         = "pacman.seed_addresses"
	GUCExpectedMembers       = "pacman.expected_members"
)

// SupportsPostgresMajor reports whether the extension scaffold is validated for
// the provided PostgreSQL major version.
func SupportsPostgresMajor(major int) bool {
	return major >= SupportedPostgresMajorMin && major <= SupportedPostgresMajorMax
}
