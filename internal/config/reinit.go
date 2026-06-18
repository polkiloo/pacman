package config

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

const (
	walgS3PrefixEnv   = "WALG_S3_PREFIX"
	walgGSPrefixEnv   = "WALG_GS_PREFIX"
	walgAZPrefixEnv   = "WALG_AZ_PREFIX"
	walgFilePrefixEnv = "WALG_FILE_PREFIX"
	awsEndpointEnv    = "AWS_ENDPOINT"
	awsRegionEnv      = "AWS_REGION"
)

// RestoreBackupName returns the WAL-G backup selector used for backup-fetch.
func (walg WALGConfig) RestoreBackupName() string {
	return walg.Restore.WithDefaults().BackupName
}

// BackupFetchCommand returns the executable and arguments for a WAL-G base
// backup fetch. The command is not executed here; reinit workflow code is
// responsible for stopping PostgreSQL and preparing the data directory first.
func (walg WALGConfig) BackupFetchCommand(dataDir string) (string, []string, error) {
	binary := strings.TrimSpace(walg.WithDefaults().Binary)
	if binary == "" {
		return "", nil, ErrReinitWALGBinaryRequired
	}

	trimmedDataDir := strings.TrimSpace(dataDir)
	if trimmedDataDir == "" {
		return "", nil, ErrReinitWALGRestoreDataDirRequired
	}

	return binary, []string{"backup-fetch", trimmedDataDir, walg.RestoreBackupName()}, nil
}

// WALFetchRestoreCommand returns a PostgreSQL restore_command that invokes
// WAL-G for archive recovery. The command embeds the resolved repository
// environment so PostgreSQL can run it before pacmand starts the server.
func (walg WALGConfig) WALFetchRestoreCommand(
	lookupEnv func(string) (string, bool),
	readFile func(string) ([]byte, error),
) (string, error) {
	defaulted := walg.WithDefaults()
	binary := strings.TrimSpace(defaulted.Binary)
	if binary == "" {
		return "", ErrReinitWALGBinaryRequired
	}

	env, err := defaulted.RestoreEnvironment(lookupEnv, readFile)
	if err != nil {
		return "", err
	}

	parts := []string{"env"}
	parts = append(parts, shellEnvironmentAssignments(env)...)
	parts = append(parts,
		shellQuote(binary),
		"wal-fetch",
		shellQuote("%f"),
		shellQuote("%p"),
	)

	return strings.Join(parts, " "), nil
}

// RestoreEnvironment returns the WAL-G environment selected by the repository
// and credential configuration.
func (walg WALGConfig) RestoreEnvironment(
	lookupEnv func(string) (string, bool),
	readFile func(string) ([]byte, error),
) (map[string]string, error) {
	if err := walg.Validate(); err != nil {
		return nil, err
	}

	env := walg.Repository.Environment()
	credentials, err := walg.Credentials.ResolveEnvironment(lookupEnv, readFile)
	if err != nil {
		return nil, err
	}

	for name, value := range credentials {
		env[name] = value
	}

	return env, nil
}

// Environment returns the WAL-G environment variables implied by the
// repository settings.
func (repository WALGRepositoryConfig) Environment() map[string]string {
	env := make(map[string]string)
	prefix := strings.TrimSpace(repository.Prefix)

	switch repository.Provider {
	case WALGRepositoryProviderS3:
		env[walgS3PrefixEnv] = prefix
		if endpoint := strings.TrimSpace(repository.Endpoint); endpoint != "" {
			env[awsEndpointEnv] = endpoint
		}
		if region := strings.TrimSpace(repository.Region); region != "" {
			env[awsRegionEnv] = region
		}
	case WALGRepositoryProviderGCS:
		env[walgGSPrefixEnv] = prefix
	case WALGRepositoryProviderAzure:
		env[walgAZPrefixEnv] = prefix
	case WALGRepositoryProviderFilesystem:
		env[walgFilePrefixEnv] = prefix
	}

	return env
}

func shellEnvironmentAssignments(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}

	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)

	assignments := make([]string, 0, len(names))
	for _, name := range names {
		assignments = append(assignments, shellQuote(name+"="+env[name]))
	}

	return assignments
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}

	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

// ResolveEnvironment returns the WAL-G environment variables described by the
// configured credential sources. Inherited variables are read first, then
// inline values, then file-backed values. Validate rejects overlapping names, so
// a later source should not overwrite an earlier source after validation.
func (credentials WALGCredentialsConfig) ResolveEnvironment(
	lookupEnv func(string) (string, bool),
	readFile func(string) ([]byte, error),
) (map[string]string, error) {
	if err := credentials.Validate(); err != nil {
		return nil, err
	}

	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}
	if readFile == nil {
		readFile = os.ReadFile
	}

	resolved := make(map[string]string)
	for _, name := range credentials.InheritEnvironment {
		normalized, _ := normalizeEnvironmentName(name)
		if value, ok := lookupEnv(normalized); ok {
			resolved[normalized] = value
		}
	}

	for name, value := range credentials.Environment {
		normalized, _ := normalizeEnvironmentName(name)
		resolved[normalized] = strings.TrimSpace(value)
	}

	for name, path := range credentials.EnvironmentFiles {
		normalized, _ := normalizeEnvironmentName(name)
		trimmedPath := strings.TrimSpace(path)
		payload, err := readFile(trimmedPath)
		if err != nil {
			return nil, fmt.Errorf("read WAL-G credential file %q for %s: %w", trimmedPath, normalized, err)
		}

		value := strings.TrimSpace(string(payload))
		if value == "" {
			return nil, fmt.Errorf("read WAL-G credential file %q for %s: credential is empty", trimmedPath, normalized)
		}
		resolved[normalized] = value
	}

	return resolved, nil
}
