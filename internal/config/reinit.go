package config

import (
	"fmt"
	"os"
	"strings"
)

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
