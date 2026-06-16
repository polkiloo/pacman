package config

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"log/slog"
	"strings"
)

const redactedSecretValue = "<redacted>"

type redactedConfig Config

// Redacted returns a copy of the config with secret-bearing fields masked so it
// can be logged or formatted safely.
func (config Config) Redacted() Config {
	redacted := config

	if redacted.DCS != nil {
		dcs := redacted.DCS.Redacted()
		redacted.DCS = &dcs
	}

	if redacted.Security != nil {
		security := redacted.Security.Redacted()
		redacted.Security = &security
	}

	if redacted.Reinit != nil {
		reinit := redacted.Reinit.Redacted()
		redacted.Reinit = &reinit
	}

	return redacted
}

// HasInlineSecrets reports whether the config document directly embeds secret
// material rather than referencing it indirectly by file path.
func (config Config) HasInlineSecrets() bool {
	return (config.DCS != nil && config.DCS.HasInlineSecrets()) ||
		(config.Security != nil && config.Security.HasInlineSecrets()) ||
		(config.Reinit != nil && config.Reinit.HasInlineSecrets())
}

// LogValue implements slog.LogValuer so structured logging redacts secret
// material by default.
func (config Config) LogValue() slog.Value {
	return slog.AnyValue(redactedConfig(config.Redacted()))
}

// String implements fmt.Stringer so ad hoc formatting redacts secret material
// by default.
func (config Config) String() string {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	// Config has a fixed schema of JSON-encodable fields, so encoding failure is
	// not expected here.
	_ = encoder.Encode(redactedConfig(config.Redacted()))

	return strings.TrimSpace(buffer.String())
}

// GoString implements fmt.GoStringer so %#v formatting redacts secret
// material by default.
func (config Config) GoString() string {
	return config.String()
}

// Redacted returns a copy of the security config with secret-bearing fields
// masked so it can be logged or formatted safely.
func (security SecurityConfig) Redacted() SecurityConfig {
	redacted := security

	if strings.TrimSpace(redacted.AdminBearerToken) != "" {
		redacted.AdminBearerToken = redactedSecretValue
	}

	if strings.TrimSpace(redacted.AdminBearerTokenFile) != "" {
		redacted.AdminBearerTokenFile = redactedSecretValue
	}

	return redacted
}

// HasInlineSecrets reports whether the security config directly embeds secret
// material rather than referencing it by file path.
func (security SecurityConfig) HasInlineSecrets() bool {
	return strings.TrimSpace(security.AdminBearerToken) != ""
}

// Redacted returns a copy of the reinit config with secret-bearing fields
// masked so it can be logged or formatted safely.
func (reinit ReinitConfig) Redacted() ReinitConfig {
	redacted := reinit

	if redacted.WALG != nil {
		walg := redacted.WALG.Redacted()
		redacted.WALG = &walg
	}

	return redacted
}

// HasInlineSecrets reports whether the reinit config directly embeds secret
// material rather than referencing it by file path or environment name.
func (reinit ReinitConfig) HasInlineSecrets() bool {
	return reinit.WALG != nil && reinit.WALG.HasInlineSecrets()
}

// Redacted returns a copy of the WAL-G config with secret-bearing fields masked
// so it can be logged or formatted safely.
func (walg WALGConfig) Redacted() WALGConfig {
	redacted := walg
	redacted.Credentials = redacted.Credentials.Redacted()

	return redacted
}

// HasInlineSecrets reports whether the WAL-G config directly embeds secret
// material rather than referencing it by file path or environment name.
func (walg WALGConfig) HasInlineSecrets() bool {
	return walg.Credentials.HasInlineSecrets()
}

// Redacted returns a copy of the WAL-G credentials config with secret-bearing
// fields masked so it can be logged or formatted safely.
func (credentials WALGCredentialsConfig) Redacted() WALGCredentialsConfig {
	redacted := credentials

	if redacted.Environment != nil {
		redacted.Environment = make(map[string]string, len(credentials.Environment))
		for name, value := range credentials.Environment {
			if strings.TrimSpace(value) == "" {
				redacted.Environment[name] = value
				continue
			}
			redacted.Environment[name] = redactedSecretValue
		}
	}

	if redacted.EnvironmentFiles != nil {
		redacted.EnvironmentFiles = make(map[string]string, len(credentials.EnvironmentFiles))
		for name, path := range credentials.EnvironmentFiles {
			if strings.TrimSpace(path) == "" {
				redacted.EnvironmentFiles[name] = path
				continue
			}
			redacted.EnvironmentFiles[name] = redactedSecretValue
		}
	}

	return redacted
}

// HasInlineSecrets reports whether the WAL-G credential config directly embeds
// secret material rather than referencing it by file path or environment name.
func (credentials WALGCredentialsConfig) HasInlineSecrets() bool {
	for _, value := range credentials.Environment {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}

	return false
}

func validateSensitiveFileMode(mode fs.FileMode) error {
	if mode.Perm()&0o077 != 0 {
		return ErrSensitiveConfigFilePermissions
	}

	return nil
}
