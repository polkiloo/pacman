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

	if redacted.Security != nil {
		security := redacted.Security.Redacted()
		redacted.Security = &security
	}

	return redacted
}

// HasInlineSecrets reports whether the config document directly embeds secret
// material rather than referencing it indirectly by file path.
func (config Config) HasInlineSecrets() bool {
	return config.Security != nil && config.Security.HasInlineSecrets()
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

func validateSensitiveFileMode(mode fs.FileMode) error {
	if mode.Perm()&0o077 != 0 {
		return ErrSensitiveConfigFilePermissions
	}

	return nil
}
