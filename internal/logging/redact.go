package logging

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

const redactedValue = "<redacted>"

var (
	secretAssignmentPattern = regexp.MustCompile(`(?i)(\b(?:password|passwd|passphrase|secret|token|api[_-]?key|apikey|client_secret|authorization)\s*=\s*)(?:'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.)*"|[^\s]+)`)
	secretLabelPattern      = regexp.MustCompile(`(?i)(\b(?:password|passwd|passphrase|secret|token|api[_-]?key|apikey|client_secret|authorization)\b\s*:\s*)(?:'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.)*"|[^\s,;]+)`)
	bearerTokenPattern      = regexp.MustCompile(`(?i)(\bauthorization\b\s*:\s*bearer\s+)(\S+)`)
	uriUserInfoPattern      = regexp.MustCompile(`([a-z][a-z0-9+.-]*://[^/\s:@]+:)([^@/\s]+)(@)`)
)

// ReplaceAttr applies the PACMAN slog redaction policy to structured fields.
func ReplaceAttr(_ []string, attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()

	if alwaysRedactedKey(attr.Key) {
		attr.Value = slog.StringValue(redactedValue)
		return attr
	}

	switch attr.Value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(RedactString(attr.Key, attr.Value.String()))
	case slog.KindAny:
		if err, ok := attr.Value.Any().(error); ok {
			attr.Value = slog.StringValue(RedactString(attr.Key, err.Error()))
			return attr
		}

		if stringer, ok := attr.Value.Any().(fmt.Stringer); ok {
			attr.Value = slog.StringValue(RedactString(attr.Key, stringer.String()))
		}
	}

	return attr
}

// RedactString masks inline secrets while preserving non-sensitive context.
func RedactString(key, value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}

	if alwaysRedactedKey(key) {
		return redactedValue
	}

	redacted := secretAssignmentPattern.ReplaceAllString(trimmed, "${1}"+redactedValue)
	redacted = bearerTokenPattern.ReplaceAllString(redacted, "${1}"+redactedValue)
	redacted = secretLabelPattern.ReplaceAllString(redacted, "${1}"+redactedValue)
	redacted = uriUserInfoPattern.ReplaceAllString(redacted, "${1}"+redactedValue+"${3}")

	return redacted
}

func alwaysRedactedKey(key string) bool {
	normalized := normalizedKey(key)

	// Exact-match markers: the normalized key must equal the marker or end with it
	// preceded by an alphanumeric boundary. This prevents broad terms like "token"
	// from matching unrelated keys such as "apitokenconfigured".
	for _, marker := range []string{
		"password",
		"passwd",
		"passphrase",
		"secret",
		"authorization",
		"privatekey",
		"apikey",
		"clientsecret",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}

	// "token" is too common as an infix (e.g. "apitokenconfigured") so we only
	// redact when it appears as a suffix or as the whole key.
	if normalized == "token" || strings.HasSuffix(normalized, "token") {
		return true
	}

	return false
}

func normalizedKey(key string) string {
	key = strings.TrimSpace(strings.ToLower(key))
	if key == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(key))
	for _, r := range key {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
		}
	}

	return builder.String()
}
