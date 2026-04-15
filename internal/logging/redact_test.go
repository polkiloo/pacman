package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNewWithOptionsDebugLevelEmitsDebugLogs(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer

	logger := NewWithOptions("pacmand", &buffer, Options{Level: slog.LevelDebug})
	logger.Debug("debug reconciliation summary", slog.String("component", "controlplane"))

	entry := decodeEntry(t, buffer.Bytes())
	assertField(t, entry, "level", "DEBUG")
	assertField(t, entry, "msg", "debug reconciliation summary")
	assertField(t, entry, "component", "controlplane")
}

func TestNewWithOptionsInfoLevelSuppressesDebugLogs(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer

	logger := NewWithOptions("pacmand", &buffer, Options{Level: slog.LevelInfo})
	logger.Debug("hidden debug log", slog.String("component", "controlplane"))

	if got := strings.TrimSpace(buffer.String()); got != "" {
		t.Fatalf("expected debug log to be suppressed, got %q", got)
	}
}

func TestReplaceAttrRedactsSecretBearingKeys(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer

	logger := New("pacmand", &buffer)
	logger.Info(
		"loaded credentials",
		slog.String("password", "super-secret"),
		slog.Group("auth",
			slog.String("token", "abc123"),
			slog.String("authorization", "Bearer real-token"),
		),
		slog.String("error", "missing bearer token"),
	)

	logs := buffer.String()
	for _, leaked := range []string{"super-secret", "abc123", "real-token"} {
		if strings.Contains(logs, leaked) {
			t.Fatalf("expected logs to redact %q, got %q", leaked, logs)
		}
	}

	for _, want := range []string{
		`"password":"<redacted>"`,
		`"token":"<redacted>"`,
		`"authorization":"<redacted>"`,
		`"error":"missing bearer token"`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("expected logs to contain %q, got %q", want, logs)
		}
	}
}

func TestReplaceAttrScrubsSecretsInsideConnectionStrings(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer

	logger := New("pacmand", &buffer)
	logger.Info(
		"connection strings",
		slog.String("primary_conninfo", "host=primary port=5432 user=replicator password='pa''ss' application_name=pacmand"),
		slog.String("dsn", "postgres://pacman:secret-password@db.example.com:5432/pacman?sslmode=disable"),
	)

	logs := buffer.String()
	for _, leaked := range []string{"secret-password", "pa''ss"} {
		if strings.Contains(logs, leaked) {
			t.Fatalf("expected logs to redact %q, got %q", leaked, logs)
		}
	}

	for _, want := range []string{
		`"primary_conninfo":"host=primary port=5432 user=replicator password=<redacted> application_name=pacmand"`,
		`"dsn":"postgres://pacman:<redacted>@db.example.com:5432/pacman?sslmode=disable"`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("expected logs to contain %q, got %q", want, logs)
		}
	}
}

func TestResolveLogLevelDefaultsToInfoForUnknownValues(t *testing.T) {
	t.Parallel()

	level := resolveLogLevel(func(string) (string, bool) {
		return "trace", true
	})

	resolved, ok := level.(slog.Level)
	if !ok {
		t.Fatalf("expected slog.Level result, got %T", level)
	}

	if resolved != slog.LevelInfo {
		t.Fatalf("expected fallback log level info, got %v", resolved)
	}
}
