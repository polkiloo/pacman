package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/polkiloo/pacman/internal/version"
)

func TestNewIncludesStructuredFields(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer

	logger := New("pacmand", &buffer)
	logger.Info("daemon starting", slog.String("component", "bootstrap"))

	entry := decodeEntry(t, buffer.Bytes())

	assertField(t, entry, "level", "INFO")
	assertField(t, entry, "msg", "daemon starting")
	assertField(t, entry, "service", "pacmand")
	assertField(t, entry, "version", version.Version)
	assertField(t, entry, "commit", version.Commit)
	assertField(t, entry, "build_date", version.BuildDate)
	assertField(t, entry, "component", "bootstrap")

	if _, ok := entry["time"]; !ok {
		t.Fatal("expected time field in structured log entry")
	}
}

func TestNewDefaultsServiceName(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer

	logger := New("   ", &buffer)
	logger.Info("default service")

	entry := decodeEntry(t, buffer.Bytes())

	assertField(t, entry, "service", defaultService)
}

func decodeEntry(t *testing.T, payload []byte) map[string]any {
	t.Helper()

	var entry map[string]any
	if err := json.Unmarshal(payload, &entry); err != nil {
		t.Fatalf("unmarshal structured log entry: %v", err)
	}

	return entry
}

func assertField(t *testing.T, entry map[string]any, key, want string) {
	t.Helper()

	got, ok := entry[key]
	if !ok {
		t.Fatalf("expected %q field in structured log entry", key)
	}

	gotString, ok := got.(string)
	if !ok {
		t.Fatalf("expected %q field to be a string, got %T", key, got)
	}

	if gotString != want {
		t.Fatalf("expected %q=%q, got %q", key, want, gotString)
	}
}
