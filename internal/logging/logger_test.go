package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/fx"

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

func TestModuleProvidesLoggerUsingRegisteredStderr(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	type resolved struct {
		fx.In

		Logger *slog.Logger
	}

	var deps resolved
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() io.Writer { return io.Discard }),
		fx.Provide(func() struct {
			fx.Out

			Stderr io.Writer `name:"stderr"`
		} {
			return struct {
				fx.Out

				Stderr io.Writer `name:"stderr"`
			}{Stderr: &stderr}
		}),
		Module("pacmand"),
		fx.Populate(&deps),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	deps.Logger.Info("module logger ready")

	entry := decodeEntry(t, stderr.Bytes())
	assertField(t, entry, "service", "pacmand")
	assertField(t, entry, "msg", "module logger ready")
}

func TestModuleAppliesRegisteredMiddlewareInOrder(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	type resolved struct {
		fx.In

		Logger *slog.Logger
	}

	var deps resolved
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() struct {
			fx.Out

			Stderr io.Writer `name:"stderr"`
		} {
			return struct {
				fx.Out

				Stderr io.Writer `name:"stderr"`
			}{Stderr: &stderr}
		}),
		Module("pacmand"),
		ProvideMiddleware(WithAttrs(
			slog.String("component", "middleware"),
			slog.String("scope", "logging"),
		)),
		ProvideMiddleware(func(logger *slog.Logger) *slog.Logger {
			if logger == nil {
				return nil
			}

			return logger.With(slog.String("phase", "post-module"))
		}),
		fx.Populate(&deps),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build fx app: %v", err)
	}

	deps.Logger.Info("module logger with middleware")

	entry := decodeEntry(t, stderr.Bytes())
	assertField(t, entry, "service", "pacmand")
	assertField(t, entry, "msg", "module logger with middleware")
	assertField(t, entry, "component", "middleware")
	assertField(t, entry, "scope", "logging")
	assertField(t, entry, "phase", "post-module")
}

func TestModuleReturnsLoggerRegistrationError(t *testing.T) {
	t.Parallel()

	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() struct {
			fx.Out

			Stderr io.Writer `name:"stderr"`
		} {
			return struct {
				fx.Out

				Stderr io.Writer `name:"stderr"`
			}{Stderr: io.Discard}
		}),
		fx.Provide(func() *slog.Logger {
			return slog.New(slog.NewTextHandler(io.Discard, nil))
		}),
		Module("pacmand"),
	)
	if err := app.Err(); err == nil {
		t.Fatal("expected logger registration error")
	}
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
