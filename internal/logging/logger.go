package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/polkiloo/pacman/internal/version"
)

const defaultService = "pacman"
const envLogLevel = "PACMAN_LOG_LEVEL"

// Options controls logger construction behavior.
type Options struct {
	Level slog.Leveler
}

// New constructs a process logger with stable build metadata attached.
func New(service string, output io.Writer) *slog.Logger {
	return NewWithOptions(service, output, Options{
		Level: resolveLogLevel(os.LookupEnv),
	})
}

// NewWithOptions constructs a process logger with explicit handler options.
func NewWithOptions(service string, output io.Writer, options Options) *slog.Logger {
	if strings.TrimSpace(service) == "" {
		service = defaultService
	}

	level := options.Level
	if level == nil {
		level = slog.LevelInfo
	}

	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: ReplaceAttr,
	})).With(
		slog.String("service", service),
		slog.String("version", version.Version),
		slog.String("commit", version.Commit),
		slog.String("build_date", version.BuildDate),
	)
}

func resolveLogLevel(lookup func(string) (string, bool)) slog.Leveler {
	value, ok := lookup(envLogLevel)
	if !ok {
		return slog.LevelInfo
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug
	case "", "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
