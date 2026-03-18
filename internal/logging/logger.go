package logging

import (
	"io"
	"log/slog"
	"strings"

	"github.com/polkiloo/pacman/internal/version"
)

const defaultService = "pacman"

// New constructs a process logger with stable build metadata attached.
func New(service string, output io.Writer) *slog.Logger {
	if strings.TrimSpace(service) == "" {
		service = defaultService
	}

	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With(
		slog.String("service", service),
		slog.String("version", version.Version),
		slog.String("commit", version.Commit),
		slog.String("build_date", version.BuildDate),
	)
}
