package postgres

import (
	"path/filepath"
	"strings"
)

const (
	DefaultRecoveryTargetTimeline = "latest"
	PostgresAutoConfFileName      = "postgresql.auto.conf"
	StandbySignalFileName         = "standby.signal"
)

// StandbyConfig describes the local PostgreSQL settings needed to start a node
// as a streaming replica.
type StandbyConfig struct {
	PrimaryConnInfo        string
	PrimarySlotName        string
	RestoreCommand         string
	RecoveryTargetTimeline string
}

// RenderedStandbyFiles contains the local standby artifacts PACMAN should
// place into the PostgreSQL data directory.
type RenderedStandbyFiles struct {
	PostgresAutoConfPath string
	PostgresAutoConf     string
	StandbySignalPath    string
}

// WithDefaults returns a copy of the standby config with omitted fields filled
// using PostgreSQL-compatible defaults.
func (config StandbyConfig) WithDefaults() StandbyConfig {
	defaulted := config
	if strings.TrimSpace(defaulted.RecoveryTargetTimeline) == "" {
		defaulted.RecoveryTargetTimeline = DefaultRecoveryTargetTimeline
	}

	return defaulted
}

// Validate reports whether the standby config is coherent enough to render.
func (config StandbyConfig) Validate() error {
	if strings.TrimSpace(config.PrimaryConnInfo) == "" {
		return ErrPrimaryConnInfoRequired
	}

	return nil
}

// RenderStandbyFiles renders the local standby configuration artifacts for the
// provided PostgreSQL data directory without touching the filesystem.
func RenderStandbyFiles(dataDir string, config StandbyConfig) (RenderedStandbyFiles, error) {
	trimmedDataDir := strings.TrimSpace(dataDir)
	if trimmedDataDir == "" {
		return RenderedStandbyFiles{}, ErrDataDirRequired
	}

	defaulted := config.WithDefaults()
	if err := defaulted.Validate(); err != nil {
		return RenderedStandbyFiles{}, err
	}

	lines := []string{
		renderConfigLine("primary_conninfo", defaulted.PrimaryConnInfo),
	}

	if slotName := strings.TrimSpace(defaulted.PrimarySlotName); slotName != "" {
		lines = append(lines, renderConfigLine("primary_slot_name", slotName))
	}

	if restoreCommand := strings.TrimSpace(defaulted.RestoreCommand); restoreCommand != "" {
		lines = append(lines, renderConfigLine("restore_command", restoreCommand))
	}

	lines = append(lines, renderConfigLine("recovery_target_timeline", defaulted.RecoveryTargetTimeline))

	return RenderedStandbyFiles{
		PostgresAutoConfPath: filepath.Join(trimmedDataDir, PostgresAutoConfFileName),
		PostgresAutoConf:     strings.Join(lines, "\n") + "\n",
		StandbySignalPath:    filepath.Join(trimmedDataDir, StandbySignalFileName),
	}, nil
}

func renderConfigLine(key, value string) string {
	return key + " = " + quoteConfigValue(value)
}

func quoteConfigValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `''`)
	return "'" + escaped + "'"
}
