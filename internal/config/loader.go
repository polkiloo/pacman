package config

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// Load reads a PACMAN node configuration document from disk.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer file.Close()

	config, err := Decode(file)
	if err != nil {
		return Config{}, fmt.Errorf("decode config file %q: %w", path, err)
	}

	return config, nil
}

// Decode parses a PACMAN node configuration document from a reader.
func Decode(reader io.Reader) (Config, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode config document: %w", err)
	}

	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config document: %w", err)
	}

	return config, nil
}
