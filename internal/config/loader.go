package config

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type configFile interface {
	io.Reader
	Stat() (os.FileInfo, error)
	Close() error
}

// DocumentFormat identifies the source configuration schema decoded from a
// configuration document.
type DocumentFormat string

const (
	// DocumentFormatPACMAN indicates a native PACMAN node configuration file.
	DocumentFormatPACMAN DocumentFormat = "pacman"
	// DocumentFormatPatroni indicates a Patroni configuration file translated to
	// PACMAN runtime config at load time.
	DocumentFormatPatroni DocumentFormat = "patroni"
)

// DecodeReport captures the translated PACMAN runtime config plus any
// migration diagnostics produced while decoding the source document.
type DecodeReport struct {
	Config   Config
	Format   DocumentFormat
	Warnings []string
}

var openConfigFile = func(path string) (configFile, error) {
	return os.Open(path)
}

// Load reads a PACMAN-compatible node configuration document from disk.
func Load(path string) (Config, error) {
	report, err := LoadWithReport(path)
	if err != nil {
		return Config{}, err
	}

	return report.Config, nil
}

// LoadWithReport reads a PACMAN or Patroni node configuration document from
// disk and returns the translated PACMAN runtime config plus any warnings.
func LoadWithReport(path string) (DecodeReport, error) {
	file, err := openConfigFile(path)
	if err != nil {
		return DecodeReport{}, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return DecodeReport{}, fmt.Errorf("stat config file %q: %w", path, err)
	}

	report, err := DecodeWithReport(file)
	if err != nil {
		return DecodeReport{}, fmt.Errorf("decode config file %q: %w", path, err)
	}

	if report.Config.HasInlineSecrets() {
		if err := validateSensitiveFileMode(info.Mode()); err != nil {
			return DecodeReport{}, fmt.Errorf("validate sensitive config file %q: %w", path, err)
		}
	}

	return report, nil
}

// Decode parses a PACMAN-compatible node configuration document from a reader.
func Decode(reader io.Reader) (Config, error) {
	report, err := DecodeWithReport(reader)
	if err != nil {
		return Config{}, err
	}

	return report.Config, nil
}

// DecodeWithReport parses a PACMAN or Patroni configuration document from a
// reader and returns the translated PACMAN runtime config plus any warnings.
func DecodeWithReport(reader io.Reader) (DecodeReport, error) {
	payload, err := io.ReadAll(reader)
	if err != nil {
		return DecodeReport{}, fmt.Errorf("read config document: %w", err)
	}

	switch detectDocumentFormat(payload) {
	case DocumentFormatPatroni:
		return decodePatroniConfig(payload)
	default:
		return decodePACMANConfig(payload)
	}
}

type documentFormatProbe struct {
	APIVersion string         `yaml:"apiVersion"`
	Kind       string         `yaml:"kind"`
	Node       map[string]any `yaml:"node"`
	DCS        map[string]any `yaml:"dcs"`
	TLS        map[string]any `yaml:"tls"`
	Security   map[string]any `yaml:"security"`
	Postgres   map[string]any `yaml:"postgres"`

	Scope      string         `yaml:"scope"`
	RestAPI    map[string]any `yaml:"restapi"`
	PostgreSQL map[string]any `yaml:"postgresql"`
	Etcd       map[string]any `yaml:"etcd"`
	Etcd3      map[string]any `yaml:"etcd3"`
	Raft       map[string]any `yaml:"raft"`
	Consul     map[string]any `yaml:"consul"`
	Zookeeper  map[string]any `yaml:"zookeeper"`
	Exhibitor  map[string]any `yaml:"exhibitor"`
	Kubernetes map[string]any `yaml:"kubernetes"`
}

func detectDocumentFormat(payload []byte) DocumentFormat {
	var probe documentFormatProbe
	if err := yaml.Unmarshal(payload, &probe); err != nil {
		return DocumentFormatPACMAN
	}

	if probe.APIVersion != "" ||
		probe.Kind != "" ||
		probe.Node != nil ||
		probe.DCS != nil ||
		probe.TLS != nil ||
		probe.Security != nil ||
		probe.Postgres != nil {
		return DocumentFormatPACMAN
	}

	if probe.Scope != "" ||
		probe.RestAPI != nil ||
		probe.PostgreSQL != nil ||
		probe.Etcd != nil ||
		probe.Etcd3 != nil ||
		probe.Raft != nil ||
		probe.Consul != nil ||
		probe.Zookeeper != nil ||
		probe.Exhibitor != nil ||
		probe.Kubernetes != nil {
		return DocumentFormatPatroni
	}

	return DocumentFormatPACMAN
}

func decodePACMANConfig(payload []byte) (DecodeReport, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(payload))
	decoder.KnownFields(true)

	var config Config
	if err := decoder.Decode(&config); err != nil {
		return DecodeReport{}, fmt.Errorf("decode config document: %w", err)
	}

	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		return DecodeReport{}, fmt.Errorf("validate config document: %w", err)
	}

	return DecodeReport{
		Config: config,
		Format: DocumentFormatPACMAN,
	}, nil
}
