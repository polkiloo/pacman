package config

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"gopkg.in/yaml.v3"
)

type patroniConfig struct {
	Scope      string                  `yaml:"scope"`
	Name       string                  `yaml:"name"`
	RestAPI    *patroniRestAPIConfig   `yaml:"restapi"`
	Etcd       *patroniEtcdConfig      `yaml:"etcd"`
	Etcd3      *patroniEtcdConfig      `yaml:"etcd3"`
	Raft       *patroniRaftConfig      `yaml:"raft"`
	Bootstrap  *patroniBootstrapConfig `yaml:"bootstrap"`
	PostgreSQL *patroniPostgresConfig  `yaml:"postgresql"`
	Consul     map[string]any          `yaml:"consul"`
	Zookeeper  map[string]any          `yaml:"zookeeper"`
	Exhibitor  map[string]any          `yaml:"exhibitor"`
	Kubernetes map[string]any          `yaml:"kubernetes"`
}

type patroniRestAPIConfig struct {
	Listen         string `yaml:"listen"`
	ConnectAddress string `yaml:"connect_address"`
}

type patroniEtcdConfig struct {
	Host     string   `yaml:"host"`
	Hosts    []string `yaml:"hosts"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type patroniRaftConfig struct {
	DataDir      string   `yaml:"data_dir"`
	SelfAddr     string   `yaml:"self_addr"`
	PartnerAddrs []string `yaml:"partner_addrs"`
}

type patroniBootstrapConfig struct {
	DCS *patroniBootstrapDCSConfig `yaml:"dcs"`
}

type patroniBootstrapDCSConfig struct {
	TTL                  *int                         `yaml:"ttl"`
	RetryTimeout         *int                         `yaml:"retry_timeout"`
	MaximumLagOnFailover *int64                       `yaml:"maximum_lag_on_failover"`
	PostgreSQL           *patroniBootstrapPostgresDCS `yaml:"postgresql"`
}

type patroniBootstrapPostgresDCS struct {
	UsePGRewind *bool `yaml:"use_pg_rewind"`
}

type patroniPostgresConfig struct {
	Listen         string `yaml:"listen"`
	ConnectAddress string `yaml:"connect_address"`
	DataDir        string `yaml:"data_dir"`
	BinDir         string `yaml:"bin_dir"`
}

func decodePatroniConfig(payload []byte) (DecodeReport, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(payload))

	var source patroniConfig
	if err := decoder.Decode(&source); err != nil {
		return DecodeReport{}, fmt.Errorf("decode Patroni config document: %w", err)
	}

	config, warnings, err := translatePatroniConfig(source)
	if err != nil {
		return DecodeReport{}, err
	}

	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		return DecodeReport{}, fmt.Errorf("validate translated Patroni config: %w", err)
	}

	return DecodeReport{
		Config:   config,
		Format:   DocumentFormatPatroni,
		Warnings: warnings,
	}, nil
}

func translatePatroniConfig(source patroniConfig) (Config, []string, error) {
	warnings := patroniMigrationWarnings(source.Name)

	apiAddress, addressWarnings, err := selectPatroniAddress(
		"restapi.listen",
		source.RestAPI.listenAddress(),
		source.RestAPI.connectAddress(),
	)
	if err != nil {
		return Config{}, nil, err
	}
	warnings = append(warnings, addressWarnings...)

	postgresListenAddress, postgresPort, postgresWarnings, err := selectPatroniPostgresAddress(source.PostgreSQL)
	if err != nil {
		return Config{}, nil, err
	}
	warnings = append(warnings, postgresWarnings...)

	clusterName := strings.TrimSpace(source.Scope)
	translated := Config{
		Node: NodeConfig{
			Name:       strings.TrimSpace(source.Name),
			APIAddress: apiAddress,
		},
		DCS: &dcs.Config{
			ClusterName: clusterName,
		},
		Postgres: &PostgresLocalConfig{
			DataDir:       strings.TrimSpace(source.PostgreSQL.dataDir()),
			BinDir:        strings.TrimSpace(source.PostgreSQL.binDir()),
			ListenAddress: postgresListenAddress,
			Port:          postgresPort,
		},
		Bootstrap: &ClusterBootstrapConfig{
			ClusterName: clusterName,
		},
	}

	dcsConfig, dcsWarnings, err := translatePatroniDCS(source, clusterName)
	if err != nil {
		return Config{}, nil, err
	}
	translated.DCS = dcsConfig
	warnings = append(warnings, dcsWarnings...)

	if source.Bootstrap != nil && source.Bootstrap.DCS != nil {
		if ttl := source.Bootstrap.DCS.TTL; ttl != nil {
			translated.DCS.TTL = time.Duration(*ttl) * time.Second
		}
		if retryTimeout := source.Bootstrap.DCS.RetryTimeout; retryTimeout != nil {
			translated.DCS.RetryTimeout = time.Duration(*retryTimeout) * time.Second
		}
		if source.Bootstrap.DCS.MaximumLagOnFailover != nil {
			warnings = append(
				warnings,
				`Patroni key "bootstrap.dcs.maximum_lag_on_failover" is not translated; PACMAN applies built-in lag checks without a config override`,
			)
		}
		if source.Bootstrap.DCS.PostgreSQL != nil && source.Bootstrap.DCS.PostgreSQL.UsePGRewind != nil {
			warnings = append(
				warnings,
				`Patroni key "bootstrap.dcs.postgresql.use_pg_rewind" is not translated; configure PACMAN rewind policy separately from bootstrap.dcs`,
			)
		}
	}

	return translated, warnings, nil
}

func translatePatroniDCS(source patroniConfig, clusterName string) (*dcs.Config, []string, error) {
	supportedBackends := 0
	warnings := make([]string, 0, 4)
	config := &dcs.Config{
		ClusterName: clusterName,
	}

	switch {
	case source.Consul != nil:
		return nil, nil, fmt.Errorf("%w: consul", ErrPatroniDCSBackendUnsupported)
	case source.Zookeeper != nil:
		return nil, nil, fmt.Errorf("%w: zookeeper", ErrPatroniDCSBackendUnsupported)
	case source.Exhibitor != nil:
		return nil, nil, fmt.Errorf("%w: exhibitor", ErrPatroniDCSBackendUnsupported)
	case source.Kubernetes != nil:
		return nil, nil, fmt.Errorf("%w: kubernetes", ErrPatroniDCSBackendUnsupported)
	}

	if source.Etcd != nil && source.Etcd.hasSettings() {
		supportedBackends++

		endpoints, err := translatePatroniEtcdEndpoints(source.Etcd)
		if err != nil {
			return nil, nil, err
		}

		config.Backend = dcs.BackendEtcd
		config.Etcd = &dcs.EtcdConfig{
			Endpoints: endpoints,
			Username:  strings.TrimSpace(source.Etcd.Username),
			Password:  strings.TrimSpace(source.Etcd.Password),
		}
	}

	if source.Etcd3 != nil && source.Etcd3.hasSettings() {
		supportedBackends++

		endpoints, err := translatePatroniEtcdEndpoints(source.Etcd3)
		if err != nil {
			return nil, nil, err
		}

		config.Backend = dcs.BackendEtcd
		config.Etcd = &dcs.EtcdConfig{
			Endpoints: endpoints,
			Username:  strings.TrimSpace(source.Etcd3.Username),
			Password:  strings.TrimSpace(source.Etcd3.Password),
		}
		warnings = append(warnings, `Patroni "etcd3" settings were translated to PACMAN's native etcd backend`)
	}

	if source.Raft != nil && source.Raft.hasSettings() {
		supportedBackends++

		config.Backend = dcs.BackendRaft
		config.Raft = &dcs.RaftConfig{
			DataDir:     strings.TrimSpace(source.Raft.DataDir),
			BindAddress: strings.TrimSpace(source.Raft.SelfAddr),
			Peers:       patroniRaftPeers(source.Raft.SelfAddr, source.Raft.PartnerAddrs),
		}
		warnings = append(
			warnings,
			`Patroni "raft" settings were translated to PACMAN's embedded raft backend; verify peer addresses before bootstrap`,
		)
	}

	switch {
	case supportedBackends == 0:
		return nil, nil, ErrPatroniDCSBackendRequired
	case supportedBackends > 1:
		return nil, nil, ErrPatroniDCSBackendConflict
	default:
		return config, warnings, nil
	}
}

func translatePatroniEtcdEndpoints(source *patroniEtcdConfig) ([]string, error) {
	endpoints := make([]string, 0, 1+len(source.Hosts))

	if host := strings.TrimSpace(source.Host); host != "" {
		endpoints = append(endpoints, normalizePatroniEtcdEndpoint(host))
	}
	for _, host := range source.Hosts {
		trimmed := strings.TrimSpace(host)
		if trimmed == "" {
			continue
		}
		endpoints = append(endpoints, normalizePatroniEtcdEndpoint(trimmed))
	}

	return dedupeStrings(endpoints), nil
}

func normalizePatroniEtcdEndpoint(endpoint string) string {
	if strings.Contains(endpoint, "://") {
		return endpoint
	}

	return "http://" + endpoint
}

func patroniRaftPeers(self string, partners []string) []string {
	peers := make([]string, 0, 1+len(partners))
	if trimmed := strings.TrimSpace(self); trimmed != "" {
		peers = append(peers, trimmed)
	}
	for _, partner := range partners {
		if trimmed := strings.TrimSpace(partner); trimmed != "" {
			peers = append(peers, trimmed)
		}
	}

	return dedupeStrings(peers)
}

func selectPatroniPostgresAddress(source *patroniPostgresConfig) (string, int, []string, error) {
	selected, warnings, err := selectPatroniAddress(
		"postgresql.listen",
		source.listenAddress(),
		source.connectAddress(),
	)
	if err != nil {
		return "", 0, nil, err
	}

	if strings.TrimSpace(selected) == "" {
		return "", 0, warnings, nil
	}

	host, port, err := splitHostPort(selected)
	if err != nil {
		return "", 0, nil, fmt.Errorf("translate Patroni postgresql.listen: %w", err)
	}

	return host, port, warnings, nil
}

func selectPatroniAddress(field, listenAddress, connectAddress string) (string, []string, error) {
	listenAddress = strings.TrimSpace(listenAddress)
	connectAddress = strings.TrimSpace(connectAddress)
	warnings := make([]string, 0, 2)

	switch {
	case listenAddress != "":
		if _, _, err := splitHostPort(listenAddress); err != nil {
			return "", nil, fmt.Errorf("translate Patroni %s: %w", field, err)
		}
		if connectAddress != "" && connectAddress != listenAddress {
			if _, _, err := splitHostPort(connectAddress); err != nil {
				return "", nil, fmt.Errorf("translate Patroni %s: %w", strings.Replace(field, "listen", "connect_address", 1), err)
			}
			warnings = append(
				warnings,
				fmt.Sprintf(
					`Patroni key "%s" is not translated separately; PACMAN uses %q and ignores "%s"`,
					strings.Replace(field, "listen", "connect_address", 1),
					listenAddress,
					connectAddress,
				),
			)
		}
		return listenAddress, warnings, nil
	case connectAddress != "":
		if _, _, err := splitHostPort(connectAddress); err != nil {
			return "", nil, fmt.Errorf("translate Patroni %s: %w", strings.Replace(field, "listen", "connect_address", 1), err)
		}
		warnings = append(
			warnings,
			fmt.Sprintf(
				`Patroni key "%s" is unset; PACMAN uses "%s" from "%s"`,
				field,
				connectAddress,
				strings.Replace(field, "listen", "connect_address", 1),
			),
		)
		return connectAddress, warnings, nil
	default:
		return "", warnings, nil
	}
}

func splitHostPort(address string) (string, int, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return "", 0, fmt.Errorf("invalid host:port %q", address)
	}

	value, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in %q", address)
	}

	return host, value, nil
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	deduped := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		deduped = append(deduped, value)
	}

	return deduped
}

func patroniMigrationWarnings(nodeName string) []string {
	trimmedName := strings.TrimSpace(nodeName)

	return []string{
		fmt.Sprintf(
			`Patroni config has no PACMAN equivalent for "node.controlAddress"; using default %q and requiring manual review before multi-node bootstrap`,
			DefaultControlAddress,
		),
		fmt.Sprintf(
			`Patroni config has no PACMAN equivalent for "bootstrap.initialPrimary"; PACMAN will default it to node.name %q unless you override it`,
			trimmedName,
		),
		fmt.Sprintf(
			`Patroni config has no PACMAN equivalent for "bootstrap.seedAddresses"; PACMAN will default it to [%q] unless you set the full control-plane seed list`,
			DefaultControlAddress,
		),
		fmt.Sprintf(
			`Patroni config has no PACMAN equivalent for "bootstrap.expectedMembers"; PACMAN will default it to [%q] unless you set the full member list`,
			trimmedName,
		),
	}
}

func (config *patroniRestAPIConfig) listenAddress() string {
	if config == nil {
		return ""
	}

	return config.Listen
}

func (config *patroniRestAPIConfig) connectAddress() string {
	if config == nil {
		return ""
	}

	return config.ConnectAddress
}

func (config *patroniEtcdConfig) hasSettings() bool {
	if config == nil {
		return false
	}

	return strings.TrimSpace(config.Host) != "" ||
		len(config.Hosts) > 0 ||
		strings.TrimSpace(config.Username) != "" ||
		strings.TrimSpace(config.Password) != ""
}

func (config *patroniRaftConfig) hasSettings() bool {
	if config == nil {
		return false
	}

	return strings.TrimSpace(config.DataDir) != "" ||
		strings.TrimSpace(config.SelfAddr) != "" ||
		len(config.PartnerAddrs) > 0
}

func (config *patroniPostgresConfig) listenAddress() string {
	if config == nil {
		return ""
	}

	return config.Listen
}

func (config *patroniPostgresConfig) connectAddress() string {
	if config == nil {
		return ""
	}

	return config.ConnectAddress
}

func (config *patroniPostgresConfig) dataDir() string {
	if config == nil {
		return ""
	}

	return config.DataDir
}

func (config *patroniPostgresConfig) binDir() string {
	if config == nil {
		return ""
	}

	return config.BinDir
}
