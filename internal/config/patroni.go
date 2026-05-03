package config

import (
	"bytes"
	"fmt"
	"net"
	"sort"
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
	Host     string            `yaml:"host"`
	Hosts    patroniStringList `yaml:"hosts"`
	Username string            `yaml:"username"`
	Password string            `yaml:"password"`
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
	Listen         string         `yaml:"listen"`
	ConnectAddress string         `yaml:"connect_address"`
	DataDir        string         `yaml:"data_dir"`
	BinDir         string         `yaml:"bin_dir"`
	Parameters     map[string]any `yaml:"parameters"`
}

type patroniStringList []string

type patroniFieldRule struct {
	children map[string]patroniFieldRule
	openMap  bool
	warning  string
}

var patroniFieldRules = map[string]patroniFieldRule{
	"scope": {},
	"name":  {},
	"restapi": {
		children: map[string]patroniFieldRule{
			"listen":          {},
			"connect_address": {},
			"authentication": {
				warning: `Patroni key "restapi.authentication" is not translated; PACMAN uses bearer-token authentication instead of Patroni Basic Auth`,
			},
		},
	},
	"etcd": {
		children: map[string]patroniFieldRule{
			"host":     {},
			"hosts":    {},
			"username": {},
			"password": {},
		},
	},
	"etcd3": {
		children: map[string]patroniFieldRule{
			"host":     {},
			"hosts":    {},
			"username": {},
			"password": {},
		},
	},
	"raft": {
		children: map[string]patroniFieldRule{
			"data_dir":      {},
			"self_addr":     {},
			"partner_addrs": {},
		},
	},
	"bootstrap": {
		children: map[string]patroniFieldRule{
			"dcs": {
				children: map[string]patroniFieldRule{
					"ttl":           {},
					"retry_timeout": {},
					"loop_wait": {
						warning: `Patroni key "bootstrap.dcs.loop_wait" is not translated; PACMAN uses its own reconciliation cadence`,
					},
					"maximum_lag_on_failover": {
						warning: `Patroni key "bootstrap.dcs.maximum_lag_on_failover" is not translated; PACMAN applies built-in lag checks without a config override`,
					},
					"postgresql": {
						children: map[string]patroniFieldRule{
							"use_pg_rewind": {
								warning: `Patroni key "bootstrap.dcs.postgresql.use_pg_rewind" is not translated; configure PACMAN rewind policy separately from bootstrap.dcs`,
							},
							"pg_hba": {
								warning: `Patroni key "bootstrap.dcs.postgresql.pg_hba" is not translated; manage pg_hba.conf outside PACMAN`,
							},
							"parameters": {
								warning: `Patroni key "bootstrap.dcs.postgresql.parameters" is not translated; PACMAN does not import Patroni bootstrap-managed PostgreSQL parameter sets`,
							},
						},
					},
				},
			},
			"initdb": {
				warning: `Patroni key "bootstrap.initdb" is not translated; PACMAN manages initdb automatically on first boot`,
			},
		},
	},
	"postgresql": {
		children: map[string]patroniFieldRule{
			"listen":          {},
			"connect_address": {},
			"data_dir":        {},
			"bin_dir":         {},
			"parameters": {
				openMap: true,
			},
			"create_replica_methods": {
				warning: `Patroni key "postgresql.create_replica_methods" is not translated; PACMAN replica imaging is handled by deployment or rejoin workflows`,
			},
			"pgpass": {
				warning: `Patroni key "postgresql.pgpass" is not translated; PACMAN does not require a .pgpass file`,
			},
			"authentication": {
				warning: `Patroni key "postgresql.authentication" is not translated; manage PostgreSQL users and credentials outside PACMAN`,
			},
			"basebackup": {
				warning: `Patroni key "postgresql.basebackup" is not translated; PACMAN does not import Patroni basebackup flags into node config`,
			},
			"barman": {
				warning: `Patroni key "postgresql.barman" is not translated; configure Barman recovery tooling outside PACMAN and pass restored PostgreSQL settings through cluster policy`,
			},
			"wale": {
				warning: `Patroni key "postgresql.wale" is not translated; configure WAL-E/WAL-G tooling outside PACMAN and pass restored PostgreSQL settings through cluster policy`,
			},
			"wal_e": {
				warning: `Patroni key "postgresql.wal_e" is not translated; configure WAL-E/WAL-G tooling outside PACMAN and pass restored PostgreSQL settings through cluster policy`,
			},
			"recovery_conf": {
				warning: `Patroni key "postgresql.recovery_conf" is not translated; PACMAN renders standby recovery settings from cluster policy during rejoin`,
			},
		},
	},
	"callbacks": {
		warning: `Patroni key "callbacks" is not translated; configure cloud role-change callbacks outside PACMAN`,
	},
	"tags": {
		warning: `Patroni key "tags" is not translated; configure load-balancer and placement hints outside PACMAN`,
	},
}

func decodePatroniConfig(payload []byte) (DecodeReport, error) {
	var root yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&root); err != nil {
		return DecodeReport{}, fmt.Errorf("decode Patroni config document: %w", err)
	}

	var source patroniConfig
	if len(root.Content) == 0 {
		return DecodeReport{}, fmt.Errorf("decode Patroni config document: empty document")
	}
	if err := root.Content[0].Decode(&source); err != nil {
		return DecodeReport{}, fmt.Errorf("decode Patroni config document: %w", err)
	}

	config, warnings, err := translatePatroniConfig(source, &root)
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
		Warnings: dedupeStrings(warnings),
	}, nil
}

func translatePatroniConfig(source patroniConfig, root *yaml.Node) (Config, []string, error) {
	warnings := patroniMigrationWarnings(source.Name)
	warnings = append(warnings, patroniFieldWarnings(root)...)

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

	postgresParameters, parameterWarnings := translatePatroniPostgresParameters(source.PostgreSQL)
	warnings = append(warnings, parameterWarnings...)

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
			Parameters:    postgresParameters,
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

func translatePatroniPostgresParameters(source *patroniPostgresConfig) (map[string]string, []string) {
	if source == nil || len(source.Parameters) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(source.Parameters))
	for key := range source.Parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parameters := make(map[string]string, len(keys))
	warnings := make([]string, 0, len(keys))
	for _, key := range keys {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if _, unsafe := unsafeLocalPostgresParameters[normalized]; unsafe {
			warnings = append(
				warnings,
				fmt.Sprintf(
					`Patroni key "postgresql.parameters.%s" is not translated; PACMAN treats this PostgreSQL parameter as cluster-managed`,
					key,
				),
			)
			continue
		}

		parameters[key] = patroniParameterString(source.Parameters[key])
	}

	if len(parameters) == 0 {
		return nil, warnings
	}

	return parameters, warnings
}

func patroniParameterString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
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

func patroniFieldWarnings(root *yaml.Node) []string {
	if root == nil {
		return nil
	}

	node := root
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		node = node.Content[0]
	}
	if node.Kind != yaml.MappingNode {
		return nil
	}

	warnings := make([]string, 0, 8)
	collectPatroniFieldWarnings(node, "", patroniFieldRules, &warnings)
	return warnings
}

func collectPatroniFieldWarnings(node *yaml.Node, path string, rules map[string]patroniFieldRule, warnings *[]string) {
	if node == nil || node.Kind != yaml.MappingNode {
		return
	}

	for index := 0; index+1 < len(node.Content); index += 2 {
		keyNode := node.Content[index]
		valueNode := node.Content[index+1]
		key := strings.TrimSpace(keyNode.Value)
		if key == "" {
			continue
		}

		fullPath := key
		if path != "" {
			fullPath = path + "." + key
		}

		rule, ok := rules[key]
		if !ok {
			*warnings = append(
				*warnings,
				fmt.Sprintf(`Patroni key "%s" is not translated by PACMAN and was ignored during migration`, fullPath),
			)
			continue
		}

		if rule.warning != "" {
			*warnings = append(*warnings, rule.warning)
			continue
		}

		if rule.openMap {
			continue
		}

		if valueNode.Kind == yaml.MappingNode && len(rule.children) > 0 {
			collectPatroniFieldWarnings(valueNode, fullPath, rule.children, warnings)
		}
	}
}

func (values *patroniStringList) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.SequenceNode:
		decoded := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			trimmed := strings.TrimSpace(item.Value)
			if trimmed == "" {
				continue
			}
			decoded = append(decoded, trimmed)
		}
		*values = decoded
		return nil
	case yaml.ScalarNode:
		parts := strings.Split(node.Value, ",")
		decoded := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed == "" {
				continue
			}
			decoded = append(decoded, trimmed)
		}
		*values = decoded
		return nil
	case 0:
		*values = nil
		return nil
	default:
		return fmt.Errorf("decode Patroni hosts: expected string or list, got yaml kind %d", node.Kind)
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
