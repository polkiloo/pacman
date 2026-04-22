package observability

import (
	"bytes"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
)

// StateReader exposes the cluster and node state needed to build
// Prometheus-compatible observability metrics.
type StateReader interface {
	NodeStatuses() []agentmodel.NodeStatus
	ClusterSpec() (cluster.ClusterSpec, bool)
	ClusterStatus() (cluster.ClusterStatus, bool)
	MaintenanceStatus() cluster.MaintenanceModeStatus
}

// TraceReader exposes the recorded control-plane operation trace points used by
// the metrics collector.
type TraceReader interface {
	OperationTraceCounts() []cluster.OperationTraceCount
}

// NewPrometheusGatherer constructs a Prometheus gatherer for the current PACMAN
// daemon state.
func NewPrometheusGatherer(state StateReader) prometheus.Gatherer {
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewPrometheusCollector(state))
	return registry
}

// EncodePrometheusText renders the current metrics snapshot in Prometheus text
// exposition format together with the matching content type.
func EncodePrometheusText(gatherer prometheus.Gatherer) ([]byte, string, error) {
	metricFamilies, err := gatherer.Gather()
	if err != nil {
		return nil, "", err
	}

	format := expfmt.NewFormat(expfmt.TypeTextPlain)
	var buffer bytes.Buffer
	encoder := expfmt.NewEncoder(&buffer, format)
	for _, family := range metricFamilies {
		if err := encoder.Encode(family); err != nil {
			return nil, "", err
		}
	}

	return buffer.Bytes(), string(format), nil
}

// NewPrometheusCollector builds the PACMAN Prometheus collector for the
// provided live state source.
func NewPrometheusCollector(state StateReader) prometheus.Collector {
	collector := &prometheusCollector{
		state: state,
		clusterPhase: prometheus.NewDesc(
			"pacman_cluster_phase",
			"Current cluster phase encoded as a one-hot gauge by phase label.",
			[]string{"phase"},
			nil,
		),
		clusterMaintenanceMode: prometheus.NewDesc(
			"pacman_cluster_maintenance_mode",
			"Whether maintenance mode is currently enabled for the cluster.",
			nil,
			nil,
		),
		clusterCurrentEpoch: prometheus.NewDesc(
			"pacman_cluster_current_epoch",
			"Current cluster epoch observed by the control plane.",
			nil,
			nil,
		),
		clusterDesiredMembers: prometheus.NewDesc(
			"pacman_cluster_spec_members_desired",
			"Desired number of cluster members from the stored cluster specification.",
			nil,
			nil,
		),
		clusterObservedMembers: prometheus.NewDesc(
			"pacman_cluster_members_observed",
			"Observed number of cluster members in the aggregated cluster status.",
			nil,
			nil,
		),
		clusterPrimary: prometheus.NewDesc(
			"pacman_cluster_primary",
			"Current observed writable primary member.",
			[]string{"member"},
			nil,
		),
		clusterOperationActive: prometheus.NewDesc(
			"pacman_cluster_operation_active",
			"Current active control-plane operation by kind and state.",
			[]string{"kind", "state"},
			nil,
		),
		memberInfo: prometheus.NewDesc(
			"pacman_member_info",
			"Static member identity for the current observed member role and state.",
			[]string{"member", "role", "state"},
			nil,
		),
		memberHealthy: prometheus.NewDesc(
			"pacman_member_healthy",
			"Whether the observed cluster member is healthy.",
			[]string{"member"},
			nil,
		),
		memberLeader: prometheus.NewDesc(
			"pacman_member_leader",
			"Whether the observed cluster member is the current leader.",
			[]string{"member"},
			nil,
		),
		memberNeedsRejoin: prometheus.NewDesc(
			"pacman_member_needs_rejoin",
			"Whether the observed cluster member currently requires rejoin.",
			[]string{"member"},
			nil,
		),
		memberTimeline: prometheus.NewDesc(
			"pacman_member_timeline",
			"Observed PostgreSQL timeline for the cluster member.",
			[]string{"member"},
			nil,
		),
		memberReplicationLagBytes: prometheus.NewDesc(
			"pacman_member_replication_lag_bytes",
			"Observed replication lag in bytes for the cluster member.",
			[]string{"member"},
			nil,
		),
		memberLastSeenSeconds: prometheus.NewDesc(
			"pacman_member_last_seen_seconds",
			"Unix timestamp when the cluster member was last seen by the control plane.",
			[]string{"member"},
			nil,
		),
		nodeInfo: prometheus.NewDesc(
			"pacman_node_info",
			"Static node identity for the current published member role and state.",
			[]string{"node", "member", "role", "state"},
			nil,
		),
		nodePostgresUp: prometheus.NewDesc(
			"pacman_node_postgres_up",
			"Whether the node-local PostgreSQL instance is currently reachable.",
			[]string{"node"},
			nil,
		),
		nodePostgresRecoveryKnown: prometheus.NewDesc(
			"pacman_node_postgres_recovery_known",
			"Whether the node-local PostgreSQL recovery state is known.",
			[]string{"node"},
			nil,
		),
		nodePostgresInRecovery: prometheus.NewDesc(
			"pacman_node_postgres_in_recovery",
			"Whether the node-local PostgreSQL instance is currently in recovery.",
			[]string{"node"},
			nil,
		),
		nodeControlPlaneReachable: prometheus.NewDesc(
			"pacman_node_controlplane_reachable",
			"Whether publishing node state to the control plane succeeded.",
			[]string{"node"},
			nil,
		),
		nodeControlPlaneLeader: prometheus.NewDesc(
			"pacman_node_controlplane_leader",
			"Whether the node currently holds the control-plane leader lease.",
			[]string{"node"},
			nil,
		),
		nodeObservedAtSeconds: prometheus.NewDesc(
			"pacman_node_observed_at_seconds",
			"Unix timestamp of the latest published node observation.",
			[]string{"node"},
			nil,
		),
		nodePostgresCheckedAtSeconds: prometheus.NewDesc(
			"pacman_node_postgres_checked_at_seconds",
			"Unix timestamp of the latest local PostgreSQL probe.",
			[]string{"node"},
			nil,
		),
		nodePostgresServerVersion: prometheus.NewDesc(
			"pacman_node_postgres_server_version",
			"Observed PostgreSQL server version for the node-local instance.",
			[]string{"node"},
			nil,
		),
		nodePostgresTimeline: prometheus.NewDesc(
			"pacman_node_postgres_timeline",
			"Observed PostgreSQL timeline for the node-local instance.",
			[]string{"node"},
			nil,
		),
		nodePostgresDatabaseSizeBytes: prometheus.NewDesc(
			"pacman_node_postgres_database_size_bytes",
			"Observed size in bytes of the current database for the node-local PostgreSQL instance.",
			[]string{"node"},
			nil,
		),
		nodePostgresReplicationLagBytes: prometheus.NewDesc(
			"pacman_node_postgres_replication_lag_bytes",
			"Observed replication lag in bytes for the node-local PostgreSQL instance.",
			[]string{"node"},
			nil,
		),
		nodeControlPlaneLastHeartbeatSeconds: prometheus.NewDesc(
			"pacman_node_controlplane_last_heartbeat_seconds",
			"Unix timestamp of the latest heartbeat published to the control plane.",
			[]string{"node"},
			nil,
		),
		nodeControlPlaneLastDCSSeenSeconds: prometheus.NewDesc(
			"pacman_node_controlplane_last_dcs_seen_seconds",
			"Unix timestamp of the latest DCS observation seen while publishing node state.",
			[]string{"node"},
			nil,
		),
		operationTransitionsTotal: prometheus.NewDesc(
			"pacman_controlplane_operation_transitions_total",
			"Count of control-plane operation lifecycle transitions observed by kind and state.",
			[]string{"kind", "state"},
			nil,
		),
	}

	if trace, ok := state.(TraceReader); ok {
		collector.trace = trace
	}

	return collector
}

type prometheusCollector struct {
	state StateReader
	trace TraceReader

	clusterPhase                         *prometheus.Desc
	clusterMaintenanceMode               *prometheus.Desc
	clusterCurrentEpoch                  *prometheus.Desc
	clusterDesiredMembers                *prometheus.Desc
	clusterObservedMembers               *prometheus.Desc
	clusterPrimary                       *prometheus.Desc
	clusterOperationActive               *prometheus.Desc
	memberInfo                           *prometheus.Desc
	memberHealthy                        *prometheus.Desc
	memberLeader                         *prometheus.Desc
	memberNeedsRejoin                    *prometheus.Desc
	memberTimeline                       *prometheus.Desc
	memberReplicationLagBytes            *prometheus.Desc
	memberLastSeenSeconds                *prometheus.Desc
	nodeInfo                             *prometheus.Desc
	nodePostgresUp                       *prometheus.Desc
	nodePostgresRecoveryKnown            *prometheus.Desc
	nodePostgresInRecovery               *prometheus.Desc
	nodeControlPlaneReachable            *prometheus.Desc
	nodeControlPlaneLeader               *prometheus.Desc
	nodeObservedAtSeconds                *prometheus.Desc
	nodePostgresCheckedAtSeconds         *prometheus.Desc
	nodePostgresServerVersion            *prometheus.Desc
	nodePostgresTimeline                 *prometheus.Desc
	nodePostgresDatabaseSizeBytes        *prometheus.Desc
	nodePostgresReplicationLagBytes      *prometheus.Desc
	nodeControlPlaneLastHeartbeatSeconds *prometheus.Desc
	nodeControlPlaneLastDCSSeenSeconds   *prometheus.Desc
	operationTransitionsTotal            *prometheus.Desc
}

func (collector *prometheusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		collector.clusterPhase,
		collector.clusterMaintenanceMode,
		collector.clusterCurrentEpoch,
		collector.clusterDesiredMembers,
		collector.clusterObservedMembers,
		collector.clusterPrimary,
		collector.clusterOperationActive,
		collector.memberInfo,
		collector.memberHealthy,
		collector.memberLeader,
		collector.memberNeedsRejoin,
		collector.memberTimeline,
		collector.memberReplicationLagBytes,
		collector.memberLastSeenSeconds,
		collector.nodeInfo,
		collector.nodePostgresUp,
		collector.nodePostgresRecoveryKnown,
		collector.nodePostgresInRecovery,
		collector.nodeControlPlaneReachable,
		collector.nodeControlPlaneLeader,
		collector.nodeObservedAtSeconds,
		collector.nodePostgresCheckedAtSeconds,
		collector.nodePostgresServerVersion,
		collector.nodePostgresTimeline,
		collector.nodePostgresDatabaseSizeBytes,
		collector.nodePostgresReplicationLagBytes,
		collector.nodeControlPlaneLastHeartbeatSeconds,
		collector.nodeControlPlaneLastDCSSeenSeconds,
		collector.operationTransitionsTotal,
	} {
		ch <- desc
	}
}

func (collector *prometheusCollector) Collect(ch chan<- prometheus.Metric) {
	maintenance := collector.state.MaintenanceStatus()
	ch <- prometheus.MustNewConstMetric(collector.clusterMaintenanceMode, prometheus.GaugeValue, boolFloat64(maintenance.Enabled))

	if spec, ok := collector.state.ClusterSpec(); ok {
		ch <- prometheus.MustNewConstMetric(collector.clusterDesiredMembers, prometheus.GaugeValue, float64(len(spec.Members)))
	}

	if status, ok := collector.state.ClusterStatus(); ok {
		ch <- prometheus.MustNewConstMetric(collector.clusterCurrentEpoch, prometheus.GaugeValue, float64(status.CurrentEpoch))
		ch <- prometheus.MustNewConstMetric(collector.clusterObservedMembers, prometheus.GaugeValue, float64(len(status.Members)))

		for _, phase := range cluster.ClusterPhases() {
			ch <- prometheus.MustNewConstMetric(
				collector.clusterPhase,
				prometheus.GaugeValue,
				boolFloat64(status.Phase == phase),
				string(phase),
			)
		}

		if primary := strings.TrimSpace(status.CurrentPrimary); primary != "" {
			ch <- prometheus.MustNewConstMetric(collector.clusterPrimary, prometheus.GaugeValue, 1, primary)
		}

		if status.ActiveOperation != nil && !status.ActiveOperation.State.IsTerminal() {
			ch <- prometheus.MustNewConstMetric(
				collector.clusterOperationActive,
				prometheus.GaugeValue,
				1,
				string(status.ActiveOperation.Kind),
				string(status.ActiveOperation.State),
			)
		}

		for _, member := range status.Members {
			ch <- prometheus.MustNewConstMetric(
				collector.memberInfo,
				prometheus.GaugeValue,
				1,
				member.Name,
				string(member.Role),
				string(member.State),
			)
			ch <- prometheus.MustNewConstMetric(collector.memberHealthy, prometheus.GaugeValue, boolFloat64(member.Healthy), member.Name)
			ch <- prometheus.MustNewConstMetric(collector.memberLeader, prometheus.GaugeValue, boolFloat64(member.Leader), member.Name)
			ch <- prometheus.MustNewConstMetric(collector.memberNeedsRejoin, prometheus.GaugeValue, boolFloat64(member.NeedsRejoin), member.Name)
			ch <- prometheus.MustNewConstMetric(collector.memberTimeline, prometheus.GaugeValue, float64(member.Timeline), member.Name)
			ch <- prometheus.MustNewConstMetric(collector.memberReplicationLagBytes, prometheus.GaugeValue, float64(member.LagBytes), member.Name)
			ch <- prometheus.MustNewConstMetric(collector.memberLastSeenSeconds, prometheus.GaugeValue, unixSeconds(member.LastSeenAt), member.Name)
		}
	}

	for _, node := range collector.state.NodeStatuses() {
		memberName := strings.TrimSpace(node.MemberName)
		if memberName == "" {
			memberName = node.NodeName
		}

		ch <- prometheus.MustNewConstMetric(
			collector.nodeInfo,
			prometheus.GaugeValue,
			1,
			node.NodeName,
			memberName,
			string(node.Role),
			string(node.State),
		)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresUp, prometheus.GaugeValue, boolFloat64(node.Postgres.Up), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresRecoveryKnown, prometheus.GaugeValue, boolFloat64(node.Postgres.RecoveryKnown), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresInRecovery, prometheus.GaugeValue, boolFloat64(node.Postgres.InRecovery), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodeControlPlaneReachable, prometheus.GaugeValue, boolFloat64(node.ControlPlane.ClusterReachable), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodeControlPlaneLeader, prometheus.GaugeValue, boolFloat64(node.ControlPlane.Leader), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodeObservedAtSeconds, prometheus.GaugeValue, unixSeconds(node.ObservedAt), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresCheckedAtSeconds, prometheus.GaugeValue, unixSeconds(node.Postgres.CheckedAt), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresServerVersion, prometheus.GaugeValue, float64(node.Postgres.Details.ServerVersion), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresTimeline, prometheus.GaugeValue, float64(node.Postgres.Details.Timeline), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresDatabaseSizeBytes, prometheus.GaugeValue, float64(node.Postgres.Details.DatabaseSizeBytes), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodePostgresReplicationLagBytes, prometheus.GaugeValue, float64(node.Postgres.Details.ReplicationLagBytes), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodeControlPlaneLastHeartbeatSeconds, prometheus.GaugeValue, unixSeconds(node.ControlPlane.LastHeartbeatAt), node.NodeName)
		ch <- prometheus.MustNewConstMetric(collector.nodeControlPlaneLastDCSSeenSeconds, prometheus.GaugeValue, unixSeconds(node.ControlPlane.LastDCSSeenAt), node.NodeName)
	}

	if collector.trace == nil {
		return
	}

	for _, trace := range collector.trace.OperationTraceCounts() {
		ch <- prometheus.MustNewConstMetric(
			collector.operationTransitionsTotal,
			prometheus.CounterValue,
			float64(trace.Count),
			string(trace.Kind),
			string(trace.State),
		)
	}
}

func boolFloat64(value bool) float64 {
	if value {
		return 1
	}

	return 0
}

func unixSeconds(value interface {
	IsZero() bool
	Unix() int64
}) float64 {
	if value.IsZero() {
		return 0
	}

	return float64(value.Unix())
}
