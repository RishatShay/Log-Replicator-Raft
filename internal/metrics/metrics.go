// Package metrics exposes the Raft state of a node in Prometheus format.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns its own registry so that every series carries the node_id label.
type Metrics struct {
	registry *prometheus.Registry

	isLeader       prometheus.Gauge
	term           prometheus.Gauge
	commitIndex    prometheus.Gauge
	lastApplied    prometheus.Gauge
	lastLogIndex   prometheus.Gauge
	snapshotIndex  prometheus.Gauge
	elections      prometheus.Counter
	rpcs           *prometheus.CounterVec
	replicationLag *prometheus.GaugeVec
	commitLatency  prometheus.Histogram
	applyLatency   prometheus.Histogram
}

func New(nodeID string) *Metrics {
	registry := prometheus.NewRegistry()
	factory := promauto.With(prometheus.WrapRegistererWith(prometheus.Labels{"node_id": nodeID}, registry))
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return &Metrics{
		registry: registry,
		isLeader: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_is_leader",
			Help: "1 when this node believes it is the leader.",
		}),
		term: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_current_term",
			Help: "Current Raft term.",
		}),
		commitIndex: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_commit_index",
			Help: "Highest log index known to be committed.",
		}),
		lastApplied: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_last_applied",
			Help: "Highest log index applied to the state machine.",
		}),
		lastLogIndex: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_last_log_index",
			Help: "Highest log index stored locally.",
		}),
		snapshotIndex: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_snapshot_index",
			Help: "Log index covered by the latest snapshot.",
		}),
		elections: factory.NewCounter(prometheus.CounterOpts{
			Name: "raft_elections_total",
			Help: "Elections started by this node.",
		}),
		rpcs: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "raft_rpc_total",
			Help: "Consensus RPCs served by this node.",
		}, []string{"method", "result"}),
		replicationLag: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_replication_lag_entries",
			Help: "Entries the leader still has to ship to a peer.",
		}, []string{"peer_id"}),
		commitLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_commit_latency_seconds",
			Help:    "Time from appending a client entry to committing it.",
			Buckets: prometheus.DefBuckets,
		}),
		applyLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_apply_latency_seconds",
			Help:    "Time from committing an entry to applying it to the state machine.",
			Buckets: prometheus.DefBuckets,
		}),
	}
}

// Handler serves the /metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SetState publishes the current Raft state of the node.
func (m *Metrics) SetState(isLeader bool, term, commitIndex, lastApplied, lastLogIndex, snapshotIndex uint64) {
	leader := 0.0
	if isLeader {
		leader = 1
	}
	m.isLeader.Set(leader)
	m.term.Set(float64(term))
	m.commitIndex.Set(float64(commitIndex))
	m.lastApplied.Set(float64(lastApplied))
	m.lastLogIndex.Set(float64(lastLogIndex))
	m.snapshotIndex.Set(float64(snapshotIndex))
}

func (m *Metrics) ElectionStarted() {
	m.elections.Inc()
}

// RPCHandled records one served consensus RPC, for example ("AppendEntries", "log_mismatch").
func (m *Metrics) RPCHandled(method, result string) {
	m.rpcs.WithLabelValues(method, result).Inc()
}

func (m *Metrics) SetReplicationLag(peerID string, entries uint64) {
	m.replicationLag.WithLabelValues(peerID).Set(float64(entries))
}

// ClearReplicationLag drops the per-peer lag series, which only a leader owns.
func (m *Metrics) ClearReplicationLag() {
	m.replicationLag.Reset()
}

func (m *Metrics) CommitLatency(d time.Duration) {
	m.commitLatency.Observe(d.Seconds())
}

func (m *Metrics) ApplyLatency(d time.Duration) {
	m.applyLatency.Observe(d.Seconds())
}
