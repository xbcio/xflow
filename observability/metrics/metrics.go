package metrics

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CounterSink is the narrow metrics boundary shared by concrete exporters.
type CounterSink interface {
	Inc(name string, labels map[string]string)
}

// DurationSink records duration observations for concrete exporters.
type DurationSink interface {
	Observe(name string, labels map[string]string, value time.Duration)
}

// GaugeSink records the latest numeric value for concrete exporters.
type GaugeSink interface {
	Set(name string, labels map[string]string, value float64)
}

// Metrics records xflow observations in a Prometheus registry.
type Metrics struct {
	mu              sync.Mutex
	registry        *prometheus.Registry
	counters        map[metricVecKey]*prometheus.CounterVec
	histograms      map[metricVecKey]*prometheus.HistogramVec
	bytesHistograms map[metricVecKey]*prometheus.HistogramVec
	gauges          map[metricVecKey]*prometheus.GaugeVec
}

type metricVecKey struct {
	name      string
	labelKeys string
}

// New creates an isolated Prometheus registry for xflow metrics.
func New() *Metrics {
	return NewWithRegistry(prometheus.NewRegistry())
}

// NewWithRegistry creates xflow metrics backed by registry. A nil registry
// creates a fresh isolated registry.
func NewWithRegistry(registry *prometheus.Registry) *Metrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	return &Metrics{
		registry:        registry,
		counters:        make(map[metricVecKey]*prometheus.CounterVec),
		histograms:      make(map[metricVecKey]*prometheus.HistogramVec),
		bytesHistograms: make(map[metricVecKey]*prometheus.HistogramVec),
		gauges:          make(map[metricVecKey]*prometheus.GaugeVec),
	}
}

// Registry returns the Prometheus registry backing this metrics collector.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

// Inc increments a counter by one.
func (m *Metrics) Inc(name string, labels map[string]string) {
	if m == nil || name == "" {
		return
	}
	counter := m.counter(name, labelNames(labels))
	if counter == nil {
		return
	}
	metric, err := counter.GetMetricWith(prometheus.Labels(labels))
	if err != nil {
		return
	}
	metric.Inc()
}

// Observe records a duration in seconds as a Prometheus histogram.
func (m *Metrics) Observe(name string, labels map[string]string, value time.Duration) {
	if m == nil || name == "" {
		return
	}
	histogram := m.histogram(name, labelNames(labels))
	if histogram == nil {
		return
	}
	metric, err := histogram.GetMetricWith(prometheus.Labels(labels))
	if err != nil {
		return
	}
	metric.Observe(value.Seconds())
}

// ObserveBytes records a byte-size observation in a Prometheus histogram using
// buckets tailored to script output sizes.
func (m *Metrics) ObserveBytes(name string, labels map[string]string, size int) {
	if m == nil || name == "" || size < 0 {
		return
	}
	histogram := m.bytesHistogram(name, labelNames(labels))
	if histogram == nil {
		return
	}
	metric, err := histogram.GetMetricWith(prometheus.Labels(labels))
	if err != nil {
		return
	}
	metric.Observe(float64(size))
}

// Set records a gauge value.
func (m *Metrics) Set(name string, labels map[string]string, value float64) {
	if m == nil || name == "" {
		return
	}
	gauge := m.gauge(name, labelNames(labels))
	if gauge == nil {
		return
	}
	metric, err := gauge.GetMetricWith(prometheus.Labels(labels))
	if err != nil {
		return
	}
	metric.Set(value)
}

// Handler serves metrics using Prometheus' text exposition format.
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) counter(name string, labels []string) *prometheus.CounterVec {
	key := newMetricVecKey(name, labels)
	m.mu.Lock()
	if counter := m.counters[key]; counter != nil {
		m.mu.Unlock()
		return counter
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: name,
		Help: helpText(name),
	}, labels)
	if err := m.registry.Register(counter); err != nil {
		m.mu.Unlock()
		log.Printf("xflow metrics: register counter %q failed: %v", name, err)
		return nil
	}
	m.counters[key] = counter
	m.mu.Unlock()
	return counter
}

func (m *Metrics) histogram(name string, labels []string) *prometheus.HistogramVec {
	key := newMetricVecKey(name, labels)
	m.mu.Lock()
	if histogram := m.histograms[key]; histogram != nil {
		m.mu.Unlock()
		return histogram
	}
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name,
		Help: helpText(name),
	}, labels)
	if err := m.registry.Register(histogram); err != nil {
		m.mu.Unlock()
		log.Printf("xflow metrics: register histogram %q failed: %v", name, err)
		return nil
	}
	m.histograms[key] = histogram
	m.mu.Unlock()
	return histogram
}

// byteBuckets spans 1 KiB to 2 MiB, comfortably covering the 1 MiB result cap.
var byteBuckets = []float64{1024, 4096, 16384, 65536, 262144, 524288, 1048576, 2097152}

func (m *Metrics) bytesHistogram(name string, labels []string) *prometheus.HistogramVec {
	key := newMetricVecKey(name, labels)
	m.mu.Lock()
	if histogram := m.bytesHistograms[key]; histogram != nil {
		m.mu.Unlock()
		return histogram
	}
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    helpText(name),
		Buckets: byteBuckets,
	}, labels)
	if err := m.registry.Register(histogram); err != nil {
		m.mu.Unlock()
		log.Printf("xflow metrics: register bytes histogram %q failed: %v", name, err)
		return nil
	}
	m.bytesHistograms[key] = histogram
	m.mu.Unlock()
	return histogram
}

func (m *Metrics) gauge(name string, labels []string) *prometheus.GaugeVec {
	key := newMetricVecKey(name, labels)
	m.mu.Lock()
	if gauge := m.gauges[key]; gauge != nil {
		m.mu.Unlock()
		return gauge
	}
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: helpText(name),
	}, labels)
	if err := m.registry.Register(gauge); err != nil {
		m.mu.Unlock()
		log.Printf("xflow metrics: register gauge %q failed: %v", name, err)
		return nil
	}
	m.gauges[key] = gauge
	m.mu.Unlock()
	return gauge
}

func newMetricVecKey(name string, labels []string) metricVecKey {
	copied := append([]string(nil), labels...)
	sort.Strings(copied)
	return metricVecKey{name: name, labelKeys: strings.Join(copied, "\xff")}
}

func labelNames(labels map[string]string) []string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// metricHelp maps metric names to human-readable help text used as the
// Prometheus metric Help string. New metrics should be registered here; an
// unknown name falls back to a generic description so a missing entry never
// panics at registration time.
var metricHelp = map[string]string{
	"xflow_audit_write_total":                      "Audit log write attempts, partitioned by operation and result.",
	"xflow_commit_outcomes_total":                   "Graph commit outcomes, partitioned by outcome (committed/aborted/failed).",
	"xflow_dispatch_transient_total":                "Transient dispatch failures scheduled for retry, partitioned by reason.",
	"xflow_execution_completed_total":               "Workflow executions completed, partitioned by terminal status.",
	"xflow_lease_acquire_total":                     "Lease acquisition attempts, partitioned by result.",
	"xflow_lease_acquire_duration_seconds":           "Latency of lease acquisition attempts.",
	"xflow_lease_age_seconds":                        "Age of reclaimed leases at the moment of reclaim.",
	"xflow_lease_expiry_scan_total":                 "Lease expiry scan cycles run, partitioned by result.",
	"xflow_lease_expiry_scan_duration_seconds":      "Duration of lease expiry scan cycles.",
	"xflow_lease_expiry_candidates":                  "Number of leases considered for expiry in the last scan.",
	"xflow_lease_reclaim_total":                      "Lease reclaim attempts, partitioned by result.",
	"xflow_lease_reclaim_duration_seconds":           "Duration of lease reclaim operations.",
	"xflow_lease_repair_runs_total":                 "Lease repair runs executed, partitioned by result.",
	"xflow_lease_repair_duration_seconds":            "Duration of lease repair runs.",
	"xflow_lease_repair_reconciled":                  "Number of leases reconciled in the last repair run.",
	"xflow_lease_sweep_scan_total":                  "Lease sweep scan cycles run, partitioned by labels.",
	"xflow_lease_sweep_scan_duration_seconds":        "Duration of lease sweep scan cycles.",
	"xflow_lease_sweep_candidates":                  "Number of leases considered during the last sweep scan.",
	"xflow_lease_sweep_reclaimed_total":             "Leases reclaimed by the sweeper, partitioned by result.",
	"xflow_lease_sweep_errors_total":                "Lease sweep errors, partitioned by reason.",
	"xflow_lease_sweep_repair_total":                "Lease sweep repair attempts, partitioned by labels.",
	"xflow_lease_sweep_repair_duration_seconds":      "Duration of lease sweep repair operations.",
	"xflow_lease_sweep_repair_reconciled":            "Number of leases reconciled in the last sweep repair run.",
	"xflow_node_started_total":                      "Nodes that started execution.",
	"xflow_node_completed_total":                    "Nodes that completed execution.",
	"xflow_node_duration_seconds":                   "Wall-clock duration of node execution.",
	"xflow_node_suspended_total":                    "Nodes that suspended pending async completion.",
	"xflow_node_timed_out_total":                    "Nodes that exceeded their timeout.",
	"xflow_node_retried_total":                      "Nodes retried after a failure.",
	"xflow_outbox_retries_total":                    "Outbox message dispatch retry attempts.",
	"xflow_outbox_dead_letters_total":               "Outbox messages sent to the dead-letter queue.",
	"xflow_outbox_dead_letters":                     "Current count of outbox messages in the dead-letter queue.",
	"xflow_outbox_dead_letters_replayed_total":      "Dead-letter messages replayed back to the ready set, partitioned by outcome.",
	"xflow_outbox_pending":                          "Current count of outbox messages pending dispatch.",
	"xflow_outbox_oldest_pending_age_seconds":        "Age of the oldest pending outbox message.",
	"xflow_outbox_errors_total":                     "Outbox dispatch errors, partitioned by operation.",
	"xflow_runner_auth_decisions_total":             "Runner authorization decisions, partitioned by result and auth mode.",
	"xflow_runner_claim_reclaimed_total":             "Runner claims reclaimed from stale leases.",
	"xflow_runner_lease_replayed_total":              "Runner leases replayed after a reclaim.",
	"xflow_script_execute_total":                    "Script execution attempts, partitioned by result.",
	"xflow_script_execute_duration_seconds":          "Wall-clock duration of script execution.",
	"xflow_script_output_bytes":                     "Size of script stdout output in bytes.",
}

func helpText(name string) string {
	if help, ok := metricHelp[name]; ok {
		return help
	}
	return "xflow metric " + name
}
