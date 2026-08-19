package metrics

import (
	"context"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/namespace"
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
	countHistograms map[metricVecKey]*prometheus.HistogramVec
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
		countHistograms: make(map[metricVecKey]*prometheus.HistogramVec),
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

// Add increments a counter by delta. Use it when the caller already knows how
// many events happened — a batch reporting three failed items at once — rather
// than calling Inc in a loop, which pays the label lookup per event for the same
// result. delta must be non-negative: a counter that goes down is a broken
// counter, and Prometheus panics on it, so a negative delta is dropped.
func (m *Metrics) Add(name string, labels map[string]string, delta float64) {
	if m == nil || name == "" || delta < 0 {
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
	metric.Add(delta)
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

// ObserveCount records an item-count observation (records per batch, entries per
// page — anything counted in small integers rather than bytes or seconds).
//
// It exists because ObserveBytes' buckets start at 1 KiB: a count bounded by a
// two-digit maximum lands entirely in the first bucket, which makes the
// histogram indistinguishable from a plain counter and destroys the very
// distribution it was added to show.
func (m *Metrics) ObserveCount(name string, labels map[string]string, count int) {
	if m == nil || name == "" || count < 0 {
		return
	}
	histogram := m.countHistogram(name, labelNames(labels))
	if histogram == nil {
		return
	}
	metric, err := histogram.GetMetricWith(prometheus.Labels(labels))
	if err != nil {
		return
	}
	metric.Observe(float64(count))
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

// countBuckets spans single-record batches to the default aggregate max_size
// (100), with headroom above it so a raised max_size still lands in a real
// bucket rather than +Inf.
var countBuckets = []float64{1, 2, 5, 10, 25, 50, 75, 100, 250, 500}

func (m *Metrics) countHistogram(name string, labels []string) *prometheus.HistogramVec {
	key := newMetricVecKey(name, labels)
	m.mu.Lock()
	if histogram := m.countHistograms[key]; histogram != nil {
		m.mu.Unlock()
		return histogram
	}
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    helpText(name),
		Buckets: countBuckets,
	}, labels)
	if err := m.registry.Register(histogram); err != nil {
		m.mu.Unlock()
		log.Printf("xflow metrics: register count histogram %q failed: %v", name, err)
		return nil
	}
	m.countHistograms[key] = histogram
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

// withNamespace returns labels with a "namespace" dimension drawn from ctx. The
// helper mutates the input map in place: when labels is non-nil it sets
// labels["namespace"] and returns the same map. Callers must not reuse the
// passed map for other metric calls unless they want to share the namespace
// label. When labels is nil a new map is allocated. Namespace is always emitted
// as a label for namespace-scoped metrics; callers that intentionally omit
// namespace (global metrics such as leader election) should not use this helper.
//
// Security/ops: namespace IDs are expected to be low-cardinality (tens to low
// hundreds). Do not use high-cardinality values such as execution IDs or node
// names as metric labels.
func withNamespace(ctx context.Context, labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{"namespace": string(namespace.FromContext(ctx))}
	}
	labels["namespace"] = string(namespace.FromContext(ctx))
	return labels
}

// metricHelp maps metric names to human-readable help text used as the
// Prometheus metric Help string. New metrics should be registered here; an
// unknown name falls back to a generic description so a missing entry never
// panics at registration time.
//
// Namespace dimension (Task 7.4): namespace-scoped metrics carry a "namespace" label
// drawn from context. Namespace IDs are expected to be low-cardinality (tens to
// low hundreds). Do not add high-cardinality dimensions such as execution IDs,
// node names, or lease tokens to metric labels. For very large namespace counts,
// disable or aggregate the namespace dimension to avoid Prometheus cardinality
// explosion.
var metricHelp = map[string]string{
	"xflow_audit_write_total":                        "Audit log write attempts, partitioned by operation and result.",
	"xflow_audit_reconcile_scan_total":               "Audit reconcile sweep cycles run, partitioned by result.",
	"xflow_audit_reconcile_scan_duration_seconds":    "Duration of audit reconcile pending-admission scans.",
	"xflow_audit_reconcile_settled_total":            "Audit admissions settled by the reconcile worker, partitioned by outcome and result.",
	"xflow_audit_reconcile_skipped_total":            "Audit admissions left pending (indeterminate authority), partitioned by reason.",
	"xflow_audit_reconcile_errors_total":             "Audit reconcile per-admission probe/append errors.",
	"xflow_audit_reconcile_backlog_age_seconds":      "Age of the oldest pending audit admission observed in the last sweep.",
	"xflow_audit_reconcile_pending":                  "Number of pending audit admissions observed in the last reconcile sweep.",
	"xflow_commit_outcomes_total":                    "Graph commit outcomes, partitioned by outcome (committed/aborted/failed).",
	"xflow_group_admission_total":                    "Trigger admission attempts, partitioned by outcome (accepted/duplicate/conflict/error).",
	"xflow_group_admission_duration_seconds":         "Duration of trigger admission attempts.",
	"xflow_group_activation_total":                   "Activation controller actions, partitioned by action (activate/deactivate/revoke/reconcile).",
	"xflow_group_activation_generation_fenced_total": "Activation attempts rejected due to generation fence.",
	"xflow_group_activation_active":                  "Number of currently active group activations.",
	"xflow_group_lease_renew_total":                  "Lease renewal attempts, partitioned by result (ok/fenced/error).",
	"xflow_group_lease_renew_duration_seconds":       "Duration of lease renewal attempts.",
	"xflow_group_emit_total":                         "Runner emit operations, partitioned by result (accepted/conflict/error/timeout).",
	"xflow_group_emit_duration_seconds":              "Duration of runner emit operations.",
	"xflow_group_emit_batch_size":                    "Batch size of runner emit operations.",
	"xflow_group_emit_inflight":                      "Number of currently in-flight emit operations.",
	"xflow_group_suspend_total":                      "Group suspend/resume lifecycle events, partitioned by action (suspended/resumed/canceled/timeout).",
	"xflow_group_backpressure_paused_total":          "Times group processing was paused due to backpressure.",
	"xflow_dispatch_transient_total":                 "Transient dispatch failures scheduled for retry, partitioned by reason.",
	"xflow_execution_completed_total":                "Workflow executions completed, partitioned by terminal status.",
	"xflow_lease_acquire_total":                      "Lease acquisition attempts, partitioned by result.",
	"xflow_lease_acquire_duration_seconds":           "Latency of lease acquisition attempts.",
	"xflow_lease_age_seconds":                        "Age of reclaimed leases at the moment of reclaim.",
	"xflow_lease_expiry_scan_total":                  "Lease expiry scan cycles run, partitioned by result.",
	"xflow_lease_expiry_scan_duration_seconds":       "Duration of lease expiry scan cycles.",
	"xflow_lease_expiry_candidates":                  "Number of leases considered for expiry in the last scan.",
	"xflow_lease_reclaim_total":                      "Lease reclaim attempts, partitioned by result.",
	"xflow_lease_reclaim_duration_seconds":           "Duration of lease reclaim operations.",
	"xflow_lease_repair_runs_total":                  "Lease repair runs executed, partitioned by result.",
	"xflow_lease_repair_duration_seconds":            "Duration of lease repair runs.",
	"xflow_lease_repair_reconciled":                  "Number of leases reconciled in the last repair run.",
	"xflow_lease_sweep_scan_total":                   "Lease sweep scan cycles run, partitioned by labels.",
	"xflow_lease_sweep_scan_duration_seconds":        "Duration of lease sweep scan cycles.",
	"xflow_lease_sweep_candidates":                   "Number of leases considered during the last sweep scan.",
	"xflow_lease_sweep_reclaimed_total":              "Leases reclaimed by the sweeper, partitioned by result.",
	"xflow_lease_sweep_errors_total":                 "Lease sweep errors, partitioned by reason.",
	"xflow_lease_sweep_repair_total":                 "Lease sweep repair attempts, partitioned by labels.",
	"xflow_lease_sweep_repair_duration_seconds":      "Duration of lease sweep repair operations.",
	"xflow_lease_sweep_repair_reconciled":            "Number of leases reconciled in the last sweep repair run.",
	"xflow_node_started_total":                       "Nodes that started execution.",
	"xflow_node_completed_total":                     "Nodes that completed execution.",
	"xflow_node_duration_seconds":                    "Wall-clock duration of node execution.",
	"xflow_node_suspended_total":                     "Nodes that suspended pending async completion.",
	"xflow_node_timed_out_total":                     "Nodes that exceeded their timeout.",
	"xflow_node_timeouts_total":                      "Node executions terminated for exceeding their deadline. Distinct from xflow_node_timed_out_total, which counts SUSPENDED nodes whose park timer fired and which wake normally.",
	"xflow_node_timeout_abandoned":                   "Handlers whose deadline passed but which have not returned. Should fall back to zero; a persistently non-zero value means a node type ignores ctx.",
	"xflow_node_execution_duration_seconds":          "Wall-clock duration of one handler invocation.",
	"xflow_node_retried_total":                       "Nodes retried after a failure.",
	"xflow_outbox_retries_total":                     "Outbox message dispatch retry attempts.",
	"xflow_outbox_dead_letters_total":                "Outbox messages sent to the dead-letter queue.",
	"xflow_outbox_dead_letters":                      "Current count of outbox messages in the dead-letter queue.",
	"xflow_outbox_dead_letters_replayed_total":       "Dead-letter messages replayed back to the ready set, partitioned by outcome.",
	"xflow_outbox_pending":                           "Current count of outbox messages pending dispatch.",
	"xflow_outbox_oldest_pending_age_seconds":        "Age of the oldest pending outbox message.",
	"xflow_outbox_errors_total":                      "Outbox dispatch errors, partitioned by operation.",
	"xflow_runner_auth_decisions_total":              "Runner authorization decisions, partitioned by result and auth mode.",
	"xflow_runner_claim_reclaimed_total":             "Runner claims reclaimed from stale leases.",
	"xflow_runner_lease_replayed_total":              "Runner leases replayed after a reclaim.",
	"xflow_runner_up":                                "1 when the runner is live by the control plane's heartbeat TTL, 0 when its last report is retained but the runner is judged dead.",
	"xflow_runner_metrics_last_report_age_seconds":   "Seconds since the server received this runner's most recent metrics report.",
	"xflow_runner_metrics_received_total":            "Runner metrics reports accepted and stored, partitioned by runner.",
	"xflow_runner_metrics_rejected_total":            "Runner metrics reports or metric families rejected, partitioned by runner and reason.",
	"xflow_runner_metrics_inbox_size":                "Number of runners with a retained metrics report in the inbox.",
	"xflow_runner_metrics_gather_errors_total":       "Errors swallowed while gathering proxied runner metrics, partitioned by reason. Non-zero means some runner series are missing from /metrics.",
	"xflow_runner_metrics_reports_total":             "Metrics reports this runner attempted to ship to the server, partitioned by result.",
	"xflow_runner_metrics_report_bytes":              "Size of the gzip-compressed metrics payloads this runner shipped.",
	"xflow_script_execute_total":                     "Script execution attempts, partitioned by result.",
	"xflow_script_execute_duration_seconds":          "Wall-clock duration of script execution.",
	"xflow_script_output_bytes":                      "Size of script stdout output in bytes.",
	"xflow_supply_age_seconds":                       "Age of the active wasm reactor content, per module. A source that stopped updating leaves this climbing.",
	"xflow_supply_fetch_total":                       "Supply content fetch attempts at activation time, partitioned by supply name and result.",
	"xflow_supply_not_ready":                         "Whether an activation is currently being declined for a missing required supply, per workflow and supply.",
	"xflow_supply_unavailable_serving":               "Whether a supply is serving traffic with content that was never successfully fetched (require_ready:false).",
	"xflow_supply_consumers":                         "Number of in-process consumers registered for a supply.",
	"xflow_trigger_messages_discarded_total":         "Trigger messages consumed but never emitted, partitioned by topic and reason (schema/schema_fail). Any nonzero rate is silent data loss unless it is expected.",
	"xflow_trigger_messages_dead_lettered_total":     "Dead-letter publish attempts for invalid trigger messages, partitioned by topic and result (ok/error). An error rate means the source partition is stalled on redelivery.",
	"xflow_wasm_config_rule_count":                   "Number of rules in the currently active wasm reactor config.",
	"xflow_wasm_config_generation":                   "SupplyResource revision of the currently active wasm reactor config.",
	"xflow_wasm_pool_swap_total":                     "wasm reactor pool config swap attempts, partitioned by result (applied/rejected).",
	"xflow_wasm_pool_swap_duration_seconds":          "Duration of a wasm reactor pool rebuild-and-swap.",
	"xflow_wasm_instance_total":                      "Total resident wasm reactor pool instances across all engines (state=ready).",
	"xflow_wasm_instance_recycled_total":             "wasm reactor pool instances torn down, partitioned by cause.",
	"xflow_wasm_pool_borrow_wait_seconds":            "Time a caller waited to borrow a free wasm reactor pool instance.",
	"xflow_wasm_module_compile_total":                "wasm module compile cache outcomes, partitioned by result (hit/miss).",
}

func helpText(name string) string {
	if help, ok := metricHelp[name]; ok {
		return help
	}
	return "xflow metric " + name
}
