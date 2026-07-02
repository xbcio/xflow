package metrics

import (
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

// Metrics records xflow observations in a Prometheus registry.
type Metrics struct {
	mu         sync.Mutex
	registry   *prometheus.Registry
	counters   map[metricVecKey]*prometheus.CounterVec
	histograms map[metricVecKey]*prometheus.HistogramVec
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
		registry:   registry,
		counters:   make(map[metricVecKey]*prometheus.CounterVec),
		histograms: make(map[metricVecKey]*prometheus.HistogramVec),
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
	defer m.mu.Unlock()
	if counter := m.counters[key]; counter != nil {
		return counter
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: name,
		Help: helpText(name),
	}, labels)
	if err := m.registry.Register(counter); err != nil {
		return nil
	}
	m.counters[key] = counter
	return counter
}

func (m *Metrics) histogram(name string, labels []string) *prometheus.HistogramVec {
	key := newMetricVecKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	if histogram := m.histograms[key]; histogram != nil {
		return histogram
	}
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name,
		Help: helpText(name),
	}, labels)
	if err := m.registry.Register(histogram); err != nil {
		return nil
	}
	m.histograms[key] = histogram
	return histogram
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

func helpText(name string) string {
	return "xflow metric " + name
}
