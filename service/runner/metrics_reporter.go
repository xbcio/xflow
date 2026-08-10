package runner

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"
)

// DefaultMetricsReportInterval is the runner's local reporting cadence when the
// server expresses no opinion. It matches Prometheus' typical scrape_interval
// (15s/30s): reporting more often than the scrape is pure waste, and reporting
// less often leaves the data unchanged between two scrapes, manufacturing fake
// plateaus. The Redis retention (3 * 30s) is six of these, so several
// consecutive failures still leave the series intact.
const DefaultMetricsReportInterval = 15 * time.Second

// metricsReportTimeout bounds one report POST. The reporter is a best-effort
// side channel; a hung server must not pin its goroutine until the next tick.
const metricsReportTimeout = 10 * time.Second

// runnerIDLabel is injected into every reported series so a raw payload is
// self-describing on the wire. The server overwrites it with the authenticated
// identity (see control.MetricsInbox) -- this copy is a convenience for
// debugging, not the authority.
const runnerIDLabel = "runner_id"

// MetricsReportClient is the optional protocol capability for shipping metrics.
// The HTTP client implements it; the gRPC client does not, so a gRPC runner
// never reports (see the activationAckClient precedent in activation_acker.go).
type MetricsReportClient interface {
	ReportMetrics(ctx context.Context, runnerID, sessionID string, body []byte) error
}

// MetricsReporterConfig holds everything the reporter needs to gather and ship
// the runner's metrics registry. It is deliberately separate from Runner.Config
// because reporting is unrelated to register/poll/report/heartbeat.
type MetricsReporterConfig struct {
	// Gatherer is the runner's whole Prometheus registry. Everything it holds
	// is shipped -- adding a metric never requires touching this file.
	Gatherer prometheus.Gatherer
	Client   MetricsReportClient
	RunnerID string
	// Interval is the starting cadence. Non-positive means
	// DefaultMetricsReportInterval, NOT "suspended": suspension is a runtime
	// state set by the server via SetInterval, not a construction-time one.
	Interval time.Duration
	// Metrics, when set, records xflow_runner_metrics_reports_total{result}.
	// nil is safe -- every recorder method is nil-receiver-guarded.
	Metrics *metrics.Metrics
	Logger  *slog.Logger
}

// MetricsReporter periodically ships the runner's whole Prometheus registry to
// the server, which merges it into its own scrape endpoint. It exists because a
// runner deployed in another network domain cannot be scraped directly.
//
// It is deliberately NOT part of Runner's own state: reporting is unrelated to
// register/poll/report/heartbeat, and folding it into Config would grow an
// already-long struct for no shared behavior.
type MetricsReporter struct {
	cfg    MetricsReporterConfig
	logger *slog.Logger

	mu       sync.Mutex
	interval time.Duration

	// reset wakes the loop when interval changes, so a new cadence applies at
	// once instead of after the current (up to 300s) timer expires. Buffered
	// depth 1 + non-blocking send: coalescing several rapid changes into one
	// wakeup is correct, since the loop re-reads the current interval anyway.
	reset chan struct{}

	// after is time.After in production; tests replace it to drive rounds
	// deterministically instead of sleeping.
	after func(time.Duration) <-chan time.Time
}

// NewMetricsReporter constructs a reporter. A nil Client or Gatherer results in
// a reporter whose Run returns immediately (gRPC runner path).
func NewMetricsReporter(cfg MetricsReporterConfig) *MetricsReporter {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultMetricsReportInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &MetricsReporter{
		cfg:      cfg,
		logger:   logger,
		interval: cfg.Interval,
		reset:    make(chan struct{}, 1),
		after:    time.After,
	}
}

// SetInterval adopts a new cadence. d <= 0 suspends reporting; the loop stays
// alive and resumes on the next positive value. Called from the heartbeat loop
// on every response, so the no-change fast path matters: rearming the timer
// every 5 seconds would make the effective cadence the heartbeat's, not the
// configured one.
func (r *MetricsReporter) SetInterval(d time.Duration) {
	r.mu.Lock()
	if d == r.interval {
		r.mu.Unlock()
		return
	}
	r.interval = d
	r.mu.Unlock()
	select {
	case r.reset <- struct{}{}:
	default:
	}
}

func (r *MetricsReporter) currentInterval() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.interval
}

// Run reports until ctx is cancelled. sessionID is passed explicitly because
// the caller starts this only after Register returned one; a reconnect ends
// this Run and starts a new one with the new session. The interval survives
// that restart -- a server-pushed cadence should not be undone by a reconnect.
func (r *MetricsReporter) Run(ctx context.Context, sessionID string) {
	if r.cfg.Client == nil || r.cfg.Gatherer == nil || sessionID == "" {
		return
	}
	for {
		interval := r.currentInterval()
		if interval <= 0 {
			// Suspended: wait for a new interval only. No timer is armed, so
			// nothing is gathered and nothing is sent.
			select {
			case <-ctx.Done():
				return
			case <-r.reset:
				continue
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-r.reset:
			continue
		case <-r.after(interval):
			r.reportOnce(ctx, sessionID)
		}
	}
}

// reportOnce gathers, encodes and ships one snapshot. Failures are logged and
// dropped -- never retried, never queued. Metrics are a current-state snapshot,
// not an event stream: resending a 15-second-old one has no value, and a queue
// would turn a server recovery into a thundering herd. Counters are immune to
// the loss by construction (monotonic totals), so rate() loses resolution
// between two successful reports but never computes a wrong value.
func (r *MetricsReporter) reportOnce(ctx context.Context, sessionID string) {
	body, err := r.encode()
	if err != nil {
		r.logger.Warn("metrics report encode failed", "err", err)
		r.count("error")
		return
	}
	if len(body) > protocol.MaxRunnerMetricsBytes {
		// Sending it would earn a 413. Log the size and drop locally so the
		// signal lands on the runner where the oversized registry lives.
		r.logger.Warn("metrics report exceeds server limit, dropped",
			"bytes", len(body), "limit", protocol.MaxRunnerMetricsBytes)
		r.count("error")
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, metricsReportTimeout)
	defer cancel()
	if err := r.cfg.Client.ReportMetrics(sendCtx, r.cfg.RunnerID, sessionID, body); err != nil {
		// err carries the server's status and body excerpt, never the bearer
		// token (see protocol.Client.ReportMetrics) -- org security rules S7.
		r.logger.Warn("metrics report failed", "err", err)
		r.count("error")
		return
	}
	r.count("ok")
}

func (r *MetricsReporter) count(result string) {
	r.cfg.Metrics.Inc("xflow_runner_metrics_reports_total", map[string]string{"result": result})
}

// encode gathers the whole registry, stamps runner_id on every series, and
// writes a gzipped delimited-protobuf stream.
func (r *MetricsReporter) encode() ([]byte, error) {
	// A partial gather still yields usable families; shipping them beats
	// shipping nothing because one collector misbehaved.
	fams, gatherErr := r.cfg.Gatherer.Gather()
	if len(fams) == 0 {
		if gatherErr != nil {
			return nil, fmt.Errorf("gather: %w", gatherErr)
		}
		return nil, nil
	}
	if gatherErr != nil {
		r.logger.Warn("partial metrics gather; shipping what was collected", "err", gatherErr)
	}
	for _, fam := range fams {
		setReporterRunnerID(fam, r.cfg.RunnerID)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	enc := expfmt.NewEncoder(zw, expfmt.NewFormat(expfmt.TypeProtoDelim))
	for _, fam := range fams {
		if err := enc.Encode(fam); err != nil {
			_ = zw.Close()
			return nil, fmt.Errorf("encode %s: %w", fam.GetName(), err)
		}
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf.Bytes(), nil
}

// setReporterRunnerID replaces any existing runner_id pair rather than
// appending one. Two pairs with the same name on one metric is a measured 500
// on the server's merged /metrics -- and a runner's registry can legitimately
// already carry a runner_id label on some series.
func setReporterRunnerID(fam *dto.MetricFamily, runnerID string) {
	for _, m := range fam.Metric {
		replaced := false
		for _, lp := range m.Label {
			if lp.GetName() == runnerIDLabel {
				lp.Value = proto.String(runnerID)
				replaced = true
			}
		}
		if !replaced {
			m.Label = append(m.Label, &dto.LabelPair{
				Name:  proto.String(runnerIDLabel),
				Value: proto.String(runnerID),
			})
		}
	}
}
