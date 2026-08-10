package control

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol"
)

// runnerIDLabel is the label the inbox stamps onto every proxied series. It is
// always taken from the authenticated identity, never from the payload: a
// runner that could name its own runner_id could forge another runner's
// metrics and point alerts at an innocent instance.
const runnerIDLabel = "runner_id"

// MetricsStore holds the most recent metrics payload per runner. Values are the
// runner's bytes prefixed with the receive stamp (protocol.MetricsPayloadStamp)
// — the store never decodes protobuf, so a decode failure can only ever affect
// one read, not the accept path.
type MetricsStore interface {
	// Put replaces the runner's retained payload and refreshes its retention.
	Put(ctx context.Context, runnerID string, stamped []byte) error
	// List returns every retained payload keyed by runner id.
	List(ctx context.Context) (map[string][]byte, error)
}

// MemoryMetricsStore is the single-replica MetricsStore. It is the fallback
// when the backend exposes no Redis client, mirroring how
// selectRunnerDirectory falls back to MemoryRunnerDirectory: a single-node or
// test deployment behaves exactly as before this feature existed.
//
// Retention semantics mirror RedisMetricsStore: entries older than the
// configured retention (based on the protocol stamp embedded in the value) are
// evicted on Put and List. This prevents unbounded growth when short-lived
// runners are started and stopped repeatedly.
type MemoryMetricsStore struct {
	mu        sync.RWMutex
	vals      map[string][]byte
	retention time.Duration
	now       func() time.Time
}

// NewMemoryMetricsStore creates a MemoryMetricsStore with the same default
// retention the Redis store uses (DefaultMetricsRetention = 3 × DefaultRunnerLiveTTL).
// Use NewMemoryMetricsStoreWith for explicit control over retention and clock.
func NewMemoryMetricsStore() *MemoryMetricsStore {
	return NewMemoryMetricsStoreWith(DefaultMetricsRetention, nil)
}

// NewMemoryMetricsStoreWith creates a MemoryMetricsStore with explicit
// retention and clock. If retention <= 0 it defaults to DefaultMetricsRetention.
// If now is nil it defaults to time.Now.
func NewMemoryMetricsStoreWith(retention time.Duration, now func() time.Time) *MemoryMetricsStore {
	if retention <= 0 {
		retention = DefaultMetricsRetention
	}
	if now == nil {
		now = time.Now
	}
	return &MemoryMetricsStore{
		vals:      make(map[string][]byte),
		retention: retention,
		now:       now,
	}
}

func (s *MemoryMetricsStore) Put(_ context.Context, runnerID string, stamped []byte) error {
	if runnerID == "" {
		return errors.New("control: metrics store: empty runner id")
	}
	cp := make([]byte, len(stamped))
	copy(cp, stamped)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vals[runnerID] = cp
	s.evictLocked()
	return nil
}

func (s *MemoryMetricsStore) List(context.Context) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictLocked()
	out := make(map[string][]byte, len(s.vals))
	for id, v := range s.vals {
		out[id] = v
	}
	return out, nil
}

// evictLocked removes entries whose protocol stamp is older than the retention
// window, mirroring the TTL RedisMetricsStore sets on every key. Must be called
// with s.mu held for writing.
//
// A value whose stamp cannot be read is deliberately KEPT: Gather counts it
// into xflow_runner_metrics_gather_errors_total{reason="decode_error"}, and
// evicting it here would swallow that signal. Accept always stamps, so such a
// value means a bug worth seeing rather than a leak worth reclaiming.
func (s *MemoryMetricsStore) evictLocked() {
	cutoff := s.now().Add(-s.retention)
	for id, stamped := range s.vals {
		receivedAt, _, ok := protocol.MetricsPayloadUnstamp(stamped)
		if ok && receivedAt.Before(cutoff) {
			delete(s.vals, id)
		}
	}
}

// RunnerLiveness reports whether a runner is currently live. Satisfied by the
// control plane's existing directory + selector pair, so metrics visibility and
// scheduling availability share one verdict: there is never a window where the
// scheduler has given up on a runner but monitoring still shows it healthy.
type RunnerLiveness interface {
	IsRunnerLive(ctx context.Context, runnerID string, now time.Time) bool
}

// MetricsInboxConfig configures a MetricsInbox.
type MetricsInboxConfig struct {
	// Store retains one payload per runner. Required.
	Store MetricsStore
	// Self is the server's OWN gatherer (its Prometheus registry). The inbox
	// reads the families it currently produces on every Gather to detect
	// same-name help/type conflicts. It deliberately does NOT consult the
	// static help table: that table returns a generic fallback for unregistered
	// names, which would make every third-party runner collector look like a
	// conflict and get silently dropped. Nil disables conflict checking (tests
	// only; production must set it).
	Self prometheus.Gatherer
	// Live decides which runners' series are still emitted. Nil means every
	// retained payload is emitted (tests only).
	Live RunnerLiveness
	// Metrics, when set, records the inbox's own counters. Optional.
	Metrics *metrics.Metrics
	// Logger receives read-path diagnostics. Optional.
	Logger engine.Logger
	// Now is the clock. Nil means time.Now. Injected so expiry tests can
	// advance time instead of sleeping.
	Now func() time.Time
}

// MetricsInbox holds the most recent metric snapshot each runner reported and
// implements prometheus.Gatherer so prometheus.Gatherers can merge it into the
// server's own scrape endpoint.
//
// It has no mutable state of its own beyond configuration: the read path does
// no read-modify-write, so concurrent Gather calls across replicas produce the
// same output with no locking.
type MetricsInbox struct {
	cfg MetricsInboxConfig
}

func NewMetricsInbox(cfg MetricsInboxConfig) *MetricsInbox {
	return &MetricsInbox{cfg: cfg}
}

func (in *MetricsInbox) now() time.Time {
	if in == nil || in.cfg.Now == nil {
		return time.Now()
	}
	return in.cfg.Now()
}

// Accept stores one runner's report. runnerID must already be the
// authenticated identity — Accept does not verify it and does not look inside
// gzipped, which is exactly the point: the write path spends no CPU on
// protobuf, and a payload that cannot be decoded degrades one scrape rather
// than rejecting the report.
func (in *MetricsInbox) Accept(ctx context.Context, runnerID string, gzipped []byte) error {
	if in == nil || in.cfg.Store == nil {
		return errors.New("control: metrics inbox not configured")
	}
	if runnerID == "" {
		return ErrRunnerIDRequired
	}
	stamped := protocol.MetricsPayloadStamp(in.now(), gzipped)
	if err := in.cfg.Store.Put(ctx, runnerID, stamped); err != nil {
		in.cfg.Metrics.Inc("xflow_runner_metrics_rejected_total",
			map[string]string{"runner_id": runnerID, "reason": "store_error"})
		return err
	}
	in.cfg.Metrics.Inc("xflow_runner_metrics_received_total",
		map[string]string{"runner_id": runnerID})
	in.cfg.Metrics.ObserveBytes("xflow_runner_metrics_report_bytes",
		map[string]string{"runner_id": runnerID}, len(gzipped))
	return nil
}

// Gather implements prometheus.Gatherer.
//
// It NEVER returns an error. Measured: a Gatherer that returns (partial
// results, error) still makes promhttp's default HTTPErrorOnError emit a 500,
// which would take the server's OWN metrics down with the runner proxy every
// time Redis hiccups. Failures are therefore counted into
// xflow_runner_metrics_gather_errors_total and the successfully decoded part is
// returned.
//
// Conflicts are likewise intercepted here rather than left to
// prometheus.Gatherers: with the default handler a conflict is a 500, and with
// ContinueOnError it is a SILENT whole-family drop, which is worse because it
// produces no signal at all. Losing one family with a counter beats both.
func (in *MetricsInbox) Gather() ([]*dto.MetricFamily, error) {
	if in == nil || in.cfg.Store == nil {
		return nil, nil
	}
	ctx := context.Background()
	now := in.now()

	stored, err := in.cfg.Store.List(ctx)
	if err != nil {
		in.cfg.Metrics.Inc("xflow_runner_metrics_gather_errors_total",
			map[string]string{"reason": "store_error"})
		if in.cfg.Logger != nil {
			in.cfg.Logger.Warn("runner metrics inbox: store read failed", "error", err)
		}
		return nil, nil
	}
	in.cfg.Metrics.Set("xflow_runner_metrics_inbox_size", nil, float64(len(stored)))

	selfFamilies := in.selfFamilies()

	// Deterministic order so a scrape is reproducible; promhttp sorts anyway,
	// but a stable order makes test failures readable.
	ids := make([]string, 0, len(stored))
	for id := range stored {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var out []*dto.MetricFamily
	for _, runnerID := range ids {
		receivedAt, payload, ok := protocol.MetricsPayloadUnstamp(stored[runnerID])
		if !ok {
			in.cfg.Metrics.Inc("xflow_runner_metrics_gather_errors_total",
				map[string]string{"reason": "decode_error"})
			continue
		}
		live := in.isLive(ctx, runnerID, now)
		in.cfg.Metrics.Set("xflow_runner_up",
			map[string]string{"runner_id": runnerID}, boolGauge(live))
		in.cfg.Metrics.Set("xflow_runner_metrics_last_report_age_seconds",
			map[string]string{"runner_id": runnerID}, now.Sub(receivedAt).Seconds())
		if !live {
			// Retained but not emitted: the runner is dead by the same verdict
			// the scheduler uses, so its series must disappear exactly when the
			// scheduler stops sending it work.
			continue
		}
		fams, decErr := decodeMetricsPayload(payload)
		if decErr != nil {
			in.cfg.Metrics.Inc("xflow_runner_metrics_gather_errors_total",
				map[string]string{"reason": "decode_error"})
			if in.cfg.Logger != nil {
				in.cfg.Logger.Warn("runner metrics inbox: payload decode failed",
					"runner", runnerID, "error", decErr)
			}
			continue
		}
		for _, fam := range fams {
			if reason, conflict := conflictReason(fam, selfFamilies); conflict {
				in.cfg.Metrics.Inc("xflow_runner_metrics_rejected_total",
					map[string]string{"runner_id": runnerID, "reason": reason})
				if in.cfg.Logger != nil {
					in.cfg.Logger.Warn("runner metrics inbox: dropping conflicting family",
						"runner", runnerID, "family", fam.GetName(), "reason", reason)
				}
				continue
			}
			setRunnerIDLabel(fam, runnerID)
			out = append(out, fam)
		}
	}
	return out, nil
}

func (in *MetricsInbox) isLive(ctx context.Context, runnerID string, now time.Time) bool {
	if in.cfg.Live == nil {
		return true
	}
	return in.cfg.Live.IsRunnerLive(ctx, runnerID, now)
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// selfFamily is the (help, type) pair prometheus.Gatherers requires every
// same-named family to agree on.
type selfFamily struct {
	help string
	typ  dto.MetricType
}

// selfFamilies snapshots what the server's own registry currently produces. A
// gather error here is not fatal: an empty map only means no conflict is
// detected this round, and the merge itself would still surface a real problem.
func (in *MetricsInbox) selfFamilies() map[string]selfFamily {
	if in.cfg.Self == nil {
		return nil
	}
	fams, err := in.cfg.Self.Gather()
	if err != nil && len(fams) == 0 {
		return nil
	}
	out := make(map[string]selfFamily, len(fams))
	for _, f := range fams {
		out[f.GetName()] = selfFamily{help: f.GetHelp(), typ: f.GetType()}
	}
	return out
}

// conflictReason reports whether merging fam would make prometheus.Gatherers
// fail. Only families the server ALSO produces can conflict; a name the server
// has never heard of passes through untouched.
func conflictReason(fam *dto.MetricFamily, self map[string]selfFamily) (string, bool) {
	mine, ok := self[fam.GetName()]
	if !ok {
		return "", false
	}
	if mine.help != fam.GetHelp() {
		return "help_mismatch", true
	}
	if mine.typ != fam.GetType() {
		return "type_mismatch", true
	}
	return "", false
}

// setRunnerIDLabel forces runner_id onto every series in fam, REPLACING any
// existing pair. Appending instead would produce "has two or more labels with
// the same name", a measured 500 — and the runner does inject its own runner_id
// (a convenience for reading raw payloads), so the pair is normally present.
func setRunnerIDLabel(fam *dto.MetricFamily, runnerID string) {
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

// decodeMetricsPayload gunzips and decodes the delimited-protobuf stream a
// runner sent. The reader is bounded by protocol.MaxRunnerMetricsBytes so a
// gzip bomb cannot exhaust memory on the read path (the accept path's
// MaxBytesReader only bounds the COMPRESSED size).
func decodeMetricsPayload(gzipped []byte) ([]*dto.MetricFamily, error) {
	zr, err := gzip.NewReader(bytes.NewReader(gzipped))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	limited := io.LimitReader(zr, int64(protocol.MaxRunnerMetricsBytes)*32)
	dec := expfmt.NewDecoder(limited, expfmt.NewFormat(expfmt.TypeProtoDelim))
	var out []*dto.MetricFamily
	for {
		fam := &dto.MetricFamily{}
		if err := dec.Decode(fam); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
		out = append(out, fam)
	}
}

var _ prometheus.Gatherer = (*MetricsInbox)(nil)
