package runner

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/xbcio/xflow/service/protocol"
)

// TestReporterDropsAnOversizedSnapshotLocallyAndKeepsReporting drives
// metrics_reporter.go:177-183, the size guard none of the four existing
// reporter tests reaches — all of them gather testGatherer's single counter,
// which encodes to a few dozen bytes against a 1 MiB limit, and nothing in the
// package mentions MaxRunnerMetricsBytes.
//
// The branch exists because the alternative is worse than dropping: the server
// answers an oversized report with a 413, so shipping it wastes a round trip
// and lands the diagnosis on the wrong host. What makes it worth pinning is the
// second half — the reporter must drop the round and carry on. A registry
// crosses this limit through label cardinality, which is a bug that gets fixed;
// a reporter that stopped, or wedged, would take the runner's whole metrics
// channel down with it until a restart, long after the cardinality was gone.
//
// Both directions are asserted in one run against one reporter, because "no
// report was shipped" on its own also passes if reporting is broken outright.
// The oversized round must ship nothing and the following normal round must
// ship exactly one, on the same reporter instance.
//
// The oversized fixture is checked rather than assumed: encode() is called
// directly first and its length compared against the limit. Without that, a
// gzip ratio better than I guessed would silently turn the first half of this
// test into an assertion that a perfectly normal round ships nothing — which
// would then fail, but for a reason the failure message would not explain.
//
// Measured. Two mutations of the guard, each run against all six packages that
// depend on service/runner, and each also run with this file removed:
//
//   - the guard disabled (`if false && len(body) > ...`): the "shipped 0
//     reports" assertion goes red; without this file the whole scope stays
//     green.
//   - the "bytes" attr dropped from the Warn call: the log-attr assertion goes
//     red; without this file the whole scope stays green.
//
// The second mutation is the one that shows the size guard had no coverage at
// all rather than incidental coverage — nothing else in the module reads this
// log line.
func TestReporterDropsAnOversizedSnapshotLocallyAndKeepsReporting(t *testing.T) {
	gatherer := &swappableGatherer{inner: oversizedGatherer(t)}
	client := &fakeMetricsClient{}
	logs := &recordingHandler{}
	r := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: gatherer,
		Client:   client,
		RunnerID: "runner-a",
		Logger:   slog.New(logs),
	})

	body, err := r.encode()
	if err != nil {
		t.Fatalf("encode() error = %v", err)
	}
	if len(body) <= protocol.MaxRunnerMetricsBytes {
		t.Fatalf("oversized fixture encodes to %d bytes, limit is %d: the "+
			"fixture is not oversized, so the round below would be an ordinary "+
			"one and this test would be asserting the opposite of its name",
			len(body), protocol.MaxRunnerMetricsBytes)
	}

	// Round one: oversized. Invoke the round synchronously so this correctness
	// assertion does not also impose a wall-clock budget on gathering and gzip
	// under -race and atomic coverage. Run's timer loop is covered separately.
	r.reportOnce(t.Context(), "session-1")

	if got := client.snapshot(); len(got) != 0 {
		t.Fatalf("oversized round shipped %d reports, want 0: the server answers "+
			"an over-limit body with a 413, so sending it burns a round trip and "+
			"puts the diagnosis on the wrong host", len(got))
	}
	if rec, ok := logs.find("exceeds server limit"); !ok {
		t.Fatalf("no log line reported the drop; records = %v\nthe body never "+
			"leaves the runner, so this line is the only place the oversized "+
			"registry is visible at all", logs.messages())
	} else if bytesAttr, ok := rec.attr("bytes"); !ok || bytesAttr.(int64) <= int64(protocol.MaxRunnerMetricsBytes) {
		t.Fatalf("drop log bytes attr = %v (present=%v), want > %d: without the "+
			"measured size the line cannot tell an operator how far over the "+
			"registry is", bytesAttr, ok, protocol.MaxRunnerMetricsBytes)
	}

	// Round two: back under the limit. The same reporter must ship again, so a
	// mutation that turns the drop into a stop cannot pass.
	gatherer.swap(testGatherer(t))
	r.reportOnce(t.Context(), "session-1")
	got := client.snapshot()
	if len(got) != 1 {
		t.Fatalf("reports after recovery = %d, want exactly 1", len(got))
	}
}

// swappableGatherer lets one reporter see an oversized registry and then a
// normal one, so both halves of the behaviour are asserted against the same
// instance rather than against two reporters that might differ.
type swappableGatherer struct {
	mu    sync.Mutex
	inner prometheus.Gatherer
}

func (g *swappableGatherer) swap(next prometheus.Gatherer) {
	g.mu.Lock()
	g.inner = next
	g.mu.Unlock()
}

func (g *swappableGatherer) Gather() ([]*dto.MetricFamily, error) {
	g.mu.Lock()
	inner := g.inner
	g.mu.Unlock()
	return inner.Gather()
}

// oversizedGatherer builds a registry that encodes to more than the server's
// limit even after gzip. The shape is the one that produces this in production:
// a label whose values are effectively unique, so the series count explodes and
// the payload does not compress. The values are drawn from a fixed seed so the
// size is the same on every run.
func oversizedGatherer(t *testing.T) prometheus.Gatherer {
	t.Helper()
	reg := prometheus.NewRegistry()
	vec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "probe_cardinality", Help: "probe."},
		[]string{"request_id"},
	)
	if err := reg.Register(vec); err != nil {
		t.Fatalf("register: %v", err)
	}
	rng := rand.New(rand.NewSource(1))
	const (
		series      = 12000
		valueLength = 220
	)
	buf := make([]byte, valueLength)
	for i := 0; i < series; i++ {
		for j := range buf {
			// Printable ASCII, so the label value stays valid UTF-8 while
			// carrying close to 6.5 bits of entropy per byte -- gzip cannot
			// give most of it back.
			buf[j] = byte(33 + rng.Intn(94))
		}
		vec.WithLabelValues(string(buf)).Set(float64(i))
	}
	return reg
}

// recordingHandler keeps every slog record so a test can assert on the one
// diagnostic a dropped round leaves behind.
type recordingHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

type capturedRecord struct {
	message string
	attrs   map[string]any
}

func (r capturedRecord) attr(key string) (any, bool) {
	v, ok := r.attrs[key]
	return v, ok
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, rec slog.Record) error {
	captured := capturedRecord{message: rec.Message, attrs: map[string]any{}}
	rec.Attrs(func(a slog.Attr) bool {
		captured.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, captured)
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) find(substr string) (capturedRecord, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, rec := range h.records {
		if strings.Contains(rec.message, substr) {
			return rec, true
		}
	}
	return capturedRecord{}, false
}

func (h *recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.records))
	for _, rec := range h.records {
		out = append(out, fmt.Sprintf("%s %v", rec.message, rec.attrs))
	}
	return out
}
