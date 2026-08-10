package runner

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	dto "github.com/prometheus/client_model/go"
)

// --- fakes ---

type capturedReport struct {
	runnerID  string
	sessionID string
	body      []byte
}

type fakeMetricsClient struct {
	mu      sync.Mutex
	reports []capturedReport
	err     error
}

func (f *fakeMetricsClient) ReportMetrics(_ context.Context, runnerID, sessionID string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, capturedReport{runnerID, sessionID, append([]byte(nil), body...)})
	return f.err
}

func (f *fakeMetricsClient) snapshot() []capturedReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capturedReport(nil), f.reports...)
}

// countingGatherer counts Gather calls so a test can assert reporting STOPPED
// (a reverse assertion: the count must stop growing, not merely be zero once).
type countingGatherer struct {
	mu    sync.Mutex
	calls int
	inner prometheus.Gatherer
}

func (g *countingGatherer) Gather() ([]*dto.MetricFamily, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	return g.inner.Gather()
}

func (g *countingGatherer) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// manualTicks lets a test decide exactly when a report round happens. Each send
// on ch is one due timer.
type manualTicks struct {
	mu       sync.Mutex
	ch       chan time.Time
	requests int
}

func newManualTicks() *manualTicks { return &manualTicks{ch: make(chan time.Time, 1)} }

func (m *manualTicks) after(time.Duration) <-chan time.Time {
	m.mu.Lock()
	m.requests++
	m.mu.Unlock()
	return m.ch
}

func (m *manualTicks) fire(t *testing.T) {
	t.Helper()
	select {
	case m.ch <- time.Unix(0, 0):
	case <-time.After(2 * time.Second):
		t.Fatal("reporter never waited on the timer")
	}
}

func (m *manualTicks) requestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

// testGatherer returns a registry holding one counter at value 1.
func testGatherer(t *testing.T) prometheus.Gatherer {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "probe_total", Help: "probe."})
	c.Inc()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg
}

func decodeReport(t *testing.T, body []byte) []*dto.MetricFamily {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	dec := expfmt.NewDecoder(zr, expfmt.NewFormat(expfmt.TypeProtoDelim))
	var out []*dto.MetricFamily
	for {
		var fam dto.MetricFamily
		if err := dec.Decode(&fam); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode: %v", err)
		}
		out = append(out, &fam)
	}
	return out
}

func waitForReports(t *testing.T, c *fakeMetricsClient, want int) []capturedReport {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.snapshot(); len(got) >= want {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d reports after 2s, want %d", len(c.snapshot()), want)
	return nil
}

func waitForTimerRequests(t *testing.T, m *manualTicks, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.requestCount() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timer requests = %d after 2s, want %d", m.requestCount(), want)
}

// The payload must be gzipped delimited protobuf carrying runner_id, and the
// runner must be identified by the value passed to Run — not by anything the
// gatherer happened to contain.
func TestReporterShipsGzippedProtobufWithRunnerID(t *testing.T) {
	client := &fakeMetricsClient{}
	ticks := newManualTicks()
	r := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: testGatherer(t),
		Client:   client,
		RunnerID: "runner-a",
		Interval: time.Hour, // never fires on its own; ticks.fire drives it
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	r.after = ticks.after

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx, "session-1")

	ticks.fire(t)
	got := waitForReports(t, client, 1)[0]

	if got.runnerID != "runner-a" || got.sessionID != "session-1" {
		t.Fatalf("identity = (%q, %q), want (runner-a, session-1)", got.runnerID, got.sessionID)
	}
	fams := decodeReport(t, got.body)
	if len(fams) != 1 || fams[0].GetName() != "probe_total" {
		t.Fatalf("families = %v, want one probe_total", fams)
	}
	labels := fams[0].Metric[0].Label
	if len(labels) != 1 || labels[0].GetName() != "runner_id" || labels[0].GetValue() != "runner-a" {
		t.Fatalf("labels = %v, want runner_id=runner-a", labels)
	}
}

// A failed report is dropped, never queued: the next round must ship a FRESH
// gather, not a replay. Retrying a 15-second-old snapshot has no value and a
// queue turns a server recovery into a thundering herd.
func TestReporterDropsFailedRoundWithoutRetrying(t *testing.T) {
	client := &fakeMetricsClient{err: errors.New("server down")}
	ticks := newManualTicks()
	gatherer := &countingGatherer{inner: testGatherer(t)}
	r := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: gatherer,
		Client:   client,
		RunnerID: "runner-a",
		Interval: time.Hour,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	r.after = ticks.after

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx, "session-1")

	ticks.fire(t)
	waitForReports(t, client, 1)
	ticks.fire(t)
	reports := waitForReports(t, client, 2)

	if len(reports) != 2 {
		t.Fatalf("reports = %d, want exactly 2 (one per tick, no retry)", len(reports))
	}
	if got := gatherer.count(); got != 2 {
		t.Fatalf("Gather calls = %d, want 2 — a retry would reuse the old snapshot", got)
	}
}

// A positive SetInterval takes effect immediately via the reset signal rather
// than after the current (possibly 300s) timer expires.
func TestReporterSetIntervalTakesEffectImmediately(t *testing.T) {
	client := &fakeMetricsClient{}
	ticks := newManualTicks()
	r := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: testGatherer(t),
		Client:   client,
		RunnerID: "runner-a",
		Interval: 5 * time.Minute,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	r.after = ticks.after

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx, "session-1")

	// Wait until the loop is parked on its first timer.
	waitForTimerRequests(t, ticks, 1)

	r.SetInterval(20 * time.Second)
	// The reset must make the loop rearm — a second timer request — without any
	// tick having fired.
	waitForTimerRequests(t, ticks, 2)
	if got := r.currentInterval(); got != 20*time.Second {
		t.Fatalf("interval = %v, want 20s", got)
	}
	if reports := client.snapshot(); len(reports) != 0 {
		t.Fatalf("reset alone sent %d reports; it must only rearm the timer", len(reports))
	}
}

// A non-positive interval suspends reporting: the component stays alive (so a
// later positive value resumes it) but must stop gathering entirely.
//
// This is a reverse assertion. Reading the count once proves nothing — a
// reporter that had not started yet would also read 0. So: prove reporting
// worked first, then suspend, then prove the count STOPS growing while the
// still-live loop keeps accepting SetInterval.
func TestReporterSuspendsOnNonPositiveInterval(t *testing.T) {
	client := &fakeMetricsClient{}
	ticks := newManualTicks()
	gatherer := &countingGatherer{inner: testGatherer(t)}
	r := NewMetricsReporter(MetricsReporterConfig{
		Gatherer: gatherer,
		Client:   client,
		RunnerID: "runner-a",
		Interval: time.Hour,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	r.after = ticks.after

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx, "session-1")

	// Precondition: it really does report.
	ticks.fire(t)
	waitForReports(t, client, 1)
	activeCount := gatherer.count()

	r.SetInterval(-1 * time.Second)
	// The suspended loop parks on reset only — it must NOT ask for a timer
	// again. Give it room to misbehave, then confirm it did not.
	before := ticks.requestCount()
	time.Sleep(50 * time.Millisecond)
	if after := ticks.requestCount(); after != before {
		t.Fatalf("suspended reporter armed %d new timers; it must not arm any", after-before)
	}
	if got := gatherer.count(); got != activeCount {
		t.Fatalf("Gather calls grew from %d to %d while suspended", activeCount, got)
	}

	// Still alive: a positive value resumes it.
	r.SetInterval(30 * time.Second)
	ticks.fire(t)
	waitForReports(t, client, 2)
}
