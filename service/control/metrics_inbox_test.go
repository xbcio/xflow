package control

import (
	"bytes"
	"compress/gzip"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"

	"github.com/xbcio/xflow/service/protocol"

	"net/http/httptest"
)

// aliveLiveness treats every runner as live. Expiry is covered separately in
// metrics_inbox_liveness_test.go.
type aliveLiveness struct{}

func (aliveLiveness) IsRunnerLive(context.Context, string, time.Time) bool { return true }

// encodeFamilies renders families as the gzip'd delimited-protobuf stream a
// runner would POST. Shared by every inbox test so all of them go through the
// exact wire format rather than a shortcut.
func encodeFamilies(t *testing.T, fams ...*dto.MetricFamily) []byte {
	t.Helper()
	var raw bytes.Buffer
	enc := expfmt.NewEncoder(&raw, expfmt.NewFormat(expfmt.TypeProtoDelim))
	for _, f := range fams {
		if err := enc.Encode(f); err != nil {
			t.Fatalf("encode %s: %v", f.GetName(), err)
		}
	}
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	if _, err := w.Write(raw.Bytes()); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return gz.Bytes()
}

func counterFamily(name, help string, value float64, labels ...string) *dto.MetricFamily {
	m := &dto.Metric{Counter: &dto.Counter{Value: proto.Float64(value)}}
	for i := 0; i+1 < len(labels); i += 2 {
		m.Label = append(m.Label, &dto.LabelPair{
			Name: proto.String(labels[i]), Value: proto.String(labels[i+1]),
		})
	}
	return &dto.MetricFamily{
		Name:   proto.String(name),
		Help:   proto.String(help),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{m},
	}
}

func gaugeFamily(name, help string, value float64) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String(name),
		Help: proto.String(help),
		Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{
			Gauge: &dto.Gauge{Value: proto.Float64(value)},
		}},
	}
}

// serverRegistryWith builds a stand-in for the server's own registry holding
// one counter family, so tests can create genuine same-name collisions.
func serverRegistryWith(t *testing.T, name, help string) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	c.Add(7)
	if err := reg.Register(c); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return reg
}

// scrape renders the merged endpoint exactly as Prometheus would see it, so a
// 500 in these tests means a 500 in production.
func scrape(t *testing.T, gatherers prometheus.Gatherers) (int, string) {
	t.Helper()
	h := promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Code, rec.Body.String()
}

func newTestInbox(t *testing.T, self prometheus.Gatherer) *MetricsInbox {
	t.Helper()
	// The store's clock must be the inbox's clock: the store evicts by the
	// stamp the inbox writes, so two clocks that disagree would expire every
	// payload the instant it lands.
	now := func() time.Time { return time.Unix(1754000000, 0) }
	return NewMetricsInbox(MetricsInboxConfig{
		Store: NewMemoryMetricsStoreWith(DefaultMetricsRetention, now),
		Self:  self,
		Live:  aliveLiveness{},
		Now:   now,
	})
}

func protocolStamp(at time.Time, body []byte) []byte {
	return protocol.MetricsPayloadStamp(at, body)
}

func TestInboxMergesRunnerSeriesAlongsideServerOwn(t *testing.T) {
	reg := serverRegistryWith(t, "probe_total", "shared help")
	in := newTestInbox(t, reg)

	body := encodeFamilies(t, counterFamily("probe_total", "shared help", 3))
	if err := in.Accept(context.Background(), "runner-a", body); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if !strings.Contains(out, "probe_total 7") {
		t.Errorf("server's own series missing:\n%s", out)
	}
	if !strings.Contains(out, `probe_total{runner_id="runner-a"} 3`) {
		t.Errorf("runner series missing or unlabeled:\n%s", out)
	}
}

func TestInboxTwoRunnersNeverCollide(t *testing.T) {
	reg := prometheus.NewRegistry()
	in := newTestInbox(t, reg)

	// Byte-identical payloads from two runners. Without the server-side
	// runner_id injection this collides with
	// "collected before with the same name and label values" -> 500.
	body := encodeFamilies(t, counterFamily("collide_total", "same help", 1))
	for _, id := range []string{"runner-a", "runner-b"} {
		if err := in.Accept(context.Background(), id, body); err != nil {
			t.Fatalf("Accept %s: %v", id, err)
		}
	}

	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	for _, want := range []string{
		`collide_total{runner_id="runner-a"} 1`,
		`collide_total{runner_id="runner-b"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in:\n%s", want, out)
		}
	}
}

func TestInboxReplacesSelfDeclaredRunnerID(t *testing.T) {
	reg := prometheus.NewRegistry()
	in := newTestInbox(t, reg)

	// The runner injects runner_id itself (§6.1) and additionally lies about it.
	// Appending a second pair yields "has two or more labels with the same
	// name" -> 500, so the inbox must REPLACE, and the authenticated id wins.
	body := encodeFamilies(t, counterFamily("claim_total", "h", 2, "runner_id", "victim", "zone", "z1"))
	if err := in.Accept(context.Background(), "runner-a", body); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if strings.Contains(out, `"victim"`) {
		t.Errorf("self-declared runner_id was trusted:\n%s", out)
	}
	if !strings.Contains(out, `runner_id="runner-a"`) || !strings.Contains(out, `zone="z1"`) {
		t.Errorf("want authenticated runner_id and preserved zone label:\n%s", out)
	}
}

func TestInboxDropsFamilyOnHelpMismatch(t *testing.T) {
	reg := serverRegistryWith(t, "probe_total", "server help")
	in := newTestInbox(t, reg)

	body := encodeFamilies(t,
		counterFamily("probe_total", "DIFFERENT help", 3),
		counterFamily("innocent_total", "fine", 5),
	)
	if err := in.Accept(context.Background(), "runner-a", body); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if !strings.Contains(out, "probe_total 7") {
		t.Errorf("server's own series must survive the conflict:\n%s", out)
	}
	if strings.Contains(out, `probe_total{runner_id=`) {
		t.Errorf("conflicting family must be dropped:\n%s", out)
	}
	if !strings.Contains(out, `innocent_total{runner_id="runner-a"} 5`) {
		t.Errorf("only the conflicting family may be dropped, not the whole report:\n%s", out)
	}
}

func TestInboxDropsFamilyOnTypeMismatch(t *testing.T) {
	reg := serverRegistryWith(t, "probe_total", "same help")
	in := newTestInbox(t, reg)

	// Same name, same help, GAUGE instead of COUNTER. Measured: this 500s just
	// like a help mismatch, so it must be intercepted too.
	body := encodeFamilies(t, gaugeFamily("probe_total", "same help", 3))
	if err := in.Accept(context.Background(), "runner-a", body); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if !strings.Contains(out, "probe_total 7") {
		t.Errorf("server's own series must survive:\n%s", out)
	}
	if strings.Contains(out, `runner_id=`) {
		t.Errorf("type-conflicting family must be dropped:\n%s", out)
	}
}

func TestInboxKeepsFamilyServerDoesNotHave(t *testing.T) {
	// Guards the deviation from spec §5: conflict detection compares against
	// the families the server's registry actually produces, NOT a help table.
	// A third-party collector family the server has never heard of is not a
	// conflict and must pass through.
	reg := prometheus.NewRegistry()
	in := newTestInbox(t, reg)

	body := encodeFamilies(t, counterFamily("third_party_total", "arbitrary vendor text", 4))
	if err := in.Accept(context.Background(), "runner-a", body); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if !strings.Contains(out, `third_party_total{runner_id="runner-a"} 4`) {
		t.Errorf("unknown-to-server family must not be dropped:\n%s", out)
	}
}

func TestInboxGatherSwallowsStoreError(t *testing.T) {
	// Measured: a Gatherer returning (partial, error) still 500s the whole
	// endpoint under the default HTTPErrorOnError, taking the server's own
	// metrics down with it. So Gather must never return an error.
	reg := serverRegistryWith(t, "probe_total", "h")
	in := NewMetricsInbox(MetricsInboxConfig{
		Store: failingMetricsStore{},
		Self:  reg,
		Live:  aliveLiveness{},
		Now:   func() time.Time { return time.Unix(1754000000, 0) },
	})

	fams, err := in.Gather()
	if err != nil {
		t.Fatalf("Gather returned error %v; it must swallow store failures", err)
	}
	if len(fams) != 0 {
		t.Fatalf("Gather returned %d families on store failure, want 0", len(fams))
	}
	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if !strings.Contains(out, "probe_total 7") {
		t.Errorf("server's own metrics must survive a store outage:\n%s", out)
	}
}

type failingMetricsStore struct{}

func (failingMetricsStore) Put(context.Context, string, []byte) error {
	return context.DeadlineExceeded
}

func (failingMetricsStore) List(context.Context) (map[string][]byte, error) {
	return nil, context.DeadlineExceeded
}

func TestInboxSkipsUndecodablePayload(t *testing.T) {
	reg := prometheus.NewRegistry()
	now := func() time.Time { return time.Unix(1754000000, 0) }
	store := NewMemoryMetricsStoreWith(DefaultMetricsRetention, now)
	in := NewMetricsInbox(MetricsInboxConfig{
		Store: store, Self: reg, Live: aliveLiveness{},
		Now: now,
	})
	// Bypass Accept: a value already in Redis can be corrupt, and the read path
	// must not take the endpoint down over it.
	if err := store.Put(context.Background(),
		"runner-bad", protocolStamp(time.Unix(1754000000, 0), []byte("not gzip"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	fams, err := in.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(fams) != 0 {
		t.Fatalf("got %d families from a corrupt value, want 0", len(fams))
	}
}
