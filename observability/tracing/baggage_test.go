package tracing

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TestBaggageAllowlistRejectsForbiddenKeys proves tenant/token/payload (and
// the common secret aliases) never survive extraction — they are dropped and
// the parse-failure counter increments. B1 blocker 5.
func TestBaggageAllowlistRejectsForbiddenKeys(t *testing.T) {
	before := CarrierParseFailures()
	policy := DefaultBaggagePolicy()
	prop := FilteredBaggagePropagator(policy)

	cases := []string{"tenant", "TENANT", "token", "auth_token", "payload", "secret", "ak"}
	for _, key := range cases {
		h := propagation.MapCarrier{"baggage": key + "=sensitive"}
		ctx := prop.Extract(context.Background(), h)
		bag := baggage.FromContext(ctx)
		for _, m := range bag.Members() {
			if m.Key() == key || strings.EqualFold(m.Key(), key) {
				t.Fatalf("forbidden key %q survived extraction: %+v", key, bag.Members())
			}
		}
	}
	if got := CarrierParseFailures() - before; got < int64(len(cases)) {
		t.Fatalf("CarrierParseFailures incremented %d times, want >= %d (each forbidden key must be counted)", got, len(cases))
	}
}

// TestBaggageAllowlistTruncatesOversizedValue proves values exceeding
// MaxValueLen are truncated rather than dropped, and total-size/entry caps
// bound the accepted baggage.
func TestBaggageAllowlistTruncatesOversizedValue(t *testing.T) {
	policy := DefaultBaggagePolicy()
	prop := FilteredBaggagePropagator(policy)

	long := strings.Repeat("x", policy.MaxValueLen*4)
	h := propagation.MapCarrier{"baggage": "trace_id=" + long}
	ctx := prop.Extract(context.Background(), h)
	bag := baggage.FromContext(ctx)
	if len(bag.Members()) != 1 {
		t.Fatalf("expected 1 member, got %d", len(bag.Members()))
	}
	m := bag.Members()[0]
	if len(m.Value()) > policy.MaxValueLen {
		t.Fatalf("value not truncated: len=%d, max=%d", len(m.Value()), policy.MaxValueLen)
	}
	if m.Key() != "trace_id" {
		t.Fatalf("key lost on truncation: %q", m.Key())
	}
}

// TestBaggageAllowlistEnforcesEntryCount proves surplus entries beyond
// MaxEntries are dropped.
func TestBaggageAllowlistEnforcesEntryCount(t *testing.T) {
	policy := DefaultBaggagePolicy()
	prop := FilteredBaggagePropagator(policy)
	// Build more entries than MaxEntries.
	parts := make([]string, 0, policy.MaxEntries+5)
	for i := 0; i < policy.MaxEntries+5; i++ {
		parts = append(parts, "k"+strings.Repeat("a", i%3)+strconv.Itoa(i)+"=v")
	}
	h := propagation.MapCarrier{"baggage": strings.Join(parts, ",")}
	ctx := prop.Extract(context.Background(), h)
	bag := baggage.FromContext(ctx)
	if len(bag.Members()) > policy.MaxEntries {
		t.Fatalf("accepted %d entries, max %d", len(bag.Members()), policy.MaxEntries)
	}
}

// TestBaggageAllowlistAllowedKeysOnly proves a non-empty AllowedKeys list acts
// as a strict allowlist: keys not in the set are dropped.
func TestBaggageAllowlistAllowedKeysOnly(t *testing.T) {
	policy := DefaultBaggagePolicy()
	policy.AllowedKeys = []string{"trace_id", "rpc.service"}
	prop := FilteredBaggagePropagator(policy)
	h := propagation.MapCarrier{"baggage": "trace_id=ok,custom=dropped,rpc.service=ok2"}
	ctx := prop.Extract(context.Background(), h)
	bag := baggage.FromContext(ctx)
	keys := map[string]string{}
	for _, m := range bag.Members() {
		keys[m.Key()] = m.Value()
	}
	if _, ok := keys["custom"]; ok {
		t.Fatalf("non-allowlisted key 'custom' survived: %+v", keys)
	}
	if keys["trace_id"] != "ok" {
		t.Fatalf("trace_id lost: %+v", keys)
	}
	if keys["rpc.service"] != "ok2" {
		t.Fatalf("rpc.service lost: %+v", keys)
	}
}

// TestMalformedCarrierDoesNotPanicAndCounts proves a malformed traceparent does
// not panic on Extract and increments the CarrierParseFailures fallback metric.
// B1 blocker 6.
func TestMalformedCarrierDoesNotPanicAndCounts(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	before := CarrierParseFailures()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ExtractCarrier panicked on malformed carrier: %v", r)
		}
	}()
	ctx := ExtractCarrier(context.Background(), map[string]string{
		"traceparent": "not-a-valid-traceparent",
	})
	sc := oteltrace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		t.Fatal("malformed traceparent produced a valid SpanContext")
	}
	if got := CarrierParseFailures() - before; got < 1 {
		t.Fatalf("CarrierParseFailures did not increment for malformed carrier (got %d)", got)
	}
}

// TestExtractCarrierEmptyIsNoop proves an empty/nil carrier is a no-op without
// touching the failure counter.
func TestExtractCarrierEmptyIsNoop(t *testing.T) {
	before := CarrierParseFailures()
	_ = ExtractCarrier(context.Background(), nil)
	_ = ExtractCarrier(context.Background(), map[string]string{})
	if got := CarrierParseFailures() - before; got != 0 {
		t.Fatalf("empty carrier incremented failure counter: %d", got)
	}
}
