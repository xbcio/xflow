package kafka

import (
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

func validConfig() Config { return Config{Brokers: []string{"broker:9092"}} }

func TestNew_RequiresBrokers(t *testing.T) {
	for name, cfg := range map[string]Config{
		"nil":   {},
		"empty": {Brokers: []string{}},
	} {
		if tr, err := New(cfg, ""); err == nil || tr != nil {
			t.Errorf("%s brokers: New = %v, %v; want an error and no transport", name, tr, err)
		}
	}
}

func TestNew_RejectsBadSuffix(t *testing.T) {
	for _, suffix := range []string{"a/b", "tenant a", "..", strings.Repeat("x", 300)} {
		tr, err := New(validConfig(), suffix, withWriter(&fakeWriter{}))
		if err == nil || tr != nil {
			t.Errorf("suffix %q: New = %v, %v; want an error and no transport", suffix, tr, err)
		}
	}
}

func TestNew_BadSuffixErrorMatchesNames(t *testing.T) {
	_, want := newTopicNames("a/b")
	_, got := New(validConfig(), "a/b", withWriter(&fakeWriter{}))
	if want == nil || got == nil || got.Error() != want.Error() {
		t.Errorf("New error = %v, want the newTopicNames error %v passed through", got, want)
	}
}

func TestNew_BadSASL(t *testing.T) {
	cases := map[string]Config{
		"unsupported mechanism": {Brokers: []string{"b:9092"}, SASLMechanism: "gssapi", SASLUsername: "u", SASLPassword: "p"},
		"missing username":      {Brokers: []string{"b:9092"}, SASLMechanism: "plain", SASLPassword: "p"},
		"missing password":      {Brokers: []string{"b:9092"}, SASLMechanism: "scram-sha-256", SASLUsername: "u"},
		"missing both":          {Brokers: []string{"b:9092"}, SASLMechanism: "scram-sha-512"},
	}
	for name, cfg := range cases {
		if tr, err := New(cfg, ""); err == nil || tr != nil {
			t.Errorf("%s: New = %v, %v; want an error and no transport", name, tr, err)
		}
	}
}

func TestNew_DoesNotConnect(t *testing.T) {
	// Port 1 on the loopback interface refuses connections. New must succeed
	// anyway because the writer connects lazily, and Close must not block on a
	// broker that was never reached.
	tr, err := New(Config{Brokers: []string{"127.0.0.1:1"}}, "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- tr.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked on a transport that never connected")
	}
}

func TestNew_WarnsOnSuffixCollisionRisk(t *testing.T) {
	for suffix, wantWarn := range map[string]bool{
		"":         false,
		"tenant-a": false,
		"a.b":      true,
		"a_b":      true,
	} {
		log := &recordingLogger{}
		if _, err := New(validConfig(), suffix, withWriter(&fakeWriter{}), WithLogger(log)); err != nil {
			t.Fatalf("suffix %q: %v", suffix, err)
		}
		warns := log.warned()
		if wantWarn && len(warns) != 1 {
			t.Errorf("suffix %q: %d warnings, want 1", suffix, len(warns))
		}
		if !wantWarn && len(warns) != 0 {
			t.Errorf("suffix %q: unexpected warnings %+v", suffix, warns)
		}
		if wantWarn && len(warns) == 1 {
			found := false
			for _, a := range warns[0].args {
				if a == suffix {
					found = true
				}
			}
			if !found {
				t.Errorf("suffix %q: warning args %v do not name the suffix", suffix, warns[0].args)
			}
		}
	}
}

func TestNew_CollisionRiskWithoutLoggerIsSilent(t *testing.T) {
	if _, err := New(validConfig(), "a.b", withWriter(&fakeWriter{})); err != nil {
		t.Fatalf("New without a logger: %v", err)
	}
}

func TestNew_NilObserverKeepsNoop(t *testing.T) {
	tr, err := New(validConfig(), "", withWriter(&fakeWriter{}), WithObserver(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Enqueue(t.Context(), sampleTask()); err != nil {
		t.Fatalf("Enqueue with a nil observer option: %v", err)
	}
}

func TestNew_BuildsRealWriterByDefault(t *testing.T) {
	tr, err := New(validConfig(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	tw, ok := tr.writer.(transportWriter)
	if !ok || tw.Writer == nil {
		t.Fatalf("writer = %T, want a transportWriter around a kafka-go writer", tr.writer)
	}
}

func TestNewWriter_Defaults(t *testing.T) {
	w, err := newWriter(Config{Brokers: []string{"a:9092", "b:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	if w.Topic != "" {
		t.Errorf("Topic = %q, want empty: every message carries its own", w.Topic)
	}
	if got := w.Addr.String(); got != "a:9092,b:9092" {
		t.Errorf("Addr = %q, want a:9092,b:9092", got)
	}
	if _, ok := w.Balancer.(*kafkago.Hash); !ok {
		t.Errorf("Balancer = %T, want *kafka.Hash", w.Balancer)
	}
	if w.RequiredAcks != kafkago.RequireAll {
		t.Errorf("RequiredAcks = %v, want RequireAll", w.RequiredAcks)
	}
	if w.AllowAutoTopicCreation {
		t.Error("AllowAutoTopicCreation = true, want false")
	}
	if w.Async {
		t.Error("Async = true, want false: a nil return must mean the broker acknowledged")
	}
	if w.BatchTimeout != 5*time.Millisecond {
		t.Errorf("BatchTimeout = %v, want 5ms", w.BatchTimeout)
	}
	if w.BatchBytes != 8<<20 {
		t.Errorf("BatchBytes = %d, want 8 MiB", w.BatchBytes)
	}
	if w.WriteTimeout != 10*time.Second {
		t.Errorf("WriteTimeout = %v, want 10s", w.WriteTimeout)
	}
	if w.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", w.MaxAttempts)
	}
	tr, ok := w.Transport.(*kafkago.Transport)
	if !ok {
		t.Fatalf("Transport = %T, want *kafka.Transport", w.Transport)
	}
	if tr.TLS != nil || tr.SASL != nil {
		t.Errorf("default transport has TLS=%v SASL=%v, want plaintext without SASL", tr.TLS, tr.SASL)
	}
	if tr.MetadataTTL != 3*time.Second {
		t.Errorf("MetadataTTL = %v, want 3s", tr.MetadataTTL)
	}
}

func TestNewWriter_Overrides(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	w, err := newWriter(Config{
		Brokers:       []string{"b:9093"},
		TLS:           tlsCfg,
		SASLMechanism: "SCRAM-SHA-512",
		SASLUsername:  "user",
		SASLPassword:  "secret",
		BatchBytes:    1 << 20,
		WriteTimeout:  3 * time.Second,
		MaxAttempts:   9,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.BatchBytes != 1<<20 || w.WriteTimeout != 3*time.Second || w.MaxAttempts != 9 {
		t.Errorf("overrides lost: BatchBytes=%d WriteTimeout=%v MaxAttempts=%d", w.BatchBytes, w.WriteTimeout, w.MaxAttempts)
	}
	tr, ok := w.Transport.(*kafkago.Transport)
	if !ok {
		t.Fatalf("Transport = %T", w.Transport)
	}
	if tr.TLS != tlsCfg {
		t.Errorf("TLS = %p, want the configured %p", tr.TLS, tlsCfg)
	}
	if tr.SASL == nil || tr.SASL.Name() != "SCRAM-SHA-512" {
		t.Errorf("SASL = %v, want SCRAM-SHA-512", tr.SASL)
	}
	if tr.MetadataTTL != 3*time.Second {
		t.Errorf("MetadataTTL = %v, want 3s with TLS and SASL too", tr.MetadataTTL)
	}
}

func TestNewWriter_NonPositiveFallBackToDefaults(t *testing.T) {
	w, err := newWriter(Config{Brokers: []string{"b:9092"}, BatchBytes: -1, WriteTimeout: -time.Second, MaxAttempts: -2})
	if err != nil {
		t.Fatal(err)
	}
	if w.BatchBytes != 8<<20 || w.WriteTimeout != 10*time.Second || w.MaxAttempts != 5 {
		t.Errorf("negative settings were not defaulted: %d %v %d", w.BatchBytes, w.WriteTimeout, w.MaxAttempts)
	}
}

func TestBuildSASLMechanism(t *testing.T) {
	cases := []struct {
		mechanism string
		wantName  string
	}{
		{"plain", "PLAIN"},
		{"PLAIN", "PLAIN"},
		{"scram-sha-256", "SCRAM-SHA-256"},
		{"scram-sha-512", "SCRAM-SHA-512"},
		{"Scram-SHA-512", "SCRAM-SHA-512"},
	}
	for _, c := range cases {
		m, err := buildSASLMechanism(c.mechanism, "u", "p")
		if err != nil {
			t.Errorf("%q: %v", c.mechanism, err)
			continue
		}
		if m == nil || m.Name() != c.wantName {
			t.Errorf("%q: mechanism = %v, want %s", c.mechanism, m, c.wantName)
		}
	}

	m, err := buildSASLMechanism("", "", "")
	if m != nil || err != nil {
		t.Errorf("empty mechanism = %v, %v; want nil, nil", m, err)
	}
	m, err = buildSASLMechanism("", "ignored", "ignored")
	if m != nil || err != nil {
		t.Errorf("empty mechanism with credentials = %v, %v; want nil, nil", m, err)
	}
	if _, err := buildSASLMechanism("oauthbearer", "u", "p"); err == nil || !strings.Contains(err.Error(), "oauthbearer") {
		t.Errorf("unsupported mechanism error = %v, want one that names it", err)
	}
	if _, err := buildSASLMechanism("plain", "", "p"); err == nil {
		t.Error("empty username was accepted")
	}
	if _, err := buildSASLMechanism("plain", "u", ""); err == nil {
		t.Error("empty password was accepted")
	}
}

func TestClose_IdempotentAndClosesWriterOnce(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	for range 3 {
		if err := tr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if n := w.closeCount(); n != 1 {
		t.Errorf("writer closed %d times, want 1", n)
	}
}

func TestClose_ReturnsSameErrorEveryTime(t *testing.T) {
	boom := errors.New("close failed")
	w := &fakeWriter{closeErr: boom}
	tr, err := New(validConfig(), "", withWriter(w))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if got := tr.Close(); got != boom {
			t.Errorf("Close call %d = %v, want %v", i, got, boom)
		}
	}
	if n := w.closeCount(); n != 1 {
		t.Errorf("writer closed %d times, want 1", n)
	}
}

func TestClose_ConcurrentCallsCloseOnce(t *testing.T) {
	tr, w, _ := newTestTransport(t, "")
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { _ = tr.Close() })
	}
	wg.Wait()
	if n := w.closeCount(); n != 1 {
		t.Errorf("writer closed %d times under concurrent Close, want 1", n)
	}
}

func TestTransportWriter_CloseReleasesIdleConnections(t *testing.T) {
	// A writer that never produced has nothing to release; the point is that
	// Close does not panic on the transport newWriter attached and returns nil.
	kw, err := newWriter(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := (transportWriter{kw}).Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestEnqueueAfterClose(t *testing.T) {
	// A real writer reports a closed writer as io.ErrClosedPipe, which classify
	// treats as permanent, so the outbox dead-letters instead of retrying forever.
	tr, err := New(validConfig(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	err = tr.TryEnqueue(namespace.WithNamespace(t.Context(), "tenant-a"), sampleTask())
	if err == nil {
		t.Fatal("TryEnqueue on a closed transport succeeded")
	}
	if errors.Is(err, engine.ErrQueueFull) {
		t.Errorf("a closed transport must not read as backpressure: %v", err)
	}
}
