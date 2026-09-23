package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

// An operator who has heard of group execution declares it by hand. A bare
// entry with no feature is the single worst shape available: canRunRouting
// ignores Features and accepts it, MatchCapabilities does not and rejects it, so
// the runner reads as correctly configured while still receiving nothing. And a
// second, feature-bearing entry would not fix it — hasCapabilityForRequirement
// stops at the first NodeType match, so the bare one would shadow it.
func TestNewRunnerCompletesAHandDeclaredGroupCapability(t *testing.T) {
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.function", engine.GroupNodeType},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}

	var groupCaps int
	var advertised bool
	for _, c := range cfg.Capabilities {
		if c.NodeType != engine.GroupNodeType {
			continue
		}
		groupCaps++
		for _, f := range c.Features {
			if f == engine.FeatureGroupExecV1 {
				advertised = true
			}
		}
	}
	if !advertised {
		t.Errorf("capabilities %+v: a caller-declared %s was left without %s — "+
			"the one shape that passes canRunRouting and fails MatchCapabilities",
			cfg.Capabilities, engine.GroupNodeType, engine.FeatureGroupExecV1)
	}
	if groupCaps != 1 {
		t.Errorf("got %d %s capabilities, want exactly 1: a duplicate shadows the "+
			"feature-bearing entry during requirement matching", groupCaps, engine.GroupNodeType)
	}
}

// A runner in another network domain cannot be scraped, and is also the runner
// least likely to want a listening port — so reporting to the server is the only
// way its metrics are ever seen. An embedded runner has exactly that shape more
// often than the CLI one does.
func TestNewRunnerBuildsTheMetricsReporterWhenAsked(t *testing.T) {
	client, cleanup, err := newRunnerProtocolClient(RunnerConfig{ServerURL: "http://server:8080"}, runnerOptionsFrom(nil))
	if err != nil {
		t.Fatalf("newRunnerProtocolClient: %v", err)
	}
	defer cleanup()

	reporter, err := newRunnerMetricsReporter(client, metrics.New(), RunnerConfig{
		ServerURL:     "http://server:8080",
		RunnerID:      "r-1",
		ReportMetrics: true,
	})
	if err != nil {
		t.Fatalf("newRunnerMetricsReporter: %v", err)
	}
	if reporter == nil {
		t.Fatal("ReportMetrics was set but no reporter was built; this runner's " +
			"metrics never reach the server and it cannot be scraped either")
	}
}

// Reporting is off by default, and asking for it on a transport that cannot
// report must not refuse to start: that turns a merely ineffective config into
// an outage. Only the HTTP client implements MetricsReportClient.
func TestNewRunnerSkipsTheMetricsReporterWhenItCannotReport(t *testing.T) {
	httpClient, cleanupHTTP, err := newRunnerProtocolClient(RunnerConfig{ServerURL: "http://server:8080"}, runnerOptionsFrom(nil))
	if err != nil {
		t.Fatalf("newRunnerProtocolClient: %v", err)
	}
	defer cleanupHTTP()
	reporter, err := newRunnerMetricsReporter(httpClient, metrics.New(),
		RunnerConfig{ServerURL: "http://server:8080"})
	if err != nil {
		t.Fatalf("newRunnerMetricsReporter: %v", err)
	}
	if reporter != nil {
		t.Error("a reporter was built without ReportMetrics; reporting must be opt-in")
	}

	grpcCfg := RunnerConfig{
		ServerURL:  "http://server:8080",
		Transport:  RunnerTransportGRPC,
		GRPCTarget: "server:9090",
	}
	grpcClient, cleanupGRPC, err := newRunnerProtocolClient(grpcCfg, runnerOptionsFrom(nil))
	if err != nil {
		t.Fatalf("newRunnerProtocolClient(grpc): %v", err)
	}
	defer cleanupGRPC()
	grpcCfg.ReportMetrics = true
	reporter, err = newRunnerMetricsReporter(grpcClient, metrics.New(), grpcCfg)
	if err != nil {
		t.Fatalf("newRunnerMetricsReporter(grpc): %v; a transport that cannot "+
			"report must keep running, not refuse to start", err)
	}
	if reporter != nil {
		t.Error("a reporter was built over gRPC, whose client does not implement " +
			"MetricsReportClient")
	}
}

// A bad interval must be rejected at assembly, not silently ignored: a runner
// that reports on a different cadence than configured is worse than one that
// refuses the config outright.
func TestNewRunnerRejectsABadMetricsReportInterval(t *testing.T) {
	client, cleanup, err := newRunnerProtocolClient(RunnerConfig{ServerURL: "http://server:8080"}, runnerOptionsFrom(nil))
	if err != nil {
		t.Fatalf("newRunnerProtocolClient: %v", err)
	}
	defer cleanup()

	if _, err := newRunnerMetricsReporter(client, metrics.New(), RunnerConfig{
		ServerURL:             "http://server:8080",
		ReportMetrics:         true,
		ReportMetricsInterval: -1,
	}); err == nil {
		t.Error("a negative report interval was accepted")
	}
}

// The supply gate reports the readiness decisions that make a declined
// activation visible. Without an observer the gate records nothing, so a
// require_ready supply that never arrives — the failure the gate exists to
// surface — leaves no trace on /metrics or on the server's merged view.
//
// Asserted by driving a real Admit through the gate the assembly built and
// reading the resulting family out of the Prometheus registry, so it fails if
// the observer is missing OR is wired to a different registry than the caller's.
func TestNewRunnerObservesTheSupplyGate(t *testing.T) {
	m := metrics.New()
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"xflow.trigger.kafka"},
	}, WithRunnerMetrics(m))
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.SupplyGate == nil {
		t.Fatal("cfg.SupplyGate is nil — a trigger capability should have wired it")
	}

	// require_ready with an unreachable fetcher: Admit declines and the observer
	// records the not-ready gauge. The error is the expected outcome, not a
	// failure of the test.
	_ = cfg.SupplyGate.Admit(context.Background(), "wf-1", []engine.SupplyRequirement{
		{Node: "rules", Resource: "rules", RequireReady: true},
	})

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, f := range families {
		if f.GetName() == "xflow_supply_not_ready" {
			found = true
		}
	}
	if !found {
		t.Error("no xflow_supply_not_ready in the caller's registry after a declined " +
			"Admit; the supply gate has no observer, so every readiness decision " +
			"this runner makes is invisible")
	}
}

// TestNewRunnerObservesNodeExecutionTimeouts drives the observer and reads the
// resulting family out of the Prometheus registry, so it fails if the observer
// is missing OR is wired to a different registry than the caller's. Asserting
// only that cfg.TimeoutObserver is non-nil would pass on an observer bound to
// some other registry, which reports nothing the caller can scrape.
func TestNewRunnerObservesNodeExecutionTimeouts(t *testing.T) {
	m := metrics.New()
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL: "http://server:8080",
	}, WithRunnerMetrics(m))
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.TimeoutObserver == nil {
		t.Fatal("cfg.TimeoutObserver is nil — every node this runner kills on " +
			"deadline is indistinguishable from an idle runner")
	}

	cfg.TimeoutObserver.OnNodeExecutionTimeout(context.Background(), "xflow.http", "runner")

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, f := range families {
		if f.GetName() == "xflow_node_timeouts_total" {
			found = true
		}
	}
	if !found {
		t.Error("no xflow_node_timeouts_total in the caller's registry " +
			"after a recorded timeout; the observer is bound to a different registry")
	}
}

// TestNewRunnerLeavesTheTimeoutObserverUnsetWithoutMetrics pins the one case
// that must NOT wire it: with no registry there is nowhere to record, and
// execution.WithTimeoutObserver treats nil as "keep the no-op", so behaviour is
// byte-identical to a runner built before this observer existed.
func TestNewRunnerLeavesTheTimeoutObserverUnsetWithoutMetrics(t *testing.T) {
	cfg, err := buildRunnerServiceConfig(RunnerConfig{ServerURL: "http://server:8080"})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.TimeoutObserver != nil {
		t.Error("cfg.TimeoutObserver set without WithRunnerMetrics; it would record " +
			"into a registry nobody can read")
	}
}

// The Kafka trigger observer is installed by installProcessObservers, not by
// wireRunnerMetrics: it lives in a process-global slot (kafkatrigger's atomic
// observer pointer) alongside supply.Default's and the wasm/script observers,
// released only by Runner.Close. Its data path has no public seam: obs() is
// package-private and its only driver is the package-private newConsumer, so
// reading back a recorded sample from outside node/trigger/kafka would mean
// adding a getter that exists only for this test.
//
// SetObserver's install-once guard IS already a public seam, though — it
// panics on a second non-nil install specifically so two live installs can
// never silently drop one side's observations. That panic is a side effect
// this test can observe without adding anything: if NewRunner wired a real
// observer, a second SetObserver call from outside the package must panic;
// if the wiring line ever regresses to passing nil, the slot stays in its
// no-op/uninstalled state and the same call goes quiet.
func TestNewRunnerWiresTheKafkaTriggerObserver(t *testing.T) {
	// Defend against a leftover install from another test/process state before
	// asserting anything: SetObserver(nil) always succeeds and clears
	// observerInstalled, regardless of prior state.
	kafkatrigger.SetObserver(nil)

	r, err := NewRunner(RunnerConfig{
		ServerURL:    "http://127.0.0.1:1",
		Token:        "t",
		RunnerID:     "kafka-observer-probe",
		Capabilities: []string{"xflow.trigger.kafka"},
	}, WithRunnerMetrics(metrics.New()))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	func() {
		defer func() {
			if rec := recover(); rec == nil {
				t.Error("kafkatrigger.SetObserver did not panic on a second " +
					"non-nil install; NewRunner did not wire a live Kafka trigger " +
					"observer, so every discard, dead-letter, lag sample and batch " +
					"admission this runner's Kafka trigger sees is invisible")
			}
		}()
		kafkatrigger.SetObserver(probeKafkaObserver{})
	}()
}

// probeKafkaObserver is a minimal kafkatrigger.Observer used only to probe
// SetObserver's install-once guard from outside the package; none of its
// methods are ever expected to be called by this test.
type probeKafkaObserver struct{}

func (probeKafkaObserver) OnMessageDiscarded(context.Context, string, string)    {}
func (probeKafkaObserver) OnMessageDeadLettered(context.Context, string, string) {}
func (probeKafkaObserver) OnConsumerLag(context.Context, string, int, int64, time.Time) {
}
func (probeKafkaObserver) OnConsumptionBlocked(context.Context, string, int, bool) {}
func (probeKafkaObserver) OnBatchFlushed(context.Context, string, string, int)     {}
func (probeKafkaObserver) OnBatchFlushOutcome(context.Context, string, string, string) {
}
func (probeKafkaObserver) OnBatchAdmission(context.Context, string, string, string) {}
func (probeKafkaObserver) OnOffsetCommit(context.Context, string, string, int, time.Duration) {
}

func TestRunnerMapBatchConcurrencyRejectsNegativeValues(t *testing.T) {
	_, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:           "http://server:8080",
		MapBatchConcurrency: -1,
	})
	if err == nil {
		t.Fatal("negative MapBatchConcurrency was accepted")
	}
}

func TestRunnerMapItemConcurrencyRejectsNegativeValues(t *testing.T) {
	_, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:          "http://server:8080",
		MapItemConcurrency: -1,
	})
	if err == nil {
		t.Fatal("negative MapItemConcurrency was accepted")
	}
}

// TestRunnerSeedRequestTimeoutRejectsNegativeValues pins that a malformed
// admission window fails at construction rather than being silently ignored.
// Ignoring it is the dangerous outcome: the operator believes the window was
// raised, the runner keeps applying 15s, and large batches keep being
// redelivered with no sign that the setting did nothing.
func TestRunnerSeedRequestTimeoutRejectsNegativeValues(t *testing.T) {
	_, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:          "http://server:8080",
		SeedRequestTimeout: -time.Second,
	})
	if err == nil {
		t.Fatal("negative SeedRequestTimeout was accepted")
	}
}

// TestRunnerSeedRequestTimeoutZeroIsAccepted pins the default case: zero means
// "not configured" and must NOT be an error, because every existing embedder
// passes zero and option C's whole requirement is that an embedder that does
// nothing sees no behaviour change.
func TestRunnerSeedRequestTimeoutZeroIsAccepted(t *testing.T) {
	if _, err := buildRunnerServiceConfig(RunnerConfig{ServerURL: "http://server:8080"}); err != nil {
		t.Fatalf("buildRunnerServiceConfig with no SeedRequestTimeout: %v", err)
	}
}

// TestRunnerSeedClientTimeoutTracksTheAdmissionDeadline pins the derived client
// timeout against the admission deadline it has to sit above.
//
// The pairing is load-bearing and was previously a hardcoded 30s against a
// hardcoded 15s. With the deadline configurable, a client timeout left at 30s
// would cut off a 60s admission FIRST and report a transport error, so the
// attempt would be counted as "error" rather than "timeout" — the exact
// signal the metric was added to expose, silently missing for the deployment
// that needed it most.
func TestRunnerSeedClientTimeoutTracksTheAdmissionDeadline(t *testing.T) {
	cases := []struct {
		name string
		cfg  RunnerConfig
		want time.Duration
	}{
		{"unset keeps the 30s safety net", RunnerConfig{}, 30 * time.Second},
		{"raised deadline raises the client", RunnerConfig{SeedRequestTimeout: 60 * time.Second}, 120 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := seedClientTimeout(tc.cfg); got != tc.want {
				t.Errorf("seedClientTimeout = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRunnerSeedObserverIsNilWithoutMetrics pins the nil-interface discipline:
// with no registry the SDK must hand the handler a genuine nil, not a typed-nil
// *Metrics wrapped in the interface. The runtime stores what it is given and
// calls it behind a != nil test, so a typed nil would pass that test and panic
// on the first admission — inside the runner's trigger flush, on the hot path.
func TestRunnerSeedObserverIsNilWithoutMetrics(t *testing.T) {
	if obs := runnerSeedObserver(nil); obs != nil {
		t.Fatalf("runnerSeedObserver(nil) = %v, want an untyped nil", obs)
	}
	if obs := runnerSeedObserver(metrics.New()); obs == nil {
		t.Fatal("runnerSeedObserver with a registry returned nil; seed admissions would go unobserved")
	}
}

// TestWireRunnerTriggerHostingCarriesTheSeedTimeoutAndObserver asserts on the
// handler the runner's own wiring produced, which is the last point before the
// value is handed to per-activation runtimes (asserted in service/runner's own
// tests).
//
// Asserting here rather than only on RunnerConfig matters because the failure
// mode is a config field that is plumbed as far as buildRunnerServiceConfig and
// then dropped: the host sets 60s, no error is returned, the field looks
// applied, and every admission still times out at 15s.
func TestWireRunnerTriggerHostingCarriesTheSeedTimeoutAndObserver(t *testing.T) {
	reg := execution.NewRegistry()
	svcCfg := runnersvc.Config{Capabilities: []protocol.Capability{{NodeType: "xflow.trigger.kafka"}}}
	o := &runnerOptions{metrics: metrics.New(), nodeRegistry: reg}

	cfg := RunnerConfig{
		ServerURL:          "http://server:8080",
		Capabilities:       []string{"xflow.trigger.kafka"},
		SeedRequestTimeout: 45 * time.Second,
	}
	if err := wireRunnerTriggerHosting(&svcCfg, cfg, o, nil); err != nil {
		t.Fatalf("wireRunnerTriggerHosting: %v", err)
	}
	if svcCfg.ActivationTracker == nil {
		t.Fatal("no ActivationTracker was installed; the runner hosts triggers with nothing to activate them")
	}
	h, ok := svcCfg.ActivationTracker.Handler().(*runnersvc.TriggerActivationHandler)
	if !ok {
		t.Fatalf("tracker handler = %T, want *runnersvc.TriggerActivationHandler",
			svcCfg.ActivationTracker.Handler())
	}
	if got := h.SeedRequestTimeout(); got != 45*time.Second {
		t.Fatalf("handler applies %v, want 45s: RunnerConfig.SeedRequestTimeout was "+
			"carried into the config struct and then dropped before the admission call", got)
	}
}
