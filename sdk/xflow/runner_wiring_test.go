package xflow

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
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
	client, cleanup, err := newRunnerProtocolClient(RunnerConfig{ServerURL: "http://server:8080"})
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
	httpClient, cleanupHTTP, err := newRunnerProtocolClient(RunnerConfig{ServerURL: "http://server:8080"})
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
	grpcClient, cleanupGRPC, err := newRunnerProtocolClient(grpcCfg)
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
	client, cleanup, err := newRunnerProtocolClient(RunnerConfig{ServerURL: "http://server:8080"})
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

// Not covered here: the Kafka trigger observer (kafkatrigger.SetObserver in
// wireRunnerMetrics). Its only read-back is the package-private obs(), and its
// only driver is the package-private newConsumer seam, so asserting it from
// outside node/trigger/kafka would mean adding a getter that exists only for
// this test. cmd/runner's copy of the same line is untested for the same
// reason. If that package ever grows a legitimate public seam, wire this in.

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
