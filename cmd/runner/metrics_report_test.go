package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

// The reporter must be buildable without --metrics-addr. A cross-domain runner
// is exactly the deployment that does not want to open a scrape port, so tying
// reporting to the self-exposure flag would exclude the only case this feature
// exists for.
func TestReporterBuiltWithoutMetricsAddr(t *testing.T) {
	client := protocol.NewClient("http://example.invalid", nil)
	rep, err := buildMetricsReporter(client, metrics.New(), runnerConfig{
		runnerID:              "runner-a",
		reportMetrics:         true,
		reportMetricsInterval: "15s",
		metricsAddr:           "", // deliberately empty
	})
	if err != nil {
		t.Fatalf("buildMetricsReporter: %v", err)
	}
	if rep == nil {
		t.Fatal("reporter is nil with --report-metrics set and no --metrics-addr")
	}
}

// Not asking for reporting must produce no reporter at all — nil is what
// Config.MetricsReporter treats as "never report".
func TestNoReporterWhenNotRequested(t *testing.T) {
	client := protocol.NewClient("http://example.invalid", nil)
	rep, err := buildMetricsReporter(client, metrics.New(), runnerConfig{
		runnerID:      "runner-a",
		reportMetrics: false,
		metricsAddr:   ":9091", // scraping on, reporting off
	})
	if err != nil {
		t.Fatalf("buildMetricsReporter: %v", err)
	}
	if rep != nil {
		t.Fatal("reporter built without --report-metrics")
	}
}

// A transport whose client cannot report must degrade to nil rather than error:
// the gRPC client does not implement MetricsReportClient (see spec §4.3), and a
// gRPC runner asked to report should keep running, not refuse to start.
func TestNoReporterWhenClientCannotReport(t *testing.T) {
	rep, err := buildMetricsReporter(nonReportingClient{}, metrics.New(), runnerConfig{
		runnerID:              "runner-a",
		reportMetrics:         true,
		reportMetricsInterval: "15s",
	})
	if err != nil {
		t.Fatalf("buildMetricsReporter: %v", err)
	}
	if rep != nil {
		t.Fatal("reporter built for a client that cannot report")
	}
}

func TestReporterRejectsBadInterval(t *testing.T) {
	client := protocol.NewClient("http://example.invalid", nil)
	if _, err := buildMetricsReporter(client, metrics.New(), runnerConfig{
		runnerID:              "runner-a",
		reportMetrics:         true,
		reportMetricsInterval: "not-a-duration",
	}); err == nil {
		t.Fatal("bad --report-metrics-interval accepted")
	}
}

// nonReportingClient satisfies ProtocolClient without MetricsReportClient,
// standing in for the gRPC client.
type nonReportingClient struct{}

func (nonReportingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{}, nil
}
func (nonReportingClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}
func (nonReportingClient) Poll(context.Context, protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	return protocol.PollTaskResponse{}, nil
}
func (nonReportingClient) ReportResult(context.Context, protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	return protocol.ReportResultResponse{}, nil
}

// yaml must be able to turn reporting on, and a changed flag must beat the file
// — the same precedence every other runner setting already follows.
func TestMetricsReportConfigPrecedence(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
metrics:
  report: true
  report_interval: 60s
`))
	if err != nil {
		t.Fatalf("loadRunnerConfigFromBytes: %v", err)
	}
	if !cfg.reportMetrics || cfg.reportMetricsInterval != "60s" {
		t.Fatalf("yaml not applied: report=%v interval=%q", cfg.reportMetrics, cfg.reportMetricsInterval)
	}

	// A changed flag wins over the file.
	cfg.changed = map[string]bool{"report-metrics-interval": true}
	cfg.reportMetricsInterval = "30s"
	resolved, err := resolveRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("resolveRunnerConfig: %v", err)
	}
	if resolved.reportMetricsInterval != "30s" {
		t.Fatalf("changed flag lost to file: %q", resolved.reportMetricsInterval)
	}
}

// Reporting must default to off: an existing deployment that upgrades the binary
// and changes no config must send nothing new to the server.
func TestReportMetricsDefaultsOff(t *testing.T) {
	cfg := defaultRunnerConfig()
	if cfg.reportMetrics {
		t.Fatal("reportMetrics defaults on")
	}
	if cfg.reportMetricsInterval != "15s" {
		t.Fatalf("reportMetricsInterval default = %q, want 15s", cfg.reportMetricsInterval)
	}
}

// TestReportMetricsFlagWiresReporter exercises the full production command path:
// parseFlags → resolveConfig → runRunner → buildMetricsReporter → Config.MetricsReporter.
// It uses the same stubRunnerServiceFactory pattern as the SubgraphRuntime and
// GroupRuntime wiring tests to inspect the Config the runner service receives.
func TestReportMetricsFlagWiresReporter(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.MetricsReporter == nil {
			t.Error("runner service got no MetricsReporter; --report-metrics was set " +
				"with an HTTP transport, so reporting should be enabled")
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080", "--transport", "http", "--report-metrics", "--report-metrics-interval", "20s")
	if err != nil {
		t.Fatal(err)
	}
}

// TestNoReportMetricsFlagLeavesReporterNil ensures that when --report-metrics is
// not set, the runner service receives a nil MetricsReporter — byte-identical
// behavior to before this feature.
func TestNoReportMetricsFlagLeavesReporterNil(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.MetricsReporter != nil {
			t.Error("runner service got a MetricsReporter without --report-metrics")
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080")
	if err != nil {
		t.Fatal(err)
	}
}
