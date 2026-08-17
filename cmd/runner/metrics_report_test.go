package main

import (
	"bytes"
	"context"
	"testing"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// The reporter itself is assembled and tested in sdk/xflow (including the gRPC
// degrade-to-nil path and the bad-interval rejection). What is left here is the
// translation: whether --report-metrics and --report-metrics-interval reach it.

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

// The full command path: flags → resolveConfig → runRunner → SDK. A
// cross-domain runner cannot be scraped, so reporting is the only way its
// metrics are ever seen — losing the flag here loses them silently.
func TestReportMetricsFlagReachesTheSDK(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if !cfg.ReportMetrics {
			t.Error("ReportMetrics is false; --report-metrics was set, so this runner's " +
				"metrics never reach the server and it cannot be scraped either")
		}
		if cfg.ReportMetricsInterval.String() != "20s" {
			t.Errorf("ReportMetricsInterval = %v, want 20s", cfg.ReportMetricsInterval)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--transport", "http",
		"--report-metrics", "--report-metrics-interval", "20s")
}

// Without the flag the SDK must see reporting off — byte-identical behaviour to
// before the feature existed.
func TestNoReportMetricsFlagLeavesReportingOff(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.ReportMetrics {
			t.Error("ReportMetrics is true without --report-metrics")
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080")
}

// A bad interval must be rejected before the runner starts, not silently
// ignored: a runner reporting on a different cadence than configured is worse
// than one that refuses the config outright.
func TestRunCommandRejectsABadReportInterval(t *testing.T) {
	restore := stubRunnerServiceFactory(func(xflowsdk.RunnerConfig) error {
		t.Error("the runner was constructed despite an unparseable --report-metrics-interval")
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--server", "http://server:8080", "--report-metrics",
		"--report-metrics-interval", "not-a-duration")
	if err == nil {
		t.Fatal("an unparseable --report-metrics-interval was accepted")
	}
}
