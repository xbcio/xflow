package xflow

import (
	"testing"

	"github.com/xbcio/xflow/observability/metrics"
)

// TestTwoRunnersWithMetricsInOneProcess pins the lifecycle contract that makes
// the single-slot observer design usable: the process-wide observers a runner
// installs are RELEASED by Close, so a second runner can be built afterwards.
//
// Without it, the panic added to guard against silent observation loss becomes
// a crash in every legitimate embedder. The in-process embedded model (control
// plane and runner in one binary, which SAS runs) constructs a runner per test
// in a single test binary; the second construction took the whole process down
// with "observer already installed" before it could report anything.
//
// The panic is still correct — two LIVE runners sharing a process would silently
// lose one side's observations. What was missing is the release: install is
// owned by NewRunner, release by Close, and the two must be symmetric.
func TestTwoRunnersWithMetricsInOneProcess(t *testing.T) {
	for i := 0; i < 2; i++ {
		r, err := NewRunner(RunnerConfig{
			ServerURL:    "http://127.0.0.1:1",
			Token:        "t",
			RunnerID:     "lifecycle-probe",
			Capabilities: []string{"xflow.function", "xflow.trigger.kafka"},
		}, WithRunnerMetrics(metrics.New()))
		if err != nil {
			t.Fatalf("NewRunner #%d: %v", i+1, err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

// TestBuildRunnerServiceConfigInstallsNoProcessObservers pins WHY the first test
// can pass: assembling the service config must not touch process-global state.
//
// buildRunnerServiceConfig is a pure configuration constructor and the seam a
// dozen tests call directly. When it installed the process-wide observers, two
// tests in the same binary that each built a config with metrics crashed the
// run — sdk/xflow's own package went red exactly this way, and it read as
// "TestNewRunnerObservesNodeExecutionTimeouts fails" rather than as a lifecycle
// defect, because the panicking call is three frames below the test.
//
// Calling it twice with metrics is the whole assertion: it must not panic.
func TestBuildRunnerServiceConfigInstallsNoProcessObservers(t *testing.T) {
	for i := 0; i < 2; i++ {
		if _, err := buildRunnerServiceConfig(RunnerConfig{
			ServerURL:    "http://server:8080",
			Capabilities: []string{"xflow.trigger.kafka"},
		}, WithRunnerMetrics(metrics.New())); err != nil {
			t.Fatalf("buildRunnerServiceConfig #%d: %v", i+1, err)
		}
	}
}
