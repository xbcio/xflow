package xflow

import (
	"testing"
	"time"
)

// A graceful stop is bounded by the runner service, so the embedded SDK needs a
// way to raise or lower that bound: an orchestrator whose grace period differs
// from the 10s default otherwise either kills the process mid-report or waits
// longer than it must. Zero has to reach the service unchanged, so the default
// stays defined in exactly one place.
func TestRunnerShutdownTimeoutReachesTheServiceConfig(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"unset stays zero", 0, 0},
		{"configured reaches the service", 25 * time.Second, 25 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildRunnerServiceConfig(RunnerConfig{
				ServerURL:       "http://server:8080",
				ShutdownTimeout: tt.configured,
			})
			if err != nil {
				t.Fatalf("buildRunnerServiceConfig: %v", err)
			}
			if cfg.ShutdownTimeout != tt.want {
				t.Fatalf("service Config.ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, tt.want)
			}
		})
	}
}
