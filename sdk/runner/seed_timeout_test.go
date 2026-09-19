package runner

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// The standalone runner's seed-request-timeout surface: YAML, environment, and
// the conversion into the SDK config. The property under test throughout is the
// same one R3 is about — the batch-size knob is the integrator's, so the
// admission deadline it is bounded by has to be reachable from every input the
// integrator actually uses, and an unset value has to stay unset so the SDK
// default still applies.

// TestLoadRunnerConfigFromYAMLReadsSeedRequestTimeout pins the file surface.
func TestLoadRunnerConfigFromYAMLReadsSeedRequestTimeout(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
runner:
  id: file-runner
  capabilities:
    - xflow.http
server:
  url: https://file-server:8080
seed:
  request_timeout: 60s
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.seedRequestTimeout != "60s" {
		t.Fatalf("seedRequestTimeout = %q, want %q", cfg.seedRequestTimeout, "60s")
	}
}

// TestLoadRunnerConfigFromYAMLKeepsSeedRequestTimeoutUnsetByDefault pins that
// the default is NOT materialized here. If this layer wrote "15s" by default,
// the SDK's own default would stop being reachable from a YAML-configured
// runner, and the two would drift the first time one of them changed.
func TestLoadRunnerConfigFromYAMLKeepsSeedRequestTimeoutUnsetByDefault(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
runner:
  id: file-runner
  capabilities:
    - xflow.http
server:
  url: https://file-server:8080
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.seedRequestTimeout != "" {
		t.Fatalf("seedRequestTimeout = %q with no seed section, want empty (the SDK "+
			"applies protocol.DefaultEntrySeedRequestTimeout)", cfg.seedRequestTimeout)
	}
}

// TestSeedRequestTimeoutEnvOverride covers the environment input, which is the
// one a container deployment actually uses.
func TestSeedRequestTimeoutEnvOverride(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_SEED_REQUEST_TIMEOUT", "90s")

	cfg := applyEnvOverrides(defaultRunnerConfig(), os.Getenv)
	if cfg.seedRequestTimeout != "90s" {
		t.Fatalf("seedRequestTimeout = %q, want %q", cfg.seedRequestTimeout, "90s")
	}
}

// TestResolveRunnerConfigRejectsMalformedSeedRequestTimeout is the fail-closed
// half. A malformed value must not be dropped: an operator who wrote
// request_timeout: "60" (no unit) and got a runner still on 15s would see the
// batch keep timing out with no indication that the setting did nothing.
func TestResolveRunnerConfigRejectsMalformedSeedRequestTimeout(t *testing.T) {
	for _, bad := range []string{"60", "0s", "-1s", "soon"} {
		t.Run(bad, func(t *testing.T) {
			base := defaultRunnerConfig()
			base.changed = map[string]bool{}
			base.allowPlaintext = true
			base.changed["allow-plaintext"] = true
			base.seedRequestTimeout = bad
			base.changed["seed-request-timeout"] = true

			_, err := resolveRunnerConfig(base)
			if err == nil {
				t.Fatalf("seed request timeout %q was accepted; want a validation error", bad)
			}
			if !strings.Contains(err.Error(), "seed request timeout") {
				t.Fatalf("error = %v, want it to name the seed request timeout", err)
			}
		})
	}
}

// TestToSDKRunnerConfigCarriesSeedRequestTimeout is the end-to-end assertion for
// the CLI path: a value resolved from YAML/env/flag has to arrive in the SDK
// config that builds the runner.
//
// The paired case is the important one. The empty string must convert to a zero
// duration, not to a resolved 15s, so that "unset" survives the conversion and
// the SDK's own default remains the single place the default lives.
func TestToSDKRunnerConfigCarriesSeedRequestTimeout(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"unset stays zero", "", 0},
		{"configured reaches the SDK", "60s", 60 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultRunnerConfig()
			cfg.seedRequestTimeout = tt.raw

			got, err := toSDKRunnerConfig(cfg)
			if err != nil {
				t.Fatalf("toSDKRunnerConfig: %v", err)
			}
			if got.SeedRequestTimeout != tt.want {
				t.Fatalf("SDK RunnerConfig.SeedRequestTimeout = %v, want %v", got.SeedRequestTimeout, tt.want)
			}
		})
	}
}

// TestSeedRequestTimeoutUnsetReachesTheSDKDefault is the property an existing
// deployment depends on, asserted through the SDK's own resolution rather than
// against a number repeated here: a runner that configures nothing must apply
// protocol.DefaultEntrySeedRequestTimeout.
func TestSeedRequestTimeoutUnsetReachesTheSDKDefault(t *testing.T) {
	cfg := defaultRunnerConfig()
	if cfg.seedRequestTimeout != "" {
		t.Fatalf("defaultRunnerConfig set seedRequestTimeout = %q; the default has "+
			"to be established by the SDK, not by this CLI layer", cfg.seedRequestTimeout)
	}

	sdkCfg, err := toSDKRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("toSDKRunnerConfig: %v", err)
	}
	if sdkCfg.SeedRequestTimeout != 0 {
		t.Fatalf("SDK SeedRequestTimeout = %v, want 0 so the SDK applies %v",
			sdkCfg.SeedRequestTimeout, protocol.DefaultEntrySeedRequestTimeout)
	}
}

// TestRunCommandExposesSeedRequestTimeoutFlag pins the CLI input. The flag name
// is also the key resolveRunnerConfig clears a resolution issue under, so a
// rename here without the corresponding map key would make a malformed value
// from the command line unreportable.
func TestRunCommandExposesSeedRequestTimeoutFlag(t *testing.T) {
	cmd, err := NewCommand(Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Flags().Lookup("seed-request-timeout") == nil {
		t.Fatal("--seed-request-timeout is not registered; the standalone runner " +
			"cannot raise the admission deadline at all")
	}

	// The flag has to survive flag -> cfg.changed -> resolveRunnerConfig, so the
	// "changed" key and the flag name must be the same string.
	cfg := defaultRunnerConfig()
	cfg.changed = map[string]bool{}
	cfg.changed["seed-request-timeout"] = true
	cfg.seedRequestTimeout = "60s"
	cfg.allowPlaintext = true
	cfg.changed["allow-plaintext"] = true
	got, err := resolveRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("resolveRunnerConfig: %v", err)
	}
	if got.seedRequestTimeout != "60s" {
		t.Fatalf("resolved seedRequestTimeout = %q, want %q: the flag did not "+
			"survive precedence resolution", got.seedRequestTimeout, "60s")
	}
}

// TestSampleRunnerConfigDocumentsTheSeedTimeout keeps the generated sample
// honest. It is what an operator reads to learn the knob exists, and a sample
// that omits it means the only discoverable way to learn about the deadline is
// to hit it in production.
func TestSampleRunnerConfigDocumentsTheSeedTimeout(t *testing.T) {
	sample := sampleRunnerConfigYAML(Profile{})
	if !strings.Contains(sample, "seed:") || !strings.Contains(sample, "request_timeout") {
		t.Fatal("the sample runner config does not mention seed.request_timeout")
	}
}
