package runner

import (
	"os"
	"strings"
	"testing"
	"time"
)

// The standalone runner's supply.poll_interval surface: YAML, environment, flag,
// and the conversion into the SDK config. The property under test throughout is
// that the knob is reachable from every input an operator actually uses, and
// that an unset value stays unset — the poll's default (60s) and its floor (10s)
// belong to the runner service, so materializing either here would make this CLI
// layer a second place they could drift from.

// TestLoadRunnerConfigFromYAMLReadsSupplyPollInterval pins the file surface.
func TestLoadRunnerConfigFromYAMLReadsSupplyPollInterval(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
runner:
  id: file-runner
  capabilities:
    - xflow.http
server:
  url: https://file-server:8080
supply:
  poll_interval: 15s
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.supplyPollInterval != "15s" {
		t.Fatalf("supplyPollInterval = %q, want %q", cfg.supplyPollInterval, "15s")
	}
}

// TestLoadRunnerConfigFromYAMLKeepsSupplyPollIntervalUnsetByDefault pins that
// the default is NOT materialized here: a YAML-configured runner that says
// nothing about the poll must still get whatever the service's default is,
// including after that default changes.
func TestLoadRunnerConfigFromYAMLKeepsSupplyPollIntervalUnsetByDefault(t *testing.T) {
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
	if cfg.supplyPollInterval != "" {
		t.Fatalf("supplyPollInterval = %q with no supply section, want empty (the SDK "+
			"applies its own default)", cfg.supplyPollInterval)
	}
}

// TestSupplyPollIntervalEnvOverride covers the environment input, which is the
// one a container deployment actually uses.
func TestSupplyPollIntervalEnvOverride(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_SUPPLY_POLL_INTERVAL", "20s")

	cfg := applyEnvOverrides(defaultRunnerConfig(), os.Getenv)
	if cfg.supplyPollInterval != "20s" {
		t.Fatalf("supplyPollInterval = %q, want %q", cfg.supplyPollInterval, "20s")
	}
}

// A malformed value must fail closed. A runner that quietly kept the 60s default
// after an operator set 30 (no unit) would look like the knob did nothing, which
// is the failure mode the seed-timeout knob already documents for its own value.
func TestResolveRunnerConfigRejectsMalformedSupplyPollInterval(t *testing.T) {
	for _, bad := range []string{"60", "0s", "-1s", "soon"} {
		t.Run(bad, func(t *testing.T) {
			base := defaultRunnerConfig()
			base.changed = map[string]bool{}
			base.allowPlaintext = true
			base.changed["allow-plaintext"] = true
			base.supplyPollInterval = bad
			base.changed["supply-poll-interval"] = true

			_, err := resolveRunnerConfig(base)
			if err == nil {
				t.Fatalf("supply poll interval %q was accepted; want a validation error", bad)
			}
			if !strings.Contains(err.Error(), "supply poll interval") {
				t.Fatalf("error = %v, want it to name the supply poll interval", err)
			}
		})
	}
}

// A value that is well-formed but below the service's floor is deliberately NOT
// rejected here: the floor is the runner service's rule, enforced by
// xflowsdk.NewRunner (and pinned by its own test), so that this layer cannot
// hold a second, drifting copy of it. What this layer owes is the accurate
// duration.
func TestResolveRunnerConfigAcceptsAWellFormedSupplyPollInterval(t *testing.T) {
	base := defaultRunnerConfig()
	base.changed = map[string]bool{}
	base.allowPlaintext = true
	base.changed["allow-plaintext"] = true
	base.supplyPollInterval = "30s"
	base.changed["supply-poll-interval"] = true

	got, err := resolveRunnerConfig(base)
	if err != nil {
		t.Fatalf("resolveRunnerConfig: %v", err)
	}
	if got.supplyPollInterval != "30s" {
		t.Fatalf("resolved supplyPollInterval = %q, want %q: the flag did not survive "+
			"precedence resolution", got.supplyPollInterval, "30s")
	}
}

// TestToSDKRunnerConfigCarriesSupplyPollInterval is the end-to-end assertion for
// the CLI path: a value resolved from YAML/env/flag has to arrive in the SDK
// config that builds the runner. The paired case matters as much — the empty
// string must convert to a zero duration, not to a resolved 60s, so "unset"
// survives the conversion.
func TestToSDKRunnerConfigCarriesSupplyPollInterval(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"unset stays zero", "", 0},
		{"configured reaches the SDK", "30s", 30 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultRunnerConfig()
			cfg.supplyPollInterval = tt.raw

			got, err := toSDKRunnerConfig(cfg)
			if err != nil {
				t.Fatalf("toSDKRunnerConfig: %v", err)
			}
			if got.SupplyPollInterval != tt.want {
				t.Fatalf("SDK RunnerConfig.SupplyPollInterval = %v, want %v",
					got.SupplyPollInterval, tt.want)
			}
		})
	}
}

// TestRunCommandExposesSupplyPollIntervalFlag pins the CLI input. The flag name
// is also the key resolveRunnerConfig clears a resolution issue under, so a
// rename here without the corresponding map key would make a malformed value
// from the command line unreportable.
func TestRunCommandExposesSupplyPollIntervalFlag(t *testing.T) {
	cmd, err := NewCommand(Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Flags().Lookup("supply-poll-interval") == nil {
		t.Fatal("--supply-poll-interval is not registered; the standalone runner " +
			"cannot retune the supply poll at all")
	}
}

// TestSampleRunnerConfigDocumentsTheSupplyPollInterval keeps the generated
// sample honest: it is what an operator reads to learn the knob exists.
func TestSampleRunnerConfigDocumentsTheSupplyPollInterval(t *testing.T) {
	sample := sampleRunnerConfigYAML(Profile{})
	if !strings.Contains(sample, "supply:") || !strings.Contains(sample, "poll_interval") {
		t.Fatal("the sample runner config does not mention supply.poll_interval")
	}
}
