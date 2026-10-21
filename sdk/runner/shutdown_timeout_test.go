package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The standalone runner's shutdown-timeout surface: YAML, environment, flag,
// and the conversion into the SDK config. The property under test throughout is
// that the graceful-stop bound is reachable from every input an operator
// actually uses, and that an unset value stays unset — the default belongs to
// the runner service, so materializing it here would create a second place it
// could drift from. It also has to stay below the orchestrator's
// terminationGracePeriodSeconds, which is why every surface names it.

// TestLoadRunnerConfigFromYAMLReadsShutdownTimeout pins the file surface.
func TestLoadRunnerConfigFromYAMLReadsShutdownTimeout(t *testing.T) {
	cfg, err := loadRunnerConfigFromBytes([]byte(`
runner:
  id: file-runner
  capabilities:
    - xflow.http
  shutdown_timeout: 45s
server:
  url: https://file-server:8080
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.shutdownTimeout != "45s" {
		t.Fatalf("shutdownTimeout = %q, want %q", cfg.shutdownTimeout, "45s")
	}
}

// TestLoadRunnerConfigFromYAMLKeepsShutdownTimeoutUnsetByDefault pins that the
// default is NOT materialized here: a YAML-configured runner that says nothing
// about the graceful stop must still get whatever the service's default is,
// including after that default changes.
func TestLoadRunnerConfigFromYAMLKeepsShutdownTimeoutUnsetByDefault(t *testing.T) {
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
	if cfg.shutdownTimeout != "" {
		t.Fatalf("shutdownTimeout = %q with no runner.shutdown_timeout, want empty (the "+
			"runner service applies its own default)", cfg.shutdownTimeout)
	}
}

// TestShutdownTimeoutEnvOverride covers the environment input, which is the one
// a container deployment actually uses.
func TestShutdownTimeoutEnvOverride(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_SHUTDOWN_TIMEOUT", "20s")

	cfg := applyEnvOverrides(defaultRunnerConfig(), os.Getenv)
	if cfg.shutdownTimeout != "20s" {
		t.Fatalf("shutdownTimeout = %q, want %q", cfg.shutdownTimeout, "20s")
	}
}

// TestResolveRunnerConfigShutdownTimeoutPrecedence pins the order operators rely
// on: the configuration file is the base, the environment overrides it, and an
// explicit flag overrides both.
func TestResolveRunnerConfigShutdownTimeoutPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	if err := os.WriteFile(path, []byte(`
runner:
  id: file-runner
  capabilities: [xflow.function]
  shutdown_timeout: 45s
server:
  transport: http
  url: https://file-server:8080
`), 0o600); err != nil {
		t.Fatal(err)
	}

	// The file alone.
	got, err := resolveRunnerConfig(runnerConfig{configPath: path})
	if err != nil {
		t.Fatalf("resolveRunnerConfig from YAML: %v", err)
	}
	if got.shutdownTimeout != "45s" {
		t.Fatalf("shutdownTimeout from YAML = %q, want %q", got.shutdownTimeout, "45s")
	}

	// Environment overrides the file.
	t.Setenv("XFLOW_RUNNER_SHUTDOWN_TIMEOUT", "20s")
	got, err = resolveRunnerConfig(runnerConfig{configPath: path})
	if err != nil {
		t.Fatalf("resolveRunnerConfig with env: %v", err)
	}
	if got.shutdownTimeout != "20s" {
		t.Fatalf("shutdownTimeout with env = %q, want %q: the environment must override the file",
			got.shutdownTimeout, "20s")
	}

	// An explicit flag overrides both.
	base := runnerConfig{configPath: path, changed: map[string]bool{"shutdown-timeout": true}, shutdownTimeout: "35s"}
	got, err = resolveRunnerConfig(base)
	if err != nil {
		t.Fatalf("resolveRunnerConfig with flag: %v", err)
	}
	if got.shutdownTimeout != "35s" {
		t.Fatalf("shutdownTimeout with flag = %q, want %q: the flag must override the environment",
			got.shutdownTimeout, "35s")
	}
}

// A malformed value must fail closed rather than being dropped: an operator who
// wrote shutdown_timeout: "30" (no unit) and got a runner still on the default
// would see SIGTERM kill work with no indication that the setting did nothing.
func TestResolveRunnerConfigRejectsMalformedShutdownTimeout(t *testing.T) {
	for _, bad := range []string{"30", "0s", "-1s", "soon"} {
		t.Run(bad, func(t *testing.T) {
			base := defaultRunnerConfig()
			base.changed = map[string]bool{}
			base.allowPlaintext = true
			base.changed["allow-plaintext"] = true
			base.shutdownTimeout = bad
			base.changed["shutdown-timeout"] = true

			_, err := resolveRunnerConfig(base)
			if err == nil {
				t.Fatalf("shutdown timeout %q was accepted; want a validation error", bad)
			}
			if !strings.Contains(err.Error(), "shutdown timeout") {
				t.Fatalf("error = %v, want it to name the shutdown timeout", err)
			}
		})
	}
}

// TestToSDKRunnerConfigCarriesShutdownTimeout is the end-to-end assertion for
// the CLI path: a value resolved from YAML/env/flag has to arrive in the SDK
// config that builds the runner. The paired case matters as much — the empty
// string must convert to a zero duration, so that "unset" survives the
// conversion and the service's own default remains the single place the default
// lives.
func TestToSDKRunnerConfigCarriesShutdownTimeout(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"unset stays zero", "", 0},
		{"configured reaches the SDK", "25s", 25 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultRunnerConfig()
			cfg.shutdownTimeout = tt.raw

			got, err := toSDKRunnerConfig(cfg)
			if err != nil {
				t.Fatalf("toSDKRunnerConfig: %v", err)
			}
			if got.ShutdownTimeout != tt.want {
				t.Fatalf("SDK RunnerConfig.ShutdownTimeout = %v, want %v",
					got.ShutdownTimeout, tt.want)
			}
		})
	}
}

// TestRunCommandExposesShutdownTimeoutFlag pins the CLI input. The flag name is
// also the key resolveRunnerConfig honours under "changed", so a rename here
// without the corresponding map key would silently drop a flag-set value.
func TestRunCommandExposesShutdownTimeoutFlag(t *testing.T) {
	cmd, err := NewCommand(Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Flags().Lookup("shutdown-timeout") == nil {
		t.Fatal("--shutdown-timeout is not registered; the standalone runner " +
			"cannot tune its graceful-stop bound at all")
	}
}

// TestShutdownTimeoutFlagHelpNamesTheGracePeriod keeps the guidance the
// orchestrator integration depends on discoverable at the point of use: a value
// larger than terminationGracePeriodSeconds turns a drain into a SIGKILL.
func TestShutdownTimeoutFlagHelpNamesTheGracePeriod(t *testing.T) {
	cmd, err := NewCommand(Profile{})
	if err != nil {
		t.Fatal(err)
	}
	flag := cmd.Flags().Lookup("shutdown-timeout")
	if flag == nil {
		t.Fatal("--shutdown-timeout is not registered")
	}
	if !strings.Contains(flag.Usage, "terminationGracePeriodSeconds") {
		t.Fatalf("--shutdown-timeout help = %q, want it to warn about the orchestrator's "+
			"terminationGracePeriodSeconds", flag.Usage)
	}
}

// TestSampleRunnerConfigDocumentsTheShutdownTimeout keeps the generated sample
// honest: it is what an operator reads to learn the knob exists, and the
// orchestrator-grace-period relationship is the part that is expensive to get
// wrong.
func TestSampleRunnerConfigDocumentsTheShutdownTimeout(t *testing.T) {
	sample := sampleRunnerConfigYAML(Profile{})
	if !strings.Contains(sample, "shutdown_timeout") {
		t.Fatal("the sample runner config does not mention runner.shutdown_timeout")
	}
	if !strings.Contains(sample, "terminationGracePeriodSeconds") {
		t.Fatal("the sample runner config does not explain the relationship to the " +
			"orchestrator's terminationGracePeriodSeconds")
	}
}
