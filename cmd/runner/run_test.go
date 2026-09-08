package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// These tests assert the one thing cmd/runner still owns after the assembly
// moved to sdk/xflow: that the resolved CLI/YAML config reaches the SDK intact.
// The assembly's own invariants — GroupRuntime, SubgraphRuntime, the group
// capability's feature, the supply gate's TLS material — are asserted in
// sdk/xflow, against the same code this command now calls.

type runnerServiceFunc func(context.Context) error

func (f runnerServiceFunc) Run(ctx context.Context) error { return f(ctx) }
func (runnerServiceFunc) Close() error                    { return nil }

// stubRunnerServiceFactory intercepts the config handed to the SDK. The command
// still runs end to end — flags, YAML, env, precedence, validation — only the
// runner itself is replaced, since starting one needs a live control plane.
func stubRunnerServiceFactory(check func(xflowsdk.RunnerConfig) error) func() {
	previous := newRunnerService
	newRunnerService = func(cfg xflowsdk.RunnerConfig, _ ...xflowsdk.RunnerOption) (runnerService, error) {
		return runnerServiceFunc(func(context.Context) error {
			return check(cfg)
		}), nil
	}
	return func() { newRunnerService = previous }
}

func runCommand(t *testing.T, args ...string) {
	t.Helper()
	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, args...)
	if err != nil {
		t.Fatal(err)
	}
}

// The durations survive the full precedence chain — file, then env, then a
// changed flag — and arrive as time.Duration rather than being validated and
// dropped.
//
// The heartbeat half is a regression pin. It was parsed, merged with correct
// precedence, validated as positive, and then discarded (`_, err := parse...`),
// so every runner heartbeated at the runner service's hardcoded 5s regardless.
// That is worse than an ignored flag: the heartbeat interval is what the control
// plane's liveness window derives from, so widening it for a slow link or
// narrowing it for faster failure detection had no effect — with a config that
// verifies clean and a log that says nothing.
func TestRunCommandPropagatesResolvedDurationsToTheSDK(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
server:
  url: http://file-server:8080
heartbeat:
  interval: 7s
poll:
  wait: 2s
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("XFLOW_RUNNER_HEARTBEAT_INTERVAL", "9s")
	t.Setenv("XFLOW_RUNNER_POLL_WAIT", "3s")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.PollWait != 4*time.Second {
			t.Errorf("PollWait = %s, want 4s (the changed flag)", cfg.PollWait)
		}
		if cfg.HeartbeatInterval != 11*time.Second {
			t.Errorf("HeartbeatInterval = %v, want 11s — the resolved value never "+
				"reached the SDK, which then defaults to 5s", cfg.HeartbeatInterval)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--config", path, "--heartbeat-interval", "11s", "--poll-wait", "4s", "--allow-plaintext")
}

// --cap is the only way an operator names the node types this runner claims,
// and the SDK matches on that list rather than on the process's node registry.
// A dropped capability is a runner that registers successfully and is then
// never sent work.
func TestRunCommandPropagatesCapabilitiesToTheSDK(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		want := map[string]bool{"xflow.map": false, "xflow.function": false}
		for _, c := range cfg.Capabilities {
			if _, ok := want[c]; ok {
				want[c] = true
			}
		}
		for nodeType, seen := range want {
			if !seen {
				t.Errorf("capability %q missing from %v; the runner claims no leases for it",
					nodeType, cfg.Capabilities)
			}
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--cap", "xflow.map,xflow.function", "--allow-plaintext")
}

// Labels are what a node-level RunnerSelector matches against, so a runner that
// drops them is invisible to every selector-pinned workflow.
func TestRunCommandPropagatesLabelsToTheSDK(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.Labels["env"] != "test" || cfg.Labels["app"] != "sas" {
			t.Errorf("Labels = %v, want env=test app=sas", cfg.Labels)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--label", "env=test", "--label", "app=sas", "--allow-plaintext")
}

// The transport fields decide which protocol client the SDK builds; sending the
// gRPC target under an http transport (or the reverse) connects to the wrong
// port and fails at first poll, far from the cause.
func TestRunCommandPropagatesTransportToTheSDK(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.Transport != transportGRPC || cfg.GRPCTarget != "server:9090" {
			t.Errorf("Transport/GRPCTarget = %q/%q, want grpc/server:9090", cfg.Transport, cfg.GRPCTarget)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080",
		"--transport", "grpc", "--grpc-target", "server:9090", "--allow-plaintext")
}

// resolveRunnerConfig re-loads the config from disk and then copies each changed
// flag over it. Four flags were never copied: --token and the three --tls-*
// paths. They bind, they parse, they validate, and then resolveRunnerConfig
// returns a config where they are empty — only the XFLOW_RUNNER_TLS_* and
// XFLOW_RUNNER_TOKEN environment variables ever took effect.
//
// Both halves fail closed in a way that names no cause. Without the CA every
// client falls back to DefaultTransport, so supply fetch fails, SupplyGate.Admit
// declines every activation, and the runner never hosts its triggers. Without
// the token the server rejects registration outright. In both cases the operator
// passed the flag, `verify` reported success, and nothing logged that the value
// had been dropped.
func TestRunCommandPropagatesTLSAndTokenFlagsToTheSDK(t *testing.T) {
	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.TLSServerCA != "/etc/xflow/ca.pem" {
			t.Errorf("TLSServerCA = %q, want /etc/xflow/ca.pem — without it every "+
				"client falls back to DefaultTransport and the supply gate declines forever",
				cfg.TLSServerCA)
		}
		if cfg.TLSClientCert != "/etc/xflow/client.pem" || cfg.TLSClientKey != "/etc/xflow/client.key" {
			t.Errorf("TLSClientCert/Key = %q/%q, want the flag values; an mTLS server "+
				"rejects the handshake without them", cfg.TLSClientCert, cfg.TLSClientKey)
		}
		if cfg.Token != "s3cret-token" {
			t.Errorf("Token = %q, want the flag value; the server rejects registration without it", cfg.Token)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "https://server:8080",
		"--tls-server-ca", "/etc/xflow/ca.pem",
		"--tls-client-cert", "/etc/xflow/client.pem",
		"--tls-client-key", "/etc/xflow/client.key",
		"--token", "s3cret-token")
}

// The environment must still work, and a changed flag must still beat it — the
// same precedence every other runner setting follows. Pinned because the fix
// adds the flag branch to a resolve path where the env override already ran.
func TestRunCommandTLSFlagBeatsTheEnvironment(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TLS_SERVER_CA", "/from/env.pem")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.TLSServerCA != "/from/flag.pem" {
			t.Errorf("TLSServerCA = %q, want the flag to win over XFLOW_RUNNER_TLS_SERVER_CA", cfg.TLSServerCA)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "https://server:8080", "--tls-server-ca", "/from/flag.pem")
}

// And with no flag the environment still applies.
func TestRunCommandTLSEnvAppliesWithoutAFlag(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TLS_SERVER_CA", "/from/env.pem")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.TLSServerCA != "/from/env.pem" {
			t.Errorf("TLSServerCA = %q, want /from/env.pem", cfg.TLSServerCA)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "https://server:8080")
}

// XFLOW_ARTIFACT_CACHE_DIR is the operator's only control over where fetched
// wasm modules land — a read-only or full default cache dir is otherwise a
// per-execution refetch of a multi-megabyte module.
func TestRunCommandPropagatesTheArtifactCacheDirToTheSDK(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XFLOW_ARTIFACT_CACHE_DIR", dir)

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.ArtifactCacheDir != dir {
			t.Errorf("ArtifactCacheDir = %q, want %q", cfg.ArtifactCacheDir, dir)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--allow-plaintext")
}

// XFLOW_ARTIFACT_CACHE_MAX_BYTES is the operator's only control over
// FSStore.MaxBytes for the runner's local artifact cache (see
// sdk/xflow/runner.go's newRunnerArtifactResolver). Without this test, a
// wired-looking `os.Getenv("XFLOW_ARTIFACT_CACHE_MAX_BYTES")` call that never
// reached toSDKRunnerConfig's returned struct would go unnoticed the way the
// heartbeat interval once did before TestRunCommandPropagatesResolvedDurationsToTheSDK
// pinned it: parsed, and then discarded.
func TestRunCommandPropagatesTheArtifactCacheMaxBytesToTheSDK(t *testing.T) {
	t.Setenv("XFLOW_ARTIFACT_CACHE_MAX_BYTES", "12345")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.ArtifactCacheMaxBytes != 12345 {
			t.Errorf("ArtifactCacheMaxBytes = %d, want 12345", cfg.ArtifactCacheMaxBytes)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--allow-plaintext")
}

// A malformed XFLOW_ARTIFACT_CACHE_MAX_BYTES must not take the runner down
// over a tuning-knob typo: it degrades to 0, which tells RunnerConfig to use
// the SDK's own default cap, mirroring the sibling wasm compilation cache's
// fail-open handling of XFLOW_WASM_CACHE_MAX_BYTES.
func TestRunCommandToleratesMalformedArtifactCacheMaxBytes(t *testing.T) {
	t.Setenv("XFLOW_ARTIFACT_CACHE_MAX_BYTES", "not-a-number")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.ArtifactCacheMaxBytes != 0 {
			t.Errorf("ArtifactCacheMaxBytes = %d, want 0 (fail open to the SDK default) for a malformed value",
				cfg.ArtifactCacheMaxBytes)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--allow-plaintext")
}

// A negative XFLOW_ARTIFACT_CACHE_MAX_BYTES is NOT a malformed value: unlike
// "not-a-number", strconv.ParseInt("-1", ...) succeeds, so
// artifactCacheMaxBytesFromEnv must pass it through unchanged rather than
// folding it into the same "fall back to 0" branch as a parse failure.
// RunnerConfig.ArtifactCacheMaxBytes documents a negative value as an
// explicit, distinct-from-zero signal to disable the cap entirely (see
// sdk/xflow/runner.go's artifactCacheMaxBytes), so an operator who sets this
// env var to -1 needs it to actually reach that path rather than silently
// becoming "use the default cap" the way a typo does.
func TestRunCommandPropagatesNegativeArtifactCacheMaxBytesToTheSDK(t *testing.T) {
	t.Setenv("XFLOW_ARTIFACT_CACHE_MAX_BYTES", "-1")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.ArtifactCacheMaxBytes != -1 {
			t.Errorf("ArtifactCacheMaxBytes = %d, want -1 -- a negative value is a valid, "+
				"distinct-from-zero setting (explicitly unbounded), not a parse failure to fail open from",
				cfg.ArtifactCacheMaxBytes)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--allow-plaintext")
}

// An XFLOW_ARTIFACT_CACHE_MAX_BYTES value outside int64's range IS a parse
// failure (strconv.ParseInt returns strconv.ErrRange), and must take the same
// fail-open path as "not-a-number" above rather than wrapping around to some
// other in-range value or taking the runner down.
func TestRunCommandToleratesOverflowingArtifactCacheMaxBytes(t *testing.T) {
	t.Setenv("XFLOW_ARTIFACT_CACHE_MAX_BYTES", "99999999999999999999999999")

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		if cfg.ArtifactCacheMaxBytes != 0 {
			t.Errorf("ArtifactCacheMaxBytes = %d, want 0 (fail open to the SDK default) for a value "+
				"outside int64's range", cfg.ArtifactCacheMaxBytes)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--server", "http://server:8080", "--allow-plaintext")
}

// --metrics-addr never reaches xflowsdk.RunnerConfig — runRunner opens the
// scrape listener itself, unconditionally on cfg.metricsAddr, so
// stubRunnerServiceFactory's xflowsdk.RunnerConfig has no field to observe it
// on. No test in this package asserted on cfg.metricsAddr at all (grep
// "metricsAddr" across every _test.go in this package returns nothing before
// this test), so the changed-flag copy in resolveRunnerConfig
// (`if base.changed["metrics-addr"] { cfg.metricsAddr = base.metricsAddr }`)
// could be deleted and every existing test would stay green while the flag
// silently stopped opening a listener at the requested address.
//
// This captures the resolved runnerConfig the same way
// TestRunCommandUsesConfigFile does — via a runFunc override — and asserts on
// the field resolveRunnerConfig actually produces, one level before runRunner
// would use it to open the net.Listener.
func TestRunCommandPropagatesMetricsAddrFlag(t *testing.T) {
	var ran runnerConfig
	cmd := newRootCommand(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			ran = cfg
			return nil
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	})
	cmd.SetArgs([]string{"run", "--server", "http://server:8080", "--metrics-addr", ":9099", "--allow-plaintext"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if ran.metricsAddr != ":9099" {
		t.Fatalf("metricsAddr = %q, want :9099 — the flag never reached the "+
			"resolved config, so runRunner would open no scrape listener at all",
			ran.metricsAddr)
	}
}
