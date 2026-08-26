package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// toSDKRunnerConfig is a field-by-field copy from the resolved CLI/YAML config
// into the SDK's config, and it had no test of its own — the name appears in no
// _test.go file in this package. run_test.go covers durations, capabilities,
// labels, transport, TLS and the artifact cache dir; the rest of the struct was
// copied on trust.
//
// That shape is the one that fails silently. config_test.go proves each of
// these values is PARSED — it reads them back off runnerConfig — and the SDK
// supplies a default for every one of them, so a line deleted from the literal
// in toSDKRunnerConfig produces a runner that starts, registers, verifies clean,
// and behaves as if the operator had never written the setting. There is no
// error and no log line anywhere on that path.
//
// A projection built by hand-copying fields has already dropped a field in this
// repo once: the group package projection copied six NodeDef fields and new ones
// silently never reached the inner engine. This test exists so the next added
// field is a failing test rather than a degraded runner.
//
// Verified by deleting each line of that literal in turn. RunnerID, Concurrency
// and the whole ResourcePoolConfig block were genuinely unguarded: only this test
// goes red for them. Namespaces and Credentials were ALREADY covered — deleting
// either also fails TestRunCommandPropagatesNamespacesToTheSDK and
// TestRunCommandPropagatesExpandedCredentialsToTheSDK. They are asserted here
// anyway because the claim being pinned is the literal as a whole, but they are
// not new coverage and should not be counted as such.

func TestRunCommandPropagatesIdentityAndPoolSettingsToTheSDK(t *testing.T) {
	// A single YAML carrying every field run_test.go does not already cover.
	// Values are deliberately unlike any default, so "the field never arrived"
	// and "the field arrived holding its zero value" are different observations.
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
server:
  url: http://server:8080
runner:
  id: runner-alpha-7
  concurrency: 23
  namespaces:
    - tenant-a
    - tenant-b
credentials:
  db:
    driver: mysql
    dsn: "user:pw@tcp(db:3306)/xflow"
resource_pool:
  sql:
    max_open_conns: 41
    max_idle_conns: 7
    conn_max_lifetime: "23m"
  grpc:
    keepalive_time: "37s"
    keepalive_timeout: "11s"
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	restore := stubRunnerServiceFactory(func(cfg xflowsdk.RunnerConfig) error {
		// The runner ID is the identity every lease, claim and reclaim is keyed
		// by. Dropped, the SDK generates one, so a restarted runner comes back
		// under a new name and the control plane has to wait out the old lease
		// instead of the operator's fixed ID reclaiming its own work.
		if cfg.RunnerID != "runner-alpha-7" {
			t.Errorf("RunnerID = %q, want runner-alpha-7: the configured identity "+
				"never reached the SDK, which then invents one per process start",
				cfg.RunnerID)
		}
		// Concurrency is the only lever over how much work this process pulls.
		// Dropped, it falls back to the SDK default and a runner sized for a
		// large box quietly runs at the small-box setting.
		if cfg.Concurrency != 23 {
			t.Errorf("Concurrency = %d, want 23: the configured width never reached "+
				"the SDK and the runner is running at its default", cfg.Concurrency)
		}
		// Namespaces decide which tenants' work this runner is offered at all.
		// Dropped, the runner polls the default namespace only, and every
		// tenant-scoped workflow waits forever with no runner claiming it.
		want := []namespace.Namespace{"tenant-a", "tenant-b"}
		if len(cfg.Namespaces) != len(want) {
			t.Errorf("Namespaces = %v, want %v: this runner is not offered the "+
				"tenants it was configured for", cfg.Namespaces, want)
		} else {
			for i := range want {
				if cfg.Namespaces[i] != want[i] {
					t.Errorf("Namespaces[%d] = %q, want %q", i, cfg.Namespaces[i], want[i])
				}
			}
		}
		// Credentials become the runner's CredentialResolver. Dropped, every
		// node that resolves a credential fails at execution time rather than
		// at startup, so the failure surfaces per-task and looks like a node bug.
		if cfg.Credentials["db"]["driver"] != "mysql" {
			t.Errorf("Credentials[db][driver] = %v, want mysql: the resolver is "+
				"empty and every credential lookup fails at task time",
				cfg.Credentials["db"]["driver"])
		}
		// The pool tunables are the case where a dropped field is invisible even
		// in principle: the zero value means "use defaults", so the pool is
		// built successfully either way and only the connection counts differ.
		// Each value below is distinct from both the zero and the default.
		if got := cfg.ResourcePoolConfig.SQL.MaxOpenConns; got != 41 {
			t.Errorf("ResourcePoolConfig.SQL.MaxOpenConns = %d, want 41: the zero "+
				"value here reads as \"use defaults\", so the tuning silently did "+
				"not happen", got)
		}
		if got := cfg.ResourcePoolConfig.SQL.MaxIdleConns; got != 7 {
			t.Errorf("ResourcePoolConfig.SQL.MaxIdleConns = %d, want 7", got)
		}
		if got := cfg.ResourcePoolConfig.SQL.ConnMaxLifetime; got != 23*time.Minute {
			t.Errorf("ResourcePoolConfig.SQL.ConnMaxLifetime = %v, want 23m", got)
		}
		if got := cfg.ResourcePoolConfig.GRPC.KeepaliveTime; got != 37*time.Second {
			t.Errorf("ResourcePoolConfig.GRPC.KeepaliveTime = %v, want 37s", got)
		}
		if got := cfg.ResourcePoolConfig.GRPC.KeepaliveTimeout; got != 11*time.Second {
			t.Errorf("ResourcePoolConfig.GRPC.KeepaliveTimeout = %v, want 11s", got)
		}
		return nil
	})
	defer restore()

	runCommand(t, "run", "--config", path)
}
