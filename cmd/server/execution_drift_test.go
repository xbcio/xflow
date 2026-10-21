package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// clearExecutionDriftEnv neutralises any XFLOW_EXECUTION_DRIFT_* value in the
// test process so a flag-only case observes the flags, not the environment.
func clearExecutionDriftEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		envExecutionDriftRepairEnabled,
		envExecutionDriftInterval,
		envExecutionDriftStaleAfter,
		envExecutionDriftBatch,
		envExecutionDriftDryRun,
	} {
		t.Setenv(name, "")
	}
}

// The worker writes to the durable projection, so the flag must default off
// and its knobs must default to the control worker's own defaults — a
// deployment that upgrades this binary must not start repairing on its own.
func TestParseServerConfigExecutionDriftRepairDefaults(t *testing.T) {
	clearExecutionDriftEnv(t)
	cfg, err := parseServerConfig([]string{"-memory"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.executionDriftRepairEnabled {
		t.Fatal("--execution-drift-repair-enabled defaults to true; the worker writes to the durable store")
	}
	if cfg.executionDriftInterval != control.DefaultExecutionDriftPeriod {
		t.Fatalf("interval = %v, want %v", cfg.executionDriftInterval, control.DefaultExecutionDriftPeriod)
	}
	if cfg.executionDriftStaleAfter != control.DefaultExecutionDriftStaleAfter {
		t.Fatalf("stale after = %v, want %v", cfg.executionDriftStaleAfter, control.DefaultExecutionDriftStaleAfter)
	}
	if cfg.executionDriftBatch != control.DefaultExecutionDriftBatch {
		t.Fatalf("batch = %d, want %d", cfg.executionDriftBatch, control.DefaultExecutionDriftBatch)
	}
	if cfg.executionDriftDryRun {
		t.Fatal("--execution-drift-dry-run defaults to true")
	}
}

func TestParseServerConfigExecutionDriftRepairFlags(t *testing.T) {
	clearExecutionDriftEnv(t)
	cfg, err := parseServerConfig([]string{
		"-memory",
		"-execution-drift-repair-enabled",
		"-execution-drift-interval", "30s",
		"-execution-drift-stale-after", "2m",
		"-execution-drift-batch", "17",
		"-execution-drift-dry-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.executionDriftRepairEnabled || !cfg.executionDriftDryRun {
		t.Fatalf("enabled/dry run = (%v, %v), want (true, true)",
			cfg.executionDriftRepairEnabled, cfg.executionDriftDryRun)
	}
	if cfg.executionDriftInterval != 30*time.Second || cfg.executionDriftStaleAfter != 2*time.Minute {
		t.Fatalf("interval/stale = (%v, %v), want (30s, 2m)",
			cfg.executionDriftInterval, cfg.executionDriftStaleAfter)
	}
	if cfg.executionDriftBatch != 17 {
		t.Fatalf("batch = %d, want 17", cfg.executionDriftBatch)
	}
}

// The XFLOW_EXECUTION_DRIFT_* overlay follows the XFLOW_QUEUE_* rule: a
// non-empty value wins over the flag (including false, which is why the
// booleans are parsed rather than treated as presence), and an unparseable
// value fails startup.
func TestParseServerConfigExecutionDriftRepairEnv(t *testing.T) {
	clearExecutionDriftEnv(t)
	t.Setenv(envExecutionDriftRepairEnabled, "true")
	t.Setenv(envExecutionDriftInterval, "45s")
	t.Setenv(envExecutionDriftStaleAfter, "90s")
	t.Setenv(envExecutionDriftBatch, "33")
	t.Setenv(envExecutionDriftDryRun, "true")

	cfg, err := parseServerConfig([]string{
		"-memory",
		"-execution-drift-interval", "5s",
		"-execution-drift-stale-after", "5s",
		"-execution-drift-batch", "5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.executionDriftRepairEnabled || !cfg.executionDriftDryRun {
		t.Fatalf("enabled/dry run = (%v, %v), want (true, true)",
			cfg.executionDriftRepairEnabled, cfg.executionDriftDryRun)
	}
	if cfg.executionDriftInterval != 45*time.Second || cfg.executionDriftStaleAfter != 90*time.Second {
		t.Fatalf("interval/stale = (%v, %v), want env (45s, 90s)",
			cfg.executionDriftInterval, cfg.executionDriftStaleAfter)
	}
	if cfg.executionDriftBatch != 33 {
		t.Fatalf("batch = %d, want env 33", cfg.executionDriftBatch)
	}

	// A baked-in true must be reversible from the environment too.
	t.Setenv(envExecutionDriftRepairEnabled, "false")
	cfg, err = parseServerConfig([]string{"-memory", "-execution-drift-repair-enabled"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.executionDriftRepairEnabled {
		t.Fatal("XFLOW_EXECUTION_DRIFT_REPAIR_ENABLED=false did not override the flag")
	}

	for env, val := range map[string]string{
		envExecutionDriftRepairEnabled: "sometimes",
		envExecutionDriftInterval:      "soon",
		envExecutionDriftStaleAfter:    "later",
		envExecutionDriftBatch:         "many",
		envExecutionDriftDryRun:        "perhaps",
	} {
		t.Run(env, func(t *testing.T) {
			clearExecutionDriftEnv(t)
			t.Setenv(env, val)
			if _, err := parseServerConfig([]string{"-memory"}); err == nil {
				t.Fatalf("%s=%q accepted", env, val)
			}
		})
	}
}

// Zero or negative knobs are typos this binary cannot honour; they fail even
// while the worker is disabled so the mistake surfaces at startup rather than
// on the day it is enabled.
func TestParseServerConfigRejectsInvalidExecutionDriftKnobs(t *testing.T) {
	clearExecutionDriftEnv(t)
	for _, args := range [][]string{
		{"-execution-drift-interval", "0s"},
		{"-execution-drift-interval", "-1s"},
		{"-execution-drift-stale-after", "0s"},
		{"-execution-drift-stale-after", "-1s"},
		{"-execution-drift-batch", "0"},
		{"-execution-drift-batch", "-1"},
	} {
		t.Run(args[0]+"="+args[1], func(t *testing.T) {
			if _, err := parseServerConfig(append([]string{"-memory"}, args...)); err == nil {
				t.Fatalf("parseServerConfig(%v) accepted an unusable value", args)
			}
		})
	}
}

// driftScanningStore adds the optional scanner capability to an in-memory
// store and records the scan arguments, so the option-to-worker chain can be
// observed from this binary's own assembly.
type driftScanningStore struct {
	store.Store
	mu     sync.Mutex
	calls  int
	limit  int
	before time.Time
	owned  []namespace.Namespace
}

func (s *driftScanningStore) ListStaleRunningExecutions(_ context.Context, owned []namespace.Namespace, before time.Time, limit int) ([]store.ExecutionDriftCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.limit = limit
	s.before = before
	s.owned = append([]namespace.Namespace(nil), owned...)
	return nil, nil
}

func (s *driftScanningStore) scan() (int, int, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.limit, s.before
}

func newDriftServerForTest(t *testing.T, st store.Store, opts []xflowsdk.ServerOption) *xflowsdk.Server {
	t.Helper()
	srv, err := xflowsdk.NewServer(xflowsdk.ServerConfig{Store: st}, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

// Disabled (the default) must not build the worker even when every capability
// is present.
func TestBuildServerOptionsExecutionDriftRepairDisabledByDefault(t *testing.T) {
	clearExecutionDriftEnv(t)
	cfg, err := parseServerConfig([]string{"-mode", "dev", "-memory"})
	if err != nil {
		t.Fatal(err)
	}
	st := &driftScanningStore{Store: memstore.New()}
	srv := newDriftServerForTest(t, st, buildServerOptions(cfg, serverDeps{}))
	if srv.ExecutionDriftRepair() != nil {
		t.Fatal("ExecutionDriftRepair() non-nil with the flag off: the worker writes to the durable store and must be opt-in")
	}
}

// The full chain: flags -> parseServerConfig -> buildServerOptions -> a real
// NewServer -> a worker whose scan uses the configured staleness window and
// batch bound.
func TestBuildServerOptionsReachesExecutionDriftRepair(t *testing.T) {
	clearExecutionDriftEnv(t)
	cfg, err := parseServerConfig([]string{
		"-mode", "dev", "-memory",
		"-execution-drift-repair-enabled",
		"-execution-drift-interval", "30s",
		"-execution-drift-stale-after", "3m",
		"-execution-drift-batch", "11",
	})
	if err != nil {
		t.Fatal(err)
	}
	st := &driftScanningStore{Store: memstore.New()}
	srv := newDriftServerForTest(t, st, buildServerOptions(cfg, serverDeps{}))
	worker := srv.ExecutionDriftRepair()
	if worker == nil {
		t.Fatal("ExecutionDriftRepair() = nil with the flag on and a scanning store: buildServerOptions dropped the option")
	}
	// The in-memory backend's status reader cannot find the candidate, so this
	// sweep repairs nothing; the scan arguments are what this test asserts.
	if repaired := worker.ReconcileOnce(context.Background()); repaired != 0 {
		t.Fatalf("ReconcileOnce repaired %d rows with no candidates, want 0", repaired)
	}
	calls, limit, before := st.scan()
	if calls != 1 {
		t.Fatalf("scanner calls = %d, want 1", calls)
	}
	if limit != 11 {
		t.Fatalf("scan limit = %d, want the flag's 11", limit)
	}
	if age := time.Since(before); age < 2*time.Minute+50*time.Second || age > 3*time.Minute+30*time.Second {
		t.Fatalf("scan updatedBefore is %v old, want about the flag's 3m staleness window", age)
	}
}

// A store without the scanner capability leaves the worker unbuilt rather than
// building one that scans nothing.
func TestBuildServerOptionsExecutionDriftRepairAbsentWithoutScanner(t *testing.T) {
	clearExecutionDriftEnv(t)
	cfg, err := parseServerConfig([]string{
		"-mode", "dev", "-memory", "-execution-drift-repair-enabled",
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := newDriftServerForTest(t, memstore.New(), buildServerOptions(cfg, serverDeps{}))
	if srv.ExecutionDriftRepair() != nil {
		t.Fatal("ExecutionDriftRepair() non-nil with a store that cannot scan: the worker would scan nothing")
	}
}
