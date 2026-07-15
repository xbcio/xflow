package observability

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

func TestMetricsHooksDoNotExportExecutionIDAsLabel(t *testing.T) {
	metrics := metrics.New()
	hooks := NewMetricsHooks(metrics)

	hooks.OnNodeStart(context.Background(), types.ExecutionID("exec-123"), "send_email")
	hooks.OnNodeComplete(context.Background(), types.ExecutionID("exec-123"), "send_email", types.NodeStatusSuccess)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, "exec-123") {
		t.Fatalf("metrics body leaked execution id: %s", body)
	}
	if !strings.Contains(body, `xflow_node_completed_total{node="send_email",node_type="unknown",status="success"} 1`) {
		t.Fatalf("node completed metric missing from body: %s", body)
	}
}

func TestHookChainContinuesAfterPanic(t *testing.T) {
	called := false
	chain := HookChain{
		panicHook{},
		nodeStartHook{fn: func() { called = true }},
	}

	chain.OnNodeStart(context.Background(), "exec-1", "node-1")
	if !called {
		t.Fatal("second hook was not called after first hook panicked")
	}
}

func TestObserverAdaptersIncrementExpectedMetrics(t *testing.T) {
	metrics := metrics.New()

	NewAuditMetrics(metrics).OnAuditFailed("save_signal", assertErr{})
	NewSweepMetrics(metrics).OnSweepReclaim("exec-1", "node-1", 1500)
	NewSweepMetrics(metrics).OnSweepReclaimResult("reclaimed", time.Millisecond)
	NewDispatcherMetrics(metrics).OnDispatchTransient("no_capacity")
	NewAuthMetrics(metrics).OnAuthDecision("register", "deny", "enforcing")
	NewCommitMetrics(metrics).OnCommitOutcome(context.Background(), engine.CommitOutcomeAccepted)
	outbox := NewOutboxMetrics(metrics)
	outbox.OnOutboxRetry(context.Background(), 1)
	outbox.OnOutboxDeadLetter(context.Background())
	outbox.OnOutboxPending(context.Background(), 2, 1, time.Second)
	NewLeaseMetrics(metrics).OnLeaseAcquire("acquired", time.Millisecond)
	NewRunnerClaimMetrics(metrics).OnRunnerClaimReclaimed(2)
	NewRunnerClaimMetrics(metrics).OnRunnerLeaseReplayed()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	for _, want := range []string{
		`xflow_audit_write_total{op="save_signal",result="failed"} 1`,
		`xflow_lease_sweep_reclaimed_total{result="reclaimed"} 1`,
		`xflow_dispatch_transient_total{reason="no_capacity"} 1`,
		`xflow_runner_auth_decisions_total{auth_mode="enforcing",result="deny"} 1`,
		`xflow_lease_age_seconds_count{result="reclaimed"} 1`,
		`xflow_commit_outcomes_total{outcome="accepted"} 1`,
		`xflow_outbox_retries_total 1`,
		`xflow_outbox_dead_letters_total 1`,
		`xflow_outbox_pending 2`,
		`xflow_outbox_dead_letters 1`,
		`xflow_lease_acquire_total{result="acquired"} 1`,
		`xflow_runner_claim_reclaimed_total 2`,
		`xflow_runner_lease_replayed_total 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }

type panicHook struct {
	engine.BaseHooks
}

func (panicHook) OnNodeStart(context.Context, types.ExecutionID, string) {
	panic("boom")
}

type nodeStartHook struct {
	engine.BaseHooks
	fn func()
}

func (h nodeStartHook) OnNodeStart(context.Context, types.ExecutionID, string) {
	h.fn()
}
