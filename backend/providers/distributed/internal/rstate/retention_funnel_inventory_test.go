package rstate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// productionUses returns "file.go:Func" for every function in the package's
// non-test sources that uses the selector name. With calls true only call
// expressions count (x.name(...)); otherwise only plain field reads (x.name).
func productionUses(t *testing.T, name string, calls bool) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		file := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(file, ".go") || strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			callees := map[*ast.SelectorExpr]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						callees[sel] = true
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != name || callees[sel] != calls {
					return true
				}
				found[file+":"+fn.Name.Name] = true
				return true
			})
		}
	}
	out := make([]string, 0, len(found))
	for k := range found {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// requireAllowlisted fails on a use outside the allowlist and on an allowlist
// entry nothing uses any more, so neither a new bypass nor a stale excuse
// survives unnoticed. Every entry carries the reason it is allowed.
func requireAllowlisted(t *testing.T, what string, got []string, allowed map[string]string) {
	t.Helper()
	seen := map[string]bool{}
	for _, use := range got {
		seen[use] = true
		if _, ok := allowed[use]; !ok {
			t.Errorf("%s is used by %s, which is not on the allowlist: a status write there bypasses getExecTTL, "+
				"so it would not keep the execution past its deadline. Route it through getExecTTL, "+
				"or add it to the allowlist with the reason it is safe", what, use)
		}
	}
	for use, reason := range allowed {
		if !seen[use] {
			t.Errorf("allowlist entry %s (%s) no longer uses %s: remove the stale entry", use, reason, what)
		}
	}
}

// TestRetentionFunnelHasNoUnreviewedBypass pins the shape of the fix for B1. The
// retention of the status key is resolved in exactly one place, getExecTTL, which
// layers the execution's deadline over the base retention. Anything that resolves
// the base retention directly, or reads the raw default, can write a TTL that is
// shorter than the time budget still running.
//
// This is a static check on the sources, so it cannot see inside a Lua script;
// the behavioural cover for those is TestStatusRetentionSurvivesAnotherReplica.
// What it does catch is a new Go call site that sidesteps the funnel.
func TestRetentionFunnelHasNoUnreviewedBypass(t *testing.T) {
	t.Run("baseExecTTL", func(t *testing.T) {
		requireAllowlisted(t, "baseExecTTL", productionUses(t, "baseExecTTL", true), map[string]string{
			"state.go:getExecTTL": "the funnel itself: it layers the deadline over the base",
			"execution_seed.go:SeedExecution": "creation start: the deadline is sized in from the snapshot in hand, " +
				"and getExecTTL would read deadline_ms before this call has written it",
		})
	})

	t.Run("execTTL default", func(t *testing.T) {
		requireAllowlisted(t, "the raw execTTL default", productionUses(t, "execTTL", false), map[string]string{
			"state.go:baseExecTTL": "the funnel's base: the default retention when no override or transient rule applies",
			"state.go:ttlSec":      "definition of ttlSec; its callers are allowlisted separately below",
			"state.go:ttlSecForExec": "definition only; it has no callers, and a new one must answer to this test " +
				"through the ttlSecForExec subtest",
			"state_execution.go:createExecution": "creation start: the retention is sized past the deadline right " +
				"after, from the snapshot in hand",
			"entry_admission.go:seedExecutionFromEntry": "creation start, sized past the deadline right after " +
				"from the graph's time budget",
			"concurrency_state.go:createExecutionAdmitted": "fallback only when getExecTTL reports unbounded " +
				"(non-positive); the key it writes is the concurrency tag, not the status",
			"concurrency_state.go:promoteExecution": "fallback only when getExecTTL reports unbounded " +
				"(non-positive); the status TTL otherwise comes from getExecTTL",
			"concurrency_state.go:concurrencySlotTTL": "lifetime of the slot set, not an execution key",
			"idempotency.go:idempotencyClaimTTL":      "lifetime of an idempotency claim, not an execution key",
			"state_execution.go:recordedRetention":    "caps the value written into the retention record",
			"state_execution.go:retentionRecordTTL":   "lifetime of the retention record, not the status",
		})
	})

	t.Run("ttlSec", func(t *testing.T) {
		requireAllowlisted(t, "ttlSec", productionUses(t, "ttlSec", true), map[string]string{
			"state_schedule.go:CheckCompletion": "the one status write: it only ever writes the terminal " +
				"success or failed, after which the deadline is withdrawn and no timeout can follow",
			"state_suspend.go:SuspendOrConsume": "signal, waiter and suspended-set keys; the park renews the " +
				"execution's keys afterwards through extendExecTTL(suspendTTL), which goes through getExecTTL",
			"state_suspend.go:DeliverSignal":     "signal and waiter keys; no status key",
			"state_suspend.go:AcquireResumeLock": "the resume lock key; no status key",
			"state_suspend.go:ResuspendAtomic":   "waiter, signal and resume-lock keys; renewed by the park that follows",
		})
	})

	t.Run("ttlSecForExec", func(t *testing.T) {
		requireAllowlisted(t, "ttlSecForExec", productionUses(t, "ttlSecForExec", true), map[string]string{})
	})
}
