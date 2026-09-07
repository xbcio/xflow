package metrics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strconv"
	"strings"
	"testing"
)

// helpText falls back to "xflow metric <name>", which tells an operator nothing
// they could not already read off the series name. A metric that reaches
// /metrics without a metricHelp entry therefore ships with no description at
// all, and nothing in the build says so — the fallback is silent by design.
// Every gap found so far got there that way, including four trigger batch
// metrics and both consumer-lag gauges.
//
// This test enumerates the names this package can emit by reading its own
// source, and requires each to have help text or be listed as unwired below.
//
// Source-scanning is the only way to enumerate them: the names are unexported
// consts and inline literals with no registry behind them, so there is nothing
// to iterate at runtime, and a hand-maintained list in the test would drift
// exactly like metricHelp did.
//
// RANGE — this covers names written in observability/metrics only. A metric name
// can also be a string literal in the package that emits it: service/control/
// metrics_inbox.go writes "xflow_runner_up" and seven other xflow_runner_metrics_*
// names directly. Those have help text today, but nothing here would notice if
// the next one did not, because reaching them means resolving const identifiers
// across package boundaries (go/types), and pointing this test at service/ would
// also put its failures in a package whose owners do not own metricHelp.
func TestEveryMetricNameInThisPackageHasHelpText(t *testing.T) {
	// unwired names are emitted by a method in this package that no production
	// code outside observability/metrics calls. An unwired metric is never
	// created, so it never reaches /metrics and its missing description is a
	// symptom of the dead wiring rather than an independent defect.
	//
	// When one gets wired, add its help text and delete it from this list. Do not
	// add a name here to silence this test for a metric that IS emitted in
	// production: that trades a build failure for a useless description on a live
	// series.
	//
	// This list is NOT the full inventory of dead xflow_group_* names, and help
	// text is not evidence of wiring. Every method on GroupMetrics used to be
	// dead — NewGroupMetrics had no caller anywhere, and nothing outside this
	// package wrote an "xflow_group_" literal. package_cache_total (see
	// execution/subgraph/cache.go), the six lease/commit names — acquired,
	// expired, renew total+duration, commit total+exec-duration (see
	// engine/observers.go's GroupObserver and observability/metrics/
	// group_observer.go) — the admission pair — admission total+duration,
	// wired the same way via GroupObserver.OnGroupAdmission
	// (engine/entry_admission.go's SeedExecutionFromEntry, GROUP entry units
	// only) — the three activation-controller names — activation
	// total, generation-fenced total, active gauge — and selector_fallback_total
	// (both the latter two wired via
	// service/control/entry_activation_reconciler.go's assignUnowned/Reconcile)
	// are all wired now. That closes out every name this family ever had, which
	// is why the map below is empty rather than deleted outright: the mechanism
	// stays in place for whatever lands unwired next.
	//
	// (Historical note: an earlier version of this comment flagged activation's
	// "reconcile" action-label value as a separate, still-open gap tracked
	// outside this map. That value was removed from the action enum entirely in
	// #48 — before activation-controller metrics were wired at all — so there is
	// nothing left for that note to describe.)
	unwired := map[string]string{}

	names := metricNamesInPackage(t)
	// 76 consts plus the 13 inline literals in group.go was the count when this
	// guard was written. A floor guards against the parse silently seeing nothing
	// and the test passing vacuously; it is deliberately below that count so
	// deleting a metric does not fail here.
	if len(names) < 80 {
		t.Fatalf("found only %d metric names; the parse is not seeing the package, "+
			"so this test would pass vacuously", len(names))
	}

	for name, where := range names {
		if _, ok := metricHelp[name]; ok {
			continue
		}
		if reason, known := unwired[name]; known {
			t.Logf("%s: no help text, allowed as unwired (%s)", name, reason)
			continue
		}
		t.Errorf("%s (%s) has no metricHelp entry, so it exports the useless "+
			"fallback %q", name, where, helpText(name))
	}

	// The reverse direction: an allowlist entry that has since gained help text is
	// stale, and leaving it there hides the next real gap.
	for name := range unwired {
		if _, ok := metricHelp[name]; ok {
			t.Errorf("%s is listed as unwired but now has help text; remove it "+
				"from the allowlist", name)
		}
	}
}

// TestEveryMetricHelpEntryHasEmitter is the reverse direction of
// TestEveryMetricNameInThisPackageHasHelpText: it catches an orphan help
// entry, a metricHelp key with no emission point anywhere in this package,
// which the forward test cannot see because it only walks name->help and
// `if _, ok := metricHelp[name]; ok { continue }` lets an orphan slip past
// without ever being checked in the other direction.
//
// "Has an emission point" means the name appears in this package's non-test
// source as the name argument to a metrics sink call (Inc/Add/Observe/
// ObserveBytes/ObserveCount/Set) — either as a string literal directly (as
// group.go does) or via a const identifier declared with that string value
// (as engine.go, supply.go, trigger.go, etc. do). metricNamesInPackage
// already enumerates exactly that surface (see its doc comment: both
// spellings, literals and const declarations), which is why this test reuses
// it instead of re-deriving a second, narrower call-argument walk: a walk
// that only followed literal/const arguments directly would miss names like
// xflow_node_started_total, which reach Inc through an intermediate
// engineMetricNames struct field (h.names.nodeStarted) rather than a bare
// identifier.
//
// The check here is deliberately "does this name appear anywhere in the
// package's non-test source as a declared/emitted literal", not "is the
// method that emits it ever called by production code outside this
// package". Three names in group.go — xflow_group_activation_total,
// _activation_generation_fenced_total, and _activation_active — have both
// help text and a real g.m.Inc/Observe/Set call site, yet nothing outside
// this package calls the GroupMetrics method that reaches them. Judging by
// reachability would flag all three as orphans and this test would then need
// a whitelist to un-flag a live, correctly-described-if-unwired family —
// exactly the "silently loosen the sieve" failure mode this suite must not
// fall into. Judging by static presence in an emit call's name position
// correctly leaves them alone and only catches a help entry with no
// emission point at all, like xflow_group_suspend_total: a capability
// (durable group suspend) that was removed (engine/group_lease.go references
// it as "since removed"; service/runner/group_runtime.go has
// WithSuspendDisabled) while its help text was left behind.
//
// (xflow_group_admission_total and _admission_duration_seconds used to be
// listed alongside these three, for the same reason: help text and a real
// call site, but no production caller of the GroupMetrics method. That
// stopped being true once engine.GroupObserver grew OnGroupAdmission and
// engine/entry_admission.go's SeedExecutionFromEntry started calling it for
// GROUP entry units — the same OnGroupCommit fan-out shape as the six
// lease/commit names below.)
//
// The three are named rather than counted because this paragraph has gone
// stale once already: it used to say the whole family had "zero production
// callers of NewGroupMetrics anywhere in the repo", which stopped being true
// when 7f8a4df/bdade0c wired package_cache and the six lease/commit names.
// A claim about specific names fails loudly when someone greps them; a claim
// about a count keeps reading as true long after it isn't.
func TestEveryMetricHelpEntryHasEmitter(t *testing.T) {
	// emittedOutsidePackage covers the same RANGE gap documented on
	// metricNamesInPackage above: these names are emitted for real, just not
	// by a literal or const declared inside observability/metrics, so the
	// scan below cannot see them. Each has a verified call site — unlike
	// xflow_group_suspend_total, which has none anywhere in the repo. This is
	// not a whitelist for an unresolved orphan: every entry names the file
	// that actually emits it, and a name only belongs here if grepping the
	// repo for its literal turns up a real Inc/Set/Observe/ObserveBytes call
	// outside this package.
	emittedOutsidePackage := map[string]string{
		"xflow_runner_up": "service/control/metrics_inbox.go",
		"xflow_runner_metrics_last_report_age_seconds": "service/control/metrics_inbox.go",
		"xflow_runner_metrics_received_total":          "service/control/metrics_inbox.go",
		"xflow_runner_metrics_rejected_total":          "service/control/metrics_inbox.go",
		"xflow_runner_metrics_inbox_size":              "service/control/metrics_inbox.go",
		"xflow_runner_metrics_gather_errors_total":     "service/control/metrics_inbox.go",
		"xflow_runner_metrics_reports_total":           "service/runner/metrics_reporter.go",
		"xflow_runner_metrics_report_bytes":            "service/control/metrics_inbox.go",
	}

	names := metricNamesInPackage(t)
	// Same floor rationale as TestEveryMetricNameInThisPackageHasHelpText:
	// without it, a parse that silently sees nothing would make every
	// metricHelp entry look orphaned, or a parse that silently sees nothing
	// AND has no metricHelp entries would pass vacuously either way. This
	// test needs its own guard because it does not share a call stack with
	// the forward test — each test function gets a fresh names map.
	if len(names) < 80 {
		t.Fatalf("found only %d metric names; the parse is not seeing the package, "+
			"so this test would pass vacuously", len(names))
	}

	for name := range metricHelp {
		if _, ok := names[name]; ok {
			continue
		}
		if where, known := emittedOutsidePackage[name]; known {
			t.Logf("%s: no in-package emission point, allowed (emitted by %s)", name, where)
			continue
		}
		t.Errorf("%s has a metricHelp entry but no emission point anywhere in "+
			"this package's non-test source (not a literal or const-declared "+
			"name argument to Inc/Add/Observe/ObserveBytes/ObserveCount/Set); "+
			"it is dead help text for a metric nothing emits", name)
	}

	// The reverse direction: an allowlist entry whose name now also appears
	// in this package's own scan is stale — either it gained a real
	// in-package emitter (drop it, the general check above now covers it) or
	// something renamed/removed the only real emitter (worth a second look).
	for name := range emittedOutsidePackage {
		if _, ok := names[name]; ok {
			t.Errorf("%s is listed as emitted-outside-package but the in-package "+
				"scan now finds it too; remove it from the allowlist", name)
		}
	}
}

// metricNamesInPackage returns every xflow_-prefixed string literal in the
// package's non-test files, mapped to the file it came from.
//
// Both spellings have to be picked up: most names are consts (engine.go,
// supply.go, trigger.go), but group.go passes them to Inc/Set as inline
// literals, and scanning only const declarations missed thirteen names — a
// fifth of the surface.
//
// The metricHelp declaration is skipped, because its keys are the same literals:
// including them would make every name look declared and the test could then
// only ever pass.
func metricNamesInPackage(t *testing.T) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing package: %v", err)
	}

	names := map[string]string{}
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				if isMetricHelpDecl(n) {
					return false
				}
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				unquoted, err := strconv.Unquote(lit.Value)
				if err != nil || !strings.HasPrefix(unquoted, "xflow_") {
					return true
				}
				names[unquoted] = path
				return true
			})
		}
	}
	return names
}

// isMetricHelpDecl reports whether n is the `var metricHelp = map[...]` spec.
func isMetricHelpDecl(n ast.Node) bool {
	spec, ok := n.(*ast.ValueSpec)
	if !ok {
		return false
	}
	for _, name := range spec.Names {
		if name.Name == "metricHelp" {
			return true
		}
	}
	return false
}
