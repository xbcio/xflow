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
	// code outside observability/metrics calls — the xflow_group_* family
	// (group.go) has no wiring point installed. An unwired metric is never
	// created, so it never reaches /metrics and its missing description is a
	// symptom of the dead wiring rather than an independent defect.
	//
	// When one gets wired, add its help text and delete it from this list. Do not
	// add a name here to silence this test for a metric that IS emitted in
	// production: that trades a build failure for a useless description on a live
	// series.
	//
	// This list is NOT the full inventory of dead xflow_group_* names, and help
	// text is not evidence of wiring. Every method on GroupMetrics is dead —
	// NewGroupMetrics has no caller anywhere, and nothing outside this package
	// writes an "xflow_group_" literal — but the ~13 names that happen to have
	// help text pass through the branch above and never reach this map. Reading
	// the six entries below as "these are the unwired ones" is exactly backwards.
	// Wiring the family is a control-plane and runner change (see group.go); the
	// two label values with no signal source behind them, lease_renew's "fenced"
	// and activation's "reconcile", have to be dropped or given one first.
	unwired := map[string]string{
		"xflow_group_commit_total":            "group.go, no caller outside this package",
		"xflow_group_exec_duration_seconds":   "group.go, no caller outside this package",
		"xflow_group_lease_acquired_total":    "group.go, no caller outside this package",
		"xflow_group_lease_expired_total":     "group.go, no caller outside this package",
		"xflow_group_package_cache_total":     "group.go, no caller outside this package",
		"xflow_group_selector_fallback_total": "group.go, no caller outside this package",
	}

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
