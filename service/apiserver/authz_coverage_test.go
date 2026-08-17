package apiserver

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This file holds the authz coverage guards (guards 2 and 3). Both were born
// green — T8/T9 cleaned the last dead operations — so the R53 injection proofs
// (see the report) are what establish that each guard can actually go red. A
// guard that has never been red is indistinguishable from one whose assertion
// is silently vacuous; the injections are not ceremony.

// opConst is one Op* string constant read from authz.go's source.
type opConst struct {
	name  string
	value string
}

// opConsts parses authz.go and returns every Op* string constant (name + value).
// Go cannot reflect over constants, so the guards read them from source. The
// parser is the single source of truth for both guard 2 (consumer check) and
// guard 3 (scopeForOperation default check), so a constant added in authz.go is
// seen by both automatically — no hand-written vocabulary list to drift.
func opConsts(t *testing.T) []opConst {
	t.Helper()
	fset := token.NewFileSet()
	// Go test runs with cwd = the package source directory, so "authz.go" is
	// the file defining the vocabulary. Use filepath.Abs defensively in case a
	// future runner resets cwd.
	path := "authz.go"
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []opConst
	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "Op") {
					continue
				}
				if i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}
				out = append(out, opConst{name: name.Name, value: val})
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("no Op* constants parsed from authz.go — the guards cannot see the vocabulary; check the parser")
	}
	return out
}

// opConsumedOutsideAuthz reports whether opName appears in any non-test,
// non-authz.go .go file in the package directory. This is guard 2's static half:
// an operation defined but referenced nowhere outside its own declaration is
// dead code. The non-test filter is load-bearing — sqlaudit_test.go (and any
// future test) may use an Op as a placeholder, and a grep that counted that
// would resurrect a dead operation (brief risk point 5).
func opConsumedOutsideAuthz(t *testing.T, opName string) bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir .: %v", err)
	}
	needle := []byte(opName)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "authz.go" {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		if bytes.Contains(data, needle) {
			return true
		}
	}
	return false
}

// TestScopeForOperationHasNoDefault is guard 3: no Op* constant may fall
// through scopeForOperation to the default "" branch. A "" scope is denied by
// BOTH ScopeAuthorizer and NamespaceAwareAuthorizer (fail-closed), which means
// a route bound to that operation is silently unreachable — callers get 403
// forever and nothing tells the operator why. This guard was born green; it
// prevents the next unmapped operation.
//
// The check is behavioral (call scopeForOperation(value)), not syntactic (grep
// for a case), so it catches a typo'd case label as well as a missing one.
func TestScopeForOperationHasNoDefault(t *testing.T) {
	for _, c := range opConsts(t) {
		got := scopeForOperation(c.value)
		if got == "" {
			t.Errorf("scopeForOperation(%q = const %s) = %q; an operation in the default branch is denied by both authorizers, making its route silently unreachable", c.value, c.name, got)
		}
	}
}

// TestEveryOperationHasRouteConsumer is guard 2's static half: every Op*
// constant must be referenced by at least one non-test, non-authz.go source
// file in this package. An operation with no consumer is dead code — it was
// defined, possibly bound into scopeForOperation, but no route actually carries
// it, so the authz surface advertises a permission that does nothing.
//
// The non-test filter is the whole point: sqlaudit_test.go has historically used
// an Op as an arbitrary placeholder value, and counting that would mask a
// genuinely dead operation. Today no Op is test-only-referenced; if one ever
// is, the right fix is to change the test (or remove the dead Op), never to
// relax this guard.
func TestEveryOperationHasRouteConsumer(t *testing.T) {
	var orphans []string
	for _, c := range opConsts(t) {
		if !opConsumedOutsideAuthz(t, c.name) {
			orphans = append(orphans, c.name)
		}
	}
	if len(orphans) > 0 {
		sort.Strings(orphans)
		t.Errorf("Op* constants with no non-test consumer outside authz.go (dead operations): %v", orphans)
	}
}

// TestEveryProtectedUserPathRejectsUnauthenticated is guard 2's behavioral half
// and the real safety net: every user-facing route EXCEPT the open probes
// (/healthz, /readyz) must be behind the authz wrapper, so an unauthenticated
// request is refused with 401/403 before any handler runs. A route registered
// without the authz wrapper would return something else (200/400/404) and that
// is an authorization hole — this is the half a static scan cannot prove
// because bare-mode routes use string op labels, not Op constants.
//
// Coverage: every entry in guardSamples except PathHealthz/PathReadyz. The
// entry-seed route POST /v1/executions is deliberately absent from
// UserFacingPaths (spec §0.1, runner protocol face) so it is not exercised
// here; it is behind the same authz wrapper in module_control.registerAuthzRoutes
// and is covered by entry_seed_endpoint_test.go.
func TestEveryProtectedUserPathRejectsUnauthenticated(t *testing.T) {
	mux := newFullGuardMux(t)

	for _, gs := range guardSamples {
		if gs.pathConst == PathHealthz || gs.pathConst == PathReadyz {
			continue
		}
		req := httptest.NewRequest(gs.method, gs.sample, nil)
		// No Authorization header — the request carries no credential at all.
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		switch rec.Code {
		case http.StatusUnauthorized, http.StatusForbidden:
			// expected: authz wrapper refused before the handler ran
		default:
			t.Errorf("%s (%s %s): unauthenticated request got status %d, want 401 or 403 (route not behind authz wrapper — an authorization hole)",
				gs.constName, gs.method, gs.sample, rec.Code)
		}
	}
}
