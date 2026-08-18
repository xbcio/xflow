package protocol

import (
	"fmt"
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

// stubRunnerHandler is a no-op RunnerHTTPHandler so the dead-constant guard can
// mount the real RegisterRunnerRoutes without depending on a control plane.
// Its methods never run in this test — the guard probes mux.Handler (path
// matching only), so the handler body is irrelevant.
type stubRunnerHandler struct{}

func (stubRunnerHandler) HandleRegisterRunner(http.ResponseWriter, *http.Request) {}
func (stubRunnerHandler) HandleHeartbeat(http.ResponseWriter, *http.Request)      {}
func (stubRunnerHandler) HandlePollTask(http.ResponseWriter, *http.Request)       {}
func (stubRunnerHandler) HandleReportResult(http.ResponseWriter, *http.Request)   {}
func (stubRunnerHandler) HandleRenewLease(http.ResponseWriter, *http.Request)     {}
func (stubRunnerHandler) HandleActivationAck(http.ResponseWriter, *http.Request)  {}
func (stubRunnerHandler) HandleReportMetrics(http.ResponseWriter, *http.Request)  {}

// runnerPathConst is one runner-facing path string constant read from source.
type runnerPathConst struct {
	name  string
	value string
}

// runnerPathConsts parses every non-test .go file in this package directory and
// returns every string constant whose value starts with "/v1/runners/" — the
// runner-protocol path vocabulary declared across server.go, group.go,
// activation.go and metrics.go. Go cannot reflect over constants, so the guard
// reads them from source; the parser is the single source of truth, so a path
// constant added to any of those files is seen automatically without a
// hand-written list to drift. This mirrors the apiserver authz guard's
// opConsts helper (service/apiserver/authz_coverage_test.go).
//
// The "/v1/runners/" prefix is load-bearing: it scopes the parser to the
// runner-protocol face and excludes the entry-seed "/v1/executions" literal,
// which lives in a function body (not a const declaration) and is deliberately
// not a runner-facing path (spec §0.1). Were it ever promoted to a const, the
// prefix filter would still skip it — and that skip would be the correct
// signal to review whether it belongs in RunnerFacingPaths.
func runnerPathConsts(t *testing.T) []runnerPathConst {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir .: %v", err)
	}
	fset := token.NewFileSet()
	var out []runnerPathConst
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := name
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
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
				for i, cn := range vs.Names {
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
					// Filter on "/v1/" rather than "/v1/runners/": the runner
					// face is not confined to that subtree. Spec §0.1 puts
					// POST /v1/executions (entry-seed) on the runner face too,
					// and it lives in this package. Narrowing to
					// "/v1/runners/" would make the guard silently skip any
					// runner-facing constant declared outside that subtree --
					// reintroducing, one prefix down, the blind spot this
					// guard exists to close. Every string constant in this
					// package that names a "/v1/" path is a protocol path by
					// construction, so a false positive here is a loud test
					// failure, never a silent pass.
					if !strings.HasPrefix(val, "/v1/") {
						continue
					}
					out = append(out, runnerPathConst{name: cn.Name, value: val})
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("no runner-facing path constants parsed from package sources — the guard cannot see the vocabulary; check the parser")
	}
	return out
}

// TestRunnerPathsHaveMuxRegistration is the protocol-face twin of the apiserver
// dead-constant guard. It has two halves:
//
//   - Direction A (source -> list): every runner-path constant declared in the
//     package source must appear in the production-side RunnerFacingPaths
//     enumerable. A constant declared and even route-registered but not listed
//     in RunnerFacingPaths is a dead contract for any consumer (e.g. a spec
//     generator) that reads RunnerFacingPaths as the vocabulary — the previous
//     shape of this guard iterated that same list and so could not see a
//     constant that had escaped it. Reading the source via go/parser closes
//     that hole: there is no third hand-written table to drift.
//   - Direction B (list -> route): every entry in RunnerFacingPaths must resolve
//     to a route registered by RegisterRunnerRoutes. An entry with no
//     registration is a dead constant — a client coded against it gets a 404
//     and nothing flagged the gap.
//
// This guard was born green. The three dead constants it would have caught
// (ActivatePath/DeactivatePath/ActivationListPath) were already removed in T8,
// so the guard exists to prevent the next dead constant, not to fix a present
// defect. The R53-style injection proofs in the task-10 report establish that
// each direction can actually go red; a guard that has never been red is
// indistinguishable from one whose assertion is silently vacuous.
func TestRunnerPathsHaveMuxRegistration(t *testing.T) {
	parsed := runnerPathConsts(t)

	// Direction A: every source-declared runner path constant must be in
	// RunnerFacingPaths. Missing entries are constants the guard would never
	// see otherwise — exactly the drift the guard exists to catch.
	listSet := make(map[string]bool, len(RunnerFacingPaths))
	for _, p := range RunnerFacingPaths {
		listSet[p] = true
	}
	var leaked []string
	for _, c := range parsed {
		if !listSet[c.value] {
			leaked = append(leaked, fmt.Sprintf("%s = %q", c.name, c.value))
		}
	}
	if len(leaked) > 0 {
		sort.Strings(leaked)
		t.Errorf("runner-facing path constants declared in source but missing from RunnerFacingPaths (add them or the guard silently skips them): %v", leaked)
	}

	// Direction B: every RunnerFacingPaths entry must have a route registered by
	// RegisterRunnerRoutes. Every runner-protocol route is an exact path (no
	// placeholders), and registration uses mux.HandleFunc(path, ...) without a
	// method constraint, so probing with POST matches any method.
	mux := http.NewServeMux()
	RegisterRunnerRoutes(mux, stubRunnerHandler{})
	var unregistered []string
	for _, p := range RunnerFacingPaths {
		req := httptest.NewRequest(http.MethodPost, p, nil)
		_, pattern := mux.Handler(req)
		if pattern == "" {
			unregistered = append(unregistered, p)
		}
	}
	if len(unregistered) > 0 {
		sort.Strings(unregistered)
		t.Errorf("RunnerFacingPaths entries with no registered route (dead constants or wrong path): %v", unregistered)
	}
}
