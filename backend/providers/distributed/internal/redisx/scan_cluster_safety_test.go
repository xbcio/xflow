package redisx

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type helperCallCounts struct {
	scanAll  int
	scanPage int
}

// TestProductionScansUseClusterAwareHelpers is a static Redis Cluster safety
// audit. miniredis and a single-node client both accept a bare keyless SCAN and
// cannot expose that go-redis routes it to only one randomly selected cluster
// master. The defect therefore stays silent until a real multi-master cluster
// is used. Keep every production SCAN under internal/ behind ScanAll or
// ScanPage; scan.go is the sole audited location allowed to issue node-local
// SCAN commands.
func TestProductionScansUseClusterAwareHelpers(t *testing.T) {
	internalRoot := internalSourceRoot(t)
	expected := map[string]helperCallCounts{
		"rstate/entry_activation.go": {scanAll: 1},
		"rstate/lease_repair.go":     {scanPage: 1},
		"rstate/receipt_reader.go":   {scanAll: 1},
		"rstate/state_lease.go":      {scanAll: 1},
		"rstate/state_outbox.go":     {scanAll: 3},
		"timeout/monitor.go":         {scanAll: 1},
	}
	actual := make(map[string]helperCallCounts)
	var nakedScans []string
	helperNodeScans := 0

	err := filepath.WalkDir(internalRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(internalRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "Scan" {
				if rel == "redisx/scan.go" {
					helperNodeScans++
				} else {
					position := fileSet.Position(call.Pos())
					nakedScans = append(nakedScans, rel+":"+strconv.Itoa(position.Line))
				}
			}
			qualifier, ok := selector.X.(*ast.Ident)
			if !ok || qualifier.Name != "redisx" {
				return true
			}
			counts := actual[rel]
			switch selector.Sel.Name {
			case "ScanAll":
				counts.scanAll++
			case "ScanPage":
				counts.scanPage++
			default:
				return true
			}
			actual[rel] = counts
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("audit production SCAN calls: %v", err)
	}
	if len(nakedScans) != 0 {
		sort.Strings(nakedScans)
		t.Fatalf("production code contains bare SCAN calls outside redisx helpers: %v", nakedScans)
	}
	if helperNodeScans != 2 {
		t.Fatalf("redisx/scan.go node-local SCAN call count = %d, want 2 audited helper internals", helperNodeScans)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("cluster-aware production scan call sites = %+v, want %+v; audit any new or removed scan explicitly", actual, expected)
	}
}

type scanFunctionStats struct {
	clusterAssertions int
	nonClusterGuards  int
	clusterMasters    int
	masterVisits      int
	namedMasterVisits int
	calls             map[string]int
}

// TestClusterHelpersRetainClusterWiring complements the behavioral tests of
// cursor and merge logic with a static check of the concrete go-redis seam.
// Constructing a realistic *redis.ClusterClient is deliberately left to the
// real-cluster follow-up; miniredis cannot exercise ForEachMaster.
func TestClusterHelpersRetainClusterWiring(t *testing.T) {
	path := filepath.Join(filepath.Dir(currentSourceFile(t)), "scan.go")
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	stats := make(map[string]scanFunctionStats)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		stat := scanFunctionStats{calls: make(map[string]int)}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.TypeAssertExpr:
				if isClusterClientPointer(value.Type) {
					stat.clusterAssertions++
				}
			case *ast.IfStmt:
				if isNegatedIdentifier(value.Cond, "ok") {
					stat.nonClusterGuards++
				}
			case *ast.CallExpr:
				switch called := value.Fun.(type) {
				case *ast.Ident:
					stat.calls[called.Name]++
					if called.Name == "visit" {
						if isMasterVisit(value.Args) {
							stat.masterVisits++
						}
						if isNamedMasterVisit(value.Args) {
							stat.namedMasterVisits++
						}
					}
				case *ast.SelectorExpr:
					if called.Sel.Name == "ForEachMaster" && isIdentifier(called.X, "cluster") {
						stat.clusterMasters++
					}
				}
			}
			return true
		})
		stats[function.Name.Name] = stat
	}

	assertStats := func(name string, clusterAssertions, nonClusterGuards, clusterMasters, masterVisits, namedMasterVisits int, calls map[string]int) {
		t.Helper()
		got, ok := stats[name]
		if !ok {
			t.Fatalf("helper function %s not found", name)
		}
		wantShape := scanFunctionStats{
			clusterAssertions: clusterAssertions,
			nonClusterGuards:  nonClusterGuards,
			clusterMasters:    clusterMasters,
			masterVisits:      masterVisits,
			namedMasterVisits: namedMasterVisits,
		}
		if got.clusterAssertions != wantShape.clusterAssertions ||
			got.nonClusterGuards != wantShape.nonClusterGuards ||
			got.clusterMasters != wantShape.clusterMasters ||
			got.masterVisits != wantShape.masterVisits ||
			got.namedMasterVisits != wantShape.namedMasterVisits {
			t.Fatalf("%s cluster shape = assertions:%d guards:%d masters:%d visits:%d named-visits:%d, want assertions:%d guards:%d masters:%d visits:%d named-visits:%d",
				name,
				got.clusterAssertions, got.nonClusterGuards, got.clusterMasters, got.masterVisits, got.namedMasterVisits,
				wantShape.clusterAssertions, wantShape.nonClusterGuards, wantShape.clusterMasters, wantShape.masterVisits, wantShape.namedMasterVisits,
			)
		}
		for called, want := range calls {
			if got.calls[called] != want {
				t.Fatalf("%s calls %s %d times, want %d", name, called, got.calls[called], want)
			}
		}
	}

	assertStats("ScanAll", 1, 1, 1, 1, 0, map[string]int{
		"scanNode": 1, "scanMasters": 1, "visit": 1,
	})
	assertStats("ScanPage", 1, 1, 1, 0, 1, map[string]int{
		"scanPage": 1, "clusterMasterCounts": 1, "scanMasterPages": 1,
		"visit": 1, "masterID": 1,
	})
	assertStats("clusterMasterCounts", 0, 0, 1, 0, 1, map[string]int{
		"collectMasterCounts": 1, "visit": 1, "masterID": 1,
	})
	assertStats("collectMasterCounts", 0, 0, 0, 0, 0, map[string]int{
		"splitScanCount": 1, "forEach": 1,
	})
	assertStats("scanMasters", 0, 0, 0, 0, 0, map[string]int{
		"scanNode": 1, "sortedKeys": 1, "forEach": 1,
	})
	assertStats("scanMasterPages", 0, 0, 0, 0, 0, map[string]int{
		"scanPage": 1, "sortedKeys": 1, "forEach": 1,
	})
}

func isNegatedIdentifier(expression ast.Expr, name string) bool {
	unary, ok := expression.(*ast.UnaryExpr)
	return ok && unary.Op == token.NOT && isIdentifier(unary.X, name)
}

func isIdentifier(expression ast.Expr, name string) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == name
}

func isMasterVisit(arguments []ast.Expr) bool {
	return len(arguments) == 2 &&
		isIdentifier(arguments[0], "ctx") &&
		isIdentifier(arguments[1], "master")
}

func isNamedMasterVisit(arguments []ast.Expr) bool {
	if len(arguments) != 3 || !isIdentifier(arguments[0], "ctx") || !isIdentifier(arguments[2], "master") {
		return false
	}
	call, ok := arguments[1].(*ast.CallExpr)
	return ok && len(call.Args) == 1 &&
		isIdentifier(call.Fun, "masterID") &&
		isIdentifier(call.Args[0], "master")
}

func isClusterClientPointer(expression ast.Expr) bool {
	pointer, ok := expression.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "ClusterClient" {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	return ok && qualifier.Name == "redis"
}

func internalSourceRoot(t *testing.T) string {
	t.Helper()
	return filepath.Dir(filepath.Dir(currentSourceFile(t)))
}

func currentSourceFile(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("locate scan cluster safety test source")
	}
	return file
}
