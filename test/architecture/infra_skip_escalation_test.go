package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// infraSkipDirs are the build-tag-gated suites that talk to real Redis / Kafka
// / MySQL and therefore skip themselves when the dependency is missing. Each
// lives behind its own tag, so none of them compiles during an ordinary
// `make test` and none can assert this invariant about itself.
//
// If a directory legitimately stops using real infrastructure, drop it from
// this list rather than leaving an entry that matches nothing.
var infraSkipDirs = []string{
	"test/perf",
	"test/soak",
	"test/integration",
}

// infraRequireEnv maps an infrastructure name, as it appears in a skip
// message, to the environment variable that must turn that skip into a
// failure.
var infraRequireEnv = map[string]string{
	"redis": "XFLOW_REQUIRE_REDIS_INTEGRATION",
	"kafka": "XFLOW_REQUIRE_KAFKA_INTEGRATION",
	"mysql": "XFLOW_REQUIRE_MYSQL_INTEGRATION",
}

// unavailabilityPhrases distinguish "the dependency is not there" skips, which
// must escalate, from ordinary conditional skips such as short mode.
var unavailabilityPhrases = []string{"unavailable", "not reachable", "unreachable"}

// TestInfraSkipsEscalateUnderRequireEnv pins the contract that XFLOW_REQUIRE_*_INTEGRATION=1
// is honoured by every helper that skips on a missing dependency.
//
// The hazard is asymmetric and quiet. A skipped test prints "--- SKIP" only
// under -v, and `go test -bench` prints nothing at all for a skipped
// benchmark: the package still reports "ok". So a half-wired gate does not
// look broken, it looks green. test/perf demonstrated exactly that — its
// *testing.T helper escalated while its *testing.B helpers did not, and a
// -tags=perf run with XFLOW_REQUIRE_REDIS_INTEGRATION=1 against a dead Redis
// reported ok while seven real-Redis benchmarks silently vanished.
//
// Nothing else can catch this: each suite is behind a build tag, so the
// ordinary test run never compiles these files, and a run that does compile
// them only detects the gap when the dependency happens to be down.
func TestInfraSkipsEscalateUnderRequireEnv(t *testing.T) {
	repoRoot := findRepositoryRoot(t)

	for _, dir := range infraSkipDirs {
		t.Run(dir, func(t *testing.T) {
			found := 0
			walkErr := filepath.WalkDir(filepath.Join(repoRoot, dir), func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() || !strings.HasSuffix(path, ".go") {
					return nil
				}
				found += checkInfraSkipsInFile(t, repoRoot, path)
				return nil
			})
			if walkErr != nil {
				t.Fatalf("walk %s: %v", dir, walkErr)
			}
			if found == 0 {
				t.Errorf("no dependency-unavailable skip found; either %s stopped using real infrastructure (drop it from infraSkipDirs) or the skip messages no longer say %q", dir, unavailabilityPhrases)
			}
		})
	}
}

// checkInfraSkipsInFile reports how many dependency-unavailable skips the file
// contains, failing t for each one whose enclosing function lacks the matching
// escalation branch.
func checkInfraSkipsInFile(t *testing.T, repoRoot, path string) int {
	t.Helper()

	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fileSet := token.NewFileSet()
	// Parsed directly rather than via go/build, so the file's build tag does
	// not exclude it the way it excludes these suites from `make test`.
	parsed, err := parser.ParseFile(fileSet, path, source, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	relative, err := filepath.Rel(repoRoot, path)
	if err != nil {
		relative = path
	}

	found := 0
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		body := string(source[fileSet.Position(function.Body.Pos()).Offset:fileSet.Position(function.Body.End()).Offset])

		ast.Inspect(function.Body, func(node ast.Node) bool {
			infra, line, ok := infraSkipAt(fileSet, node)
			if !ok {
				return true
			}
			found++
			if requireEnv := infraRequireEnv[infra]; !strings.Contains(body, requireEnv) {
				t.Errorf("%s:%d: %s skips when %s is unavailable but never checks %s, so setting it to 1 leaves this a silent skip; mirror the escalation branch in test/integration/harness.go:requireRedis", relative, line, function.Name.Name, infra, requireEnv)
			}
			return true
		})
	}
	return found
}

// infraSkipAt reports the infrastructure named by a Skip/Skipf call whose
// message says the dependency is unavailable. Skips with any other message —
// short mode, an unsupported platform — are not the subject of this invariant.
func infraSkipAt(fileSet *token.FileSet, node ast.Node) (infra string, line int, ok bool) {
	call, isCall := node.(*ast.CallExpr)
	if !isCall || len(call.Args) == 0 {
		return "", 0, false
	}
	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector || (selector.Sel.Name != "Skip" && selector.Sel.Name != "Skipf") {
		return "", 0, false
	}
	literal, isLiteral := call.Args[0].(*ast.BasicLit)
	if !isLiteral || literal.Kind != token.STRING {
		return "", 0, false
	}
	message, err := strconv.Unquote(literal.Value)
	if err != nil {
		return "", 0, false
	}
	message = strings.ToLower(message)

	unavailable := false
	for _, phrase := range unavailabilityPhrases {
		if strings.Contains(message, phrase) {
			unavailable = true
			break
		}
	}
	if !unavailable {
		return "", 0, false
	}
	for name := range infraRequireEnv {
		if strings.Contains(message, name) {
			return name, fileSet.Position(call.Pos()).Line, true
		}
	}
	return "", 0, false
}
