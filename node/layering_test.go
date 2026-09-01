package node_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNodeSubtreeDoesNotDependOnServiceOrCmd enforces the layering rule stated
// in AGENTS.md and docs/CODING-STANDARDS.md: lower-layer packages (engine,
// node, types, store, execution, observability) must never import service/ or
// cmd/.
//
// Before the trigger package split, node/internal/trigger/entry_seed_runtime.go
// imported service/protocol, which pulled service/protocol AND
// service/protocol/runnerpb (hence the whole protobuf runtime) into node's
// dependency graph for the sake of four DTOs. Nothing failed — the rule was
// documentation only. This test is what makes it enforceable.
//
// Scope note: `go list -deps` reports the PRODUCTION dependency graph; imports
// that appear only in _test.go files are excluded. That is deliberate and
// correct — the rule constrains what ships, not what tests reach for. Verified:
// backend/providers/local's test imports service/control, yet
// `go list -deps ./backend/providers/local/` contains zero xflow/service.
//
// This package-local guard checks node's complete transitive dependency graph
// and gives node contributors fast feedback. The repository-wide source-import
// rule for engine, node, types, store, execution, and observability is enforced
// separately by test/architecture/production_dependencies_test.go.
func TestNodeSubtreeDoesNotDependOnServiceOrCmd(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "./...").Output()
	if err != nil {
		if _, lookErr := exec.LookPath("go"); lookErr != nil {
			t.Skipf("go toolchain not available: %v", lookErr)
		}
		t.Fatalf("go list -deps ./...: %v", err)
	}

	const (
		servicePrefix = "github.com/xbcio/xflow/service"
		cmdPrefix     = "github.com/xbcio/xflow/cmd"
	)

	var violations []string
	for _, line := range strings.Split(string(out), "\n") {
		pkg := strings.TrimSpace(line)
		if pkg == "" {
			continue
		}
		if strings.HasPrefix(pkg, servicePrefix) || strings.HasPrefix(pkg, cmdPrefix) {
			violations = append(violations, pkg)
		}
	}

	if len(violations) > 0 {
		t.Fatalf("node subtree must not depend on service/ or cmd/, found %d:\n\t%s\n"+
			"See AGENTS.md and docs/CODING-STANDARDS.md. If a node package needs a "+
			"type from service/, the dependency is backwards: define an interface in "+
			"types/ and put the implementation on the service side.",
			len(violations), strings.Join(violations, "\n\t"))
	}
}
