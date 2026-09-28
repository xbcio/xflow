package architecture

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"
)

// builtinNodePackage is the package whose import graph self-registers every
// builtin action and trigger node type.
const builtinNodePackage = "github.com/xbcio/xflow/node"

// TestServerAssemblyImportsBuiltinNodesDirectly pins that each place a server
// process is assembled imports the builtin node package DIRECTLY. A transitive
// import would register the same types today, but only by accident of some
// other package's needs (the pre-A3 server saw the builtins that way), and it
// disappears silently when that package stops needing node. cmd/server's
// builtin_types_test.go checks the resulting type set; this checks that it is
// reached on purpose.
func TestServerAssemblyImportsBuiltinNodesDirectly(t *testing.T) {
	repoRoot := findRepositoryRoot(t)
	for _, rel := range []string{
		"cmd/server/main.go",   // the server binary
		"sdk/xflow/builtin.go", // package-level for sdk/xflow, covers NewServer
	} {
		path := filepath.Join(repoRoot, rel)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		found := false
		for _, imp := range file.Imports {
			ip, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: unquote import %s: %v", rel, imp.Path.Value, err)
			}
			if ip == builtinNodePackage {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s does not import %q directly; the server process must link the builtin node types explicitly", rel, builtinNodePackage)
		}
	}
}
