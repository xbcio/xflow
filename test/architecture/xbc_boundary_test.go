package architecture

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// xbcModulePath is the dependency the optional infrastructure integration adds.
// It belongs to the composition root only: an embedder that links sdk/xflow must
// not inherit xbc's plugin framework, its configuration loader, or its
// transports.
const xbcModulePath = "github.com/xbcio/xbc"

// TestXBCImportsStayInTheCompositionRoot pins the integration boundary. cmd/ is
// the one place allowed to translate xbc values into xflow ones; every
// non-test source file outside it must stay xbc-free.
func TestXBCImportsStayInTheCompositionRoot(t *testing.T) {
	repoRoot := findRepositoryRoot(t)

	var violations []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}

		relativePath, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return fmt.Errorf("make %s relative to repository root: %w", path, err)
		}
		relativePath = filepath.ToSlash(relativePath)
		if relativePath == "cmd" || strings.HasPrefix(relativePath, "cmd/") {
			return nil
		}

		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("parse imports from %s: %w", relativePath, err)
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return fmt.Errorf("unquote import in %s: %w", relativePath, err)
			}
			if isXBCImport(importPath) {
				violations = append(violations, fmt.Sprintf(
					"%s:%d imports %s", relativePath, fileSet.Position(spec.Pos()).Line, importPath))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan production imports: %v", err)
	}

	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf(
			"xbc escaped the composition root:\n  %s\n"+
				"Keep xbc values and configuration in cmd/; expose what the rest of xflow needs "+
				"through narrow xflow-owned interfaces instead.",
			strings.Join(violations, "\n  "),
		)
	}
}

func isXBCImport(importPath string) bool {
	return importPath == xbcModulePath || strings.HasPrefix(importPath, xbcModulePath+"/")
}
