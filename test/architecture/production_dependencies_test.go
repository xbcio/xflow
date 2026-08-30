package architecture

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var productionSourceRoots = []string{
	"engine",
	"node",
	"types",
	"store",
	"execution",
	"observability",
}

type importRule string

const (
	processLayerRule  importRule = "PROCESS-LAYER"
	providerLayerRule importRule = "PROVIDER-LAYER"
	exporterLayerRule importRule = "EXPORTER-LAYER"
)

type importFinding struct {
	file       string
	sourceRoot string
	importPath string
	line       int
	rule       importRule
}

func TestProductionSourceRoots(t *testing.T) {
	const want = "engine,node,types,store,execution,observability"
	if got := strings.Join(productionSourceRoots, ","); got != want {
		t.Fatalf("production source roots = %q, want %q", got, want)
	}
}

func TestProductionImportsRespectLayerBoundaries(t *testing.T) {
	repoRoot := findRepositoryRoot(t)
	modulePath := readModulePath(t, repoRoot)

	var violations []importFinding
	for _, sourceRoot := range productionSourceRoots {
		root := filepath.Join(repoRoot, sourceRoot)
		if info, err := os.Stat(root); err != nil {
			t.Fatalf("inspect production source root %s: %v", sourceRoot, err)
		} else if !info.IsDir() {
			t.Fatalf("production source root %s is not a directory", sourceRoot)
		}

		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			fileSet := token.NewFileSet()
			parsed, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
			if err != nil {
				return fmt.Errorf("parse production imports from %s: %w", path, err)
			}

			relativePath, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return fmt.Errorf("make %s relative to repository root: %w", path, err)
			}
			relativePath = filepath.ToSlash(relativePath)

			for _, spec := range parsed.Imports {
				importPath, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return fmt.Errorf("unquote import in %s: %w", relativePath, err)
				}
				rule, forbidden := forbiddenProductionImport(sourceRoot, importPath, modulePath)
				if !forbidden {
					continue
				}
				violations = append(violations, importFinding{
					file:       relativePath,
					sourceRoot: sourceRoot,
					importPath: importPath,
					line:       fileSet.Position(spec.Pos()).Line,
					rule:       rule,
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s production imports: %v", sourceRoot, err)
		}
	}

	sortFindings(violations)
	if len(violations) == 0 {
		return
	}

	problems := make([]string, 0, len(violations))
	for _, finding := range violations {
		detail := fmt.Sprintf(
			"FORBIDDEN %s IMPORT: %s:%d (%s root) directly imports %s",
			finding.rule,
			finding.file,
			finding.line,
			finding.sourceRoot,
			finding.importPath,
		)
		if finding.rule == providerLayerRule {
			detail += "; engine and observability must not depend on backend/providers"
		}
		if finding.rule == exporterLayerRule {
			detail += "; engine must not depend on concrete metrics/logging exporters" +
				" (observability/metrics, observability/logger) - depend only on the" +
				" narrow observability/tracing facade, declare a narrow observer" +
				" interface in engine (see engine/observers.go's GroupObserver) and put" +
				" the concrete exporter adapter under observability/metrics (see" +
				" observability/metrics/group_observer.go's GroupObserverAdapter)"
		}
		problems = append(problems, detail)
	}
	t.Fatalf(
		"production package dependency boundaries were violated:\n  %s\n"+
			"Move shared contracts below process/provider/exporter implementations; no exceptions are allowlisted.",
		strings.Join(problems, "\n  "),
	)
}

func TestForbiddenProductionImportRules(t *testing.T) {
	const modulePath = "example.com/xflow"
	tests := []struct {
		name       string
		sourceRoot string
		importPath string
		wantRule   importRule
		forbidden  bool
	}{
		{name: "engine service", sourceRoot: "engine", importPath: modulePath + "/service/control", wantRule: processLayerRule, forbidden: true},
		{name: "node cmd", sourceRoot: "node", importPath: modulePath + "/cmd", wantRule: processLayerRule, forbidden: true},
		{name: "types service root", sourceRoot: "types", importPath: modulePath + "/service", wantRule: processLayerRule, forbidden: true},
		{name: "store cmd child", sourceRoot: "store", importPath: modulePath + "/cmd/server", wantRule: processLayerRule, forbidden: true},
		{name: "store service supply encryption", sourceRoot: "store", importPath: modulePath + "/service/crypto/supplyenc", wantRule: processLayerRule, forbidden: true},
		{name: "execution service", sourceRoot: "execution", importPath: modulePath + "/service/protocol", wantRule: processLayerRule, forbidden: true},
		{name: "observability cmd", sourceRoot: "observability", importPath: modulePath + "/cmd/server", wantRule: processLayerRule, forbidden: true},
		{name: "engine provider", sourceRoot: "engine", importPath: modulePath + "/backend/providers/distributed", wantRule: providerLayerRule, forbidden: true},
		{name: "observability provider root", sourceRoot: "observability", importPath: modulePath + "/backend/providers", wantRule: providerLayerRule, forbidden: true},
		{name: "engine backend contract", sourceRoot: "engine", importPath: modulePath + "/backend", forbidden: false},
		{name: "execution provider currently outside rule", sourceRoot: "execution", importPath: modulePath + "/backend/providers/local", forbidden: false},
		{name: "service name boundary", sourceRoot: "store", importPath: modulePath + "/servicekit", forbidden: false},
		{name: "provider name boundary", sourceRoot: "engine", importPath: modulePath + "/backend/providerset", forbidden: false},
		{name: "external lookalike", sourceRoot: "observability", importPath: "example.net/xflow/backend/providers/local", forbidden: false},
		{name: "engine exporter metrics", sourceRoot: "engine", importPath: modulePath + "/observability/metrics", wantRule: exporterLayerRule, forbidden: true},
		{name: "engine exporter metrics child", sourceRoot: "engine", importPath: modulePath + "/observability/metrics/internal/adapter", wantRule: exporterLayerRule, forbidden: true},
		{name: "engine exporter logger", sourceRoot: "engine", importPath: modulePath + "/observability/logger", wantRule: exporterLayerRule, forbidden: true},
		{name: "engine tracing facade allowed", sourceRoot: "engine", importPath: modulePath + "/observability/tracing", forbidden: false},
		{name: "engine observability root allowed", sourceRoot: "engine", importPath: modulePath + "/observability", forbidden: false},
		{name: "observability metrics self reference allowed", sourceRoot: "observability", importPath: modulePath + "/observability/metrics", forbidden: false},
		{name: "node exporter metrics allowed", sourceRoot: "node", importPath: modulePath + "/observability/metrics", forbidden: false},
		{name: "engine exporter metrics external lookalike", sourceRoot: "engine", importPath: "example.net/xflow/observability/metrics", forbidden: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRule, gotForbidden := forbiddenProductionImport(tt.sourceRoot, tt.importPath, modulePath)
			if gotForbidden != tt.forbidden || gotRule != tt.wantRule {
				t.Fatalf(
					"forbiddenProductionImport(%q, %q) = (%q, %t), want (%q, %t)",
					tt.sourceRoot,
					tt.importPath,
					gotRule,
					gotForbidden,
					tt.wantRule,
					tt.forbidden,
				)
			}
		})
	}
}

func forbiddenProductionImport(sourceRoot, importPath, modulePath string) (importRule, bool) {
	for _, processLayer := range []string{"service", "cmd"} {
		if importsPackageOrChild(importPath, modulePath+"/"+processLayer) {
			return processLayerRule, true
		}
	}

	if sourceRoot == "engine" || sourceRoot == "observability" {
		if importsPackageOrChild(importPath, modulePath+"/backend/providers") {
			return providerLayerRule, true
		}
	}

	if sourceRoot == "engine" {
		for _, exporterPackage := range []string{"observability/metrics", "observability/logger"} {
			if importsPackageOrChild(importPath, modulePath+"/"+exporterPackage) {
				return exporterLayerRule, true
			}
		}
	}
	return "", false
}

func importsPackageOrChild(importPath, packagePath string) bool {
	return importPath == packagePath || strings.HasPrefix(importPath, packagePath+"/")
}

func findRepositoryRoot(t *testing.T) string {
	t.Helper()

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate architecture test source file")
	}
	if !filepath.IsAbs(sourceFile) {
		absolute, err := filepath.Abs(sourceFile)
		if err != nil {
			t.Fatalf("resolve architecture test source path %s: %v", sourceFile, err)
		}
		sourceFile = absolute
	}

	for dir := filepath.Dir(sourceFile); ; dir = filepath.Dir(dir) {
		if info, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil && !info.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	t.Fatalf("find repository root from architecture test source %s", sourceFile)
	return ""
}

func readModulePath(t *testing.T, repoRoot string) string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "module" {
			continue
		}
		modulePath := fields[1]
		if unquoted, err := strconv.Unquote(modulePath); err == nil {
			modulePath = unquoted
		}
		if modulePath == "" {
			t.Fatal("go.mod contains an empty module path")
		}
		return modulePath
	}
	t.Fatal("go.mod does not contain a module directive")
	return ""
}

func sortFindings(findings []importFinding) {
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].file != findings[j].file {
			return findings[i].file < findings[j].file
		}
		if findings[i].line != findings[j].line {
			return findings[i].line < findings[j].line
		}
		if findings[i].importPath != findings[j].importPath {
			return findings[i].importPath < findings[j].importPath
		}
		return findings[i].rule < findings[j].rule
	})
}
