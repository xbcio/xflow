package workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/namespace"
	_ "github.com/xbcio/xflow/node" // builtin node and trigger types
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
)

// The acceptance corpus for switching ParamSpec validation to enforce: every
// DSL sample and every tier definition must register with zero error-severity
// param issues. A failure here means either a sample is genuinely invalid or a
// Descriptor is stricter than its handler -- fix whichever is wrong; do not
// weaken the rule to make the corpus pass.

func newEnforceAPIServer(t *testing.T) *apiserver.APIServer {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{Concurrency: 1, ParamValidation: types.ParamValidationEnforce}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv
}

func errorIssues(issues []graph.ParamIssue) []string {
	var out []string
	for _, is := range issues {
		if is.Severity == graph.ParamIssueError {
			out = append(out, is.Node+" "+is.Path+" "+is.Code+": "+is.Message)
		}
	}
	sort.Strings(out)
	return out
}

// jsonRoundTrip normalizes a value to what the HTTP path decodes: JSON types.
func jsonRoundTrip(t *testing.T, v any) *types.WorkflowDef {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var def types.WorkflowDef
	if err := json.Unmarshal(data, &def); err != nil {
		t.Fatalf("decode WorkflowDef: %v", err)
	}
	return &def
}

// stringKeys converts YAML mapping keys to strings. yaml.v3 decodes an
// unquoted true/false key (an xflow.if port name in connections) as a bool,
// which JSON cannot carry; the DSL means the port name.
func stringKeys(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			x[k] = stringKeys(child)
		}
		return x
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, child := range x {
			out[fmt.Sprint(k)] = stringKeys(child)
		}
		return out
	case []any:
		for i, child := range x {
			x[i] = stringKeys(child)
		}
		return x
	}
	return v
}

// knownInvalidSamples lists DSL samples that legitimately fail enforce, with
// the exact error issues they produce. They are real defects in the sample,
// not validator over-reach: each is asserted to fail exactly this way, so
// fixing the sample turns this test red until the entry is removed.
var knownInvalidSamples = map[string][]string{}

// knownUnrunnableSamples names samples that declare nodes WITHOUT a type and so
// can never execute: the compiler accepts an empty type (an unregistered custom
// type is a legitimate shape), registration succeeds, and the failure only
// appears when the node is first dispatched. The entry is a written
// acknowledgement, not a silencer -- a sample that gains an untyped node without
// appearing here fails the corpus, and an entry whose sample turns out to be
// fully typed fails too.
var knownUnrunnableSamples = map[string]string{
	"purchase-approval.yaml": "12 approval nodes carry only `template:`, and node_templates expansion is not implemented (see the file header)",
}

// registerUnderEnforce validates def directly (to report every issue, not just
// the first failure) and then registers it through the real enforce-mode
// registration path. wantErrors, when non-nil, is the exact expected set of
// error issues for a known-invalid definition. untypedAllowance is the reason
// this definition may declare untyped nodes ("" means it may not); the untyped
// node names are returned so a caller that grants the allowance can check it is
// actually needed.
func registerUnderEnforce(t *testing.T, srv *apiserver.APIServer, def *types.WorkflowDef, wantErrors []string, untypedAllowance string) []string {
	t.Helper()
	var skipped []string
	var untyped []string
	issues := execution.ValidateWorkflowParamsWithOptions(def, execution.RegistryDescriptorLookup,
		execution.ParamValidationOptions{OnUnknownType: func(nodeType string, _ int, node string) {
			// An empty type is not "unknown in this process": no type was
			// declared at all, and no process would register it.
			if nodeType == "" {
				untyped = append(untyped, node)
				return
			}
			skipped = append(skipped, node+" ("+nodeType+")")
		}})
	errs := errorIssues(issues)
	if wantErrors != nil {
		if strings.Join(errs, "\n") != strings.Join(wantErrors, "\n") {
			t.Fatalf("known-invalid definition: error issues\n  %s\nwant\n  %s",
				strings.Join(errs, "\n  "), strings.Join(wantErrors, "\n  "))
		}
		t.Logf("known invalid (see knownInvalidSamples): %s", strings.Join(errs, "; "))
		var paramErr *execution.ParamIssuesError
		if _, err := srv.RegisterWorkflowReport(context.Background(), namespace.Default, def); !errors.As(err, &paramErr) {
			t.Fatalf("known-invalid definition registered under enforce: err=%v", err)
		}
		return untyped
	}
	if len(errs) > 0 {
		t.Errorf("error-severity param issues:\n  %s", strings.Join(errs, "\n  "))
	}
	for _, is := range issues {
		if is.Severity != graph.ParamIssueError {
			t.Logf("warning: %s %s %s: %s", is.Node, is.Path, is.Code, is.Message)
		}
	}
	if len(skipped) > 0 {
		t.Logf("skipped (type not registered in this process): %s", strings.Join(skipped, ", "))
	}
	switch {
	case len(untyped) == 0 && untypedAllowance == "":
	case len(untyped) == 0:
		t.Errorf("untypedAllowance is set but every node declares a type; remove the allowance: %s", untypedAllowance)
	case untypedAllowance == "":
		t.Errorf("nodes with no type can register but never run (%s); give them a type or record the sample in knownUnrunnableSamples",
			strings.Join(untyped, ", "))
	default:
		t.Logf("known unrunnable (%s): %s", untypedAllowance, strings.Join(untyped, ", "))
	}

	res, err := srv.RegisterWorkflowReport(context.Background(), namespace.Default, def)
	var paramErr *execution.ParamIssuesError
	switch {
	case errors.As(err, &paramErr):
		t.Errorf("enforce registration rejected: %v", err)
	case err != nil:
		t.Errorf("registration failed for a non-param reason: %v", err)
	case res.ID == "":
		t.Error("registration returned no id")
	}
	return untyped
}

func TestDSLSamplesRegisterUnderEnforce(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "docs", "dsl-samples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no DSL samples found")
	}
	for _, path := range paths {
		name := filepath.Base(path)
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("parse YAML: %v", err)
			}
			registerUnderEnforce(t, newEnforceAPIServer(t), jsonRoundTrip(t, stringKeys(doc)), knownInvalidSamples[name], knownUnrunnableSamples[name])
		})
	}
	for name := range knownInvalidSamples {
		if _, err := os.Stat(filepath.Join("..", "..", "docs", "dsl-samples", name)); err != nil {
			t.Errorf("knownInvalidSamples names %s, which no longer exists: %v", name, err)
		}
	}
	for name := range knownUnrunnableSamples {
		if _, err := os.Stat(filepath.Join("..", "..", "docs", "dsl-samples", name)); err != nil {
			t.Errorf("knownUnrunnableSamples names %s, which no longer exists: %v", name, err)
		}
	}
}

// TestDSLSampleExpressionsCompile closes a gap the registration corpus leaves
// open: a `${{ ... }}` template is compiled when its node first evaluates a
// parameter, so a sample can register cleanly and still fail on its first run.
// `toJson` where the expression language spells the builtin `toJSON`, and
// `?? null` where its null literal is `nil`, are both invisible to every other
// check here -- and both sat in docs/dsl-samples/purchase-approval.yaml until
// this test existed.
func TestDSLSampleExpressionsCompile(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "docs", "dsl-samples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no DSL samples found")
	}
	total := 0
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			templates := templateExpressions(string(raw))
			total += len(templates)
			for _, expr := range templates {
				// Compiled against a runtime-shaped env, so a name the language
				// does not know is rejected here rather than on first
				// evaluation.
				if err := exprx.CheckExprSyntaxInEnv(expr, sampleTemplateEnv()); err != nil {
					t.Errorf("${{ %s }}: %v", expr, err)
				}
			}
		})
	}
	// A corpus that extracted nothing would pass every subtest above: the
	// extractor, not the samples, would be the thing under test.
	if total == 0 {
		t.Fatal("no ${{ }} templates found in any sample; the extractor is broken")
	}
	t.Logf("checked %d templates across %d samples", total, len(paths))
}

// sampleTemplateEnv is the runtime expression environment with every root
// present but empty. Its SHAPE is what the check needs -- the roots are
// map[string]any at runtime too -- so an expression that names a root or a
// function that does not exist fails to compile.
func sampleTemplateEnv() map[string]any {
	return map[string]any{
		"$input":     map[string]any{},
		"$inputs":    map[string]any{},
		"$nodes":     map[string]any{},
		"$params":    map[string]any{},
		"$vars":      map[string]any{},
		"$config":    map[string]any{},
		"$runtime":   map[string]any{},
		"$supplies":  map[string]any{},
		"$execution": map[string]any{},
		"$workflow":  map[string]any{},
	}
}

// templateExprPattern matches the ${{ ... }} form. The non-greedy body stops at
// the first `}}`; an expression containing a `}}` (a nested map literal) would
// be cut short, which is why the check reports the fragment it compiled.
var templateExprPattern = regexp.MustCompile(`\$\{\{(.*?)\}\}`)

func templateExpressions(doc string) []string {
	matches := templateExprPattern.FindAllStringSubmatch(doc, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, strings.TrimSpace(m[1]))
	}
	return out
}

func TestTierDefinitionsRegisterUnderEnforce(t *testing.T) {
	for _, d := range workflows.Definitions() {
		t.Run(d.Name, func(t *testing.T) {
			def, err := d.Build().Definition()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			// A fresh server per definition: tiers may share a name@version.
			registerUnderEnforce(t, newEnforceAPIServer(t), jsonRoundTrip(t, def), nil, "")
		})
	}
}
