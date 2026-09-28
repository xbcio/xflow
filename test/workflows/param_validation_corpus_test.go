package workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
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
var knownInvalidSamples = map[string][]string{
	// persist_orders is an xflow.function with no parameters at all. Its
	// handler answers that with the permanent error function.config_required
	// ("either function_name or code is required") on every execution, so
	// the sample as written cannot run.
	"kafka-batch-overflow.yaml": {
		"persist_orders /parameters/function_name one_of: at least one of function_name, code must be set, 0 are",
	},
}

// registerUnderEnforce validates def directly (to report every issue, not just
// the first failure) and then registers it through the real enforce-mode
// registration path. wantErrors, when non-nil, is the exact expected set of
// error issues for a known-invalid definition.
func registerUnderEnforce(t *testing.T, srv *apiserver.APIServer, def *types.WorkflowDef, wantErrors []string) {
	t.Helper()
	var skipped []string
	issues := execution.ValidateWorkflowParamsWithOptions(def, execution.RegistryDescriptorLookup,
		execution.ParamValidationOptions{OnUnknownType: func(nodeType string, _ int, node string) {
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
		return
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
			registerUnderEnforce(t, newEnforceAPIServer(t), jsonRoundTrip(t, stringKeys(doc)), knownInvalidSamples[name])
		})
	}
	for name := range knownInvalidSamples {
		if _, err := os.Stat(filepath.Join("..", "..", "docs", "dsl-samples", name)); err != nil {
			t.Errorf("knownInvalidSamples names %s, which no longer exists: %v", name, err)
		}
	}
}

func TestTierDefinitionsRegisterUnderEnforce(t *testing.T) {
	for _, d := range workflows.Definitions() {
		t.Run(d.Name, func(t *testing.T) {
			def, err := d.Build().Definition()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			// A fresh server per definition: tiers may share a name@version.
			registerUnderEnforce(t, newEnforceAPIServer(t), jsonRoundTrip(t, def), nil)
		})
	}
}
