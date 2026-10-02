package node_test

// Coverage index for the Default / fallback contract tests (descriptor-contract
// design §2.4). The contract tests live next to the code that applies each
// fallback -- the handler, engine expansion, or trigger Activate -- because
// that is the only place the effective value can be observed. This file is the
// one list that ties them together:
//
//   - every ParamSpec.Default on a builtin descriptor must name a contract test,
//     so a new Default cannot land without proof it matches the runtime;
//   - every BuiltinFallbacks entry must resolve to a declared ParamSpec that
//     has NO Default (promoting one to a Default moves SDK workflow hashes),
//     and its type must name the test that exercises it;
//   - every named test must exist in the named directory, so renaming or
//     deleting one breaks this test rather than silently dropping coverage.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
)

// contractCase names the test that proves a Default or fallback. dir is
// relative to this package's directory.
type contractCase struct {
	layer string // handler | engine | trigger
	dir   string
	test  string
}

// defaultContractCases is keyed exactly like TestBuiltinDefaultsGolden.
var defaultContractCases = map[string]contractCase{
	"xflow.approval@1/mode":                  {"handler", ".", "TestDefaultContractApprovalMode"},
	"xflow.approval@1/timeout_action":        {"handler", ".", "TestDefaultContractApprovalTimeoutAction"},
	"xflow.browser.cdp@1/timeout_ms":         {"handler", "internal/action", "TestDefaultContractBrowserTimeouts"},
	"xflow.browser.cdp@1/total_timeout_ms":   {"handler", "internal/action", "TestDefaultContractBrowserTimeouts"},
	"xflow.http@1/method":                    {"handler", ".", "TestDefaultContractHTTPMethod"},
	"xflow.map@1/batch_size":                 {"handler", ".", "TestDefaultContractMapBatchSize"},
	"xflow.map@1/body_concurrency":           {"engine", "../engine", "TestDefaultContractMapBodyConcurrency"},
	"xflow.map@1/continue_on_error":          {"engine", "../engine", "TestDefaultContractMapContinueOnError"},
	"xflow.merge@1/on_others":                {"handler", ".", "TestDefaultContractMergeOnOthers"},
	"xflow.supply.external@0/require_ready":  {"handler", ".", "TestDefaultContractSupplyRequireReady"},
	"xflow.supply.static@0/require_ready":    {"handler", ".", "TestDefaultContractSupplyRequireReady"},
	"xflow.trigger.cron@1/timezone":          {"trigger", "trigger/cron", "TestDefaultContractCronTimezone"},
	"xflow.trigger.kafka@1/max_inflight":     {"trigger", "trigger/kafka", "TestDefaultContractKafkaActivate"},
	"xflow.trigger.kafka@1/start_offset":     {"trigger", "trigger/kafka", "TestDefaultContractKafkaActivate"},
	"xflow.trigger.redis@1/max_inflight":     {"trigger", "trigger/redis", "TestDefaultContractRedisActivate"},
	"xflow.trigger.redis@1/mode":             {"trigger", "trigger/redis", "TestDefaultContractRedisActivate"},
	"xflow.trigger.webhook@1/max_body_bytes": {"trigger", "trigger/webhook", "TestDefaultContractWebhookMaxBodyBytes"},
	"xflow.wait@1/mode":                      {"handler", ".", "TestDefaultContractWaitMode"},
}

// fallbackContractCases is keyed by type@version; each named test runs one
// check per BuiltinFallbacks entry of that type and fails on a missing one.
var fallbackContractCases = map[string]contractCase{
	"xflow.browser.cdp@1":   {"handler", "internal/action", "TestFallbackContractBrowser"},
	"xflow.http@1":          {"handler", ".", "TestFallbackContractHTTP"},
	"xflow.merge@1":         {"handler", ".", "TestFallbackContractMerge"},
	"xflow.switch@1":        {"handler", ".", "TestFallbackContractSwitch"},
	"xflow.wait@1":          {"handler", ".", "TestFallbackContractWait"},
	"xflow.trigger.kafka@1": {"trigger", "trigger/kafka", "TestFallbackContractKafka"},
	"xflow.trigger.redis@1": {"trigger", "trigger/redis", "TestFallbackContractRedis"},
}

func TestDefaultContractCoverage(t *testing.T) {
	declared := map[string]bool{}
	for _, vd := range builtinDescriptors(t) {
		walkParams(vd.desc.Params, "", func(path string, spec types.ParamSpec, _ []types.ParamSpec) {
			if spec.Default != nil {
				declared[fmt.Sprintf("%s@%d/%s", vd.desc.Type, vd.version, path)] = true
			}
		})
	}
	for key := range declared {
		if _, ok := defaultContractCases[key]; !ok {
			t.Errorf("Default %s has no contract case: add a test proving the runtime fallback equals it "+
				"(absent vs Default vs JSON Default) and list it in defaultContractCases", key)
		}
	}
	for key := range defaultContractCases {
		if !declared[key] {
			t.Errorf("defaultContractCases lists %s, which is no longer a declared Default", key)
		}
	}
	for key, c := range defaultContractCases {
		requireTestFunc(t, key, c)
	}
}

func TestBuiltinFallbacksContract(t *testing.T) {
	descs := map[string]versionedDescriptor{}
	for _, vd := range builtinDescriptors(t) {
		descs[fmt.Sprintf("%s@%d", vd.desc.Type, vd.version)] = vd
	}

	seen := map[string]bool{}
	typeSet := map[string]bool{}
	for _, f := range nodeinternal.BuiltinFallbacks() {
		key := fmt.Sprintf("%s@%d/%s", f.Type, f.Version, f.Param)
		if seen[key] {
			t.Errorf("BuiltinFallbacks lists %s twice", key)
		}
		seen[key] = true
		typeSet[fmt.Sprintf("%s@%d", f.Type, f.Version)] = true

		if (f.Value == nil) == (f.Derived == "") {
			t.Errorf("%s: exactly one of Value and Derived must be set", key)
		}
		switch f.Value.(type) {
		case nil, string, float64, bool:
		default:
			t.Errorf("%s: Value %#v is %T; want a JSON-shaped string, float64 or bool", key, f.Value, f.Value)
		}

		vd, ok := descs[fmt.Sprintf("%s@%d", f.Type, f.Version)]
		if !ok {
			t.Errorf("%s: no builtin descriptor %s@%d", key, f.Type, f.Version)
			continue
		}
		spec, ok := findParam(vd.desc, f.Param)
		if !ok {
			t.Errorf("%s: not a declared ParamSpec; the node-types projection has nowhere to attach it", key)
			continue
		}
		if spec.Default != nil {
			t.Errorf("%s now has Default %#v; drop the BuiltinFallbacks entry (and see TestBuiltinDefaultsGolden)", key, spec.Default)
		}
	}

	for typ := range typeSet {
		if _, ok := fallbackContractCases[typ]; !ok {
			t.Errorf("BuiltinFallbacks has entries for %s but fallbackContractCases names no test for them", typ)
		}
	}
	for typ, c := range fallbackContractCases {
		if !typeSet[typ] {
			t.Errorf("fallbackContractCases lists %s, which has no BuiltinFallbacks entry", typ)
		}
		requireTestFunc(t, typ, c)
	}

	// The accessor hands out a copy.
	a := nodeinternal.BuiltinFallbacks()
	a[0].Param = "mutated"
	if nodeinternal.BuiltinFallbacks()[0].Param == "mutated" {
		t.Error("BuiltinFallbacks returned the backing slice")
	}
}

var testFuncCache = map[string]map[string]bool{}

// requireTestFunc fails unless c.dir declares func c.test in a _test.go file.
func requireTestFunc(t *testing.T, key string, c contractCase) {
	t.Helper()
	funcs, ok := testFuncCache[c.dir]
	if !ok {
		funcs = map[string]bool{}
		fset := token.NewFileSet()
		entries, err := os.ReadDir(c.dir)
		if err != nil {
			t.Fatalf("%s: read %s: %v", key, c.dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, filepath.Join(c.dir, e.Name()), nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse %s: %v", e.Name(), err)
			}
			for _, d := range file.Decls {
				if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil {
					funcs[fn.Name.Name] = true
				}
			}
		}
		testFuncCache[c.dir] = funcs
	}
	if !funcs[c.test] {
		t.Errorf("%s: contract test %s not found in %s", key, c.test, c.dir)
	}
}

// TestDefaultContractLayers keeps the layer tags honest against the design
// table: exactly the two map params are engine-layer, and only trigger params
// are trigger-layer.
func TestDefaultContractLayers(t *testing.T) {
	byLayer := map[string][]string{}
	for key, c := range defaultContractCases {
		byLayer[c.layer] = append(byLayer[c.layer], key)
	}
	for _, keys := range byLayer {
		sort.Strings(keys)
	}
	wantEngine := []string{"xflow.map@1/body_concurrency", "xflow.map@1/continue_on_error"}
	if fmt.Sprint(byLayer["engine"]) != fmt.Sprint(wantEngine) {
		t.Errorf("engine-layer cases = %v, want %v", byLayer["engine"], wantEngine)
	}
	for _, key := range byLayer["trigger"] {
		if !strings.HasPrefix(key, "xflow.trigger.") {
			t.Errorf("trigger-layer case %s is not a trigger param", key)
		}
	}
}
