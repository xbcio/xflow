package script

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/wasm"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// This file exists because every existing reactor test (reactor_test.go,
// abi_test.go, warmup_test.go, config_loader_test.go, all in package wasm)
// hand-builds a globals map and calls engine.Execute directly — none of them
// go through ScriptNode.Execute -> buildScriptGlobals -> exprx.BuildExprEnv.
// That seam is exactly where the $config key collision lived:
// exprx.go sets env["$config"] = input.Config (the WORKFLOW config) while
// reactor.go's legacy path reads that same key as the RULE SET
// (reactorConfigGlobal = "$config"). The engine layer is well covered and the
// bug still shipped, because nothing exercised the node layer above it.
//
// It lives in package script (not package wasm) because wasm cannot import
// script (script already imports wasm via warmup.go — the reverse would be an
// import cycle), and driving ScriptNode.Execute requires the script package.

var (
	seamGuestWasm []byte
	seamBuildOnce sync.Once
)

// ensureSeamGuestBuilt compiles testdata/reactorseam/main.go once per test
// binary run. reactorseam was originally required here (rather than the
// package's existing reactor/reactormin fixtures) for a memory reason that no
// longer applies: driving a module through ScriptNode.Execute used to put the
// executing module's own multi-MB base64 string into $params.code, and both
// existing fixtures copy their whole eval input via encoding/json — which,
// added to that multi-MB value inside the guest's 16 MiB linear memory cap
// (engine.DefaultWasmMemoryPages), trapped on alloc before any assertion ran.
// script.go's paramsWithoutCode fixed that at the source, so this fixture is now
// only a smaller/faster guest, not a workaround; see its doc comment.
func ensureSeamGuestBuilt(t *testing.T) {
	t.Helper()
	seamBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "scriptseamtest")
		if err != nil {
			t.Fatalf("mkdir temp: %v", err)
		}
		out := filepath.Join(dir, "reactorseam.wasm")
		cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out,
			"./wasm/testdata/reactorseam/main.go")
		cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build reactorseam guest: %s", b)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("read reactorseam guest: %v", err)
		}
		seamGuestWasm = b
	})
	if seamGuestWasm == nil {
		t.Fatal("reactorseam guest build failed on an earlier subtest")
	}
}

// testSeamCode returns a base64-encoded reactorseam guest, made unique per
// call by appending a random-content WASM custom section (id 0). Custom
// sections are unconditionally skipped by any conformant loader (wazero
// included), so this changes the module's content hash — and therefore its
// identity in the process-wide sharedReactorHost's engine/codeCache maps —
// without altering guest behavior. Needed because RegisterWasmSupplyConsumer
// touches process-wide state that would otherwise leak between test runs.
func testSeamCode(t *testing.T) string {
	t.Helper()
	ensureSeamGuestBuilt(t)
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	return base64.StdEncoding.EncodeToString(appendSeamCustomSection(seamGuestWasm, "xflow-test-nonce", nonce))
}

func appendSeamCustomSection(mod []byte, name string, payload []byte) []byte {
	content := appendSeamULEB128(nil, uint64(len(name)))
	content = append(content, name...)
	content = append(content, payload...)

	out := append([]byte(nil), mod...)
	out = append(out, 0x00) // custom section id
	out = appendSeamULEB128(out, uint64(len(content)))
	out = append(out, content...)
	return out
}

func appendSeamULEB128(b []byte, v uint64) []byte {
	for {
		c := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			c |= 0x80
		}
		b = append(b, c)
		if v == 0 {
			return b
		}
	}
}

// registerWasmSupplyConsumerForTest registers code as a consumer of a supply
// whose name is unique to the calling test, and returns that name for the
// caller's Apply. RegisterWasmSupplyConsumer (the script package's forwarding
// function over wasm.RegisterSupplyConsumer) hardcodes supply.Default with no
// registry parameter — adding one just for this test would be a production-code
// change for no production benefit — so isolation has to come from the name.
//
// The name MUST be per-test: supply.Default is a process-wide singleton and
// UnregisterConsumer only drops the consumer, never the applied Snapshot, so a
// shared name leaves one test's content resident for the next. Two tests here
// previously both used "rules" with different revisions (11 and 3); under
// -count=2 the leftover revision 3 was what
// TestScriptNodeRulesComeFromSupplyNotConfig observed, failing with
// config_generation = 0x3.
func registerWasmSupplyConsumerForTest(t *testing.T, code string) string {
	t.Helper()
	// t.Name() is unique per test and stable within a run; sanitized because a
	// supply name stands in for a workflow node name.
	supplyNode := "rules-" + strings.NewReplacer("/", "-", " ", "_").Replace(t.Name())
	if err := RegisterWasmSupplyConsumer(code, supplyNode); err != nil {
		t.Fatalf("RegisterWasmSupplyConsumer: %v", err)
	}
	t.Cleanup(func() { wasm.UnregisterSupplyConsumer(code, supplyNode, supply.Default) })
	return supplyNode
}

// TestScriptNodeRulesComeFromSupplyNotConfig drives the real node path and
// asserts:
//  1. rules that actually drive the guest come from the supply channel —
//     proven both by config_generation echoing the SUPPLY's server revision
//     (11, not 0 which is the legacy-globals marker) and by "matched"
//     containing the supply-registered rule name;
//  2. putting rules in input.Data under the key "$config" does NOT make them
//     reachable as the rule set: exprx.BuildExprEnv merges input.Data into env
//     first and then unconditionally overwrites env["$config"] with
//     input.Config, so the decoy is discarded before reactor.go ever sees it
//     — its rule name must be absent from "matched".
//
// This is the regression the design's spec flags as highest priority: the
// $config key collision between exprx.go (workflow config) and reactor.go
// (rule set) was real and shipped past a well-covered engine layer, because
// nothing before this test crossed ScriptNode.Execute -> buildScriptGlobals.
func TestScriptNodeRulesComeFromSupplyNotConfig(t *testing.T) {
	code := testSeamCode(t)
	supplyNode := registerWasmSupplyConsumerForTest(t, code)

	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      supplyNode,
		Content:   []byte(`{"rules":[{"name":"from-supply"}]}`),
		Hash:      "h1",
		Revision:  11,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply supply: %v", err)
	}

	node := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"code":     code,
			"language": "wasm",
			"runtime":  "wazero-reactor",
		},
		// Workflow-level config. Irrelevant to the rule set; must not be
		// treated as one.
		Config: map[string]any{"env": "prod", "region": "cn-north"},
		// The decoy: an upstream node trying to smuggle rules into $config via
		// input.Data. exprx overwrites the key after merging Data, so this
		// must NOT become the rule set that drives the guest.
		Data: map[string]any{
			"$config": map[string]any{"rules": []any{map[string]any{"name": "from-data"}}},
			"payload": "x",
		},
	}

	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("ScriptNode.Execute: %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("guest errored: %#v", out.Data)
	}

	// (1) The generation proves which content configured the pool: 11 is the
	// supply revision. The legacy-globals path stamps 0; the decoy has no
	// revision concept at all, so any non-11 value here means rules did not
	// come from the supply channel.
	if got := out.Data[wasm.ConfigGenerationKey]; got != uint64(11) {
		t.Fatalf("%s = %#v, want 11 (the supply revision) — rules did not come from the supply channel", wasm.ConfigGenerationKey, got)
	}

	matchedNames := matchedRuleNames(t, out.Data)
	if !matchedNames["from-supply"] {
		t.Fatalf("matched = %v, want from-supply present (rules must come from the supply channel)", matchedNames)
	}
	if matchedNames["from-data"] {
		t.Fatalf("matched = %v, want from-data absent — the decoy from input.Data must never reach the rule engine", matchedNames)
	}
}

// TestLegacyPathDataDecoyMustNotOverrideWorkflowConfig is the legacy-globals
// counterpart of the assertion above: even with no supply consumer at all,
// putting a decoy rule set in input.Data["$config"] must never override the
// real rules the caller passed as input.Config (the workflow config).
// exprx.BuildExprEnv merges input.Data into env FIRST, then unconditionally
// overwrites env["$config"] = input.Config — so reversing that order (Data
// merged after $config is set) is exactly the class of regression this whole
// task guards, and this test fails immediately under that reversal (verified
// by hand while writing this test: swapping the two blocks in exprx.go made
// "decoy-legacy-rule" win and "real-legacy-rule" disappear from "matched").
func TestLegacyPathDataDecoyMustNotOverrideWorkflowConfig(t *testing.T) {
	code := testSeamCode(t) // no supply consumer registered: legacy globals path

	node := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"code":     code,
			"language": "wasm",
			"runtime":  "wazero-reactor",
		},
		Config: map[string]any{
			"rules": []any{map[string]any{"name": "real-legacy-rule"}},
		},
		Data: map[string]any{
			"$config": map[string]any{"rules": []any{map[string]any{"name": "decoy-legacy-rule"}}},
		},
	}

	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("ScriptNode.Execute: %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("guest errored: %#v", out.Data)
	}

	matchedNames := matchedRuleNames(t, out.Data)
	if !matchedNames["real-legacy-rule"] {
		t.Fatalf("matched = %v, want real-legacy-rule present — the workflow Config must win", matchedNames)
	}
	if matchedNames["decoy-legacy-rule"] {
		t.Fatalf("matched = %v, want decoy-legacy-rule absent — input.Data must never override $config", matchedNames)
	}
}

// matchedRuleNames extracts the reactorseam guest's {"matched":[...]} rule-name
// set from a ScriptNode.Execute result (after engine.MapResult flattened the
// guest's JSON object into out.Data).
func matchedRuleNames(t *testing.T, data map[string]any) map[string]bool {
	t.Helper()
	arr, ok := data["matched"].([]any)
	if !ok {
		t.Fatalf("data[\"matched\"] = %#v (%T), want []any", data["matched"], data["matched"])
	}
	set := make(map[string]bool, len(arr))
	for _, v := range arr {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("matched entry = %#v, want string", v)
		}
		set[s] = true
	}
	return set
}

// TestScriptNodeSourceDrivenGuestNeverSeesConfigKey locks down the corollary
// required by this task's brief addendum: once a module is source-driven, the
// eval input handed to the guest must NOT contain a "$config" key at all.
//
// This is deliberate production behavior, not a gap to fix: reactor.go's
// stripConfig removes $config from the eval input on the source-driven branch
// precisely BECAUSE "$config" is the rule-engine's key for "the rule set"
// (reactorConfigGlobal). Once rules come from the supply channel, re-
// introducing $config into eval input would put the workflow config in the
// same slot the engine's contract reserves for rules — the same class of
// collision this whole regression is about, just approached from the other
// direction. So the assertion here is the ABSENCE of "$config" from the raw
// bytes the guest evaluated.
func TestScriptNodeSourceDrivenGuestNeverSeesConfigKey(t *testing.T) {
	code := testSeamCode(t)
	supplyNode := registerWasmSupplyConsumerForTest(t, code)

	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      supplyNode,
		Content:   []byte(`{"rules":[]}`),
		Hash:      "h1",
		Revision:  3,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply supply: %v", err)
	}

	node := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"code":     code,
			"language": "wasm",
			"runtime":  "wazero-reactor",
		},
		Config: map[string]any{"env": "prod"},
		Data:   map[string]any{"payload": "x"},
	}

	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("ScriptNode.Execute: %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("guest errored: %#v", out.Data)
	}

	if got, _ := out.Data["hasConfigKey"].(bool); got {
		t.Fatalf(`guest eval input contained "$config" on the source-driven path; `+
			`$config is the rule-engine's key for the rule set and must be stripped `+
			`once rules come from the supply channel (data=%#v)`, out.Data)
	}
}

// TestScriptNodeWithoutSupplyStillRuns guards the legacy globals path: a
// workflow with no supply consumer registered for its module must still run,
// with rules supplied the legacy way (globals["$config"] via input.Config).
// This is the embedded/SDK path that predates supply resources; the supply
// work (Task 12-17) must not have broken it.
func TestScriptNodeWithoutSupplyStillRuns(t *testing.T) {
	code := testSeamCode(t) // fresh, never registered as a supply consumer

	node := &ScriptNode{}
	input := &types.Input{
		Params: map[string]any{
			"code":     code,
			"language": "wasm",
			"runtime":  "wazero-reactor",
		},
		// Legacy path: rules travel as the workflow config, read back out of
		// $config by reactor.go's globals-based branch.
		Config: map[string]any{
			"rules": []any{map[string]any{"name": "legacy"}},
		},
	}

	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("ScriptNode.Execute (legacy globals path): %v", err)
	}
	if out.Port == "error" {
		t.Fatalf("guest errored on legacy globals path: %#v", out.Data)
	}
	matchedNames := matchedRuleNames(t, out.Data)
	if !matchedNames["legacy"] {
		t.Fatalf("matched = %v, want legacy present — the legacy globals path must still configure the guest", matchedNames)
	}
	// The legacy globals path has no SupplyResource revision; annotateGeneration
	// stamps 0 for it (a real field value, not "absent").
	if got := out.Data[wasm.ConfigGenerationKey]; got != uint64(0) {
		t.Fatalf("%s = %#v, want 0 on the legacy globals path", wasm.ConfigGenerationKey, got)
	}
}
