package xflow

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// canonicalCorpus builds one workflow per builtin node type, through the
// public builders, the way an SDK user would. Required params are set; every
// optional param is left to the builder, so the SDK writes whatever Defaults
// it writes.
func canonicalCorpus() map[string]func(*WorkflowBuilder) {
	bodyOf := func(name string) *WorkflowBuilder {
		body := Workflow(name)
		body.Node("wait", node.Wait("go"))
		body.Node("call", node.HTTP("", "https://example.invalid"))
		body.Node("approve", node.Approval([]string{"u1"}, ""))
		return body
	}
	return map[string]func(*WorkflowBuilder){
		"xflow.http":                 func(w *WorkflowBuilder) { w.Node("n", node.HTTP("", "https://example.invalid")) },
		"xflow.http explicit method": func(w *WorkflowBuilder) { w.Node("n", node.HTTP(string(node.HTTPPost), "https://example.invalid")) },
		"xflow.database":             func(w *WorkflowBuilder) { w.Node("n", node.Database("select", "t", "cred")) },
		"xflow.grpc":                 func(w *WorkflowBuilder) { w.Node("n", node.GRPC("svc", "M", "h:1")) },
		"xflow.browser.cdp": func(w *WorkflowBuilder) {
			w.Node("n", node.BrowserCDP(map[string]any{"debugging_url": "ws://x", "entry_url": "https://x"}))
		},
		"xflow.function":               func(w *WorkflowBuilder) { w.Node("n", node.Expr("1 + 1")) },
		"xflow.script":                 func(w *WorkflowBuilder) { w.Node("n", node.Script("return 1").Language("js").Runtime("goja")) },
		"xflow.transform.set":          func(w *WorkflowBuilder) { w.Node("n", node.Set(map[string]any{"a": 1})) },
		"xflow.transform.pick":         func(w *WorkflowBuilder) { w.Node("n", node.Pick("a")) },
		"xflow.transform.rename":       func(w *WorkflowBuilder) { w.Node("n", node.Rename(map[string]string{"a": "b"})) },
		"xflow.transform.filter":       func(w *WorkflowBuilder) { w.Node("n", node.Filter("{{ $input.items }}", "true")) },
		"xflow.transform.sort":         func(w *WorkflowBuilder) { w.Node("n", node.Sort("{{ $input.items }}", node.SortAsc("a"))) },
		"xflow.transform.limit":        func(w *WorkflowBuilder) { w.Node("n", node.Limit("{{ $input.items }}", 3)) },
		"xflow.transform.remove_dupes": func(w *WorkflowBuilder) { w.Node("n", node.RemoveDuplicates("{{ $input.items }}", "a")) },
		"xflow.transform.aggregate":    func(w *WorkflowBuilder) { w.Node("n", node.Aggregate("{{ $input.items }}").Count("n")) },
		"xflow.start":                  func(w *WorkflowBuilder) { w.Node("n", node.Start()) },
		"xflow.end":                    func(w *WorkflowBuilder) { w.Node("n", node.End()) },
		"xflow.if":                     func(w *WorkflowBuilder) { w.Node("n", node.IF("true")) },
		"xflow.switch rules": func(w *WorkflowBuilder) {
			w.Node("n", node.Switch([]node.SwitchRule{{Condition: "true", Output: "a"}}, "b"))
		},
		"xflow.switch expression":         func(w *WorkflowBuilder) { w.Node("n", node.SwitchExpr("'a'", "b")) },
		"xflow.merge wait_all":            func(w *WorkflowBuilder) { w.Node("n", node.Merge(node.MergeWaitAll)) },
		"xflow.merge wait_any":            func(w *WorkflowBuilder) { w.Node("n", node.Merge(node.MergeWaitAny)) },
		"xflow.map without body":          func(w *WorkflowBuilder) { w.Node("n", node.Map("{{ $input.items }}", 0)) },
		"xflow.map with body":             func(w *WorkflowBuilder) { w.Node("n", node.Map("{{ $input.items }}", 2)).Body(bodyOf("body")) },
		"xflow.map with nested body":      nestedMap(bodyOf),
		"xflow.wait signal":               func(w *WorkflowBuilder) { w.Node("n", node.Wait("go")) },
		"xflow.wait timer":                func(w *WorkflowBuilder) { w.Node("n", node.WaitDuration("1m")) },
		"xflow.approval":                  func(w *WorkflowBuilder) { w.Node("n", node.Approval([]string{"u1"}, node.ApprovalAll)) },
		"xflow.approval empty mode":       func(w *WorkflowBuilder) { w.Node("n", node.Approval([]string{"u1"}, "")) },
		"xflow.notification":              func(w *WorkflowBuilder) { w.Node("n", node.Notification("email", "a@example.invalid")) },
		"xflow.trigger.timer":             func(w *WorkflowBuilder) { w.Node("n", trigger.Timer().Every(time.Minute)) },
		"xflow.trigger.cron":              func(w *WorkflowBuilder) { w.Node("n", trigger.Cron().Cron("@every 1m")) },
		"xflow.trigger.webhook":           func(w *WorkflowBuilder) { w.Node("n", trigger.Webhook().Method("POST").Path("/hook")) },
		"xflow.trigger.kafka":             func(w *WorkflowBuilder) { w.Node("n", trigger.Kafka().Brokers("b:9092").Topic("t").Group("g")) },
		"xflow.trigger.redis":             func(w *WorkflowBuilder) { w.Node("n", trigger.Redis().Addr("r:6379").Stream("s").Group("g")) },
		"xflow.supply.static":             func(w *WorkflowBuilder) { w.Node("n", node.SupplyStatic([]byte("x"))) },
		"xflow.supply.external":           func(w *WorkflowBuilder) { w.Node("n", node.SupplyExternal("res")) },
		"local node and builtin together": func(w *WorkflowBuilder) { w.LocalNode("local", nil); w.Node("n", node.Wait("go")) },
	}
}

func nestedMap(bodyOf func(string) *WorkflowBuilder) func(*WorkflowBuilder) {
	return func(w *WorkflowBuilder) {
		outer := Workflow("outer-body")
		outer.Node("inner", node.Map("{{ $input.items }}", 0)).Body(bodyOf("inner-body"))
		outer.Node("wait", node.Wait("go"))
		w.Node("n", node.Map("{{ $input.items }}", 0)).Body(outer)
	}
}

// TestCanonicalIsIdentityOnSDKBuilds guards G2 of the cross-path hash design:
// the SDK builder already writes every builtin Default canonicalization would
// fill, so canonicalizing an SDK build changes nothing and the runtime hash of
// every SDK workflow stays byte-identical. It also checks the stored form: the
// same definition after a JSON round trip (as a registry stores it).
func TestCanonicalIsIdentityOnSDKBuilds(t *testing.T) {
	covered := map[string]bool{}
	for name, add := range canonicalCorpus() {
		t.Run(name, func(t *testing.T) {
			wf := Workflow("canonical-identity")
			add(wf)
			def, err := wf.build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			collectNodeTypes(def.Nodes, covered)

			if got := workflowhash.Canonical(def, node.BuiltinParamSpecs); got != def {
				t.Fatalf("Canonical(%s) rewrote an SDK build:\n got %s\nwant %s", name, mustJSON(t, got), mustJSON(t, def))
			}
			raw, err := workflowhash.Runtime(def, nil)
			if err != nil {
				t.Fatalf("Runtime(raw): %v", err)
			}
			canonical, err := workflowhash.Runtime(def, node.BuiltinParamSpecs)
			if err != nil {
				t.Fatalf("Runtime(canonical): %v", err)
			}
			if canonical != raw {
				t.Fatalf("canonical hash %q != SDK hash %q for %s", canonical, raw, name)
			}

			var stored types.WorkflowDef
			if err := json.Unmarshal([]byte(mustJSON(t, def)), &stored); err != nil {
				t.Fatalf("decode stored form: %v", err)
			}
			if got := workflowhash.Canonical(&stored, node.BuiltinParamSpecs); got != &stored {
				t.Fatalf("Canonical(%s) rewrote the stored form:\n got %s\nwant %s", name, mustJSON(t, got), mustJSON(t, &stored))
			}
			storedHash, err := workflowhash.Runtime(&stored, node.BuiltinParamSpecs)
			if err != nil {
				t.Fatalf("Runtime(stored): %v", err)
			}
			if storedHash != raw {
				t.Fatalf("stored-form hash %q != SDK hash %q for %s", storedHash, raw, name)
			}
		})
	}

	var missing []string
	for _, typ := range builtinNodeTypes() {
		if !covered[typ] {
			missing = append(missing, typ)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("corpus misses builtin types %v; add a builder for each", missing)
	}

	// Control: the identity above is not vacuous. The same SDK build with a
	// written Default removed -- the form an HTTP author sends -- is filled
	// back to the SDK build and hashes like it.
	t.Run("stripped SDK build is filled back", func(t *testing.T) {
		wf := Workflow("canonical-identity")
		wf.Node("n", node.Wait("go"))
		def, err := wf.build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		stripped := *def
		stripped.Nodes = append([]types.NodeDef(nil), def.Nodes...)
		stripped.Nodes[0].Parameters = map[string]any{"signal_name": "go"}
		if def.Nodes[0].Parameters["mode"] != string(node.WaitModeSignal) {
			t.Fatalf("SDK build params = %v, want the mode Default written", def.Nodes[0].Parameters)
		}
		want := mustRuntimeHash(t, def)
		got, err := workflowhash.Runtime(&stripped, node.BuiltinParamSpecs)
		if err != nil {
			t.Fatalf("Runtime(stripped): %v", err)
		}
		if got != want {
			t.Fatalf("stripped hash %q != SDK hash %q", got, want)
		}
	})
}

// collectNodeTypes records every node type in nodes, descending into
// sub-graph bodies.
func collectNodeTypes(nodes []types.NodeDef, into map[string]bool) {
	for _, n := range nodes {
		into[n.Type] = true
		if members, ok := graph.SubgraphBodyMembers(n.Parameters); ok {
			collectNodeTypes(members, into)
		}
	}
}

// builtinNodeTypes lists every registered "xflow." action and trigger type
// plus the two supply declarations, which have no registered handler.
func builtinNodeTypes() []string {
	seen := map[string]bool{
		node.SupplyStatic(nil).NodeType():  true,
		node.SupplyExternal("").NodeType(): true,
	}
	for _, typ := range append(registry.Types(), registry.TriggerTypes()...) {
		if strings.HasPrefix(typ, "xflow.") {
			seen[typ] = true
		}
	}
	out := make([]string, 0, len(seen))
	for typ := range seen {
		out = append(out, typ)
	}
	sort.Strings(out)
	return out
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}
