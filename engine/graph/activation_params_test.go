package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
)

// activationGraph compiles a workflow carrying $config/$vars, so the trigger's
// parameters have something to render against.
func activationGraph(t *testing.T) *Graph {
	t.Helper()
	g, err := Compile(&types.WorkflowDef{
		Name: "activation",
		Context: &types.WorkflowContext{
			Config: map[string]any{"topic": "sas-traffic", "workers": 4},
			Vars:   map[string]any{"env": "prod"},
		},
		Nodes: []types.NodeDef{
			{Name: "k", Type: "xflow.trigger.kafka", Version: 1, Kind: types.NodeKindTrigger},
			{Name: "sink", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"k": {"main": types.PortConnections{Targets: []types.Connection{{Node: "sink", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestEvaluateActivationParams_RejectsRootsThatDoNotExistYet is the reason this
// layer authors its own diagnostic instead of letting expr report the problem.
//
// Every case WARMS THE EXPRESSION CACHE FIRST, the way any earlier node
// execution in the same process would. That step is what gives the test its
// teeth: exprx.CompileExpr caches programs by (code, asBool) and ignores env on
// a hit, so on a COLD cache expr rejects every one of these itself with
// "unknown name $input" -- a message that happens to contain the root name and
// would satisfy the assertions below even with the guard deleted. Measured
// without the guard, once warm:
//
//	${{ $input.topic }}    -> "cannot fetch topic from <nil>"  (root name gone)
//	topic-{{ $input }}     -> "topic-<nil>", err == nil        (silent)
//	${{ $supplies }}       -> the control plane's own supplies  (silent)
//
// The last two are the silent wrong answers this layer exists to remove.
func TestEvaluateActivationParams_RejectsRootsThatDoNotExistYet(t *testing.T) {
	g := activationGraph(t)
	for _, tc := range []struct {
		name  string
		value string
		// warm is the expression source to compile under the full runtime env
		// before evaluating, reproducing a process that already ran a node.
		warm string
		root string
	}{
		{"expression form", "${{ $input.topic }}", "$input.topic", "$input"},
		{"interpolation form", "prefix-{{ $nodes.upstream.out }}", "$nodes", "$nodes"},
		// A bare root in interpolation form is the worst case: warm, and with
		// no guard, it renders "<nil>" into the parameter and returns no error
		// at all.
		{"bare root renders <nil>", "topic-{{ $execution }}", "$execution", "$execution"},
		// $supplies RESOLVES here -- BuildExprEnv reads a process global -- which
		// is precisely why it must be rejected: the value would be whatever the
		// control-plane process held, frozen into the activation params and
		// never refreshed when the supply changes.
		{"supplies root", "${{ $supplies }}", "$supplies", "$supplies"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtimeEnv := exprx.BuildExprEnv(&types.Input{}, map[string]any{
				"$nodes": map[string]any{"upstream": map[string]any{"out": "x"}},
			})
			if _, err := exprx.CompileExpr(tc.warm, runtimeEnv, false); err != nil {
				t.Fatalf("warming %q: %v", tc.warm, err)
			}

			_, err := EvaluateActivationParams(g, "k", "xflow.trigger.kafka",
				map[string]any{"topic": tc.value})
			if err == nil {
				t.Fatalf("EvaluateActivationParams(%q) = nil error, want a rejection -- "+
					"an unavailable root renders to <nil> or the literal template", tc.value)
			}
			if !strings.Contains(err.Error(), tc.root) {
				t.Errorf("error %q does not name the offending root %q", err, tc.root)
			}
			// §7: the error may carry the node name, parameter name, and the
			// authored expression source -- never a rendered value.
			if !strings.Contains(err.Error(), `parameter "topic"`) {
				t.Errorf("error %q does not name the parameter", err)
			}
		})
	}
}

// TestEvaluateActivationParams_DoesNotRejectALiteralDollar pins that the root
// scan looks only inside {{ }} segments. A trigger parameter holding a literal
// "$" outside a template (a shell-style default in a URL, a Kafka SASL config
// string) is not an expression and must pass through untouched.
func TestEvaluateActivationParams_DoesNotRejectALiteralDollar(t *testing.T) {
	g := activationGraph(t)
	out, err := EvaluateActivationParams(g, "k", "xflow.trigger.kafka", map[string]any{
		"sasl_password_ref": "vault://kv/$input/kafka",
		"pattern":           "topic-${suffix}",
	})
	if err != nil {
		t.Fatalf("EvaluateActivationParams: %v", err)
	}
	if got := out["sasl_password_ref"]; got != "vault://kv/$input/kafka" {
		t.Errorf("sasl_password_ref = %#v, want the literal unchanged", got)
	}
	if got := out["pattern"]; got != "topic-${suffix}" {
		t.Errorf("pattern = %#v, want the literal unchanged", got)
	}
}

// TestEvaluateActivationParams_HonoursHandlerEvaluatedParams pins the
// (nodeType, paramName) keying. xflow.trigger.cron has a parameter called
// "expression" holding a cron spec, and xflow.script's "code" is program text;
// a name-only exemption table would mangle one and a missing table would mangle
// both.
func TestEvaluateActivationParams_HonoursHandlerEvaluatedParams(t *testing.T) {
	g := activationGraph(t)

	// cron's "expression" is NOT in evaluableParams for that type, so it is
	// evaluated -- and must survive, because it contains no "{{".
	cron, err := EvaluateActivationParams(g, "c", "xflow.trigger.cron",
		map[string]any{"expression": "0 */5 * * *"})
	if err != nil {
		t.Fatalf("cron: %v", err)
	}
	if got := cron["expression"]; got != "0 */5 * * *" {
		t.Errorf("cron expression = %#v, want the cron spec unchanged", got)
	}

	// xflow.script's "code" IS exempt: it is host source text whose own syntax
	// may contain braces. Evaluating it would corrupt the program.
	const js = "const o = `${{a:1}.a}`; return o"
	script, err := EvaluateActivationParams(g, "s", "xflow.script",
		map[string]any{"code": js, "language": "js"})
	if err != nil {
		t.Fatalf("script: %v", err)
	}
	if got := script["code"]; got != js {
		t.Errorf("script code = %#v, want the source unchanged", got)
	}
}

// TestEvaluateActivationParams_RendersNestedValues pins that a template inside
// a list or map is rendered too -- a Kafka broker list built from $config is
// the shape SAS deploys.
func TestEvaluateActivationParams_RendersNestedValues(t *testing.T) {
	g := activationGraph(t)
	out, err := EvaluateActivationParams(g, "k", "xflow.trigger.kafka", map[string]any{
		// Interpolation form, not "${{ }}-1:9092": text around a "${{ }}" is
		// the mixed form validateTemplateForm rejects at compile time, so it
		// cannot reach this layer.
		"brokers": []any{"{{ $config.topic }}-1:9092", "static:9092"},
		"sasl":    map[string]any{"mechanism": "{{ $vars.env }}-scram"},
		"count":   4,
	})
	if err != nil {
		t.Fatalf("EvaluateActivationParams: %v", err)
	}
	brokers := out["brokers"].([]any)
	if brokers[0] != "sas-traffic-1:9092" || brokers[1] != "static:9092" {
		t.Errorf("brokers = %#v, want the first rendered and the second unchanged", brokers)
	}
	if got := out["sasl"].(map[string]any)["mechanism"]; got != "prod-scram" {
		t.Errorf("sasl.mechanism = %#v, want %q", got, "prod-scram")
	}
	if got := out["count"]; got != 4 {
		t.Errorf("count = %#v, want the non-string leaf unchanged", got)
	}
}

// TestEvaluateActivationParams_LeavesTheSourceMapAlone pins that the input map
// is never written through. The caller passes a map read off the shared
// process-wide *Graph on one path and off the raw WorkflowDef on another;
// rendering in place would burn the value in after the first activation, so a
// later $config change could never re-render.
func TestEvaluateActivationParams_LeavesTheSourceMapAlone(t *testing.T) {
	g := activationGraph(t)
	src := map[string]any{"topic": "${{ $config.topic }}"}
	out, err := EvaluateActivationParams(g, "k", "xflow.trigger.kafka", src)
	if err != nil {
		t.Fatalf("EvaluateActivationParams: %v", err)
	}
	if got := src["topic"]; got != "${{ $config.topic }}" {
		t.Errorf("source map topic = %#v, want the template unchanged", got)
	}
	if got := out["topic"]; got != "sas-traffic" {
		t.Errorf("result topic = %#v, want %q", got, "sas-traffic")
	}
}

// TestProjectGroupPackage_LeavesNonEntryMembersAlone pins the other half of the
// group split. Every member except the entry IS executed as a task by the inner
// engine, so its parameters must reach execution/params.go as source text --
// rendering them here would double-evaluate, and a member reading $input would
// abort the projection and, since assignPackageHashes runs inside Compile, the
// whole workflow compile.
func TestProjectGroupPackage_LeavesNonEntryMembersAlone(t *testing.T) {
	g, err := Compile(&types.WorkflowDef{
		Name:    "grp",
		Context: &types.WorkflowContext{Config: map[string]any{"topic": "sas-traffic"}},
		Nodes: []types.NodeDef{
			{
				Name: "k", Type: "xflow.trigger.kafka", Version: 1, Kind: types.NodeKindTrigger,
				Parameters: map[string]any{"topic": "${{ $config.topic }}"},
			},
			{
				Name: "tagger", Type: "xflow.http", Version: 1, Kind: types.NodeKindAction,
				Parameters: map[string]any{"url": "https://api/{{ $input.id }}"},
			},
		},
		Groups: []types.GroupDef{{
			Name:           "g",
			Members:        []string{"k", "tagger"},
			RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired},
		}},
		Connections: types.Connections{
			"k": {"main": types.PortConnections{Targets: []types.Connection{{Node: "tagger", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	pkg, _, err := ProjectGroupPackage(g, 0)
	if err != nil {
		t.Fatalf("ProjectGroupPackage: %v", err)
	}
	for _, n := range pkg.Def.Nodes {
		switch n.Name {
		case "k":
			if got := n.Parameters["topic"]; got != "sas-traffic" {
				t.Errorf("entry topic = %#v, want %q", got, "sas-traffic")
			}
		case "tagger":
			if got := n.Parameters["url"]; got != "https://api/{{ $input.id }}" {
				t.Errorf("member url = %#v, want the template unrendered -- "+
					"execution/params.go renders it against a real $input", got)
			}
		}
	}
}
