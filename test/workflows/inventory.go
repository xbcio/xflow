// Package workflows holds the tiered end-to-end workflow definitions that the
// functional-coverage and load suites drive through a real engine. It exists so
// the same definitions that prove node coverage are the ones carrying traffic in
// the stress suite: a definition verified once and then thrown away proves
// nothing about the run that follows.
//
// The definitions are pure builders. Anything they cannot know at build time —
// a test server URL, a MySQL DSN, a gRPC endpoint — arrives as a runtime
// variable ($vars.*) supplied by the caller at Invoke time, so the same builder
// works against a local httptest server and a deployed one.
//
// No build tag: the definitions are ordinary code, and the suites that run them
// carry the tags.
package workflows

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// BodyParam is the parameter under which a node carries its xflow.subgraph
// body. sdk/xflow writes it from WorkflowBuilder.Body and the compiler reads it
// as a nested node definition.
const BodyParam = "body"

// SubgraphNodeType is the body-only node type. A body is not authored directly:
// it is the shape WorkflowBuilder.Body compiles a nested builder into.
const SubgraphNodeType = types.SubgraphNodeType

// DeprecatedNodeTypes are registered action types that no workflow may contain.
// xflow.split is the sole entry: graph.Compile rejects any definition carrying
// it, because split expands into batch tasks and batches need a projected body
// that split has no parameter for. Before that rejection existed such a workflow
// did not fail — it hung until its deadline.
//
// The coverage suite subtracts these from the registry's type set, so the reason
// is recorded once here instead of being open-coded into the assertion.
var DeprecatedNodeTypes = []string{"xflow.split"}

// DeclarationOnlyNodeTypes are node types a definition may carry that the
// handler registry never lists. Both supply kinds are declarations: neither
// registers a handler and the engine skips both at execution, so they are absent
// from registry.Types() by design rather than by omission.
//
// The coverage suite subtracts these before checking that every type a
// definition names is registered, so the exception is stated once here instead
// of being open-coded as an allowlist in the assertion.
var DeclarationOnlyNodeTypes = []string{
	"xflow.supply.external",
	"xflow.supply.static",
}

// ArmThreshold is the amount the low and medium tiers branch on. The branch
// conditions and the two inputs that select an arm all derive from it, because
// restating it as literals let the threshold move without the inputs moving
// with it: a threshold raised past 500 would leave the "bulk" input taking the
// single arm, and the arm tests would still pass while exercising the other
// branch.
const ArmThreshold = 100.0

// BranchCondition is the expression both tiers branch on, so the threshold is
// written once and read by the compiler rather than restated per tier.
func BranchCondition() string { return fmt.Sprintf("$input.amount >= %v", ArmThreshold) }

// AboveThresholdInput selects the branch BranchCondition routes to when true.
func AboveThresholdInput() map[string]any { return map[string]any{"amount": ArmThreshold * 5} }

// BelowThresholdInput selects the other branch.
func BelowThresholdInput() map[string]any { return map[string]any{"amount": ArmThreshold / 10} }

// switchWithPorts wraps xflow.switch to supply the one parameter the node cannot
// infer: the list of output ports its rules route to.
//
// The descriptor marks both `mode` and `outputs` required, and RawParams emits
// `mode` but not `outputs`, so a switch built with node.Switch fails the
// builder's own required-parameter check before the graph is ever compiled. The
// node does not need the list to route — it needs it so the compiler can bind an
// edge to each port name the rules mention, which is why the caller must state
// them here.
type switchWithPorts struct {
	*node.SwitchNode
	ports []string
}

func (b switchWithPorts) RawParams() any {
	params, ok := b.SwitchNode.RawParams().(map[string]any)
	if !ok {
		return b.SwitchNode.RawParams()
	}
	outputs := make([]any, len(b.ports))
	for i, p := range b.ports {
		outputs[i] = p
	}
	params["outputs"] = outputs
	return params
}

// Switch builds an xflow.switch whose rules route to the given ports. ports must
// name every non-default output the rules reference.
func Switch(rules []node.SwitchRule, defaultOutput string, ports ...string) switchWithPorts {
	return switchWithPorts{SwitchNode: node.Switch(rules, defaultOutput), ports: ports}
}

// Definition names one tier definition.
type Definition struct {
	Name  string
	Build func() *xflow.WorkflowBuilder
}

// Definitions returns every definition this package publishes, so the coverage
// suite measures one list rather than a hand-copied subset.
//
// The cron tier is expressed as "@every 1s" rather than a five-field schedule:
// the node's parser carries the Descriptor arm, and a five-field schedule fires
// at minute granularity, which is longer than the suite should wait to observe a
// real event.
func Definitions() []Definition {
	return []Definition{
		{Name: "low", Build: LowWorkflow},
		{Name: "medium", Build: MediumWorkflow},
		{Name: "high", Build: HighWorkflow},
		{Name: "error-port", Build: func() *xflow.WorkflowBuilder { return ErrorPortWorkflow(MissingFunction()) }},
		{Name: "browser", Build: BrowserCDPWorkflow},
		{Name: "trigger-timer", Build: func() *xflow.WorkflowBuilder { return TimerTriggerWorkflow(100 * time.Millisecond) }},
		{Name: "trigger-cron", Build: func() *xflow.WorkflowBuilder { return CronTriggerWorkflow("@every 1s") }},
		{Name: "trigger-redis", Build: func() *xflow.WorkflowBuilder {
			return RedisTriggerWorkflow("localhost:6379", RedisStreamName, RedisStreamGroup)
		}},
		{Name: "trigger-kafka", Build: func() *xflow.WorkflowBuilder {
			return KafkaTriggerWorkflow(KafkaQABrokers(), KafkaQATopic, KafkaQAGroup)
		}},
		{Name: "trigger-webhook", Build: WebhookTriggerWorkflow},
	}
}

// WithVars attaches static workflow context vars to a built definition, which is
// how $vars reaches an execution submitted over HTTP.
//
// A definition sent to the server carries its Context with it, while per-invoke
// runtime vars (xflow.WithRuntimeVars) have no field in the submit body. The
// tier definitions read their endpoints through $vars so the same builder runs
// against a local test server and a deployed one; this is the setter that makes
// that work for a topology the caller does not drive in-process.
func WithVars(def *types.WorkflowDef, vars map[string]any) {
	if len(vars) == 0 {
		return
	}
	if def.Context == nil {
		def.Context = &types.WorkflowContext{}
	}
	if def.Context.Vars == nil {
		def.Context.Vars = make(map[string]any, len(vars))
	}
	for k, v := range vars {
		def.Context.Vars[k] = v
	}
}

// NodeTypesIn returns how many times each node type appears in a compiled
// workflow, counting nodes nested inside a body the same as top-level ones.
//
// Coverage is measured from the definition rather than from a hand-written list
// of expectations: a list drifts the moment a node is dropped from a workflow,
// and a suite that asserts against a stale list reports coverage it no longer
// has.
func NodeTypesIn(def *types.WorkflowDef) map[string]int {
	counts := make(map[string]int)
	walkNodes(def.Nodes, counts)
	return counts
}

func walkNodes(nodes []types.NodeDef, counts map[string]int) {
	for _, n := range nodes {
		counts[n.Type]++
		raw, ok := n.Parameters[BodyParam]
		if !ok {
			continue
		}
		body, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		walkBody(body, counts)
	}
}

func walkBody(body map[string]any, counts map[string]int) {
	nested, err := decodeBodyMembers(body)
	if err != nil {
		// A malformed body is the graph compiler's error to report, not this
		// walker's. Returning nothing here keeps the walk total; the compile in
		// the calling test fails loudly on the same definition.
		return
	}
	walkNodes(nested, counts)
}

// decodeBodyMembers pulls the member node definitions out of the body shape
// WorkflowBuilder.Body produces: a node definition whose type is
// xflow.subgraph and whose own parameters hold {nodes, connections}.
func decodeBodyMembers(body map[string]any) ([]types.NodeDef, error) {
	if got, _ := body["type"].(string); got != SubgraphNodeType {
		return nil, fmt.Errorf("body type = %q, want %q", got, SubgraphNodeType)
	}
	params, ok := body["parameters"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("body parameters missing")
	}
	// Round-trip through JSON for the same reason sdk/xflow does when it builds
	// a body: parameter values are decoded from opaque JSON by the compiler, so
	// reading them back the same way is the only shape-stable decode.
	encoded, err := json.Marshal(params["nodes"])
	if err != nil {
		return nil, fmt.Errorf("encode body nodes: %w", err)
	}
	var out []types.NodeDef
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, fmt.Errorf("decode body nodes: %w", err)
	}
	return out, nil
}
