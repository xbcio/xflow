package flow

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	"slices"

	"github.com/xbcio/xflow/exprx"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/xbcio/xflow/node/registry"
	"github.com/spf13/cast"
)

// MapNode implements xflow.map — runs a body sub-graph once per item.
type MapNode struct {
	nodeinternal.BaseNode
	Items     string
	BatchSize int
	KeepGoing bool
}

// Map creates a map node that runs a body sub-graph over a collection.
//
//	node.Map("items", 5)
func Map(itemsExpr string, batchSize int) *MapNode {
	if batchSize <= 0 {
		batchSize = 1
	}
	return &MapNode{Items: itemsExpr, BatchSize: batchSize}
}

// ContinueOnError tolerates a failed item instead of abandoning the rest of
// its batch. Failed items keep their slot in results as {_error, _index}, so
// a downstream consumer must filter them out.
//
//	node.Map("$input.messages", 20).ContinueOnError()
//
// Off by default, matching the descriptor. Without it, the first failed item
// stops its batch at execution/subgraph.MapBodyExecutor — which makes one
// malformed item cost every later item in the same batch.
func (n *MapNode) ContinueOnError() *MapNode {
	n.KeepGoing = true
	return n
}

func (n *MapNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.map",
		DisplayName: "Map",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to iterate"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: types.ParamNumber, Required: false, Default: 1, Description: "Number of items processed per batch"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: types.ParamBool, Required: false, Default: false, Description: "Continue iteration when an item fails"},
			{Name: "body", DisplayName: "Body", Type: types.ParamObject, Required: false, Description: "Sub-graph executed once per item; mutually exclusive with expression, and exactly one of the two is required"},
			{Name: "expression", DisplayName: "Expression", Type: types.ParamString, Required: false, Description: "Expression evaluated once per item over $item/$index/$items; mutually exclusive with body, and exactly one of the two is required"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *MapNode) NodeType() string { return "xflow.map" }
func (n *MapNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *MapNode) RawParams() any {
	return map[string]any{
		"items":             n.Items,
		"batch_size":        n.BatchSize,
		"continue_on_error": n.KeepGoing,
	}
}

func (n *MapNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	itemsExpr, _ := input.Params["items"].(string)
	if itemsExpr == "" {
		return nil, fmt.Errorf("xflow.map: items parameter is required")
	}

	// The {expression | body} choice is types.TransformSpec's, not this
	// handler's: reading it by hand here is what let this node and the compiler
	// disagree about `expression: ""`.
	spec, err := types.ParseTransformSpec(input.Params)
	if err != nil {
		return nil, fmt.Errorf("xflow.map: %w", err)
	}

	env := exprx.BuildExprEnv(input, nil)
	result, err := exprx.EvalExpr(itemsExpr, env, false)
	if err != nil {
		return nil, fmt.Errorf("xflow.map: %w", err)
	}

	items, err := conv.ToSlice(result)
	if err != nil {
		return nil, fmt.Errorf("xflow.map: items must evaluate to an array: %w", err)
	}

	if spec.Expression != "" {
		return evalItemsInline(spec.Expression, env, items, continueOnError(input.Params))
	}

	batchSize := 1
	if bs, err := cast.ToIntE(input.Params["batch_size"]); err == nil && bs > 0 {
		batchSize = bs
	}

	batches := slices.Collect(slices.Chunk(items, batchSize))

	// No marker key. The engine reads "does this node expand?" off the compiled
	// graph (a projected body), not off this map — a payload cannot answer a
	// structural question, and while it was asked to, any handler that happened
	// to name a field "_loop" turned itself into a fan-out node with no body to
	// run.
	return &types.Output{
		Data: map[string]any{
			"items":       items,
			"batches":     batches,
			"batch_size":  batchSize,
			"total":       len(items),
			"batch_count": len(batches),
		},
	}, nil
}

func continueOnError(params map[string]any) bool {
	v, _ := params["continue_on_error"].(bool)
	return v
}

// evalItemsInline runs the expression form: one evaluation per item, right here,
// producing the finished result rather than a fan-out descriptor.
//
// The engine decides whether to expand a map node from whether the compiler
// projected a body for it, and this form projects none — so whatever this
// returns is committed verbatim as the node's output. That is why the shape must
// match what the body form's completeLoopSplit emits ({results, count}) and why
// the failure accounting must match BatchResultForCommit: an author who switches
// a node between the two forms must not have to rewrite everything downstream.
//
// It does not batch. batch_size is a durability policy for the body form — it
// decides how many items share a sub-execution and therefore a retry unit —
// and there is no sub-execution here to size.
func evalItemsInline(expression string, baseEnv map[string]any, items []any, keepGoing bool) (*types.Output, error) {
	results := make([]any, 0, len(items))
	var firstErr error
	succeeded := 0

	for index, item := range items {
		value, err := exprx.EvalExpr(expression, itemEnv(baseEnv, item, index, items), false)
		if err != nil {
			err = fmt.Errorf("xflow.map: item %d: %w", index, err)
			if firstErr == nil {
				firstErr = err
			}
			// The failed item occupies its slot rather than being dropped, which is
			// what makes "count equals the input length" true and lets a downstream
			// filter tell a failure apart from data. Same contract as
			// engine.batchResultData — see the warning in DSL-SPECIFICATION.md about
			// a result that fabricates its own _error.
			results = append(results, map[string]any{"_error": err.Error(), "_index": index})
			if !keepGoing {
				return nil, firstErr
			}
			continue
		}
		succeeded++
		results = append(results, value)
	}

	// continue_on_error means "tolerate a partial failure", not "never fail": a run
	// where every item failed is a failure under either setting.
	if firstErr != nil && succeeded == 0 {
		return nil, firstErr
	}

	return &types.Output{Data: map[string]any{
		"results": results,
		"count":   len(results),
	}}, nil
}

// itemEnv layers one item's three roots over the node's base environment.
//
// The "$" prefix is not cosmetic and must match execution/subgraph's
// bodyItemInput exactly: the filter node binds the unprefixed "item"/"index" for
// its own per-element condition, so a filter reachable from a map would
// otherwise shadow the map's iteration variables silently.
func itemEnv(baseEnv map[string]any, item any, index int, items []any) map[string]any {
	env := make(map[string]any, len(baseEnv)+3)
	for key, value := range baseEnv {
		env[key] = value
	}
	env["$item"] = item
	env["$index"] = index
	env["$items"] = items
	return env
}

func init() { registry.Register(&MapNode{}) }
