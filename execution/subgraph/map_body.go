package subgraph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// MapBodyExecutor adapts this package's caller-agnostic Executor to the engine's
// batch shape: one batch in, one result per item out.
//
// It exists as a thin adapter rather than as logic inside Executor because a
// group or a subflow executes ONCE and has no notion of a current item — giving
// Executor an $item to inject would push a meaningless root onto those callers.
// Executor takes a Request.Input and does not care who filled it.
type MapBodyExecutor struct {
	executor *Executor
	// suspendDisabled forbids suspend nodes inside a body. A body runs once per
	// item with no external identity to resume against, so a suspended body item
	// would park a sub-execution nothing can ever signal.
	suspendDisabled bool
	// deadline bounds every item's nested Executor.Execute call. Zero means no
	// bound, which is what the two long-lived callers (sdk/xflow's engine-startup
	// wiring, service/runner's SubgraphRuntime) always pass: they construct their
	// MapBodyExecutor once at startup, before any per-call deadline exists, and a
	// top-level map's body cannot itself contain another xflow.map/split/subgraph
	// (bannedBodyMemberTypes in engine/graph/compile.go), so there is no further
	// recursion from those two paths for a deadline to bound anyway.
	//
	// The one caller that DOES have a real per-call deadline to give is
	// Executor.Execute itself (subgraph.go): when req.Package is a projected GROUP
	// package and a member is an xflow.map, that member's batch recurses back into
	// a fresh Executor.Execute call for each item, and Execute already knows
	// req.Deadline at the moment it builds this type's constructor call — see the
	// WithBatchBodyExecutor wiring comment in subgraph.go for why this is the ONLY
	// place a deadline can be captured, since ExecuteBatchBody's own signature
	// (ctx, engine.BatchBodyRequest) carries no Deadline field to read one back
	// from.
	deadline time.Time
}

// NewMapBodyExecutor wraps an Executor for map-body use. deadline, when
// non-zero, is forwarded to every item's nested Executor.Execute call so a
// group-member map's batch cannot outlive the group's own deadline; pass the
// zero value when no outer deadline exists yet (see the deadline field doc).
func NewMapBodyExecutor(executor *Executor, suspendDisabled bool, deadline time.Time) *MapBodyExecutor {
	return &MapBodyExecutor{executor: executor, suspendDisabled: suspendDisabled, deadline: deadline}
}

// ExecuteBatchBody runs the body once per item, in order, and reports one result
// per item.
//
// Items run serially rather than concurrently: a batch is the durability unit, and
// running its items in parallel would multiply the peak resource use of a
// deliberately-sized batch without changing throughput — concurrency across the
// expansion already comes from the batches themselves being separate tasks.
func (x *MapBodyExecutor) ExecuteBatchBody(ctx context.Context, req engine.BatchBodyRequest) ([]engine.BatchItemResult, error) {
	if req.Body == nil {
		return nil, errors.New("batch body request carries no package")
	}
	results := make([]engine.BatchItemResult, 0, len(req.Items))
	for pos, item := range req.Items {
		index := globalIndex(req, pos)
		res, err := x.executor.Execute(ctx, Request{
			Package:         req.Body,
			PackageHash:     req.BodyHash,
			Input:           bodyItemInput(req),
			Scope:           bodyItemScope(req, item, index),
			SuspendDisabled: x.suspendDisabled,
			// Zero when this MapBodyExecutor was built with no outer deadline (the
			// sdk/xflow and SubgraphRuntime constructors both do this today, since
			// neither has one to give -- see the deadline field's doc on why that is
			// fine). Forwarding it here rather than dropping it is what closes the
			// gap for the one caller that DOES have one: a group-member map's item
			// must not be able to keep running after the enclosing group execution's
			// own deadline has passed.
			Deadline: x.deadline,
		})
		if err != nil {
			// The body could not be RUN — compile failure, package validation,
			// backend construction. That is a fault of the whole batch, not of
			// this item, so it aborts rather than being recorded per item: every
			// remaining item would fail identically.
			return nil, err
		}

		itemResult := engine.BatchItemResult{Index: index}
		switch res.Outcome {
		case OutcomeSuccess:
			itemResult.Data = exitsAsItemResult(res.Exits)
		default:
			itemResult.Err = fmt.Errorf("%s: %s", res.Outcome, res.Error)
		}
		results = append(results, itemResult)

		if itemResult.Err != nil && !req.ContinueOnError {
			// Stop this batch's remaining items. The items that already ran keep
			// their side effects: there is no rollback, which is why body nodes
			// with side effects must be idempotent on a business key from $item.
			break
		}
	}
	return results, nil
}

// globalIndex turns a position within this batch into the item's position in the
// map node's whole items array. batch_size is a durability policy, so $index must
// not change when it does.
func globalIndex(req engine.BatchBodyRequest, pos int) int {
	if req.BatchSize <= 0 {
		return pos
	}
	return req.BatchIndex*req.BatchSize + pos
}

// bodyItemScope builds the execution-wide roots the DSL promises a body. They
// are injected here, in the map adapter, for the reason in this type's doc
// comment.
//
// They travel as a Request.Scope rather than inside the entry Input because the
// spec scopes them to the body ("$item、$index、$items 仅在 body 内可用"), not to
// the body's entry node. As submission params they reached only members with no
// in-edges, so a two-member body failed at "unknown name $index" before its
// second member ever ran.
//
// $item and $index deliberately keep their "$" prefix: the filter node uses the
// unprefixed "item"/"index" for its own per-element condition (see
// node/internal/transform/filter.go), and a filter nested in a body would
// otherwise shadow the map's iteration variables silently.
//
// req.OuterNodes joins them under engine.ExecutionScopeNodesKey when the body
// reads an outer-graph ancestor. It belongs here for the same reason the loop
// roots do -- it is scoped to the whole body, not to its entry -- and the engine
// routes that one key to Input.Nodes instead of Input.Data (see the constant's
// doc). Omitted entirely when empty so a body that reads no outer node produces
// the same scope map it always did.
func bodyItemScope(req engine.BatchBodyRequest, item any, index int) map[string]any {
	scope := map[string]any{
		"$item":  item,
		"$index": index,
		"$items": req.AllItems,
	}
	if len(req.OuterNodes) > 0 {
		scope[engine.ExecutionScopeNodesKey] = req.OuterNodes
	}
	return scope
}

// bodyItemInput builds the entry input for one item.
//
// Runtime is forwarded rather than left nil so a body member's $vars carries the
// per-submission half too -- Executor reads it off this Input and passes it to
// the inner Submit. The static half already arrives inside the projected
// package's Def.Context.
//
// TraceID/SpanID ride the same Input for the same reason, and deliberately land
// on the field a GROUP lease's Input already populates: Executor then has one
// place to read the trace identity from, whoever the caller is, instead of a
// map-only branch.
//
// Data is deliberately empty: the body's entry member has no upstream output to
// inherit, and the loop roots travel on Request.Scope instead (bodyItemScope).
func bodyItemInput(req engine.BatchBodyRequest) *types.Input {
	return &types.Input{Runtime: req.Runtime, TraceID: req.TraceID, SpanID: req.SpanID}
}

// exitsAsItemResult folds a body's fired boundary outputs into one item's result.
//
// A body with one exit — the common shape — yields that exit's data directly, so
// downstream reads results[i].field rather than results[i].someNodeName.field. A
// body with several terminal nodes keys them by node name, because there is no
// principled way to merge them.
func exitsAsItemResult(exits []graph.SubgraphExitResult) map[string]any {
	switch len(exits) {
	case 0:
		return map[string]any{}
	case 1:
		if exits[0].Data == nil {
			return map[string]any{}
		}
		return exits[0].Data
	default:
		out := make(map[string]any, len(exits))
		for _, exit := range exits {
			out[exit.NodeName] = exit.Data
		}
		return out
	}
}
