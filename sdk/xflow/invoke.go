package xflow

import (
	"context"
	"fmt"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type Entry struct {
	nodeName string
}

func Start() Entry { return Entry{nodeName: "start"} }

func EntryNode(name string) Entry { return Entry{nodeName: name} }

func Trigger(name string) Entry { return Entry{nodeName: name} }

func (e *Engine) Invoke(ctx context.Context, workflowID types.WorkflowID, entry Entry, input map[string]any, opts ...InvokeOption) (types.ExecutionID, error) {
	if entry.nodeName == "" {
		return "", fmt.Errorf("entry node name is required")
	}
	rec, err := e.workflowRegistry.GetWorkflow(ctx, workflowID)
	if err != nil {
		return "", err
	}
	// FAF workflows have no durable execution state. Refuse before calling the
	// core engine so Invoke cannot allocate an execution, queue task, or outbox
	// entry; callers must use FireAndForget instead.
	if rec.Graph != nil && rec.Graph.FAF() {
		return "", enginecore.ErrFAFRequiresDirectDispatch
	}
	cfg := &invokeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.execTTL > 0 {
		ctx = enginecore.WithExecutionTTL(ctx, cfg.execTTL)
	}
	if cfg.testRun {
		ctx = enginecore.WithTestRun(ctx, true)
	}
	if cfg.traceID != "" {
		ctx = enginecore.WithTraceID(ctx, cfg.traceID)
	}
	if cfg.spanID != "" {
		ctx = enginecore.WithSpanID(ctx, cfg.spanID)
	}
	if cfg.idempotencyKey != "" {
		// The scope is the registered workflow, so the key means one execution of
		// this workflow whichever face a retry comes in through (this SDK call,
		// the by-id HTTP route).
		ctx = enginecore.WithIdempotency(ctx, &enginecore.Idempotency{
			Key:   cfg.idempotencyKey,
			Scope: enginecore.WorkflowIdempotencyScope(workflowID),
		})
	}
	ctx = enginecore.WithWorkflowDef(ctx, rec.Definition)
	return e.eng.Invoke(ctx, rec.Graph, entry.nodeName, input, cfg.runtime)
}
