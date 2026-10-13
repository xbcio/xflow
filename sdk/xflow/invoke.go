package xflow

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/backend"
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
	cfg := &invokeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	// A pinned version is resolved before anything else looks at the record:
	// everything below (the FAF refusal included) must judge the graph that
	// will actually run, and for a retired version that is the archived
	// snapshot, not the live one.
	if cfg.workflowVersion != "" && cfg.workflowVersion != rec.Version {
		rec, err = e.archivedWorkflowVersion(ctx, workflowID, cfg.workflowVersion)
		if err != nil {
			return "", err
		}
	}
	// FAF workflows have no durable execution state. Refuse before calling the
	// core engine so Invoke cannot allocate an execution, queue task, or outbox
	// entry; callers must use FireAndForget instead.
	if rec.Graph != nil && rec.Graph.FAF() {
		return "", enginecore.ErrFAFRequiresDirectDispatch
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

// archivedWorkflowVersion resolves a retired version of the workflow addressed
// by id, verified against the current compiler before the call acts on it: the
// runtime hash recomputed from the stored payload must equal the hash the
// version was PUBLISHED with (versioning design Q10).
//
// A version label is immutable, so a mismatch means compilation or hashing
// semantics have drifted since the version was published — the payload would
// run as the same label with different semantics. The SDK fails closed with
// backend.ErrWorkflowVersionUnavailable, the same answer the HTTP execute path
// gives, rather than run a divergent definition. Execution then proceeds from
// the archived graph snapshot (rec.Graph) as captured at retirement; the
// payload is never recompiled.
//
// A registry without backend.WorkflowVersionCapability serves no history at
// all; that is a wiring error, not "no such version", and is refused as one.
// Whether version names the current version is decided by the caller (it is
// not archived), so this is only reached for a version the live record does not
// carry; an unknown one comes back as backend.ErrWorkflowVersionNotFound.
func (e *Engine) archivedWorkflowVersion(ctx context.Context, id types.WorkflowID, version string) (backend.WorkflowRecord, error) {
	versions, ok := e.workflowRegistry.(backend.WorkflowVersionCapability)
	if !ok {
		return backend.WorkflowRecord{}, fmt.Errorf("xflow: workflow registry does not support version history (cannot pin version %q)", version)
	}
	rec, err := versions.GetWorkflowVersion(ctx, id, version)
	if err != nil {
		return backend.WorkflowRecord{}, err
	}
	hash, err := runtimeHash(rec.Definition)
	if err != nil {
		// A definition the current compiler cannot even hash cannot be
		// confirmed as the published one; that is the same fail-closed answer.
		if e.logger != nil {
			e.logger.Error("invoke_archived_workflow_version_hash_failed",
				"workflow_id", string(id), "version", version, "err", err)
		}
		return backend.WorkflowRecord{}, fmt.Errorf("%w: version %q: %v", backend.ErrWorkflowVersionUnavailable, version, err)
	}
	if hash != rec.DefinitionHash {
		if e.logger != nil {
			e.logger.Error("invoke_archived_workflow_version_hash_drift",
				"workflow_id", string(id), "version", version,
				"stored_hash", rec.DefinitionHash, "current_hash", hash)
		}
		return backend.WorkflowRecord{}, fmt.Errorf("%w: version %q", backend.ErrWorkflowVersionUnavailable, version)
	}
	return rec, nil
}
