package xflow

import (
	"context"
	"errors"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// ConcurrencyResetOptions controls ResetWorkflowConcurrency.
//
// The zero value is a DRY RUN: Apply is a positive opt-in so a caller that
// constructs the options and forgets a field previews the judgement instead of
// terminating work.
type ConcurrencyResetOptions = engine.ResetConcurrencyOptions

// ResetWorkflowConcurrency judges, and with Apply converges, the concurrency
// state of one workflow in one namespace.
//
// The default judgement is the concurrency-orphan judgement: an execution is
// reclaimed only when it is running or queued, its graph is gone, no node or
// group lease is live, and no node is suspended. IncludeLive lifts those
// guards for an explicit operator reset, and only then can an execution that
// still has a graph be terminated; every such convergence is recorded with the
// reason operator_reset in the structured log and the
// xflow_orphan_reclaimed_total metric.
//
// ns is the caller's namespace (empty means namespace.FromContext, which
// falls back to "default"). The HTTP surface derives it from the authenticated
// principal and never from the request body.
//
// The returned result reports what was inspected, what would be / was
// reclaimed (Reclaimed stays 0 for a dry run) and, for every judged execution,
// the id, status, age and evidence the reset was based on.
func (e *Engine) ResetWorkflowConcurrency(ctx context.Context, ns namespace.Namespace, workflowName string, opts ConcurrencyResetOptions) (engine.OrphanReclaimResult, error) {
	if e == nil || e.eng == nil {
		return engine.OrphanReclaimResult{}, errors.New("xflow: engine is not initialized")
	}
	return e.eng.ResetWorkflowConcurrency(ctx, ns, workflowName, opts)
}

// ResetWorkflowConcurrency is the Server-face counterpart of
// Engine.ResetWorkflowConcurrency: the HTTP route and an embedded host call the
// same implementation, so a preview returned in-process and one returned over
// HTTP cannot disagree.
func (s *Server) ResetWorkflowConcurrency(ctx context.Context, ns namespace.Namespace, workflowName string, opts ConcurrencyResetOptions) (engine.OrphanReclaimResult, error) {
	if s == nil || s.sdkEngine == nil {
		return engine.OrphanReclaimResult{}, errors.New("xflow: server has no engine")
	}
	return s.sdkEngine.ResetWorkflowConcurrency(ctx, ns, workflowName, opts)
}
