package execution

import (
	"context"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/tracing"
)

// localTaskSpanName is the span an embedded (in-process) task attempt reports
// under. It is the local counterpart of the control-plane xflow.task.dispatch /
// runner xflow.task.execute pair: in local mode there is no network hop, so
// one span covers the attempt.
const localTaskSpanName = "xflow.local.task"

// startLocalTaskSpan opens the xflow.local.task span for one attempt, or
// returns a no-op span when there is nothing to instrument.
//
// The parent comes from the lease's W3C carrier — stamped from the execution
// snapshot by engine.BuildTaskLease — because a local queue worker starts from
// context.Background(): without a real ExtractCarrier round-trip the span would
// be a detached root. A ctx that already carries a span wins instead: the
// embedded dispatcher (HandleTask) and the standalone runner
// (service/runner's xflow.task.execute) both open the attempt's span before
// calling into the executor, and a second span with the same meaning nested
// inside the first is noise, not detail.
func startLocalTaskSpan(ctx context.Context, tracer tracing.Tracer, lease *engine.TaskLease) (context.Context, tracing.Span) {
	if tracer == nil || lease == nil || tracing.TraceIDFromContext(ctx) != "" {
		// Keep the caller's ctx untouched; only the no-op span is new.
		_, span := tracing.NoopTracer{}.Start(ctx, localTaskSpanName)
		return ctx, span
	}
	return tracer.Start(tracing.ExtractCarrier(ctx, lease.TraceCarrier), localTaskSpanName,
		"execution_id", string(lease.Task.ExecutionID),
		"node_name", lease.Task.NodeName,
		"attempt", lease.Attempt,
	)
}
