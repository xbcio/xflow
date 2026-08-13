package control

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// A remote task's span is parented by the W3C carrier the control plane injects
// on its lease: service/runner's executeAndReport calls
// tracing.ExtractCarrier(ctx, lease.TraceCarrier) and starts xflow.task.execute
// from the result. A lease with no carrier therefore does not produce a
// detached-but-correlated span -- it produces a span with no remote parent at
// all, and the whole remote execution detaches from the workflow trace.
//
// RELEASE-GATES.md §4 is explicit that TraceID/SpanID cannot substitute here:
// only the carrier round-trips a real SpanContext (tracestate + sampled flag).
//
// The node path is asserted alongside the other two deliberately. It is the
// positive control: if it ever stops carrying a carrier, these tests must fail
// as a broken premise rather than silently comparing two empty maps and
// declaring the group/batch paths equal to a working one.

// Distinct submit-side trace ids, one per path, so a lease that inherited the
// wrong execution's parent is caught rather than passing on a shared constant.
const (
	nodeSubmitTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	groupSubmitTraceID = "0af7651916cd43dd8448eb211c80319c"
	batchSubmitTraceID = "1b2c3d4e5f60718293a4b5c6d7e8f901"
)

// carrierProbeTracer records every context it was asked to start a span from,
// so a test can assert the carrier was injected from the DISPATCH context (the
// one carrying the span) rather than from an unrelated one.
func newCarrierProbeTracer(t *testing.T) tracing.Tracer {
	t.Helper()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tracing.NewOTelTracer(tp.Tracer("xflow-carrier-probe"))
}

func registerCarrierProbeRunner(t *testing.T, dir *MemoryRunnerDirectory, id string, caps []protocol.Capability) RunnerSession {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     id,
		Capacity:     2,
		Capabilities: caps,
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

// TestNodeLeaseCarriesTheW3CCarrier is the positive control for the two tests
// below: it pins that the ordinary node dispatch path does inject a carrier, so
// a failure there is reported as a broken premise instead of making the
// group/batch assertions vacuous.
func TestNodeLeaseCarriesTheW3CCarrier(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	caps := []protocol.Capability{{NodeType: "xflow.function"}}
	session := registerCarrierProbeRunner(t, dir, "runner-node", caps)

	task := engine.Task{ExecutionID: "exec-node-1", NodeName: "n1", Type: engine.TaskTypeNodeExec}
	mustEnqueueAssignment(t, ctx, dir, Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
	})

	core := &Core{
		engine:   &fakeControlEngine{traceCarrier: submitCarrierOf(nodeSubmitTraceID)},
		runners:  dir,
		pollWait: time.Second,
		tracer:   newCarrierProbeTracer(t),
	}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: caps,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease == nil {
		t.Fatal("pollTask() returned no lease")
	}
	if got := traceIDOf(resp.Lease.TraceCarrier); got != nodeSubmitTraceID {
		t.Fatalf("node lease trace id = %q, want the submit-side %q (carrier %v) -- the premise of the group/batch tests is broken",
			got, nodeSubmitTraceID, resp.Lease.TraceCarrier)
	}
}

// TestGroupLeaseCarriesTheW3CCarrier pins that a group lease reaches the runner
// with a remote parent. Without it every member span a remote group emits is
// rooted at the runner, disconnected from the workflow that scheduled it.
func TestGroupLeaseCarriesTheW3CCarrier(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	caps := []protocol.Capability{{NodeType: "xflow.group", Features: []string{"group.exec.v1"}}}
	session := registerCarrierProbeRunner(t, dir, "runner-grp", caps)

	assignment := groupTestAssignment()
	mustEnqueueAssignment(t, ctx, dir, assignment)

	fake := &groupFakeEngine{
		fakeControlEngine: fakeControlEngine{traceCarrier: submitCarrierOf(groupSubmitTraceID)},
		groupBuildLease: &engine.TaskLease{
			LeaseID:    "lease-grp-1",
			LeaseToken: "token-grp-1",
			Task:       assignment.Task,
			Attempt:    1,
			NodeType:   "xflow.group",
		},
		groupBuildPayload: &engine.GroupLeasePayload{ProtocolVersion: 1, GroupExecID: "gexec-1"},
	}

	core := &Core{engine: fake, runners: dir, pollWait: time.Second, tracer: newCarrierProbeTracer(t)}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: caps,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease == nil {
		t.Fatal("pollTask() returned no lease")
	}
	if got := traceIDOf(resp.Lease.TraceCarrier); got != groupSubmitTraceID {
		t.Errorf("group lease trace id = %q, want the submit-side %q (carrier %v) -- a remote group's spans detach from the workflow trace",
			got, groupSubmitTraceID, resp.Lease.TraceCarrier)
	}
}

// TestBatchLeaseCarriesTheW3CCarrier is the same assertion for a map batch.
func TestBatchLeaseCarriesTheW3CCarrier(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	caps := []protocol.Capability{{NodeType: "xflow.map"}}
	session := registerCarrierProbeRunner(t, dir, "runner-batch", caps)

	task := engine.Task{
		ExecutionID: "exec-batch-1",
		NodeName:    "m/_batch/0",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeBatch,
	}
	mustEnqueueAssignment(t, ctx, dir, Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.map"},
	})

	fake := &subgraphFakeEngine{
		fakeControlEngine: fakeControlEngine{traceCarrier: submitCarrierOf(batchSubmitTraceID)},
		lease: &engine.TaskLease{
			LeaseID:    "lease-batch-1",
			LeaseToken: "token-batch-1",
			Task:       task,
			Attempt:    1,
			NodeType:   "xflow.map",
		},
		payload: &engine.SubgraphLeasePayload{ProtocolVersion: 1, ParentNode: "m", BatchIndex: 0},
	}

	core := &Core{engine: fake, runners: dir, pollWait: time.Second, tracer: newCarrierProbeTracer(t)}
	resp, err := core.pollTask(ctx, protocol.PollTaskRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     1,
		Capabilities: caps,
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease == nil {
		t.Fatal("pollTask() returned no lease")
	}
	if got := traceIDOf(resp.Lease.TraceCarrier); got != batchSubmitTraceID {
		t.Errorf("batch lease trace id = %q, want the submit-side %q (carrier %v) -- a remote map body's spans detach from the workflow trace",
			got, batchSubmitTraceID, resp.Lease.TraceCarrier)
	}
}

// subgraphFakeEngine is the batch counterpart of groupFakeEngine: it satisfies
// the subgraphLeaseEngine capability so dispatchSubgraphLease is exercised.
type subgraphFakeEngine struct {
	fakeControlEngine

	lease   *engine.TaskLease
	payload *engine.SubgraphLeasePayload
	err     error
}

func (s *subgraphFakeEngine) BuildSubgraphLease(_ context.Context, t *engine.Task) (*engine.TaskLease, *engine.SubgraphLeasePayload, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	lease := *s.lease
	lease.Task = *t
	return &lease, s.payload, nil
}

// submitCarrierOf returns a well-formed W3C carrier on a fixed trace id. It
// stands in for the carrier a real engine persisted on the execution snapshot
// at submit time.
func submitCarrierOf(traceID string) map[string]string {
	return map[string]string{"traceparent": "00-" + traceID + "-00f067aa0ba902b7-01"}
}

// traceIDOf pulls the trace-id field out of a traceparent. Returns "" for a
// missing or malformed header, so a comparison against a real trace id fails
// rather than passing vacuously.
func traceIDOf(carrier map[string]string) string {
	parts := strings.Split(carrier["traceparent"], "-")
	if len(parts) != 4 {
		return ""
	}
	return parts[1]
}

// ExecutionTraceCarrier makes every fake satisfy traceCarrierFetcher, so a
// dispatch has a submit-side parent to inherit. It is what makes the trace-id
// assertions below non-vacuous: without a fetched carrier, dispatchCtx falls
// back to the poll context and the dispatch span is a fresh root -- which still
// yields a NON-EMPTY carrier on the lease, on a trace of its very own.
func (f *fakeControlEngine) ExecutionTraceCarrier(_ context.Context, _ types.ExecutionID) (map[string]string, error) {
	return f.traceCarrier, nil
}
