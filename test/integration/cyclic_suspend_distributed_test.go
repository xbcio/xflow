//go:build integration

// Cyclic × suspend under the real distributed deployment.
//
// The two features had zero overlapping coverage before this file. Every
// distributed cyclic case (g1RunCyclicReset, cyclic_reliability_*) drives a
// graph with no suspend node; every approval/wait case (g1RunApprovalMultiSignal,
// g1RunApprovalTimer) drives an acyclic graph; and the one sample that has both
// shapes at once -- sdk/examples/cyclic_vulnerability_approval_test.go -- runs
// on xflow.NewLocal, in process. That intersection is exactly what the
// vulnerability approval flow will deploy: a rework loop with approval nodes
// hanging inside it (see docs/design/COMMIT-PATH-TODO.md, 待决问题 3).
//
// The gap is not theoretical. Measured with a reverse probe that stops the
// resume outbox entry from carrying the LIVE activation_id (the
// stampActivation branch in rstate/state_lua.go): this test hangs the
// execution at running and times out, while TestG1ProductionE2E/ApprovalMultiSignal
// -- the existing distributed suspend case -- stays green under the identical
// injection. Only a suspend INSIDE a cycle exercises the stamping, because
// only there does the resume task's activation have to match a node whose
// activation_id advances.
package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	nodereg "github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// cyclicSuspendReviewHandler routes to "reject" on its first invocation and to
// "main" afterwards, so one pass through the loop is guaranteed and the
// execution still terminates.
type cyclicSuspendReviewHandler struct {
	invocations atomic.Int32
}

func (*cyclicSuspendReviewHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.cyclicsuspend.review"}
}

func (h *cyclicSuspendReviewHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	if h.invocations.Add(1) == 1 {
		return &types.Output{Data: map[string]any{"round": 1}, Port: "reject"}, nil
	}
	return &types.Output{Data: map[string]any{"round": 2}, Port: "main"}, nil
}

// cyclicSuspendDef is the minimal skeleton of the vulnerability approval rework
// loop:
//
//	start ------------> review
//	review --reject---> wait(signal) --main--> start   (rework cycle, approval inside it)
//	review --main-----> end
//
// The cycle closes through xflow.start rather than back to review directly:
// a cyclic graph must have exactly one xflow.start, and compiling a
// review-to-review edge without one is rejected with "cyclic workflow requires
// exactly one xflow.start node, got 0".
func cyclicSuspendDef(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    name,
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.cyclicsuspend.review"},
			{Name: "wait", Type: "xflow.wait", Parameters: map[string]any{
				"mode":        "signal",
				"signal_name": "approval",
			}},
			{Name: "end", Type: "test.g1.real"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {
				"reject": {Targets: []types.Connection{{Node: "wait", Input: "main"}}},
				"main":   {Targets: []types.Connection{{Node: "end", Input: "main"}}},
			},
			"wait": {"main": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	}
}

func startCyclicSuspendRunner(t *testing.T, h *productionServerRunnerHarness, id string) (context.CancelFunc, chan error, *cyclicSuspendReviewHandler) {
	t.Helper()
	reg := execution.NewRegistry()
	review := &cyclicSuspendReviewHandler{}
	reg.RegisterGlobal("test.cyclicsuspend.review", review)
	reg.RegisterGlobal("test.g1.real", &g1RealHandler{})
	if wh, ok := nodereg.Lookup("xflow.wait"); ok {
		reg.RegisterGlobal("xflow.wait", wh)
	}
	if sh, ok := nodereg.Lookup("xflow.start"); ok {
		reg.RegisterGlobal("xflow.start", sh)
	}

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		reg,
		runnersvc.Config{
			RunnerID:    id,
			Concurrency: 1,
			Capabilities: []protocol.Capability{
				{NodeType: "test.cyclicsuspend.review"},
				{NodeType: "test.g1.real"},
				{NodeType: "xflow.wait"},
				{NodeType: "xflow.start"},
			},
			PollWait:   5 * time.Millisecond,
			Tracer:     h.tracer,
			Namespaces: []namespace.Namespace{"default"},
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(runnerCtx) }()
	waitForE2ERunner(t, h.runners, id)
	return runnerCancel, errCh, review
}

func TestCyclicWithSuspendDistributed(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	h := newProductionServerRunnerHarness(t, addr, dsn, g1ProductionMappings())
	otel.SetTracerProvider(h.tracerProv)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	cancel, errCh, review := startCyclicSuspendRunner(t, h, "runner-cyclic-suspend")
	defer g1StopRunner(t, cancel, errCh)

	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, cyclicSuspendDef("cyclic-suspend"))

	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokDefault, execID, "wait",
		[]types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 15*time.Second)

	if status, body := g1SignalAuth(t, h.httpSrv.URL, g1TokDefault, execID, "approval", map[string]any{"by": "sec"}); status != 200 {
		t.Fatalf("signal approval: status=%d body=%s, want 200", status, body)
	}

	detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokDefault, execID, 20*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("cyclic+suspend execution status = %s, want success", detail.Status)
	}
	// The cycle must actually have been re-entered. Without this assertion an
	// implementation that terminates straight after the signal -- never routing
	// back through start -- would still satisfy the success check above.
	if n := review.invocations.Load(); n < 2 {
		t.Fatalf("review invocations = %d, want >= 2 (the cycle never re-entered after resume)", n)
	}
}
