//go:build integration

package integration

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// action_parity_test.go implements the A3 three-topology action-error parity
// matrix (.claude/specs/2026-07-18-sdk-server-production-readiness-remediation-design.md
// §6.4). The same classified-error fixture runs across two topologies:
//
//   - local embedded — in-memory backend + engine + embedded dispatcher
//     (handler runs in-process; the ClassifiedError is a live Go error).
//   - server-runner — real Redis/Asynq backend + apiserver + HTTP runner
//     (the ClassifiedError crosses the wire via protocol.error_detail and is
//     recovered server-side before retry classification).
//
// The cluster-transient topology is intentionally excluded from this matrix:
// it has no handler-level retry (transient dispatch failures back off at the
// queue, not the engine's MaxAttempts path), so comparing its retry count
// against local/server-runner would be a category error. A3 §6.4 covers it
// under a separate collection-path exclusion.
//
// TestActionErrorParityMatrix contains five fixtures:
//
//   - transient-then-success: handler fails once with a transient ClassifiedError,
//     then succeeds on the next attempt.
//   - transient-retry-exhausted: handler always returns a transient ClassifiedError;
//     MaxAttempts is reached.
//   - permanent-no-retry: handler returns a permanent ClassifiedError; no retry.
//   - business-error-no-retry: handler returns Output.Error (structured business
//     rejection); the engine applies OnError without retry.
//   - error-port-retry-exhausted: handler always returns an explicit error-port
//     output (legacy matrix row); the engine converts this to a transient retry
//     via outputPortRetryError and exhausts MaxAttempts.
//
// The matrix asserts the topology-independent retry contract in engine/
// atomic_commit.go (tryRetryWithAttempt: no retry when settings==nil,
// types.IsPermanent(cause), or attempt>=MaxAttempts): for each fixture the
// final node.Attempt, execution status, and error message/code match across
// topologies. node.Attempt is the engine's logical attempt count (deduped via
// CommittedLeaseToken), so it is stable under at-least-once handler delivery.

const (
	parityTransientThenSuccess = "transient_then_success"
	parityTransientExhausted   = "transient_exhausted"
	parityPermanent            = "permanent"
	parityBusinessError        = "business_error"
	parityErrorPort            = "error_port"
)

// parityFixture is the configuration for the shared parity action handler.
// The build function creates a fresh handler instance for each topology so
// per-topology attempt counters are isolated.
type parityFixture struct {
	behaviour  string
	failBefore int32 // transient failures before success (transient_then_success only)
	code       string
	msg        string // error message for business_error / error_port
}

func (f *parityFixture) handler(nodeType string) *parityFixtureHandler {
	return &parityFixtureHandler{
		nodeType:   nodeType,
		behaviour:  f.behaviour,
		failBefore: f.failBefore,
		code:       f.code,
		msg:        f.msg,
	}
}

// parityFixtureHandler is the shared action fixture. It returns one of:
//   - types.ClassifiedError (transient/permanent) — exercises the engine's
//     retry classification path in-process and across the wire.
//   - types.Output with Error set — a routable business error (no retry, OnError
//     decides terminal routing).
//   - types.Output with Port="error" — the legacy explicit error-port output
//     (engine/outputPortRetryError converts it to a transient retry).
type parityFixtureHandler struct {
	nodeType   string
	behaviour  string
	failBefore int32
	code       string
	msg        string
	attempts   atomic.Int32
}

func (h *parityFixtureHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType}
}

func (h *parityFixtureHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	n := h.attempts.Add(1)
	switch h.behaviour {
	case parityPermanent:
		return nil, types.NewPermanentError(h.code, "permanent fixture failure")
	case parityTransientExhausted:
		return nil, types.NewTransientError(h.code, "transient fixture failure")
	case parityBusinessError:
		return &types.Output{Error: &types.Error{Message: h.msg}}, nil
	case parityErrorPort:
		return &types.Output{Data: map[string]any{"error": h.msg}, Port: "error"}, nil
	default: // parityTransientThenSuccess
		if n <= h.failBefore {
			return nil, types.NewTransientError(h.code, "transient fixture failure")
		}
		return &types.Output{Data: map[string]any{"attempt": n}}, nil
	}
}

// downstreamExpectation describes the expected state of a single downstream node.
type downstreamExpectation struct {
	Status types.NodeStatus
	Output map[string]any
}

// ParityOutcome is the topology-independent contract a fixture must reach.
// It is exported so that subsequent fixture files can reuse the parity runners.
type ParityOutcome struct {
	Attempt            int
	Status             types.ExecutionStatus
	SourceStatus       types.NodeStatus // terminal status of the source node
	ErrStr             string           // node.Error (ClassifiedError.Error() == "code: message")
	Port               string           // source node output port
	DownstreamStatuses map[string]types.NodeStatus
	DownstreamOutputs  map[string]map[string]any
}

// parityCase holds a single parity fixture configuration.
type parityCase struct {
	Name        string
	Build       func() (source types.NodeDef, register func(engine.HandlerRegistrar))
	MaxAttempts int
	WantAttempt int
	WantStatus  types.ExecutionStatus
	ErrContains string // substring expected in node.Error for failed fixtures
	OKNode      types.NodeDef
	ErrNode     types.NodeDef
	// WantDownstream maps downstream node name to expected terminal state.
	WantDownstream map[string]downstreamExpectation
}

// ParityWorkflow builds a single-source workflow with per-node retry settings.
// The source node's Retry field is set to retry; retry may be nil to inherit
// workflow defaults.
func ParityWorkflow(source types.NodeDef, retry *types.RetrySettings) *types.WorkflowDef {
	source.Retry = retry
	return &types.WorkflowDef{
		Name:  "action-parity-" + source.Name,
		Nodes: []types.NodeDef{source},
	}
}

// ParityWorkflowWithDownstream builds a workflow with a source node plus optional
// ok/err downstream nodes. okNode is wired to the source's "main" port; errNode
// is wired to the source's "error" port. Either downstream node may be the zero
// value to omit it. The source node's Retry field is set to retry.
func ParityWorkflowWithDownstream(source types.NodeDef, retry *types.RetrySettings, okNode, errNode types.NodeDef) *types.WorkflowDef {
	source.Retry = retry
	nodes := []types.NodeDef{source}
	conns := make(types.Connections)
	if okNode.Name != "" {
		nodes = append(nodes, okNode)
		conns[source.Name] = map[string][]types.Connection{
			"main": {{Node: okNode.Name}},
		}
	}
	if errNode.Name != "" {
		nodes = append(nodes, errNode)
		if conns[source.Name] == nil {
			conns[source.Name] = map[string][]types.Connection{}
		}
		conns[source.Name]["error"] = []types.Connection{{Node: errNode.Name}}
	}
	return &types.WorkflowDef{
		Name:        "action-parity-" + source.Name,
		Nodes:       nodes,
		Connections: conns,
	}
}

func TestActionErrorParityMatrix(t *testing.T) {
	addr := requireRedis(t)

	cases := []parityCase{
		{
			Name: "transient_then_success",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				nodeType := "test.parity.transient.then.success"
				f := &parityFixture{
					behaviour:  parityTransientThenSuccess,
					failBefore: 1, // attempt 1 transient, attempt 2 succeeds
					code:       "parity.transient_then_success",
					msg:        "business.reject",
				}
				return types.NodeDef{Name: "start", Type: nodeType},
					func(reg engine.HandlerRegistrar) { reg.RegisterGlobal(nodeType, f.handler(nodeType)) }
			},
			MaxAttempts: 3,
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusSuccess,
		},
		{
			Name: "transient_retry_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				nodeType := "test.parity.transient.retry.exhausted"
				f := &parityFixture{
					behaviour: parityTransientExhausted,
					code:      "parity.transient_retry_exhausted",
					msg:       "business.reject",
				}
				return types.NodeDef{Name: "start", Type: nodeType},
					func(reg engine.HandlerRegistrar) { reg.RegisterGlobal(nodeType, f.handler(nodeType)) }
			},
			MaxAttempts: 2, // attempt 1 + 1 retry, then exhausted
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "parity.transient_retry_exhausted",
		},
		{
			Name: "permanent_no_retry",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				nodeType := "test.parity.permanent.no.retry"
				f := &parityFixture{
					behaviour: parityPermanent,
					code:      "parity.permanent_no_retry",
					msg:       "business.reject",
				}
				return types.NodeDef{Name: "start", Type: nodeType},
					func(reg engine.HandlerRegistrar) { reg.RegisterGlobal(nodeType, f.handler(nodeType)) }
			},
			MaxAttempts: 3, // permanent bypasses retry entirely
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "parity.permanent_no_retry",
		},
		{
			Name: "error_port_retry_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				nodeType := "test.parity.error.port"
				f := &parityFixture{
					behaviour: parityErrorPort,
					code:      "parity.error_port_retry_exhausted",
					msg:       "business.reject",
				}
				return types.NodeDef{Name: "start", Type: nodeType},
					func(reg engine.HandlerRegistrar) { reg.RegisterGlobal(nodeType, f.handler(nodeType)) }
			},
			MaxAttempts: 3, // explicit error-port output is transient (outputPortRetryError)
			WantAttempt: 3,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "business.reject",
		},
		{
			Name: "business_error_no_retry",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				nodeType := "test.parity.business.error"
				f := &parityFixture{
					behaviour: parityBusinessError,
					code:      "parity.business_error_no_retry",
					msg:       "business.reject",
				}
				return types.NodeDef{Name: "start", Type: nodeType},
					func(reg engine.HandlerRegistrar) { reg.RegisterGlobal(nodeType, f.handler(nodeType)) }
			},
			MaxAttempts: 3, // business error bypasses retry (Output.Error)
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "business.reject",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register := tc.Build()
			retry := &types.RetrySettings{
				MaxAttempts:     tc.MaxAttempts,
				InitialInterval: 50, // 50ms keeps the matrix fast while exercising real backoff.
			}
			var def *types.WorkflowDef
			if tc.OKNode.Name != "" || tc.ErrNode.Name != "" {
				def = ParityWorkflowWithDownstream(source, retry, tc.OKNode, tc.ErrNode)
			} else {
				def = ParityWorkflow(source, retry)
			}

			localOut := RunParityLocal(t, def, register)
			serverOut := RunParityServerRunner(t, addr, def, register)

			assertParity(t, tc, localOut, serverOut)
		})
	}
}

func assertParity(t *testing.T, tc parityCase, localOut, serverOut ParityOutcome) {
	t.Helper()

	// Parity: both topologies reach the same logical attempt count and
	// terminal status for the same fixture.
	if localOut.Attempt != serverOut.Attempt {
		t.Errorf("attempt parity: local=%d server-runner=%d, want equal", localOut.Attempt, serverOut.Attempt)
	}
	if localOut.Status != serverOut.Status {
		t.Errorf("status parity: local=%s server-runner=%s, want equal", localOut.Status, serverOut.Status)
	}
	localHasErr, serverHasErr := localOut.ErrStr != "", serverOut.ErrStr != ""
	if localHasErr != serverHasErr {
		t.Errorf("error presence parity: local=%v server-runner=%v, want equal", localHasErr, serverHasErr)
	}

	// Contract: each topology independently matches the expected outcome.
	if localOut.Attempt != tc.WantAttempt {
		t.Errorf("local attempt=%d, want %d", localOut.Attempt, tc.WantAttempt)
	}
	if localOut.Status != tc.WantStatus {
		t.Errorf("local status=%s, want %s", localOut.Status, tc.WantStatus)
	}
	if serverOut.Attempt != tc.WantAttempt {
		t.Errorf("server-runner attempt=%d, want %d", serverOut.Attempt, tc.WantAttempt)
	}
	if serverOut.Status != tc.WantStatus {
		t.Errorf("server-runner status=%s, want %s", serverOut.Status, tc.WantStatus)
	}

	// For failed fixtures the error message/code must survive the wire and
	// be recorded in the node's Error field. ClassifiedErrors carry
	// "code: message"; explicit error-port output carries the raw message.
	if tc.WantStatus != types.ExecutionStatusSuccess && tc.ErrContains != "" {
		if !localHasErr || !strings.Contains(localOut.ErrStr, tc.ErrContains) {
			t.Errorf("local node error %q missing %q", localOut.ErrStr, tc.ErrContains)
		}
		if !serverHasErr || !strings.Contains(serverOut.ErrStr, tc.ErrContains) {
			t.Errorf("server-runner node error %q missing %q", serverOut.ErrStr, tc.ErrContains)
		}
	}

	// Downstream routing assertions.
	for name, want := range tc.WantDownstream {
		localStatus, lok := localOut.DownstreamStatuses[name]
		serverStatus, sok := serverOut.DownstreamStatuses[name]
		if !lok {
			t.Errorf("local downstream node %q not found", name)
			continue
		}
		if !sok {
			t.Errorf("server-runner downstream node %q not found", name)
			continue
		}
		if localStatus != serverStatus {
			t.Errorf("downstream status parity for %q: local=%s server-runner=%s", name, localStatus, serverStatus)
		}
		if localStatus != want.Status {
			t.Errorf("local downstream %q status=%s, want %s", name, localStatus, want.Status)
		}
		if serverStatus != want.Status {
			t.Errorf("server-runner downstream %q status=%s, want %s", name, serverStatus, want.Status)
		}
		if want.Output != nil {
			localOutMap := localOut.DownstreamOutputs[name]
			serverOutMap := serverOut.DownstreamOutputs[name]
			if !mapContains(localOutMap, want.Output) {
				t.Errorf("local downstream %q output=%v, want superset of %v", name, localOutMap, want.Output)
			}
			if !mapContains(serverOutMap, want.Output) {
				t.Errorf("server-runner downstream %q output=%v, want superset of %v", name, serverOutMap, want.Output)
			}
		}
	}
}

func mapContains(got, want map[string]any) bool {
	if got == nil {
		return len(want) == 0
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// RunParityLocal runs the fixture through the local embedded topology and
// returns the terminal outcome. The register callback installs any custom
// handlers needed by the fixture; built-in node types resolve through the
// global node registry.
func RunParityLocal(t *testing.T, def *types.WorkflowDef, register func(engine.HandlerRegistrar)) ParityOutcome {
	t.Helper()
	b := backendlocal.New(backendlocal.WithConcurrency(1))
	reg, ok := b.Registry().(engine.HandlerRegistrar)
	if !ok {
		t.Fatalf("local backend registry does not implement HandlerRegistrar: %T", b.Registry())
	}
	if register != nil {
		register(reg)
	}
	eng := engine.New(b.State(), b.Queue(), engine.WithDefaultLeaseTTL(time.Minute))
	t.Cleanup(b.Bind(eng))

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("local compile: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("local submit: %v", err)
	}
	result, err := b.WaitTimeout(id, 10*time.Second)
	if err != nil {
		t.Fatalf("local wait: %v", err)
	}
	return collectParityOutcome(t, b.State(), id, result, def)
}

// RunParityServerRunner runs the same fixture through the server-runner
// topology against real Redis. The register callback installs custom handlers
// in the runner's execution.Registry; built-in node types resolve through the
// global node registry.
func RunParityServerRunner(t *testing.T, addr string, def *types.WorkflowDef, register func(engine.HandlerRegistrar)) ParityOutcome {
	t.Helper()
	h := newServerRunnerHarness(t, addr, 1)
	if len(def.Nodes) == 0 {
		t.Fatal("RunParityServerRunner: workflow has no nodes")
	}

	registry := execution.NewRegistry()
	if register != nil {
		register(registry)
	}

	capSet := make(map[string]struct{})
	for _, n := range def.Nodes {
		capSet[n.Type] = struct{}{}
	}
	caps := make([]protocol.Capability, 0, len(capSet))
	for nodeType := range capSet {
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-parity-1",
			Concurrency:  1,
			Capabilities: caps,
			PollWait:     5 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-parity-1")

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, nil)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, def.Nodes[0].Name)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	// Fresh context: the runner ctx above was cancelled on shutdown.
	return collectParityOutcome(t, h.state, execID, result, def)
}

func collectParityOutcome(t *testing.T, state engine.StateStore, execID types.ExecutionID, result types.Result, def *types.WorkflowDef) ParityOutcome {
	t.Helper()
	if len(def.Nodes) == 0 {
		t.Fatal("collectParityOutcome: workflow has no nodes")
	}
	sourceName := def.Nodes[0].Name

	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getCancel()
	node, err := state.GetNode(getCtx, execID, sourceName)
	if err != nil || node == nil {
		t.Fatalf("GetNode %q: %v (node=%v)", sourceName, err, node)
	}

	out := ParityOutcome{
		Attempt:            node.Attempt,
		Status:             result.Status,
		SourceStatus:       node.Status,
		ErrStr:             node.Error,
		Port:               node.Port,
		DownstreamStatuses: make(map[string]types.NodeStatus),
		DownstreamOutputs:  make(map[string]map[string]any),
	}

	for _, n := range def.Nodes {
		if n.Name == sourceName {
			continue
		}
		dn, err := state.GetNode(getCtx, execID, n.Name)
		if err != nil || dn == nil {
			continue
		}
		out.DownstreamStatuses[n.Name] = dn.Status
		if v, err := state.GetOutput(getCtx, execID, n.Name); err == nil && v != nil {
			out.DownstreamOutputs[n.Name] = v
		}
	}
	return out
}
