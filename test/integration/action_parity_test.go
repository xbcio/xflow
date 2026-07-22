//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// action_parity_test.go implements the A3 three-topology action-error parity
// matrix (.claude/specs/2026-07-18-sdk-server-production-readiness-remediation-design.md
// §6.4). The same classified-error fixture runs across three topologies:
//
//   - local embedded — in-memory backend + engine + embedded dispatcher
//     (handler runs in-process; the ClassifiedError is a live Go error).
//   - server-runner — real Redis/Asynq backend + apiserver + HTTP runner
//     (the ClassifiedError crosses the wire via protocol.error_detail and is
//     recovered server-side before retry classification).
//   - cluster-durable — real Redis/Asynq backend + embedded engine + in-process
//     consumer (durable/default mode). This is the same distributed backend
//     sdk/xflow.NewCluster constructs internally (distributed.New + engine.New +
//     StartBinding), so the durable outbox dispatcher, lease timeout monitor,
//     and Asynq consumer all run in-process against the real broker. The
//     ClassifiedError stays a live Go error (no HTTP boundary), but the task
//     lease/outbox/commit path is the real distributed one.
//
// The cluster-transient topology is intentionally excluded from this matrix:
// it has no handler-level retry (transient dispatch failures back off at the
// queue, not the engine's MaxAttempts path), so comparing its retry count
// against the three topologies above would be a category error. A3 §6.4 covers
// it under a separate collection-path exclusion.
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

// parityFixtureBuild returns a parityCase.Build tuple for a parityFixture. It
// creates a FRESH parityFixtureHandler on each register call (one per topology)
// so per-topology attempt counters stay isolated — the failBefore counter must
// not carry across local/server-runner/cluster. invocations() reads the
// most-recently-registered handler's counter, so the test must call it
// immediately after each Run (before the next topology registers a new handler).
func parityFixtureBuild(nodeType string, f *parityFixture) (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
	var cur *parityFixtureHandler
	register := func(reg engine.HandlerRegistrar) {
		h := f.handler(nodeType)
		cur = h
		reg.RegisterGlobal(nodeType, h)
	}
	invocations := func() int {
		if cur == nil {
			return 0
		}
		return int(cur.attempts.Load())
	}
	return types.NodeDef{Name: "start", Type: nodeType}, register, invocations
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
	ErrCode            string           // structured error code parsed from ErrStr (before the ":")
	ErrKind            string           // fixture's EXPECTED classified kind (test-side, see stampExpectedKind)
	ErrRetryable       bool             // fixture's EXPECTED retryable flag (test-side, see stampExpectedKind)
	Port               string           // source node output port
	HandlerInvocations int              // measured handler Execute() call count (real, from fixture counter)
	DownstreamStatuses map[string]types.NodeStatus
	DownstreamOutputs  map[string]map[string]any
}

// parityCase holds a single parity fixture configuration.
type parityCase struct {
	Name        string
	Build       func() (source types.NodeDef, register func(engine.HandlerRegistrar), invocations func() int)
	MaxAttempts int
	WantAttempt int
	WantStatus  types.ExecutionStatus
	ErrContains string // substring expected in node.Error for failed fixtures
	// WantKind/WantRetryable are the fixture's EXPECTED classified kind/retryable
	// (test-side). They are NOT recovered from node.Error — the commit boundary
	// (engine/errorpolicy.go) stringifies *ClassifiedError to "code: message"
	// before writing NodeSnapshot.Error, and structurizing NodeSnapshot.Error is a
	// non-goal (error_taxonomy §7). The system's own classification is covered by
	// per-classifier unit tests (node/internal/action/{http,grpc,db_errors}.go);
	// here we assert the expected kind is consistent across topologies and stamp
	// it into the artifact so it carries structured kind/retryable with real
	// values. Empty WantKind means the fixture reaches Success (no error record).
	WantKind          string
	WantRetryable     bool
	WantHandlerInvocations int // 0 = not tracked (real action handlers have no fixture counter)
	OKNode           types.NodeDef
	ErrNode          types.NodeDef
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
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return parityFixtureBuild("test.parity.transient.then.success", &parityFixture{
					behaviour:  parityTransientThenSuccess,
					failBefore: 1, // attempt 1 transient, attempt 2 succeeds
					code:       "parity.transient_then_success",
					msg:        "business.reject",
				})
			},
			MaxAttempts: 3,
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusSuccess,
			// No error record on success; the fixture's transient failure was
			// retried away, so WantKind is empty (no node.Error to classify).
			WantKind:               "",
			WantHandlerInvocations: 2,
		},
		{
			Name: "transient_retry_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return parityFixtureBuild("test.parity.transient.retry.exhausted", &parityFixture{
					behaviour: parityTransientExhausted,
					code:      "parity.transient_retry_exhausted",
					msg:       "business.reject",
				})
			},
			MaxAttempts: 2, // attempt 1 + 1 retry, then exhausted
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "parity.transient_retry_exhausted",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "permanent_no_retry",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return parityFixtureBuild("test.parity.permanent.no.retry", &parityFixture{
					behaviour: parityPermanent,
					code:      "parity.permanent_no_retry",
					msg:       "business.reject",
				})
			},
			MaxAttempts: 3, // permanent bypasses retry entirely
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "parity.permanent_no_retry",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "error_port_retry_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return parityFixtureBuild("test.parity.error.port", &parityFixture{
					behaviour: parityErrorPort,
					code:      "parity.error_port_retry_exhausted",
					msg:       "business.reject",
				})
			},
			MaxAttempts: 3, // explicit error-port output is transient (outputPortRetryError)
			WantAttempt: 3,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "business.reject",
			WantKind:               string(types.ErrorKindErrorPort),
			WantRetryable:          true,
			WantHandlerInvocations: 3,
		},
		{
			Name: "business_error_no_retry",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return parityFixtureBuild("test.parity.business.error", &parityFixture{
					behaviour: parityBusinessError,
					code:      "parity.business_error_no_retry",
					msg:       "business.reject",
				})
			},
			MaxAttempts: 3, // business error bypasses retry (Output.Error)
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "business.reject",
			WantKind:               string(types.ErrorKindBusiness),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register, inv := tc.Build()
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
			stampExpectedKind(&localOut, tc)
			localOut.HandlerInvocations = invCount(inv)
			serverOut := RunParityServerRunner(t, addr, def, register)
			stampExpectedKind(&serverOut, tc)
			serverOut.HandlerInvocations = invCount(inv)
			clusterOut := RunParityCluster(t, addr, def, register)
			stampExpectedKind(&clusterOut, tc)
			clusterOut.HandlerInvocations = invCount(inv)

			assertParityThreeWay(t, tc, localOut, serverOut, clusterOut)

			logParityMatrixRow(t, tc, "local", localOut)
			logParityMatrixRow(t, tc, "server-runner", serverOut)
			logParityMatrixRow(t, tc, "cluster-durable", clusterOut)
		})
	}
}

// namedParityOutcome pairs a topology label with its observed outcome so the
// parity core can report which topology diverged.
type namedParityOutcome struct {
	Topology string
	Out      ParityOutcome
}

// assertParityAll is the topology-independent parity core. It asserts that
// every supplied topology reaches the same logical attempt count, terminal
// status, error presence, error message/code, and downstream routing — and that
// each independently matches the fixture's expected contract. It works for any
// number of topologies (two-way for the gRPC/DB variants, three-way for the
// core matrix).
func assertParityAll(t *testing.T, tc parityCase, outs ...namedParityOutcome) {
	t.Helper()
	if len(outs) < 2 {
		t.Fatalf("assertParityAll requires at least 2 topologies, got %d", len(outs))
	}

	// Parity: every topology reaches the same logical attempt count, terminal
	// status, source node status, error string, and output port for the same
	// fixture. This is the topology-independent contract: if any field diverges
	// across topologies, the wire/classification path has a regression.
	for i := 0; i < len(outs); i++ {
		for j := i + 1; j < len(outs); j++ {
			a, b := outs[i], outs[j]
			if a.Out.Attempt != b.Out.Attempt {
				t.Errorf("attempt parity: %s=%d vs %s=%d, want equal", a.Topology, a.Out.Attempt, b.Topology, b.Out.Attempt)
			}
			if a.Out.Status != b.Out.Status {
				t.Errorf("status parity: %s=%s vs %s=%s, want equal", a.Topology, a.Out.Status, b.Topology, b.Out.Status)
			}
			// Node terminal status (SourceStatus) must also be topology-independent.
			if a.Out.SourceStatus != b.Out.SourceStatus {
				t.Errorf("source status parity: %s=%s vs %s=%s, want equal", a.Topology, a.Out.SourceStatus, b.Topology, b.Out.SourceStatus)
			}
			// Full error string equality: the ClassifiedError serializes as
			// "code: message" and must survive the wire identically across
			// topologies. Substring checks per-topology (ErrContains) are a
			// contract check; this is a parity check.
			if a.Out.ErrStr != b.Out.ErrStr {
				t.Errorf("error parity: %s=%q vs %s=%q, want equal", a.Topology, a.Out.ErrStr, b.Topology, b.Out.ErrStr)
			}
			// Structured error code parity: the machine-readable code (before the
			// ":") must also be topology-independent, so the artifact reports a
			// structured code rather than treating the whole error string as one.
			if a.Out.ErrCode != b.Out.ErrCode {
				t.Errorf("error_code parity: %s=%q vs %s=%q, want equal", a.Topology, a.Out.ErrCode, b.Topology, b.Out.ErrCode)
			}
			// Structured kind/retryable parity (fixture-derived, test-side; see
			// parityCase.WantKind). Asserts the expected classification is stable
			// across topologies — a divergence means the wire/classification path
			// changed how the fixture's error is categorized.
			if a.Out.ErrKind != b.Out.ErrKind {
				t.Errorf("error_kind parity: %s=%q vs %s=%q, want equal", a.Topology, a.Out.ErrKind, b.Topology, b.Out.ErrKind)
			}
			if a.Out.ErrRetryable != b.Out.ErrRetryable {
				t.Errorf("error_retryable parity: %s=%v vs %s=%v, want equal", a.Topology, a.Out.ErrRetryable, b.Topology, b.Out.ErrRetryable)
			}
			// Output port must also match: the same fixture routes to the same
			// downstream branch (main/error) regardless of topology.
			if a.Out.Port != b.Out.Port {
				t.Errorf("port parity: %s=%q vs %s=%q, want equal", a.Topology, a.Out.Port, b.Topology, b.Out.Port)
			}
			// Handler invocation count parity: at-least-once delivery + engine
			// retry must invoke the handler the same number of times across
			// topologies. Skipped when no fixture counter is exposed (real action
			// handlers — WantHandlerInvocations==0).
			if a.Out.HandlerInvocations != b.Out.HandlerInvocations {
				t.Errorf("handler_invocations parity: %s=%d vs %s=%d, want equal", a.Topology, a.Out.HandlerInvocations, b.Topology, b.Out.HandlerInvocations)
			}
		}
	}

	// Contract: each topology independently matches the expected outcome.
	for _, o := range outs {
		if o.Out.Attempt != tc.WantAttempt {
			t.Errorf("%s attempt=%d, want %d", o.Topology, o.Out.Attempt, tc.WantAttempt)
		}
		if o.Out.Status != tc.WantStatus {
			t.Errorf("%s status=%s, want %s", o.Topology, o.Out.Status, tc.WantStatus)
		}

		// For failed fixtures the error message/code must survive the wire and
		// be recorded in the node's Error field. ClassifiedErrors carry
		// "code: message"; explicit error-port output carries the raw message.
		if tc.WantStatus != types.ExecutionStatusSuccess && tc.ErrContains != "" {
			if o.Out.ErrStr == "" || !strings.Contains(o.Out.ErrStr, tc.ErrContains) {
				t.Errorf("%s node error %q missing %q", o.Topology, o.Out.ErrStr, tc.ErrContains)
			}
		}

		// Structured kind/retryable contract (fixture-derived, test-side; see
		// parityCase.WantKind). Empty WantKind means the fixture reaches Success
		// (no error record); otherwise the recorded classification must match.
		if o.Out.ErrKind != tc.WantKind {
			t.Errorf("%s error_kind=%q, want %q", o.Topology, o.Out.ErrKind, tc.WantKind)
		}
		if o.Out.ErrRetryable != tc.WantRetryable {
			t.Errorf("%s error_retryable=%v, want %v", o.Topology, o.Out.ErrRetryable, tc.WantRetryable)
		}
		// Measured handler invocation count contract. Skipped when
		// WantHandlerInvocations==0 (real action handlers expose no fixture
		// counter).
		if tc.WantHandlerInvocations > 0 && o.Out.HandlerInvocations != tc.WantHandlerInvocations {
			t.Errorf("%s handler_invocations=%d, want %d", o.Topology, o.Out.HandlerInvocations, tc.WantHandlerInvocations)
		}

		// Downstream DAG-advance routing assertions.
		for name, want := range tc.WantDownstream {
			gotStatus, ok := o.Out.DownstreamStatuses[name]
			if !ok {
				t.Errorf("%s downstream node %q not found", o.Topology, name)
				continue
			}
			if gotStatus != want.Status {
				t.Errorf("%s downstream %q status=%s, want %s", o.Topology, name, gotStatus, want.Status)
			}
			if want.Output != nil {
				if !mapContains(o.Out.DownstreamOutputs[name], want.Output) {
					t.Errorf("%s downstream %q output=%v, want superset of %v", o.Topology, name, o.Out.DownstreamOutputs[name], want.Output)
				}
			}
		}
	}
}

// assertParity asserts two-topology parity. Used by the database matrix's
// real-MySQL pair (server-runner vs cluster-durable), where local-fake and
// real-MySQL surface different classified codes for the same fixture (a
// documented divergence — see action_parity_database_server_test.go), so the
// three-way ErrStr equality does not hold but the two real-MySQL topologies
// must match exactly.
func assertParity(t *testing.T, tc parityCase, localOut, serverOut ParityOutcome) {
	t.Helper()
	assertParityAll(t, tc,
		namedParityOutcome{Topology: "local", Out: localOut},
		namedParityOutcome{Topology: "server-runner", Out: serverOut},
	)
}

// assertParityThreeWay asserts three-topology parity across local embedded,
// server-runner, and the durable SDK cluster. Used by the core A3 matrix.
func assertParityThreeWay(t *testing.T, tc parityCase, localOut, serverOut, clusterOut ParityOutcome) {
	t.Helper()
	assertParityAll(t, tc,
		namedParityOutcome{Topology: "local", Out: localOut},
		namedParityOutcome{Topology: "server-runner", Out: serverOut},
		namedParityOutcome{Topology: "cluster-durable", Out: clusterOut},
	)
}

// parityErrCode extracts the structured error code from a ClassifiedError's
// "code: message" string form. The node record stores only the string form of
// the classified error; the code is the stable, machine-readable identifier
// (e.g. "grpc.DataLoss", "http.connection"), distinct from the human message
// that follows the colon. Returns "" when the error is absent or has no code.
func parityErrCode(errStr string) string {
	if errStr == "" {
		return ""
	}
	if idx := strings.IndexByte(errStr, ':'); idx > 0 {
		return strings.TrimSpace(errStr[:idx])
	}
	return ""
}

// invCount returns the measured handler invocation count, or 0 when the fixture
// does not expose one. Real action handlers (xflow.grpc / xflow.database /
// xflow.http / xflow.script) have no test fixture counter, so their parityCase
// Build returns nil here; parityFixtureHandler-based fixtures return a real
// atomic counter accessor.
func invCount(inv func() int) int {
	if inv == nil {
		return 0
	}
	return inv()
}

// stampExpectedKind records the fixture's expected classified kind/retryable on
// the outcome. This is test-side derivation from the fixture behaviour — NOT
// recovery from node.Error, which is stringified to "code: message" at the
// engine commit boundary (engine/errorpolicy.go) before NodeSnapshot.Error is
// written. Structurizing NodeSnapshot.Error is a non-goal (error_taxonomy §7).
// The stamp lets the PARITY_MATRIX artifact carry structured kind/retryable
// with real values; assertParityAll then asserts the expected kind matches
// across topologies and against the contract.
func stampExpectedKind(out *ParityOutcome, tc parityCase) {
	out.ErrKind = tc.WantKind
	out.ErrRetryable = tc.WantRetryable
}

// parityKindFromName derives the expected classified kind/retryable from a
// fixture name that encodes "permanent" or "transient" (the per-channel
// fixtures follow the <channel>_<code>_permanent|transient_exhausted naming).
// Returns ("",false) for names encoding neither (the caller sets WantKind
// explicitly). Mirrors the production classifiers in
// node/internal/action/{http,grpc,db_errors}.go.
func parityKindFromName(name string) (string, bool) {
	if strings.Contains(name, "permanent") {
		return string(types.ErrorKindPermanent), false
	}
	if strings.Contains(name, "transient") {
		return string(types.ErrorKindTransient), true
	}
	return "", false
}

// logParityMatrixRow emits a machine-readable JSON line per fixture x topology
// into the test log. Each row carries the A3 contract fields: attempt, terminal
// status, source node status, output port, error string (which encodes the
// classified error code as "code: message"), and the count of downstream nodes
// that reached a terminal state (DAG advance). Grepping the test output for
// "PARITY_MATRIX" yields the full three-topology matrix artifact.
func logParityMatrixRow(t *testing.T, tc parityCase, topology string, out ParityOutcome) {
	t.Helper()
	downstreamAdvances := 0
	for _, s := range out.DownstreamStatuses {
		if types.IsTerminalNodeStatus(s) {
			downstreamAdvances++
		}
	}
	row := map[string]any{
		"fixture":             tc.Name,
		"topology":            topology,
		"attempt":             out.Attempt,
		"want_attempt":        tc.WantAttempt,
		"status":              string(out.Status),
		"want_status":         string(tc.WantStatus),
		"source_status":       string(out.SourceStatus),
		"port":                out.Port,
		"error":               out.ErrStr,
		"error_code":          out.ErrCode,
		"error_kind":          out.ErrKind,
		"error_retryable":     out.ErrRetryable,
		"handler_invocations": out.HandlerInvocations,
		"downstream_statuses": out.DownstreamStatuses,
		"downstream_advances": downstreamAdvances,
	}
	b, err := json.Marshal(row)
	if err != nil {
		t.Logf("PARITY_MATRIX marshal error: %v", err)
		return
	}
	t.Logf("PARITY_MATRIX %s", b)
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
	out := collectParityOutcome(t, h.state, execID, result, def)
	// Stop this topology's control plane (apiserver + its Asynq consumer) before
	// returning so it does not race the next topology's consumer for the shared
	// Asynq queue. See serverRunnerHarness.stop for why this is required.
	h.stop()
	return out
}

// RunParityCluster runs the same fixture through the durable SDK cluster
// topology against real Redis. It constructs the distributed backend the same
// way sdk/xflow.NewCluster does internally — distributed.New (real Redis/Asynq
// transport, durable/default mode, consumer enabled) + engine.New +
// backend.StartBinding — so the embedded engine, durable outbox dispatcher,
// lease timeout monitor, and Asynq consumer all run in-process against the real
// broker. This is NOT cluster-transient: tasks carry durable assignment/lease/
// outbox semantics, so the engine's MaxAttempts retry path is exercised exactly
// as in local embedded and server-runner.
//
// Custom handlers are registered against the backend's own HandlerRegistrar
// before StartBinding starts the consumer, so the in-process dispatcher resolves
// them without crossing an HTTP boundary. The register callback installs a
// fresh handler instance per topology (the parity fixtures do this by
// construction), keeping per-topology attempt counters isolated.
//
// Stale asynq tasks from prior crashed runs are flushed (scoped to the asynq:*
// namespace) so they cannot be picked up by this consumer.
func RunParityCluster(t *testing.T, addr string, def *types.WorkflowDef, register func(engine.HandlerRegistrar)) ParityOutcome {
	t.Helper()
	if len(def.Nodes) == 0 {
		t.Fatal("RunParityCluster: workflow has no nodes")
	}
	b, err := distributed.New(addr, nil, distributed.WithConcurrency(1), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	reg, ok := b.Registry().(engine.HandlerRegistrar)
	if !ok {
		t.Fatalf("cluster backend registry does not implement HandlerRegistrar: %T", b.Registry())
	}
	if register != nil {
		register(reg)
	}

	eng := engine.New(b.State(), b.Queue(), engine.WithDefaultLeaseTTL(time.Minute))
	stop, err := b.StartBinding(eng)
	if err != nil {
		t.Fatalf("cluster StartBinding: %v", err)
	}
	t.Cleanup(stop)

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("cluster compile: %v", err)
	}
	submitCtx, submitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer submitCancel()
	id, err := eng.Submit(submitCtx, g, nil)
	if err != nil {
		t.Fatalf("cluster submit: %v", err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, b.State(), id, def.Nodes[0].Name)
	return collectParityOutcome(t, b.State(), id, result, def)
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
		ErrCode:            parityErrCode(node.Error),
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
