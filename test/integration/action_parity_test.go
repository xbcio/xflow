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
	failBefore int32 // transient failures before success (transient_then_success only)
	code       string
	msg        string // error message for business_error / error_port
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

// parityOutcome is the topology-independent contract a fixture must reach.
type parityOutcome struct {
	attempt int
	status  types.ExecutionStatus
	errStr  string // node.Error (ClassifiedError.Error() == "code: message")
}

func parityWorkflow(nodeType string, maxAttempts int) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "action-parity-" + nodeType,
		Nodes: []types.NodeDef{{
			Name: "start",
			Type: nodeType,
			// InitialInterval 50ms keeps the matrix fast while still exercising
			// the real retryBackoff + delayed-outbox delivery path.
			Retry: &types.RetrySettings{
				MaxAttempts:     maxAttempts,
				InitialInterval: 50,
			},
		}},
	}
}

func TestActionErrorParityMatrix(t *testing.T) {
	addr := requireRedis(t)

	cases := []struct {
		name        string
		behaviour   string
		failBefore  int32
		maxAttempts int
		wantAttempt int
		wantStatus  types.ExecutionStatus
		wantErr     bool
		errContains string // substring expected in node.Error for failed fixtures
	}{
		{
			name:        "transient_then_success",
			behaviour:   parityTransientThenSuccess,
			failBefore:  1, // attempt 1 transient, attempt 2 succeeds
			maxAttempts: 3,
			wantAttempt: 2,
			wantStatus:  types.ExecutionStatusSuccess,
			wantErr:     false,
			errContains: "",
		},
		{
			name:        "transient_retry_exhausted",
			behaviour:   parityTransientExhausted,
			maxAttempts: 2, // attempt 1 + 1 retry, then exhausted
			wantAttempt: 2,
			wantStatus:  types.ExecutionStatusFailed,
			wantErr:     true,
			errContains: "",
		},
		{
			name:        "permanent_no_retry",
			behaviour:   parityPermanent,
			maxAttempts: 3, // permanent bypasses retry entirely
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusFailed,
			wantErr:     true,
			errContains: "",
		},
		{
			name:        "error_port_retry_exhausted",
			behaviour:   parityErrorPort,
			maxAttempts: 3, // explicit error-port output is transient (outputPortRetryError)
			wantAttempt: 3,
			wantStatus:  types.ExecutionStatusFailed,
			wantErr:     true,
			errContains: "business.reject",
		},
		{
			name:        "business_error_no_retry",
			behaviour:   parityBusinessError,
			maxAttempts: 3, // business error bypasses retry (Output.Error)
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusFailed,
			wantErr:     true,
			errContains: "business.reject",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodeType := "test.parity." + strings.ReplaceAll(tc.name, "_", ".")
			code := "parity." + tc.name
			def := parityWorkflow(nodeType, tc.maxAttempts)

			localHandler := &parityFixtureHandler{
				nodeType: nodeType, behaviour: tc.behaviour,
				failBefore: tc.failBefore, code: code, msg: "business.reject",
			}
			serverHandler := &parityFixtureHandler{
				nodeType: nodeType, behaviour: tc.behaviour,
				failBefore: tc.failBefore, code: code, msg: "business.reject",
			}

			localOut := runParityLocal(t, def, localHandler)
			serverOut := runParityServerRunner(t, addr, def, serverHandler, nodeType)

			// Parity: both topologies reach the same logical attempt count and
			// terminal status for the same fixture.
			if localOut.attempt != serverOut.attempt {
				t.Errorf("attempt parity: local=%d server-runner=%d, want equal", localOut.attempt, serverOut.attempt)
			}
			if localOut.status != serverOut.status {
				t.Errorf("status parity: local=%s server-runner=%s, want equal", localOut.status, serverOut.status)
			}
			localHasErr, serverHasErr := localOut.errStr != "", serverOut.errStr != ""
			if localHasErr != serverHasErr {
				t.Errorf("error presence parity: local=%v server-runner=%v, want equal", localHasErr, serverHasErr)
			}
			// Contract: each topology independently matches the expected outcome.
			if localOut.attempt != tc.wantAttempt {
				t.Errorf("local attempt=%d, want %d", localOut.attempt, tc.wantAttempt)
			}
			if localOut.status != tc.wantStatus {
				t.Errorf("local status=%s, want %s", localOut.status, tc.wantStatus)
			}
			if serverOut.attempt != tc.wantAttempt {
				t.Errorf("server-runner attempt=%d, want %d", serverOut.attempt, tc.wantAttempt)
			}
			if serverOut.status != tc.wantStatus {
				t.Errorf("server-runner status=%s, want %s", serverOut.status, tc.wantStatus)
			}
			// For failed fixtures the error message/code must survive the wire and
			// be recorded in the node's Error field. ClassifiedErrors carry
			// "code: message"; explicit error-port output carries the raw message.
			if tc.wantErr {
				wantSubstr := tc.errContains
				if wantSubstr == "" {
					wantSubstr = code
				}
				if !localHasErr || !strings.Contains(localOut.errStr, wantSubstr) {
					t.Errorf("local node error %q missing %q", localOut.errStr, wantSubstr)
				}
				if !serverHasErr || !strings.Contains(serverOut.errStr, wantSubstr) {
					t.Errorf("server-runner node error %q missing %q", serverOut.errStr, wantSubstr)
				}
			}
		})
	}
}

// runParityLocal runs the fixture through the local embedded topology and
// returns the terminal outcome. The handler runs in-process via the embedded
// dispatcher, so the ClassifiedError never serializes.
func runParityLocal(t *testing.T, def *types.WorkflowDef, handler *parityFixtureHandler) parityOutcome {
	t.Helper()
	b := backendlocal.New(backendlocal.WithConcurrency(1))
	reg, ok := b.Registry().(engine.HandlerRegistrar)
	if !ok {
		t.Fatalf("local backend registry does not implement HandlerRegistrar: %T", b.Registry())
	}
	reg.RegisterGlobal(handler.nodeType, handler)
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
	node, err := b.State().GetNode(ctx, id, "start")
	if err != nil || node == nil {
		t.Fatalf("local GetNode: %v (node=%v)", err, node)
	}
	return parityOutcome{attempt: node.Attempt, status: result.Status, errStr: node.Error}
}

// runParityServerRunner runs the same fixture through the server-runner
// topology against real Redis. The handler runs in a runner process and its
// ClassifiedError crosses the wire via protocol.error_detail, recovered
// server-side before retry classification.
func runParityServerRunner(t *testing.T, addr string, def *types.WorkflowDef, handler *parityFixtureHandler, nodeType string) parityOutcome {
	t.Helper()
	h := newServerRunnerHarness(t, addr, 1)

	registry := execution.NewRegistry()
	registry.RegisterGlobal(nodeType, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-parity-1",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: nodeType}},
			PollWait:     5 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-parity-1")

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, nil)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, "start")

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
	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getCancel()
	node, err := h.state.GetNode(getCtx, execID, "start")
	if err != nil || node == nil {
		t.Fatalf("server-runner GetNode: %v (node=%v)", err, node)
	}
	return parityOutcome{attempt: node.Attempt, status: result.Status, errStr: node.Error}
}
