package rstate

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestMiniredisExecutionInputContextContract runs the shared narrow-input
// contract against the Redis backend under miniredis.
func TestMiniredisExecutionInputContextContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statestoretest.RunExecutionInputContextContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.ExecutionInputBackend {
		return statestoretest.ExecutionInputBackend{Store: New(rdb, nil, time.Minute), Namespaced: true}
	})
}

// TestRedisExecutionInputContextContract runs the same contract against a real
// Redis when XFLOW_TEST_REDIS_ADDR is set (use 127.0.0.1:6380).
func TestRedisExecutionInputContextContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunExecutionInputContextContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.ExecutionInputBackend {
		return statestoretest.ExecutionInputBackend{Store: New(freshRealRedis(t, addr), nil, time.Minute), Namespaced: true}
	})
}

// TestMiniredisExecutionTraceCarrierContract runs the shared carrier contract
// against the Redis backend under miniredis.
func TestMiniredisExecutionTraceCarrierContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statestoretest.RunExecutionTraceCarrierContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.ExecutionInputBackend {
		return statestoretest.ExecutionInputBackend{Store: New(rdb, nil, time.Minute), Namespaced: true}
	})
}

// TestRedisExecutionTraceCarrierContract runs the same contract against a real
// Redis when XFLOW_TEST_REDIS_ADDR is set.
func TestRedisExecutionTraceCarrierContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunExecutionTraceCarrierContract(t, rstateCapabilityManifest(), func(t *testing.T) statestoretest.ExecutionInputBackend {
		return statestoretest.ExecutionInputBackend{Store: New(freshRealRedis(t, addr), nil, time.Minute), Namespaced: true}
	})
}

// commandCapture is a go-redis hook that records the command stream: one entry
// per pipeline batch, plus every command issued on its own. Round-trip
// assertions cannot distinguish one six-GET pipeline from six sequential GETs by
// outcome; only the command stream can.
type commandCapture struct {
	mu        sync.Mutex
	pipelines [][]string
	singles   []string
}

func (h *commandCapture) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *commandCapture) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.recordSingle(cmd)
		return next(ctx, cmd)
	}
}

func (h *commandCapture) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.recordPipeline(cmds)
		return next(ctx, cmds)
	}
}

func (h *commandCapture) recordSingle(cmd redis.Cmder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.singles = append(h.singles, commandKey(cmd))
}

func (h *commandCapture) recordPipeline(cmds []redis.Cmder) {
	batch := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		batch = append(batch, commandKey(cmd))
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pipelines = append(h.pipelines, batch)
}

// commandKey renders "name key" for a command, which is what the round-trip
// assertions below compare.
func commandKey(cmd redis.Cmder) string {
	args := cmd.Args()
	if len(args) < 2 {
		return strings.ToLower(cmd.Name())
	}
	return strings.ToLower(cmd.Name()) + " " + fmt.Sprint(args[1])
}

func (h *commandCapture) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pipelines = nil
	h.singles = nil
}

func (h *commandCapture) snapshot() ([][]string, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pipelines := make([][]string, len(h.pipelines))
	for i := range h.pipelines {
		pipelines[i] = append([]string(nil), h.pipelines[i]...)
	}
	return pipelines, append([]string(nil), h.singles...)
}

// TestGetExecutionInputContextIssuesOnePipeline pins the read budget: the six
// keys buildInput needs (status, params, runtime, scope, and the trace identity
// a sub-execution re-submits with) are one pipeline of six GETs and zero
// sequential commands.
func TestGetExecutionInputContextIssuesOnePipeline(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	hook := &commandCapture{}
	rdb.AddHook(hook)

	ctx := context.Background()
	id := types.ExecutionID("exec-input-pipeline")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:      id,
		Graph:   statestoretest.ContractGraph(),
		Status:  types.ExecutionStatusRunning,
		Params:  map[string]any{"claim_id": "c-1"},
		Runtime: &types.Runtime{Vars: map[string]any{"tenant": "acme"}},
		Scope:   map[string]any{"$index": float64(1)},
		TraceID: "trace-input-pipeline",
		SpanID:  "span-input-pipeline",
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	hook.reset()

	narrow, found, err := state.GetExecutionInputContext(ctx, id)
	if err != nil {
		t.Fatalf("GetExecutionInputContext() error = %v", err)
	}
	if !found {
		t.Fatal("GetExecutionInputContext() found=false for a created execution")
	}
	if narrow.Status != types.ExecutionStatusRunning {
		t.Fatalf("GetExecutionInputContext().Status = %q, want running", narrow.Status)
	}

	pipelines, singles := hook.snapshot()
	if len(singles) != 0 {
		t.Fatalf("GetExecutionInputContext issued %d sequential commands (%v), want zero: the whole read must be one pipeline", len(singles), singles)
	}
	if len(pipelines) != 1 {
		t.Fatalf("GetExecutionInputContext issued %d pipelines, want exactly 1", len(pipelines))
	}
	want := []string{
		"get xflow:ns:default:exec:{" + string(id) + "}:status",
		"get xflow:ns:default:exec:{" + string(id) + "}:params",
		"get xflow:ns:default:exec:{" + string(id) + "}:runtime",
		"get xflow:ns:default:exec:{" + string(id) + "}:scope",
		"get xflow:ns:default:exec:{" + string(id) + "}:trace_id",
		"get xflow:ns:default:exec:{" + string(id) + "}:span_id",
	}
	got := append([]string(nil), pipelines[0]...)
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("pipeline commands =\n%v\nwant\n%v", got, want)
	}
}

// TestGetExecutionInputContextAbsentIsStillOnePipeline pins the absent path: a
// missing execution is answered from the same single pipeline (the status key's
// redis.Nil), not by a second probe.
func TestGetExecutionInputContextAbsentIsStillOnePipeline(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	hook := &commandCapture{}
	rdb.AddHook(hook)
	// Warm the connection first: go-redis issues its HELLO/CLIENT handshake on
	// the first command, and those would otherwise be recorded as the read's own
	// sequential commands.
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	hook.reset()

	narrow, found, err := state.GetExecutionInputContext(context.Background(), types.ExecutionID("exec-input-pipeline-gone"))
	if err != nil {
		t.Fatalf("GetExecutionInputContext(absent) error = %v", err)
	}
	if found || narrow != nil {
		t.Fatalf("GetExecutionInputContext(absent) = (%#v, %v), want (nil, false)", narrow, found)
	}
	pipelines, singles := hook.snapshot()
	if len(singles) != 0 {
		t.Fatalf("absent read issued %d sequential commands (%v), want zero", len(singles), singles)
	}
	if len(pipelines) != 1 {
		t.Fatalf("absent read issued %d pipelines, want exactly 1", len(pipelines))
	}
	if len(pipelines[0]) != 6 {
		t.Fatalf("absent read pipeline carried %d commands (%v), want the same 6-key read", len(pipelines[0]), pipelines[0])
	}
}

// TestGetExecutionTraceCarrierIssuesOneGet pins the carrier read: one GET of the
// single carrier key, decoded into map[string]string.
func TestGetExecutionTraceCarrierIssuesOneGet(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	hook := &commandCapture{}
	rdb.AddHook(hook)

	ctx := context.Background()
	id := types.ExecutionID("exec-carrier-pipeline")
	want := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=opaque",
	}
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:           id,
		Graph:        statestoretest.ContractGraph(),
		Status:       types.ExecutionStatusRunning,
		TraceCarrier: want,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	hook.reset()

	got, found, err := state.GetExecutionTraceCarrier(ctx, id)
	if err != nil {
		t.Fatalf("GetExecutionTraceCarrier() error = %v", err)
	}
	if !found {
		t.Fatal("GetExecutionTraceCarrier() found=false for an execution with a carrier")
	}
	if len(got) != len(want) || got["traceparent"] != want["traceparent"] || got["tracestate"] != want["tracestate"] {
		t.Fatalf("GetExecutionTraceCarrier() = %#v, want %#v", got, want)
	}

	pipelines, singles := hook.snapshot()
	if len(pipelines) != 0 {
		t.Fatalf("GetExecutionTraceCarrier issued %d pipelines (%v), want none: the read is a single GET", len(pipelines), pipelines)
	}
	wantCmd := "get xflow:ns:default:exec:{" + string(id) + "}:trace_carrier"
	if len(singles) != 1 || singles[0] != wantCmd {
		t.Fatalf("GetExecutionTraceCarrier commands = %v, want exactly [%q]", singles, wantCmd)
	}
}
