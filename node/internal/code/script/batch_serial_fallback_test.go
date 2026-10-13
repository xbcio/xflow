package script

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	scriptengine "github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/types"
)

// The engine behind these tests implements no BatchEngine, so a batch-shaped
// input takes ScriptNode's serial fallback branch. Its (language, runtime) pair
// is unique to this file on purpose: the warning under test fires once per pair
// per process, so borrowing a real runtime's name would make the assertion
// depend on which other test in this binary ran first. The language is "wasm"
// so the wasm-only migration hint is exercised too.
const (
	batchFallbackLanguage = "wasm"
	batchFallbackRuntime  = "batch-fallback-test"
)

type batchFallbackEngine struct{ calls atomic.Int64 }

func (e *batchFallbackEngine) Name() string {
	return batchFallbackLanguage + "/" + batchFallbackRuntime
}

func (e *batchFallbackEngine) Execute(_ context.Context, _ scriptengine.Source, globals map[string]any, _ scriptengine.Helpers) (any, error) {
	e.calls.Add(1)
	return map[string]any{"seen": globals["$input"]}, nil
}

var (
	sharedBatchFallbackEngine = &batchFallbackEngine{}
	batchFallbackRegisterOnce sync.Once
)

func batchFallbackNode() *ScriptNode {
	batchFallbackRegisterOnce.Do(func() {
		scriptengine.Register(batchFallbackLanguage, batchFallbackRuntime, func() scriptengine.Engine {
			return sharedBatchFallbackEngine
		})
	})
	return &ScriptNode{}
}

// batchFallbackObserver satisfies Observer through noopObserver and implements
// the optional BatchFallbackObserver extension, recording each fallback.
type batchFallbackObserver struct {
	noopObserver
	mu    sync.Mutex
	langs []string
	rtns  []string
}

func (o *batchFallbackObserver) OnScriptBatchSerialFallback(_ context.Context, language, runtime string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.langs = append(o.langs, language)
	o.rtns = append(o.rtns, runtime)
}

func (o *batchFallbackObserver) recorded() ([]string, []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.langs...), append([]string(nil), o.rtns...)
}

// warnRecorder is a slog.Handler that keeps every record it saw, so a test can
// assert both that the fallback warned and that it warned exactly once.
type warnRecorder struct {
	mu      sync.Mutex
	records []warnRecord
}

type warnRecord struct {
	message string
	attrs   map[string]any
}

func (h *warnRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, warnRecord{message: r.Message, attrs: attrs})
	h.mu.Unlock()
	return nil
}

func (h *warnRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnRecorder) WithGroup(string) slog.Handler      { return h }

func (h *warnRecorder) containing(substr string) []warnRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []warnRecord
	for _, r := range h.records {
		if strings.Contains(r.message, substr) {
			out = append(out, r)
		}
	}
	return out
}

// TestScriptNode_BatchSerialFallbackCountsAndWarnsOnce is the #28 contract for
// a runtime with no batch fast path: the batch still produces per-record
// results (via one Execute per record), the fallback counter rises once per
// batch execution, and the once-per-(language, runtime) warning is emitted on
// the first batch and NOT repeated on the second.
func TestScriptNode_BatchSerialFallbackCountsAndWarnsOnce(t *testing.T) {
	recorder := &warnRecorder{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(recorder))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	observer := &batchFallbackObserver{}
	SetObserver(observer)
	t.Cleanup(func() { SetObserver(nil) })

	node := batchFallbackNode()
	input := &types.Input{
		Params: map[string]any{
			"language": batchFallbackLanguage,
			"runtime":  batchFallbackRuntime,
			"code":     "never interpreted: the test engine only counts calls",
		},
		Data: map[string]any{
			"messages": []any{map[string]any{"v": 1}, map[string]any{"v": 2}},
			"count":    2,
		},
	}

	before := sharedBatchFallbackEngine.calls.Load()
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out.Port != "main" {
		t.Fatalf("port = %q, want main (data: %v)", out.Port, out.Data)
	}
	results, ok := out.Data["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("results = %#v, want 2 entries", out.Data["results"])
	}
	// The serial shape: one Execute call per record, not one call for the batch.
	if got := sharedBatchFallbackEngine.calls.Load() - before; got != 2 {
		t.Fatalf("engine Execute calls = %d, want 2 (one per record)", got)
	}

	langs, rtns := observer.recorded()
	if len(langs) != 1 {
		t.Fatalf("fallback observations = %v, want exactly 1\n"+
			"an engine with no BatchEngine must be counted on the serial branch", langs)
	}
	if langs[0] != batchFallbackLanguage || rtns[0] != batchFallbackRuntime {
		t.Fatalf("fallback reported (%q, %q), want (%q, %q)", langs[0], rtns[0], batchFallbackLanguage, batchFallbackRuntime)
	}

	warnings := recorder.containing("no batch fast path")
	if len(warnings) != 1 {
		t.Fatalf("fallback warnings = %d, want exactly 1\n%+v", len(warnings), warnings)
	}
	if warnings[0].attrs["language"] != batchFallbackLanguage || warnings[0].attrs["runtime"] != batchFallbackRuntime {
		t.Fatalf("warning attributes = %v, want language=%q runtime=%q", warnings[0].attrs, batchFallbackLanguage, batchFallbackRuntime)
	}
	if hint, _ := warnings[0].attrs["hint"].(string); !strings.Contains(hint, "wazero-reactor") {
		t.Fatalf("wasm fallback warning hint = %q, want the pooled runtime named", hint)
	}

	// Second batch on the same (language, runtime): the counter is per fallback
	// event and rises; the log line is per placement and must not repeat, or a
	// Kafka batch trigger would re-log it for every batch forever.
	if _, err := node.Execute(context.Background(), input); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	langs, _ = observer.recorded()
	if len(langs) != 2 {
		t.Fatalf("fallback observations = %d after the second batch, want 2", len(langs))
	}
	if got := len(recorder.containing("no batch fast path")); got != 1 {
		t.Fatalf("fallback warnings = %d after the second batch, want still 1", got)
	}
}

// TestWazeroLegacyRuntimeHasNoBatchEngine pins the fact the runtime description,
// the metric help and WASM-ENGINE-POOLING.md all state: the legacy command model
// implements no batch fast path. If this ever fails, wazero gained one and every
// one of those statements is stale.
func TestWazeroLegacyRuntimeHasNoBatchEngine(t *testing.T) {
	eng, ok := scriptengine.Lookup(wasmScriptLanguage, "wazero")
	if !ok {
		t.Fatalf("engine.Lookup(%q, %q) found no engine", wasmScriptLanguage, "wazero")
	}
	if _, hasBatch := eng.(scriptengine.BatchEngine); hasBatch {
		t.Fatal("wasm/wazero implements engine.BatchEngine; the ParamSpec description, the batch-serial " +
			"fallback metric help and WASM-ENGINE-POOLING.md §8 all document the opposite")
	}
}

// TestScriptDescriptorMarksWazeroAsBatchSerial pins the user-facing half of #28:
// the parameter panel must say which runtime takes the serial batch path, and
// the reactor option must say what it is for.
func TestScriptDescriptorMarksWazeroAsBatchSerial(t *testing.T) {
	desc := (&ScriptNode{}).Descriptor()

	runtimeDesc := ""
	for _, p := range desc.Params {
		if p.Name == "runtime" {
			runtimeDesc = p.Description
		}
	}
	if !strings.Contains(runtimeDesc, "batch") {
		t.Fatalf("runtime parameter description %q does not mention the batch fast path", runtimeDesc)
	}

	opts := wasmRuntimeOptions()
	byValue := make(map[any]string, len(opts))
	for _, o := range opts {
		byValue[o.Value] = o.Description
	}
	if got := byValue["wazero"]; !strings.Contains(got, "batch") {
		t.Fatalf("wazero option description %q does not warn about the batch path", got)
	}
	if got := byValue["wazero-reactor"]; !strings.Contains(got, "batch") {
		t.Fatalf("wazero-reactor option description %q does not name the batch fast path", got)
	}
}
