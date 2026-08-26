package execution

import (
	"context"
	"errors"
	"fmt"
	nodereg "github.com/xbcio/xflow/node/registry"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/types"
)

// versionedHandler is a minimal ActionHandler whose Descriptor.Type + custom
// NodeVersion() let us drive the global node registry into known states for
// these tests. Each test uses a unique Type so they don't collide.
type versionedHandler struct {
	typ     string
	version int
}

func (h versionedHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.typ}
}

func (h versionedHandler) NodeVersion() int { return h.version }

func (h versionedHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"v": h.version, "t": h.typ}}, nil
}

// recordingLogger captures the messages emitted by VersionWarnFallback.
type recordingLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *recordingLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.msgs))
	copy(out, l.msgs)
	return out
}

func TestRegistry_VersionExactMatchHitsRequestedHandler(t *testing.T) {
	const typ = "test.versioned/exact"
	nodereg.Register(versionedHandler{typ: typ, version: 1})
	nodereg.Register(versionedHandler{typ: typ, version: 2})

	r := NewRegistry()
	got, err := r.Get("exec-1", "node-a", typ, 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if vh, ok := got.(versionedHandler); !ok || vh.version != 1 {
		t.Fatalf("Get() returned wrong handler: %#v", got)
	}
}

func TestRegistry_VersionStrictRejectsMiss(t *testing.T) {
	const typ = "test.versioned/strict"
	nodereg.Register(versionedHandler{typ: typ, version: 1})

	r := NewRegistry()
	r.SetVersionPolicy(VersionStrict)
	_, err := r.Get("exec-1", "node-a", typ, 9)
	var mismatch *ErrHandlerVersionMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf("Get() error = %v, want ErrHandlerVersionMismatch", err)
	}
	if mismatch.RequestedVersion != 9 || mismatch.LatestAvailable != 1 {
		t.Fatalf("mismatch = %+v", mismatch)
	}
}

func TestRegistry_VersionWarnFallbackReturnsLatestAndLogs(t *testing.T) {
	const typ = "test.versioned/warn"
	nodereg.Register(versionedHandler{typ: typ, version: 1})
	nodereg.Register(versionedHandler{typ: typ, version: 3})

	logger := &recordingLogger{}
	r := NewRegistry()
	r.SetVersionPolicy(VersionWarnFallback)
	r.SetLogger(logger)

	got, err := r.Get("exec-1", "node-a", typ, 9)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	vh, ok := got.(versionedHandler)
	if !ok || vh.version != 3 {
		t.Fatalf("Get() returned %#v, want latest v3", got)
	}
	msgs := logger.messages()
	if len(msgs) != 1 {
		t.Fatalf("logger messages = %v, want 1", msgs)
	}
	// Counting the message proves the branch ran; it does not prove the message is
	// usable, and usable is the whole point -- this line is the only signal that a
	// workflow pinned to v9 is actually executing v3, and an operator acts on it by
	// grepping for the node it names. Swapping node_type and node_name (adjacent %s
	// args) keeps the count at one and sends them to a workflow that is fine.
	for _, want := range []string{
		"node_type=" + typ,
		"node_name=node-a",
		"requested_version=9",
		"resolved_version=3",
	} {
		if !strings.Contains(msgs[0], want) {
			t.Errorf("warn message %q does not contain %q", msgs[0], want)
		}
	}
}

func TestRegistry_VersionSilentFallbackReturnsLatestQuietly(t *testing.T) {
	const typ = "test.versioned/silent"
	nodereg.Register(versionedHandler{typ: typ, version: 2})
	nodereg.Register(versionedHandler{typ: typ, version: 4})

	logger := &recordingLogger{}
	r := NewRegistry()
	r.SetVersionPolicy(VersionSilentFallback)
	r.SetLogger(logger)

	got, err := r.Get("exec-1", "node-a", typ, 9)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	vh, ok := got.(versionedHandler)
	if !ok || vh.version != 4 {
		t.Fatalf("Get() returned %#v, want latest v4", got)
	}
	if msgs := logger.messages(); len(msgs) != 0 {
		t.Fatalf("logger messages = %v, want none under silent", msgs)
	}
}

func TestRegistry_StrictWithNoRegisteredHandlerReturnsLatestNegOne(t *testing.T) {
	r := NewRegistry()
	r.SetVersionPolicy(VersionStrict)
	_, err := r.Get("exec-1", "node-a", "test.versioned/missing", 2)
	var mismatch *ErrHandlerVersionMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf("Get() error = %v, want ErrHandlerVersionMismatch", err)
	}
	if mismatch.LatestAvailable != -1 {
		t.Fatalf("LatestAvailable = %d, want -1", mismatch.LatestAvailable)
	}
}

func TestRegistry_NoVersionRequestedFallsThroughToLatest(t *testing.T) {
	const typ = "test.versioned/no-pin"
	nodereg.Register(versionedHandler{typ: typ, version: 1})
	nodereg.Register(versionedHandler{typ: typ, version: 2})

	r := NewRegistry()
	r.SetVersionPolicy(VersionStrict) // still strict — but version=0 skips the gate
	got, err := r.Get("exec-1", "node-a", typ, 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	vh, ok := got.(versionedHandler)
	if !ok || vh.version != 2 {
		t.Fatalf("Get() returned %#v, want latest", got)
	}
}

func TestRegistry_LocalOverridesBypassVersionPolicy(t *testing.T) {
	const typ = "test.versioned/override"
	// Note: nothing registered globally — execution-scoped wins anyway.
	override := versionedHandler{typ: typ, version: 7}
	r := NewRegistry()
	r.SetVersionPolicy(VersionStrict)
	r.RegisterExecutionHandler("exec-1", "node-a", override)

	got, err := r.Get("exec-1", "node-a", typ, 99)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	vh, ok := got.(versionedHandler)
	if !ok || vh.version != 7 {
		t.Fatalf("Get() returned %#v, want override v7", got)
	}
}

// I1: RegisterExecutionHandler had no corresponding unregister, so every
// per-item collector registration (execution/subgraph/collector.go, one call
// per map-body ITEM via MapBodyExecutor.ExecuteBatchBody) accumulated in
// executionHandlers for the life of the process. This is the actual count
// assertion the finding requires: not "no panic", but the map back to its
// prior size after N register+unregister cycles.
func TestRegistry_UnregisterExecutionRemovesItsHandlers(t *testing.T) {
	r := NewRegistry()
	before := len(r.executionHandlers)

	const n = 1000
	for i := 0; i < n; i++ {
		id := types.ExecutionID(fmt.Sprintf("exec-%d", i))
		r.RegisterExecutionHandler(id, "step-a", versionedHandler{typ: "t", version: 1})
		r.RegisterExecutionHandler(id, "step-b", versionedHandler{typ: "t", version: 1})
		r.UnregisterExecution(id)
	}

	if got := len(r.executionHandlers); got != before {
		t.Fatalf("executionHandlers has %d entries after %d register+unregister cycles, want %d "+
			"(back to the size before any of them ran) -- entries are leaking", got, n, before)
	}
}

// UnregisterExecution must only remove ITS OWN execution's entries, never a
// different execution's -- a batch body executing concurrently with another
// must not have its collector torn down by an unrelated cleanup.
func TestRegistry_UnregisterExecutionLeavesOtherExecutionsIntact(t *testing.T) {
	r := NewRegistry()
	r.RegisterExecutionHandler("exec-keep", "node-a", versionedHandler{typ: "t", version: 1})
	r.RegisterExecutionHandler("exec-drop", "node-a", versionedHandler{typ: "t", version: 1})

	r.UnregisterExecution("exec-drop")

	if _, err := r.Get("exec-keep", "node-a", "t", 0); err != nil {
		t.Fatalf("Get() for the execution that was NOT unregistered failed: %v", err)
	}
	if _, ok := r.executionHandlers["exec-drop/node-a"]; ok {
		t.Fatal("exec-drop's handler is still registered after UnregisterExecution")
	}
}
