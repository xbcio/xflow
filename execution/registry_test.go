package execution

import (
	"context"
	"errors"
	"fmt"
	nodereg "github.com/xbcio/xflow/node/registry"
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
	if msgs := logger.messages(); len(msgs) != 1 {
		t.Fatalf("logger messages = %v, want 1", msgs)
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
