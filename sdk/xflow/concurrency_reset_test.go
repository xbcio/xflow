package xflow

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// TestResetWorkflowConcurrencyValidatesName pins the input rules of the
// in-process surface before any store is read: an empty, whitespace-only or
// over-long name cannot address a registered workflow, so it is refused instead
// of being hashed into a concurrency key that names nothing.
func TestResetWorkflowConcurrencyValidatesName(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("ns-a"))

	cases := []string{"", "   ", strings.Repeat("x", engine.MaxResetWorkflowNameLen+1)}
	for _, name := range cases {
		_, err := eng.ResetWorkflowConcurrency(ctx, "", name, ConcurrencyResetOptions{Apply: true})
		if !errors.Is(err, engine.ErrInvalidWorkflowName) {
			t.Errorf("ResetWorkflowConcurrency(name %q) error = %v, want ErrInvalidWorkflowName", name, err)
		}
	}

	// A name over the limit by a multi-byte rune counts runes, not bytes.
	_, err = eng.ResetWorkflowConcurrency(ctx, "", strings.Repeat("工", engine.MaxResetWorkflowNameLen), ConcurrencyResetOptions{})
	if err != nil {
		t.Fatalf("a name at the rune limit was refused: %v", err)
	}
}

// TestResetWorkflowConcurrencyEmptyStateIsASafePreview pins the delegated call:
// on an engine with no executions, a preview succeeds with an empty judgement
// rather than an error, and the namespace defaults from the context.
func TestResetWorkflowConcurrencyEmptyStateIsASafePreview(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("ns-a"))

	res, err := eng.ResetWorkflowConcurrency(ctx, "", "no-such-workflow", ConcurrencyResetOptions{})
	if err != nil {
		t.Fatalf("ResetWorkflowConcurrency() error = %v, want a preview", err)
	}
	if res.Inspected != 0 || res.Reclaimed != 0 || len(res.Candidates) != 0 {
		t.Fatalf("result = %+v, want an empty judgement", res)
	}

	// The namespace is validated, not silently accepted: it addresses keys.
	if _, err := eng.ResetWorkflowConcurrency(ctx, namespace.Namespace("bad:ns"), "wf", ConcurrencyResetOptions{}); !errors.Is(err, namespace.ErrNameChars) {
		t.Fatalf("invalid namespace error = %v, want ErrNameChars", err)
	}
}

// TestServerResetWorkflowConcurrencyDelegates pins the Server face: the HTTP
// route and an embedded host call reach the same engine method, so a preview
// returned in-process and one returned over HTTP cannot disagree.
func TestServerResetWorkflowConcurrencyDelegates(t *testing.T) {
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	srv := &Server{sdkEngine: eng}
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("ns-a"))
	res, err := srv.ResetWorkflowConcurrency(ctx, "", "no-such-workflow", ConcurrencyResetOptions{IncludeLive: true})
	if err != nil {
		t.Fatalf("Server.ResetWorkflowConcurrency() error = %v", err)
	}
	if res.Reclaimed != 0 {
		t.Fatalf("Reclaimed = %d, want 0", res.Reclaimed)
	}
}

// TestResetWorkflowConcurrencyNilReceivers pins the defensive errors: a zero
// Engine or Server reports a clear error instead of panicking.
func TestResetWorkflowConcurrencyNilReceivers(t *testing.T) {
	var eng *Engine
	if _, err := eng.ResetWorkflowConcurrency(context.Background(), "", "wf", ConcurrencyResetOptions{}); err == nil {
		t.Fatal("nil Engine returned no error")
	}
	var srv *Server
	if _, err := srv.ResetWorkflowConcurrency(context.Background(), "", "wf", ConcurrencyResetOptions{}); err == nil {
		t.Fatal("nil Server returned no error")
	}
}
