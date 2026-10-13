package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func idempotencyTestGraph(t *testing.T, def *types.WorkflowDef) *graph.Graph {
	t.Helper()
	if def == nil {
		def = &types.WorkflowDef{
			Name:    "idem",
			Version: "1",
			Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.start"}},
		}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func TestValidateIdempotencyKey(t *testing.T) {
	valid := []string{"a", "order-1", "A.b_c:d-9", strings.Repeat("k", 255)}
	for _, key := range valid {
		if err := ValidateIdempotencyKey(key); err != nil {
			t.Errorf("ValidateIdempotencyKey(%q) = %v, want nil", key, err)
		}
	}
	invalid := []string{"", strings.Repeat("k", 256), "has space", "slash/es", "ü", "new\nline"}
	for _, key := range invalid {
		if err := ValidateIdempotencyKey(key); !errors.Is(err, ErrIdempotencyKeyInvalid) {
			t.Errorf("ValidateIdempotencyKey(%q) = %v, want ErrIdempotencyKeyInvalid", key, err)
		}
	}
}

// TestIdempotentExecutionIDIsScoped pins the derivation: a pure function of
// (namespace, scope, key), disjoint from the other deterministic ID spaces, and
// sensitive to every component — including where one ends and the next begins.
func TestIdempotentExecutionIDIsScoped(t *testing.T) {
	base := IdempotentExecutionID("ns", "workflow:wf", "k")
	if base != IdempotentExecutionID("ns", "workflow:wf", "k") {
		t.Fatal("IdempotentExecutionID is not deterministic")
	}
	if !strings.HasPrefix(string(base), "exec-ik-") {
		t.Fatalf("id = %q, want the exec-ik- prefix", base)
	}
	others := []types.ExecutionID{
		IdempotentExecutionID("other", "workflow:wf", "k"),
		IdempotentExecutionID("ns", "workflow:wf2", "k"),
		IdempotentExecutionID("ns", "workflow:wf", "k2"),
		IdempotentExecutionID("ns", "workflow:w", "fk"),
	}
	for _, other := range others {
		if other == base {
			t.Fatalf("distinct inputs derived the same id %q", base)
		}
	}
}

// TestSubmissionFingerprintIsCanonical: map order and the Go type a number
// arrived as do not change a request, and every part of the request that
// changes what runs does.
func TestSubmissionFingerprintIsCanonical(t *testing.T) {
	g := idempotencyTestGraph(t, nil)
	fp := func(entry string, params map[string]any, test bool, digest string) IdempotencyFingerprint {
		t.Helper()
		got, err := SubmissionFingerprint(g, entry, params, test, digest)
		if err != nil {
			t.Fatalf("SubmissionFingerprint() error = %v", err)
		}
		return got
	}
	base := fp("", map[string]any{"a": 1, "b": map[string]any{"x": "y", "z": []any{1, 2}}}, false, "")
	same := fp("", map[string]any{"b": map[string]any{"z": []any{float64(1), float64(2)}, "x": "y"}, "a": float64(1)}, false, "")
	if base != same {
		t.Fatal("fingerprint depends on map order or numeric type")
	}
	if fp("", nil, false, "") != fp("", map[string]any{}, false, "") {
		t.Fatal("nil and empty params fingerprint differently")
	}
	for name, other := range map[string]IdempotencyFingerprint{
		"params": fp("", map[string]any{"a": 2, "b": map[string]any{"x": "y", "z": []any{1, 2}}}, false, ""),
		"entry":  fp("start", map[string]any{"a": 1, "b": map[string]any{"x": "y", "z": []any{1, 2}}}, false, ""),
		"test":   fp("", map[string]any{"a": 1, "b": map[string]any{"x": "y", "z": []any{1, 2}}}, true, ""),
		"digest": fp("", map[string]any{"a": 1, "b": map[string]any{"x": "y", "z": []any{1, 2}}}, false, "sha256:x"),
	} {
		if other == base {
			t.Errorf("changing %s did not change the fingerprint", name)
		}
	}
}

// TestIdempotencyWindowIsCappedByRetention: the claim is never asked to
// outlive what the engine knows the execution will get.
func TestIdempotencyWindowIsCappedByRetention(t *testing.T) {
	eng := New(newFakeState(), &fakeQueue{}, WithIdempotencyTTL(time.Hour))
	g := idempotencyTestGraph(t, nil)
	if got := eng.idempotencyWindow(context.Background(), g); got != time.Hour {
		t.Fatalf("window = %v, want the configured hour", got)
	}
	if got := eng.idempotencyWindow(WithExecutionTTL(context.Background(), time.Minute), g); got != time.Minute {
		t.Fatalf("window under a 1m execution TTL = %v, want 1m", got)
	}
	transient := idempotencyTestGraph(t, &types.WorkflowDef{
		Name:    "idem-transient",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.start"}},
		Options: &types.WorkflowOptions{Transient: true, TransientTTL: 90 * time.Second},
	})
	if got := eng.idempotencyWindow(context.Background(), transient); got != 90*time.Second {
		t.Fatalf("window for a transient graph = %v, want its 90s TTL", got)
	}
	if got := New(newFakeState(), &fakeQueue{}).idempotencyWindow(context.Background(), g); got != DefaultIdempotencyTTL {
		t.Fatalf("default window = %v, want %v", got, DefaultIdempotencyTTL)
	}
}
