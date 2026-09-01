package types

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestArtifactUseCollectorDedupesAndOrders(t *testing.T) {
	_, c := WithArtifactUseCollector(context.Background())
	c.Record("clean", "sha256:bb")
	c.Record("decode", "sha256:aa")
	c.Record("clean", "sha256:bb") // duplicate
	c.Record("decode", "sha256:aa")

	got := c.Uses()
	want := []ArtifactUse{
		{Node: "clean", Digest: "sha256:bb"},
		{Node: "decode", Digest: "sha256:aa"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Uses() = %#v, want %#v", got, want)
	}
}

// A collector is per-batch and a map body runs items concurrently, so Record
// must be safe under -race. This is the only reason the mutex exists.
func TestArtifactUseCollectorIsConcurrencySafe(t *testing.T) {
	_, c := WithArtifactUseCollector(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Record("decode", "sha256:aa")
			c.Record("clean", "sha256:bb")
		}()
	}
	wg.Wait()
	if got := len(c.Uses()); got != 2 {
		t.Fatalf("len(Uses()) = %d, want exactly 2", got)
	}
}

// Every call site reaches the collector through a context that may not carry
// one (an embedded backend, a unit test, a node run outside any batch). A nil
// collector must be a silent no-op rather than a panic, because the alternative
// is that provenance wiring turns into a crash surface for unrelated paths.
func TestArtifactUseCollectorNilIsANoOp(t *testing.T) {
	var c *ArtifactUseCollector
	c.Record("decode", "sha256:aa") // must not panic
	if got := c.Uses(); got != nil {
		t.Fatalf("nil collector Uses() = %#v, want nil", got)
	}
	if got := ArtifactUseCollectorFrom(context.Background()); got != nil {
		t.Fatalf("ArtifactUseCollectorFrom(bare ctx) = %#v, want nil", got)
	}
}

// An empty digest means the node ran inline code, not an artifact. Recording it
// would put a {node, ""} entry on the output that no consumer can resolve.
func TestArtifactUseCollectorIgnoresEmptyDigest(t *testing.T) {
	_, c := WithArtifactUseCollector(context.Background())
	c.Record("inline", "")
	if got := c.Uses(); got != nil {
		t.Fatalf("Uses() = %#v, want nil", got)
	}
}

func TestArtifactUseCollectorFromRoundTripsThroughContext(t *testing.T) {
	ctx, c := WithArtifactUseCollector(context.Background())
	if got := ArtifactUseCollectorFrom(ctx); got != c {
		t.Fatalf("ArtifactUseCollectorFrom(ctx) = %p, want %p", got, c)
	}
}

// The in-process and durable paths must put the SAME shape on the output. A
// typed []ArtifactUse would survive in-process and come back as
// []any{map[string]any} after a Redis round trip, so a test on the local
// backend would assert a shape production never produces.
func TestArtifactUsesAsDataIsTheJSONShape(t *testing.T) {
	got := ArtifactUsesAsData([]ArtifactUse{{Node: "decode", Digest: "sha256:aa"}})
	want := []any{map[string]any{"node": "decode", "digest": "sha256:aa"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ArtifactUsesAsData = %#v, want %#v", got, want)
	}
	if got := ArtifactUsesAsData(nil); got != nil {
		t.Fatalf("ArtifactUsesAsData(nil) = %#v, want nil", got)
	}
}

func TestArtifactUsesFromDataParsesTheJSONShape(t *testing.T) {
	in := []any{
		map[string]any{"node": "decode", "digest": "sha256:aa"},
		map[string]any{"node": "clean", "digest": "sha256:bb"},
		map[string]any{"node": "broken"}, // no digest — skipped
		"not a map",                      // skipped
	}
	got := ArtifactUsesFromData(in)
	want := []ArtifactUse{
		{Node: "decode", Digest: "sha256:aa"},
		{Node: "clean", Digest: "sha256:bb"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ArtifactUsesFromData = %#v, want %#v", got, want)
	}
	if got := ArtifactUsesFromData(nil); got != nil {
		t.Fatalf("ArtifactUsesFromData(nil) = %#v, want nil", got)
	}
}

// AsData and FromData are the two halves of one wire format, and nothing else
// pins them to each other: the batch layer writes with AsData
// (engine/batch_body.go) and the merge layer reads with FromData
// (engine/expand.go), on opposite sides of a process boundary on the durable
// path. Their existing tests each assert against a hand-written literal, so
// the two halves agree today only because the same literal was typed twice.
// Change AsData's shape and FromData starts returning nil for real batches --
// a manifest that is silently always empty, with no error anywhere.
func TestArtifactUsesSurviveAnAsDataFromDataRoundTrip(t *testing.T) {
	want := []ArtifactUse{
		{Node: "clean", Digest: "sha256:bb"},
		{Node: "decode", Digest: "sha256:aa"},
	}
	if got := ArtifactUsesFromData(ArtifactUsesAsData(want)); !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}
