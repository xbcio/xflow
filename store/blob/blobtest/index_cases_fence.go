package blobtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/blob"
)

// claimed registers a ref, ages it past the grace period, and claims it.
func claimed(t *testing.T, h IndexHarness, content string) (blob.Ref, blob.Fence) {
	t.Helper()
	ref := refFor(idxNS, content)
	mustRegister(t, h, ref)
	h.Clock.Advance(grace + time.Millisecond)
	return ref, mustMark(t, h, ref)
}

// caseReviveBumpsGeneration is the writer half of the fence: Register on a
// claimed object revives it, increments the generation, and tells the writer to
// rewrite the bytes. The writer's codec replaces the recorded one because the
// bytes it is about to write are the ones that will exist.
func caseReviveBumpsGeneration(t *testing.T, h IndexHarness) {
	ref, fence := claimed(t, h, "revived by a writer")

	rewrite := ref
	rewrite.Codec, rewrite.Stored = blob.CodecZstd, ref.Size-2
	reg := mustRegister(t, h, rewrite)
	if !reg.Revived {
		t.Fatalf("Register of a claimed object: Revived = false, want true")
	}
	if reg.Ref != rewrite {
		t.Errorf("revived Registration.Ref = %+v, want the reviving writer's %+v", reg.Ref, rewrite)
	}
	// Live again and freshly referenced, so no longer garbage.
	wantCandidates(t, h, "after revival")

	// Claim it again after another quiet period: the generation moved on.
	h.Clock.Advance(grace + time.Millisecond)
	next := mustMark(t, h, ref)
	if next.Generation <= fence.Generation {
		t.Fatalf("generation after revival = %d, want more than the first claim's %d", next.Generation, fence.Generation)
	}
	if next.Ref != rewrite {
		t.Errorf("fence Ref after revival = %+v, want the recorded %+v", next.Ref, rewrite)
	}
}

// casePurgeFencedByRegister is the headline guarantee: a collector that claimed
// an object, then lost a race with a writer, must not delete. The delete
// callback must not run, so the writer's bytes survive.
func casePurgeFencedByRegister(t *testing.T, h IndexHarness) {
	ref, fence := claimed(t, h, "writer won the race")
	mustRegister(t, h, ref) // The writer arrives after the claim.

	called := false
	err := h.Index.Purge(context.Background(), fence, func(context.Context) error { called = true; return nil })
	if !errors.Is(err, blob.ErrFenced) {
		t.Fatalf("Purge with a stale fence error = %v, want ErrFenced", err)
	}
	if called {
		t.Fatalf("Purge ran the delete callback although the fence was stale: the bytes would be lost")
	}
	// The object is intact: a writer can keep linking to it.
	mustLink(t, h, "exec-1", ref, horizon)
}

// caseStaleFence shows a fence is bound to ONE claim. After a revive and a fresh
// claim, the first claim's fence must still fail even though the object is
// claimed again.
func caseStaleFence(t *testing.T, h IndexHarness) {
	ref, stale := claimed(t, h, "claimed, revived, claimed")
	mustRegister(t, h, ref)
	h.Clock.Advance(grace + time.Millisecond)
	fresh := mustMark(t, h, ref)

	called := false
	err := h.Index.Purge(context.Background(), stale, func(context.Context) error { called = true; return nil })
	if !errors.Is(err, blob.ErrFenced) || called {
		t.Fatalf("Purge with the first claim's fence = (called %v, %v), want ErrFenced without a delete", called, err)
	}
	if err := h.Index.Purge(context.Background(), fresh, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Purge with the current fence error = %v", err)
	}
}

func casePurgeDeleteFails(t *testing.T, h IndexHarness) {
	ctx := context.Background()
	ref, fence := claimed(t, h, "delete fails once")
	boom := errors.New("storage is down")

	if err := h.Index.Purge(ctx, fence, func(context.Context) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Purge error = %v, want the callback's error", err)
	}
	// Still claimed and still garbage, so the next run retries with the same fence.
	wantCandidates(t, h, "after a failed delete", ref)
	if err := h.Index.Purge(ctx, fence, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("retried Purge error = %v", err)
	}
}

func casePurgeBadFence(t *testing.T, h IndexHarness) {
	_, fence := claimed(t, h, "malformed fences")
	noop := func(context.Context) error { return nil }

	zeroGen := fence
	zeroGen.Generation = 0
	for name, f := range map[string]blob.Fence{"zero fence": {}, "generation 0": zeroGen} {
		if err := h.Index.Purge(context.Background(), f, noop); !errors.Is(err, blob.ErrInvalidRef) {
			t.Errorf("Purge(%s) error = %v, want ErrInvalidRef", name, err)
		}
	}
	if err := h.Index.Purge(context.Background(), fence, nil); err == nil {
		t.Errorf("Purge with a nil delete callback error = nil, want an error")
	}
	// None of the above may have consumed the claim.
	if err := h.Index.Purge(context.Background(), fence, noop); err != nil {
		t.Errorf("Purge with the valid fence after the rejected calls error = %v", err)
	}
}

func casePurgeTwice(t *testing.T, h IndexHarness) {
	_, fence := claimed(t, h, "purged twice")
	noop := func(context.Context) error { return nil }
	if err := h.Index.Purge(context.Background(), fence, noop); err != nil {
		t.Fatalf("first Purge error = %v", err)
	}
	if err := h.Index.Purge(context.Background(), fence, noop); !errors.Is(err, blob.ErrFenced) {
		t.Fatalf("second Purge error = %v, want ErrFenced", err)
	}
}

// casePurgeExcludesRegister checks the property the two-step "recheck, then
// delete" protocol lacks: while the delete callback runs, a concurrent Register
// of the same object does not return. It either returns before Purge starts
// (and Purge is then fenced) or after the callback finished, never in the
// middle. Without this, a writer could be told the bytes are fine while the
// collector is deleting them.
func casePurgeExcludesRegister(t *testing.T, h IndexHarness) {
	ref, fence := claimed(t, h, "register waits for the delete")

	inDelete := make(chan struct{})
	release := make(chan struct{})
	purged := make(chan error, 1)
	var mu sync.Mutex
	var order []string
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }

	go func() {
		purged <- h.Index.Purge(context.Background(), fence, func(context.Context) error {
			close(inDelete)
			<-release
			note("delete finished")
			return nil
		})
	}()
	<-inDelete

	registered := make(chan blob.Registration, 1)
	go func() {
		reg, err := h.Index.Register(context.Background(), ref)
		if err != nil {
			t.Errorf("Register during Purge error = %v", err)
		}
		note("register returned")
		registered <- reg
	}()

	// Give the Register every chance to (wrongly) get through while the delete is
	// still running. The ordering assertion below is the real check; this wait
	// only widens the window, so a slow machine can make the test less
	// sensitive, never flaky.
	select {
	case <-registered:
		t.Errorf("Register returned while Purge was still deleting the bytes")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)

	if err := <-purged; err != nil {
		t.Fatalf("Purge error = %v", err)
	}
	reg := <-registered
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "delete finished" {
		t.Fatalf("event order = %v, want the delete to finish before Register returns", order)
	}
	// Purge removed the object, so the waiting writer created a new one.
	if reg.Revived || reg.Ref != ref {
		t.Errorf("Register after the Purge = %+v, want a fresh object %+v", reg, ref)
	}
}

func caseIndexContextCancelled(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "cancelled calls")
	mustRegister(t, h, ref)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := h.Index.Register(ctx, ref); !errors.Is(err, context.Canceled) {
		t.Errorf("Register under a cancelled context error = %v, want context.Canceled", err)
	}
	if err := h.Index.Link(ctx, "exec-1", ref, h.Clock.Now().Add(horizon)); !errors.Is(err, context.Canceled) {
		t.Errorf("Link under a cancelled context error = %v, want context.Canceled", err)
	}
	if _, err := h.Index.PruneLinks(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("PruneLinks under a cancelled context error = %v, want context.Canceled", err)
	}
	if _, err := h.Index.Candidates(ctx, grace, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Candidates under a cancelled context error = %v, want context.Canceled", err)
	}
	if _, err := h.Index.MarkDeleting(ctx, ref, grace); !errors.Is(err, context.Canceled) {
		t.Errorf("MarkDeleting under a cancelled context error = %v, want context.Canceled", err)
	}
	if err := h.Index.Purge(ctx, blob.Fence{Ref: ref, Generation: 1}, func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("Purge under a cancelled context error = %v, want context.Canceled", err)
	}
	// Nothing above may have changed state.
	if reg := mustRegister(t, h, ref); reg.Revived || reg.Ref != ref {
		t.Errorf("Register after the cancelled calls = %+v, want the unchanged live object", reg)
	}
}
